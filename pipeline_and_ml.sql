/******************************************************************************
 * PROJECT: Medicare ACO Risk Intelligence Platform
 * DESCRIPTION: End-to-End ELT, Snowpark ML (Random Forest), and Cortex GenAI.
 * STACK: Snowflake, Python, SQL
 ******************************************************************************/

-- 1. INFRASTRUCTURE SETUP
CREATE DATABASE IF NOT EXISTS ACO_PLATFORM;
USE DATABASE ACO_PLATFORM;
CREATE SCHEMA IF NOT EXISTS INGESTION;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;

-- 2. ROBUST DATA ENGINEERING (The "Clean" Layer)
-- Logic: Ingests raw claims and collapses line-item grain to header level
-- to ensure financial accuracy before aggregation.
USE SCHEMA INGESTION;

-- Simulated Raw Tables (For structure)
-- In a real run, these would be loaded via COPY INTO from S3/Azure Blob
CREATE OR REPLACE TABLE BENEFICIARY_CLEAN AS
SELECT 
    BENE_ID,
    TRY_TO_DATE(BENE_BIRTH_DT, 'DD-Mon-YYYY') AS BENE_BIRTH_DT,
    SEX_IDENT_CD AS SEX_CODE,
    STATE_CODE,
    DUAL_ELGBL_MONS
FROM ACOPROJECT.RAW.RAW_BENEFICIARY; -- Assuming raw data exists

CREATE OR REPLACE TABLE CLAIMS_HEADER_CLEAN AS
SELECT 
    BENE_ID, 
    CLM_ID, 
    TRY_TO_DATE(CLM_FROM_DT, 'DD-Mon-YYYY') AS CLM_DATE, 
    MAX(CAST(CLM_PMT_AMT AS FLOAT)) AS CLM_AMT, -- Grain Control Logic
    MAX(PRNCPAL_DGNS_CD) AS PRIMARY_DX 
FROM ACOPROJECT.RAW.RAW_CARRIER 
GROUP BY BENE_ID, CLM_ID, CLM_FROM_DT;

-- 3. ANALYTICS & FEATURE ENGINEERING
USE SCHEMA ANALYTICS;

CREATE OR REPLACE TABLE PATIENT_360 AS
WITH FINANCIALS AS (
    SELECT BENE_ID, SUM(CLM_AMT) AS TOTAL_SPEND 
    FROM INGESTION.CLAIMS_HEADER_CLEAN 
    GROUP BY BENE_ID
),
RISK_FACTORS AS (
    SELECT 
        BENE_ID,
        -- Charlson Comorbidity Logic (Simplified)
        MAX(CASE WHEN STARTSWITH(PRIMARY_DX, 'E11') THEN 1 ELSE 0 END) AS HAS_DIABETES,
        MAX(CASE WHEN STARTSWITH(PRIMARY_DX, 'I50') THEN 1 ELSE 0 END) AS HAS_HEART_FAILURE,
        MAX(CASE WHEN STARTSWITH(PRIMARY_DX, 'N18') THEN 1 ELSE 0 END) AS HAS_KIDNEY_DX
    FROM INGESTION.CLAIMS_HEADER_CLEAN
    GROUP BY BENE_ID
)
SELECT 
    b.BENE_ID,
    DATEDIFF(year, b.BENE_BIRTH_DT, CURRENT_DATE()) AS AGE,
    COALESCE(f.TOTAL_SPEND, 0) AS TOTAL_SPEND,
    COALESCE(r.HAS_DIABETES, 0) AS HAS_DIABETES,
    COALESCE(r.HAS_HEART_FAILURE, 0) AS HAS_HEART_FAILURE,
    COALESCE(r.HAS_KIDNEY_DX, 0) AS HAS_KIDNEY_DX,
    -- Target Variable for ML
    CASE WHEN COALESCE(f.TOTAL_SPEND, 0) >= 50000 THEN 1 ELSE 0 END AS IS_HIGH_RISK
FROM INGESTION.BENEFICIARY_CLEAN b
LEFT JOIN FINANCIALS f ON b.BENE_ID = f.BENE_ID
LEFT JOIN RISK_FACTORS r ON b.BENE_ID = r.BENE_ID;

-- 4. MACHINE LEARNING (Snowpark Python)
-- Logic: Random Forest Classifier to predict High Cost probability
CREATE OR REPLACE PROCEDURE TRAIN_RISK_MODEL()
RETURNS TABLE()
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'scikit-learn')
HANDLER = 'main'
AS
$$
import snowflake.snowpark as snowpark
import pandas as pd
from sklearn.ensemble import RandomForestClassifier

def main(session: snowpark.Session): 
    # Load Feature Table
    df = session.table("ACO_PLATFORM.ANALYTICS.PATIENT_360").to_pandas()
    
    features = ['AGE', 'HAS_DIABETES', 'HAS_HEART_FAILURE', 'HAS_KIDNEY_DX']
    X = df[features].fillna(0)
    y = df['IS_HIGH_RISK']
    
    # Train Random Forest
    rf = RandomForestClassifier(n_estimators=100)
    rf.fit(X, y)
    
    # Generate Probabilities
    df['ML_RISK_SCORE'] = rf.predict_proba(X)[:, 1]
    
    # Save Results
    session.write_pandas(df[['BENE_ID', 'ML_RISK_SCORE']], "ML_PREDICTIONS", auto_create_table=True, overwrite=True)
    
    return session.create_dataframe(pd.DataFrame(rf.feature_importances_, index=features, columns=['Importance']))
$$;

-- 5. ORCHESTRATION
-- Run the model retraining every morning at 6 AM
CREATE OR REPLACE TASK DAILY_RISK_SCORING
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 6 * * * UTC'
AS
    CALL TRAIN_RISK_MODEL();
    
ALTER TASK DAILY_RISK_SCORING RESUME;