import streamlit as st
from snowflake.snowpark.context import get_active_session

st.title("🏥 Medicare AI Assistant")
st.write("Ask questions about patient risk and financials in plain English.")

session = get_active_session()

# The user asks a question
question = st.text_input("Query your database:", placeholder="Who are the top 5 high-risk patients with diabetes?")

if question:
    # We use Snowflake Cortex (Llama 3) to generate SQL from English
    prompt = f"""
    You are a SQL Expert. Convert this question to Snowflake SQL.
    Table: ACO_PLATFORM.ANALYTICS.PATIENT_360
    Columns: BENE_ID, AGE, TOTAL_SPEND, HAS_DIABETES, HAS_HEART_FAILURE, ML_RISK_SCORE
    Question: {question}
    Return ONLY the SQL string. No markdown.
    """
    
    # Call the LLM safely using dollar quoting
    cmd = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3-70b', $${prompt}$$)"
    
    try:
        # Get SQL from AI
        sql_query = session.sql(cmd).collect()[0][0].strip()
        st.code(sql_query, language="sql")
        
        # Run SQL and show results
        results = session.sql(sql_query).to_pandas()
        st.dataframe(results)
    except Exception as e:
        st.error(f"Could not process query. Error: {e}")