# 🏥 Medicare ACO Risk Intelligence Platform
**Tech Stack:** Snowflake, Snowpark Python, Cortex AI (Llama 3), Streamlit

## 🚀 Project Overview
A cloud-native data platform designed for a Medicare Accountable Care Organization (ACO) managing 96,000 lives. This project implements a modern ELT pipeline to process medical claims, predict financial risk using in-database Machine Learning, and democratize data access via a Generative AI chatbot.

## 🏗️ Architecture
`Raw Claims` ➔ **Snowflake Ingestion** (Grain Control) ➔ **Analytics Layer** (Feature Eng) ➔ **Snowpark ML** (Random Forest) ➔ **Streamlit UI** (GenAI)

## 🏆 Key Features

### 1. Robust Data Engineering
* **Challenge:** Medical claims often suffer from "grain mismatch" (line items vs. headers), leading to duplicate financial reporting.
* **Solution:** Implemented a robust grouping and aggregation logic in the `INGESTION` schema to normalize claims data to the header level, ensuring 100% financial accuracy before downstream processing.

### 2. In-Database Machine Learning (Snowpark)
* **Goal:** Move the model to the data, not the data to the model.
* **Implementation:** Used **Snowpark Python** to train a `RandomForestClassifier` directly inside Snowflake.
* **Outcome:** The model identifies high-cost patients (>$50k/yr) with high precision, using features like Age, Comorbidities, and Historical Utilization.

### 3. Generative AI "Chat with Data"
* **Integration:** Leveraged **Snowflake Cortex (Llama 3)** to build a Text-to-SQL engine.
* **App:** Developed a **Streamlit** interface allowing non-technical stakeholders to ask questions like *"Show me the top 10 patients by risk score"* and receive real-time data tables without writing code.

## 💻 How to Run
1.  Execute `1_pipeline_and_ml.sql` in a Snowflake Worksheet to build the schemas and train the model.
2.  Open **Streamlit in Snowflake**, create a new app, and paste the code from `2_chatbot_app.py`.
3.  Ask questions in plain English to interact with the findings.
