import streamlit as st
import pandas as pd
import setup_path
import os
import snowflake.connector
import logging
from langchain_agent.metadata import get_snowflake_metadata, load_custom_metadata, merge_metadata
from langchain_agent.agent import build_agent
from langchain_agent.utils import get_openai_api_key

# ------------------- Logging Setup -------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("logs/streamlit_app.log"),
        logging.StreamHandler()
    ]
)

logging.info("Starting Snowflake SQL Assistant App")

# ------------------- Load Keys and Metadata -------------------
try:
    openai_api_key = get_openai_api_key()
    logging.info("OpenAI API key loaded")

    custom_metadata = load_custom_metadata()
    sf_metadata = get_snowflake_metadata()
    merged_metadata = merge_metadata(sf_metadata, custom_metadata)
    logging.info("Metadata loaded and merged")

    agent = build_agent(merged_metadata, openai_api_key)
    logging.info("LangChain agent built successfully")
except Exception as e:
    logging.exception("Failed during agent or metadata setup")
    st.error("Initialization error. Check logs.")
    st.stop()

# ------------------- Snowflake Config -------------------
SNOWFLAKE_CONFIG = {
    "account": os.getenv("account"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "warehouse": os.getenv("warehouse"),
    "database": os.getenv("database"),
    "schema": os.getenv("schema"),
    "role": os.getenv("role"),
}
logging.info("Snowflake configuration loaded")

# ------------------- Streamlit UI Setup -------------------
st.set_page_config(page_title="❄️ Snowflake SQL Assistant", layout="wide")

st.markdown("""<style> ... </style>""", unsafe_allow_html=True)

st.title("❄️ Snowflake SQL Assistant")
st.caption("Ask a natural language question, get SQL and query results.")

# ------------------- Session State -------------------
if "query_history" not in st.session_state:
    st.session_state.query_history = []
if "last_sql" not in st.session_state:
    st.session_state.last_sql = ""
if "last_df" not in st.session_state:
    st.session_state.last_df = pd.DataFrame()

# ------------------- Sidebar History -------------------
st.sidebar.header("🕘 Query History")
if st.session_state.query_history:
    for entry in reversed(st.session_state.query_history):
        st.sidebar.markdown(f"**{entry['question']}**")
        st.sidebar.code(entry["sql"], language="sql")
else:
    st.sidebar.caption("No queries yet.")

# ------------------- Query Input Form -------------------
with st.form("query_form"):
    user_query = st.text_input("🔍 What do you want to know?",
                               placeholder="e.g., Show top 5 categories by revenue last month")
    submitted = st.form_submit_button("Generate & Run")

# ------------------- Query Submission Logic -------------------
if submitted and user_query:
    logging.info(f"User submitted query: {user_query}")

    with st.spinner("Generating SQL and executing..."):
        try:
            sql = agent(user_query)
            logging.info(f"Generated SQL: {sql}")

            conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            cur = conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            df = pd.DataFrame(result, columns=columns)
            cur.close()
            conn.close()

            logging.info("Query executed and results fetched successfully")

            st.code(sql, language="sql")
            st.dataframe(df)

            # Save history
            st.session_state.query_history.append({
                "question": user_query,
                "sql": sql
            })

        except Exception as e:
            logging.exception("Error during query generation or execution")
            st.error(f"Query failed: {e}")
