import streamlit as st
import pandas as pd
import json
import tempfile
import os
from db.mysql_handler import load_csv_to_mysql, run_mysql_query
from db.mongo_handler import load_json_to_mongo, run_mongo_query
from llm.gemini_handler import nl_to_sql, nl_to_mongo
import mysql.connector

st.set_page_config(page_title="ChatDB", layout="wide")

st.title("ChatDB: Natural Language Interface to Databases")

# Define Streamlit tabs
upload_tab, explore_tab, query_tab = st.tabs(["📁 Upload", "🔎 Explore", "💬 Query"])

with upload_tab:
    uploaded_file = st.file_uploader("Upload a CSV (MySQL) or JSON (MongoDB) file", type=["csv", "json"])
    db_choice = st.selectbox("Choose target database", ["MySQL", "MongoDB"])

    if uploaded_file is not None:
        filename = uploaded_file.name
        base_name = filename.split(".")[0]
        st.success(f"✅ Uploaded `{filename}`")

        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{filename.split('.')[-1]}") as tmp_file:
            tmp_file.write(uploaded_file.read())
            temp_path = tmp_file.name

        if filename.endswith(".csv") and db_choice == "MySQL":
            df = pd.read_csv(temp_path)
            st.write("Preview:", df.head())
            load_csv_to_mysql(temp_path, base_name)
            st.session_state["current_db"] = base_name.lower()
            st.success("📊 Data loaded into MySQL!")

        elif filename.endswith(".json") and db_choice == "MongoDB":
            with open(temp_path, 'r') as f:
                data = json.load(f)
            load_json_to_mongo(data, base_name)
            st.session_state["current_mongo"] = base_name.lower()
            st.write("Preview:", data[:2])
            st.success("📦 Data loaded into MongoDB!")

with explore_tab:
    db = st.session_state.get("current_db", None)
    if not db:
        st.warning("Please upload a file first to explore schema.")
    else:
        st.subheader(f"📋 Schema and Sample Data for `{db}`")

        try:
            conn = mysql.connector.connect(
                host="localhost",
                user="root",
                password="12345",
                database=db
            )
            cursor = conn.cursor()

            # Get table name
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
            table = st.selectbox("Select a table", tables)

            # Get columns
            cursor.execute(f"DESCRIBE `{table}`")
            schema = cursor.fetchall()
            st.write("**📐 Table Schema:**")
            st.table(schema)

            # Get sample rows
            cursor.execute(f"SELECT * FROM `{table}` LIMIT 5")
            rows = cursor.fetchall()
            col_names = [col[0] for col in schema]
            df = pd.DataFrame(rows, columns=col_names)
            st.write("**🔍 Sample Rows:**")
            st.dataframe(df)

            cursor.close()
            conn.close()

        except Exception as e:
            st.error(f"Error retrieving schema: {e}")


with query_tab:
    db_option = st.selectbox("Choose database to query", ["MySQL", "MongoDB"])
    query = st.text_input("Enter your natural language query:")

    if query:
        with st.spinner("Processing your query..."):
            try:
                if db_option == "MySQL":
                    sql_query = nl_to_sql(query)
                    db = st.session_state.get("current_db", "ecommerce")
                    result = run_mysql_query(sql_query, db_name=db)
                else:
                    mongo_query = nl_to_mongo(query)
                    result = run_mongo_query(mongo_query)
                st.success("Query executed successfully!")
                st.write(result)
            except Exception as e:
                st.error(f"Error: {e}")
