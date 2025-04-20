import streamlit as st
import pandas as pd
import json
import tempfile
import os
import mysql.connector
from db.mysql_handler import load_csv_to_mysql, run_mysql_query
from db.mongo_handler import load_json_to_mongo, run_mongo_query
from llm.gemini_handler import nl_to_sql, nl_to_mongo
from pymongo import MongoClient

st.set_page_config(page_title="ChatDB", layout="wide")
st.title("ChatDB: Natural Language Interface to Databases")

upload_tab, explore_tab, query_tab = st.tabs(["\U0001F4C1 Upload", "\U0001F50E Explore", "\U0001F4AC Query"])

with upload_tab:
    uploaded_files = st.file_uploader(
        "Upload one or more CSV or JSON files",
        type=["csv", "json"],
        accept_multiple_files=True
    )

    if uploaded_files:
        for uploaded_file in uploaded_files:
            filename = uploaded_file.name
            base_name = filename.split(".")[0].lower()
            st.success(f"✅ Uploaded `{filename}`")

            with tempfile.NamedTemporaryFile(delete=False, suffix=f".{filename.split('.')[-1]}") as tmp_file:
                tmp_file.write(uploaded_file.read())
                temp_path = tmp_file.name

            if filename.endswith(".csv"):
                df = pd.read_csv(temp_path)
                st.write(f"Preview of `{base_name}`:", df.head())
                load_csv_to_mysql(temp_path, base_name)
                if "tables" not in st.session_state:
                    st.session_state["tables"] = []
                if base_name not in st.session_state["tables"]:
                    st.session_state["tables"].append(base_name)

            elif filename.endswith(".json"):
                with open(temp_path, 'r') as f:
                    lines = f.readlines()
                    data = [json.loads(line) for line in lines]
                db_name = st.selectbox(
                    "Select MongoDB database",
                    ["ecommerce", "university", "clinic"],
                    key=f"mongo_db_select_{base_name}"
                )
                load_json_to_mongo(data, db_name, base_name)
                st.session_state["current_mongo_db"] = db_name
                if "mongo_collections" not in st.session_state:
                    st.session_state["mongo_collections"] = {}
                if db_name not in st.session_state["mongo_collections"]:
                    st.session_state["mongo_collections"][db_name] = []
                if base_name not in st.session_state["mongo_collections"][db_name]:
                    st.session_state["mongo_collections"][db_name].append(base_name)
                st.write("Preview:", data[:2])
                st.success(f"\U0001F4E6 Loaded `{base_name}` into `{db_name}` MongoDB")

        st.session_state["current_db"] = "chatdb"
        st.success("✅ All files loaded into MySQL or MongoDB.")

with explore_tab:
    selected_db = st.selectbox("Select database to explore", ["MySQL", "MongoDB"])

    if selected_db == "MySQL":
        st.subheader(f"\U0001F4CB MySQL Schema and Sample Data for `chatdb`")
        try:
            conn = mysql.connector.connect(
                host="localhost",
                user="root",
                password="12345",
                database="chatdb"
            )
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
            table = st.selectbox("Select a table", tables)
            cursor.execute(f"DESCRIBE `{table}`")
            schema = cursor.fetchall()
            schema_df = pd.DataFrame(schema, columns=["Field", "Type", "Null", "Key", "Default", "Extra"])
            st.write("**\U0001F4D0 Table Schema:**")
            st.dataframe(schema_df)
            cursor.execute(f"SELECT * FROM `{table}` LIMIT 5")
            rows = cursor.fetchall()
            col_names = [col[0] for col in schema]
            df = pd.DataFrame(rows, columns=col_names)
            st.write("**\U0001F50D Sample Rows:**")
            st.dataframe(df)
            cursor.close()
            conn.close()
        except Exception as e:
            st.error(f"Error retrieving MySQL schema: {e}")

    else:
        st.subheader("\U0001F4CB MongoDB Collection Overview")
        try:
            db_name = st.session_state.get("current_mongo_db", "")
            if not db_name:
                st.warning("No MongoDB database selected.")
            else:
                client = MongoClient("mongodb://localhost:27017/")
                db = client[db_name]
                collections = st.session_state.get("mongo_collections", {}).get(db_name, db.list_collection_names())
                selected_collection = st.selectbox("Select a collection", collections)
                docs = list(db[selected_collection].find().limit(5))
                if docs:
                    df = pd.json_normalize(docs)
                    st.write("**\U0001F50D Sample Documents:**")
                    st.dataframe(df)
                else:
                    st.info("No documents found in this collection.")
        except Exception as e:
            st.error(f"Error retrieving MongoDB schema: {e}")

with query_tab:
    db_option = st.selectbox("Choose database to query", ["MySQL", "MongoDB"])
    query = st.text_input("Enter your natural language query:")

    if query:
        with st.spinner("Processing your query..."):
            try:
                if db_option == "MySQL":
                    sql_query = nl_to_sql(query)
                    st.code(sql_query, language='sql')
                    result = run_mysql_query(sql_query)
                    st.success("Query executed successfully!")
                    st.dataframe(result if isinstance(result, list) else [result])

                elif db_option == "MongoDB":
                    mongo_query = nl_to_mongo(query)
                    db_name = st.session_state.get("current_mongo_db", "")
                    if not db_name:
                        st.error("\u2757 No MongoDB database selected. Please upload a file first.")
                        st.stop()
                    result = run_mongo_query(mongo_query, db_name=db_name)
                    st.code(str(mongo_query), language="python")
                    st.success("Query executed successfully!")
                    st.dataframe(result if isinstance(result, list) else [result])

            except Exception as e:
                st.error(f"Error: {e}")
