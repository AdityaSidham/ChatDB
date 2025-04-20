import streamlit as st
import mysql.connector
from pymongo import MongoClient
import pandas as pd
import json
import re
import os
import google.generativeai as genai

# Gemini Setup
genai.configure(api_key="AIzaSyCjFhVxNVRXCYy29WvPI0VruJl-tst9rek")  
model = genai.GenerativeModel(model_name="models/gemini-1.5-pro-002")

# MySQL connection
def get_connection(db_name=None):
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="password",
        database=db_name if db_name else None
    )

# MongoDB client
def get_mongo_client():
    return MongoClient("mongodb://127.0.0.1:27017")

# Streamlit UI
st.markdown("""
    <h1 style='text-align: center; color: #4CAF50;'>💬 ChatDB</h1>
    <h4 style='text-align: center; color: gray;'>Powered by Gemini 1.5 Pro · MySQL + MongoDB</h4>
""", unsafe_allow_html=True)
st.divider()


st.sidebar.header("📤 Upload Data")
db_type = st.sidebar.radio("Select Database Type", ["MySQL", "MongoDB"])
upload_db_name = st.sidebar.text_input("Enter Database Name")

uploaded_files = st.sidebar.file_uploader(
    "Upload CSV (for MySQL) or JSON (for MongoDB)",
    accept_multiple_files=True,
    type=["csv", "json"]
)

if uploaded_files and upload_db_name:
    if db_type == "MySQL":
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{upload_db_name}`")
            conn.commit()
            conn = get_connection(upload_db_name)
            cursor = conn.cursor()

            for file in uploaded_files:
                df = pd.read_csv(file)
                table_name = os.path.splitext(file.name)[0]
                columns = ", ".join([f"`{col}` TEXT" for col in df.columns])
                cursor.execute(f"CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})")

                for _, row in df.iterrows():
                    placeholders = ", ".join(["%s"] * len(row))
                    cursor.execute(
                        f"INSERT INTO `{table_name}` VALUES ({placeholders})",
                        tuple(row.astype(str))
                    )

            conn.commit()
            st.sidebar.success(f"MySQL data uploaded to '{upload_db_name}' successfully.")
        except Exception as e:
            st.sidebar.error(f"Upload error: {e}")
        finally:
            cursor.close()
            conn.close()

    elif db_type == "MongoDB":
        try:
            client = get_mongo_client()

            # Drop entire database before re-uploading to prevent duplicates
            client.drop_database(upload_db_name)
            db = client[upload_db_name]

            for file in uploaded_files:
                collection_name = os.path.splitext(file.name)[0]
                try:
                    data = json.load(file)
                except json.JSONDecodeError:
                    file.seek(0)
                    data = [json.loads(line) for line in file.readlines() if line.strip()]

                if isinstance(data, dict):
                    data = [data]

                db[collection_name].insert_many(data)

            st.sidebar.success(f"MongoDB data uploaded to '{upload_db_name}' successfully.")
        except Exception as e:
            st.sidebar.error(f"Upload error: {e}")

# Query Interface
st.divider()
st.subheader("🔍 Run a Query")

query_mode = st.radio("Choose query type", ["MySQL", "MongoDB"])
query = st.text_area("Enter your query:")
run_button = st.button("Run Query")

def infer_mongo_schema(db):
    schema_summary = {}
    for collection_name in db.list_collection_names():
        doc = db[collection_name].find_one()
        if doc:
            schema_summary[collection_name] = list(doc.keys())
    return schema_summary

if run_button and query:
    try:
        # 1. Convert NL to code via Gemini
        if query_mode == "MongoDB":
            schema = infer_mongo_schema(client[upload_db_name])

            schema_prompt = "You are working with the following MongoDB database schema:\n"
            for coll, fields in schema.items():
                schema_prompt += f"- Collection `{coll}` with fields: {fields}\n"
            schema_prompt += "\nUse this schema to answer the following:\n"

            prompt = (
                schema_prompt +
                "Return a Python tuple: ('collection_name', query). Use aggregate() if joining or filtering across collections.\n"
                "Do NOT include db[...] or method calls. Return only the tuple.\n\n"
                f"{query}"
            )

        else:
            prompt = f"Convert the following natural language request into a MySQL query:\n{query}"

        response = model.generate_content(prompt)
        cleaned_text = re.sub(r"^```(?:\w+)?\s*|```$", "", response.text.strip(), flags=re.MULTILINE).strip()

        # 2. Handle MongoDB query
        if query_mode == "MongoDB":
            mongo_query = "\n".join(
                line for line in cleaned_text.splitlines()
                if not any(line.strip().lower().startswith(prefix) for prefix in (
                    "**", "--", "*", "this query does", "example", "explanation", "note", "remember"
                ))
            ).strip()

            client = get_mongo_client()
            db = client[upload_db_name]  # Use selected/uploaded DB name
            available_collections = db.list_collection_names()

            # Try to match collection from NL query
            target_collection = None
            for name in available_collections:
                if name.lower() in query.lower():
                    target_collection = name
                    break
            if not target_collection:
                target_collection = available_collections[0]  # fallback

            st.info(f"Querying collection: `{target_collection}` in database `{upload_db_name}`")
            st.code(f"db['{target_collection}'].find({mongo_query})", language="python")

            # Actually run the query
            
            try:
                parsed_result = eval(mongo_query)

                if isinstance(parsed_result, tuple) and isinstance(parsed_result[0], str):
                    collection_name = parsed_result[0]
                    mongo_query = parsed_result[1]
                else:
                    raise ValueError("Expected a tuple of (collection_name, query)")

                st.info(f"Running query on collection: `{collection_name}`")
                collection = db[collection_name]

                if isinstance(mongo_query, dict):
                    # Unwrap if mistakenly given as {$match: {...}} for a find
                    if '$match' in mongo_query and len(mongo_query) == 1:
                        mongo_query = mongo_query['$match']
                    result = collection.find(mongo_query)

                elif isinstance(mongo_query, tuple) and len(mongo_query) == 2:
                    # Also unwrap $match in filter part of the tuple
                    filter_part = mongo_query[0]
                    if isinstance(filter_part, dict) and '$match' in filter_part:
                        filter_part = filter_part['$match']
                    result = collection.find(filter_part, mongo_query[1])

                elif isinstance(mongo_query, list):
                    result = collection.aggregate(mongo_query)

                else:
                    raise ValueError("Unsupported MongoDB query structure.")

                st.dataframe(list(result), use_container_width=True)

            except Exception as mongo_e:
                st.error(f"MongoDB query failed: {mongo_e}")



        # 3. Handle SQL query
        else:
            sql_query = "\n".join(
                line for line in cleaned_text.splitlines()
                if not any(line.strip().lower().startswith(prefix) for prefix in (
                    "**", "--", "*", "this query does", "example", "explanation", "note", "remember"
                ))
            ).strip()

            st.code(sql_query, language="sql")

            conn = get_connection(upload_db_name)
            cursor = conn.cursor()

            statements = [stmt.strip() for stmt in sql_query.split(';') if stmt.strip()]
            for stmt in statements:
                try:
                    cursor.execute(stmt)
                    if cursor.with_rows:
                        rows = cursor.fetchall()
                        if rows:
                            st.dataframe(rows, use_container_width=True)
                    else:
                        conn.commit()
                except Exception as sql_e:
                    st.error(f"Failed to execute SQL: {stmt}\nError: {sql_e}")

            cursor.close()
            conn.close()

    except Exception as e:
        st.error(f"An error occurred: {e}")
