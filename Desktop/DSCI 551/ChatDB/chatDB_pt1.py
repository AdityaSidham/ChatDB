import streamlit as st
import mysql.connector
from pymongo import MongoClient
import pandas as pd
import json
import re
import os
import google.generativeai as genai
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError

# Gemini Setup
genai.configure(api_key="AIzaSyCRroUkwJqmkm22SSfZQ4eZzagMTU_k5Qg")  
model = genai.GenerativeModel(model_name="models/gemini-1.5-pro-002")

# MySQL connection
def get_connection(db_name=None):
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="12345",
        database=db_name if db_name else None,
        sql_mode="STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"  # removed ONLY_FULL_GROUP_BY
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
            # Create the database if it doesn't exist
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{upload_db_name}`")
            conn.commit()
            cursor.close()
            conn.close()

            # Create engine using SQLAlchemy
            engine = create_engine(f"mysql+mysqlconnector://root:12345@localhost/{upload_db_name}")

            for file in uploaded_files:
                table_name = os.path.splitext(file.name)[0]
                file.seek(0)
                df = pd.read_csv(file)

                # Replace table if it already exists
                df.to_sql(table_name, con=engine, if_exists="replace", index=False, method='multi')

            st.sidebar.success(f"MySQL data uploaded to '{upload_db_name}' successfully.")
        except Exception as e:
            st.sidebar.error(f"Upload error: {e}")

    elif db_type == "MongoDB":
        try:
            client = get_mongo_client()

            # Drop entire database before re-uploading to prevent duplicates
            client.drop_database(upload_db_name)
            db = client[upload_db_name]

            for file in uploaded_files:
                collection_name = os.path.splitext(file.name)[0]
                file.seek(0)  # ensure pointer is at start

                try:
                    # Try loading as a full JSON array
                    data = json.load(file)
                except json.JSONDecodeError:
                    # If it's not a JSON array, try line-delimited JSON (NDJSON)
                    file.seek(0)
                    data = []
                    for line in file:
                        if line.strip():
                            try:
                                data.append(json.loads(line))
                            except json.JSONDecodeError as line_err:
                                st.sidebar.error(f"Line skipped in {file.name}: {line.strip()}\nError: {line_err}")

                # Normalize single dict into list
                if isinstance(data, dict):
                    data = [data]

                # Insert only if there’s something to insert
                if data:
                    try:
                        db[collection_name].insert_many(data)
                    except Exception as insert_err:
                        st.sidebar.error(f"Insert error for collection `{collection_name}`: {insert_err}")
                else:
                    st.sidebar.warning(f"No valid JSON records found in {file.name}. Skipped.")

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

def infer_mysql_schema_live(db_name):
    conn = get_connection(db_name)
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = [row[0] for row in cursor.fetchall()]

    schema_summary = {}
    for table in tables:
        cursor.execute(f"DESCRIBE `{table}`")
        schema_summary[table] = [row[0] for row in cursor.fetchall()]
    
    cursor.close()
    conn.close()
    return schema_summary

if run_button and query:
    try:
        if query_mode == "MongoDB":
            client = get_mongo_client()
            db = client[upload_db_name]
            schema = infer_mongo_schema(db)

            schema_prompt = "You are working with the following MongoDB database schema:\n"
            for coll, fields in schema.items():
                schema_prompt += f"- Collection `{coll}` with fields: {fields}\n"

            schema_prompt += (
                "\nRespond with one of the following Python structures:\n"
                "- For a read query: ('collection_name', filter_dict)\n"
                "- For aggregation: ('collection_name', pipeline_list)\n"
                "- For updates: ('collection_name', (filter_dict, update_dict))\n\n"
                "Do NOT wrap anything in db[...] or method calls. Return only the tuple.\n"
                "Examples:\n"
                "('doctors', {'years_experience': {'$gt': 20}})\n"
                "('doctors', [ {'$match': {'specialty': 'ENT'}}, {'$project': {'name': 1, '_id': 0}} ])\n"
                "('doctors', ({'name': {'$regex': 'Smith$'}}, {'$set': {'years_experience': 30}}))\n\n"
            )

            prompt = schema_prompt + query

        else:  # MySQL
            schema_info = infer_mysql_schema_live(upload_db_name)

            schema_prompt = "You are generating MySQL queries. The current database schema is:\n"
            for table, columns in schema_info.items():
                schema_prompt += f"- Table `{table}` with columns: {columns}\n"
            schema_prompt += (
                "\nOnly use these tables. Do not assume any additional tables exist.\n"
                "Return a valid SQL query only. Do not include any markdown or explanation.\n\n"
            )

            prompt = schema_prompt + query

        response = model.generate_content(prompt)
        cleaned_text = re.sub(r"^```(?:\w+)?\s*|```$", "", response.text.strip(), flags=re.MULTILINE).strip()

        # Handle MongoDB
        if query_mode == "MongoDB":
            mongo_query = "\n".join(
                line for line in cleaned_text.splitlines()
                if not any(line.strip().lower().startswith(prefix) for prefix in (
                    "**", "--", "*", "this query does", "example", "explanation", "note", "remember"
                ))
            ).strip()

            try:
                parsed_result = eval(mongo_query)

                if isinstance(parsed_result, tuple) and isinstance(parsed_result[0], str):
                    collection_name = parsed_result[0]
                    mongo_query = parsed_result[1]
                else:
                    raise ValueError("Expected a tuple of (collection_name, query)")

                st.info(f"Running query on collection: `{collection_name}`")
                collection = db[collection_name]

                # Show final Mongo query
                try:
                    mongo_display_code = (
                        f"db['{collection_name}'].update_many({json.dumps(mongo_query[0], indent=2)}, {json.dumps(mongo_query[1], indent=2)})"
                        if isinstance(mongo_query, tuple)
                        else f"db['{collection_name}'].aggregate({json.dumps(mongo_query, indent=2)})"
                        if isinstance(mongo_query, list)
                        else f"db['{collection_name}'].find({json.dumps(mongo_query, indent=2)})"
                    )
                except Exception:
                    mongo_display_code = str(mongo_query)

                st.code(mongo_display_code, language="python")

                # Execute
                if isinstance(mongo_query, list):
                    result = collection.aggregate(mongo_query)
                    st.dataframe(list(result), use_container_width=True)

                elif isinstance(mongo_query, dict):
                    if '$match' in mongo_query and len(mongo_query) == 1:
                        mongo_query = mongo_query['$match']
                    result = collection.find(mongo_query)
                    st.dataframe(list(result), use_container_width=True)

                elif isinstance(mongo_query, tuple):
                    filter_q, update_q = mongo_query
                    update_result = collection.update_many(filter_q, update_q)
                    st.success(f"{update_result.modified_count} document(s) updated.")

                else:
                    raise ValueError("Unsupported MongoDB query structure.")

            except Exception as mongo_e:
                st.error(f"MongoDB query failed: {mongo_e}")

        # Handle MySQL
        else:
            sql_query = "\n".join(
                line for line in cleaned_text.splitlines()
                if not any(line.strip().lower().startswith(prefix) for prefix in (
                    "**", "--", "*", "this query does", "example", "explanation", "note", "remember"
                ))
            ).strip()

            st.subheader("📝 SQL Query")
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
                            columns = [desc[0] for desc in cursor.description]
                            df = pd.DataFrame(rows, columns=columns)
                            st.dataframe(df, use_container_width=True)
                    else:
                        conn.commit()
                except Exception as sql_e:
                    st.error(f"Failed to execute SQL: {stmt}\nError: {sql_e}")

            cursor.close()
            conn.close()

    except Exception as e:
        st.error(f"An error occurred: {e}")
