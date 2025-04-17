import streamlit as st
import mysql.connector
from pymongo import MongoClient
import google.generativeai as genai
import re

# Gemini Setup
genai.configure(api_key="AIzaSyCjFhVxNVRXCYy29WvPI0VruJl-tst9rek")  
model = genai.GenerativeModel(model_name="models/gemini-1.5-pro-002")

#MySQL Setup
def get_connection(db_name=None):
    return mysql.connector.connect(
        host="localhost",
        user="root",          
        password="password",      
        database=db_name if db_name else None
    )

def get_mongo_client():
    return MongoClient("mongodb://127.0.0.1:27017")


#Streamlit UI 
st.markdown("""
    <h1 style='text-align: center; color: #4CAF50;'>💬 ChatDB</h1>
    <h4 style='text-align: center; color: gray;'>Powered by Gemini 1.5 Pro · MySQL + MongoDB</h4>
""", unsafe_allow_html=True)

st.divider()


if "current_db" not in st.session_state:
    st.session_state.current_db = None

if "db_type" not in st.session_state:
    st.session_state.db_type = "mysql"  

query = st.text_area("Enter your query:")

def detect_db_type(nl_query):
    if "mongodb" in nl_query.lower() or "nosql" in nl_query.lower():
        return "mongodb"
    return "mysql"

if st.button("Run Query"):
    if not query:
        st.warning("Please enter a query.")
    else:
        st.session_state.db_type = detect_db_type(query)
        try:
            if st.session_state.db_type == "mongodb":
                prompt = f"Convert the following natural language request into a MongoDB Python (pymongo) expression only. Do not include explanations, comments, or shell syntax like 'use db':\n{query}"
            else:
                prompt = f"Convert the following natural language request into a MySQL query:\n{query}"
            response = model.generate_content(prompt)

            # Cleaning the response from the AI  (remove markdown like ```sql, ```javascript, etc.)
            cleaned_text = re.sub(r"^```(?:\w+)?\s*|```$", "", response.text.strip(), flags=re.MULTILINE).strip()

            if st.session_state.db_type == "mongodb":
                mongo_query_lines = cleaned_text.splitlines()
                mongo_query_cleaned = "\n".join(
                    line for line in mongo_query_lines
                    if not any(
                        line.strip().lower().startswith(prefix)
                        for prefix in (
                            "**", "--", "*", "this query does", "example", "important considerations",
                            "explanation", "note", "if you want", "remember", "you should", "it's a good practice"
                        )
                    )
                )
                mongo_query = mongo_query_cleaned.strip()
            else:
                sql_query_lines = cleaned_text.splitlines()
                sql_query_cleaned = "\n".join(
                    line for line in sql_query_lines
                    if not any(
                        line.strip().lower().startswith(prefix)
                        for prefix in (
                            "**", "--", "*", "this query does", "example", "important considerations",
                            "explanation", "note", "if you want", "remember", "you should", "it's a good practice"
                        )
                    )
                )
                sql_query = sql_query_cleaned.strip()

            if st.session_state.db_type == "mongodb":
                client = get_mongo_client()
                mongo_query = mongo_query  
                st.code(mongo_query, language="json")
                local_env = {"client": client, "MongoClient": MongoClient}
                try:
                    result = eval(mongo_query, local_env)
                except SyntaxError:
                    exec(mongo_query, local_env)
                    result = local_env.get("result")
                if hasattr(result, '__iter__') and not isinstance(result, (str, bytes, dict)):
                    st.dataframe(list(result), use_container_width=True)
                else:
                    st.write(result)
                st.stop()


            st.code(sql_query, language="sql")


            conn = get_connection(st.session_state.current_db)
            cursor = conn.cursor()

            statements = [stmt.strip() for stmt in sql_query.split(';') if stmt.strip()]
            for stmt in statements:
                try:
                    if re.match(r"use\s+(\w+)", stmt, re.IGNORECASE):
                        db_match = re.match(r"use\s+(\w+)", stmt, re.IGNORECASE)
                        db_to_use = db_match.group(1)
                        conn = get_connection(db_to_use)
                        cursor = conn.cursor()
                        st.session_state.current_db = db_to_use
                        st.success(f"Switched to database: {db_to_use}")
                        continue

                    cursor.execute(stmt)
                    if cursor.with_rows:
                        rows = cursor.fetchall()
                        if rows:
                            st.dataframe(rows, use_container_width=True)
                    else:
                        conn.commit()
                except Exception as sub_e:
                    st.error(f"Failed to execute statement: {stmt}\nError: {sub_e}")

            cursor.close()
            conn.close()

        except Exception as e:
            st.error(f"An error occurred: {e}")