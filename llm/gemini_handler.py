import os
from google.generativeai import GenerativeModel, configure
import streamlit as st
import mysql.connector
from dotenv import load_dotenv
import ast
from pymongo import MongoClient
import re

load_dotenv()

configure(api_key=os.getenv("GEMINI_API_KEY"))
model = GenerativeModel(model_name="models/gemini-1.5-pro-002")

def nl_to_sql(nl_query):
    db_name = "chatdb"
    schema_prompts = []

    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="12345",
            database=db_name
        )
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        table_names = [row[0] for row in cursor.fetchall()]

        for table in table_names:
            cursor.execute(f"DESCRIBE `{table}`")
            cols = cursor.fetchall()
            col_names = [col[0] for col in cols]
            schema_prompts.append(f"{table}({', '.join(col_names)})")

        cursor.close()
        conn.close()

    except Exception as e:
        schema_prompts = ["(Error retrieving schema)"]

    prompt = (
        f"You are an AI that converts natural language to MySQL queries.\n"
        f"The available tables and columns in the database are:\n{chr(10).join(schema_prompts)}\n"
        f"Use only these table and column names. Respond with only the SQL query, no explanations.\n\n"
        f"User: {nl_query}"
    )

    response = model.generate_content(prompt).text.strip()

    if response.startswith("```sql"):
        response = response.replace("```sql", "").replace("```", "").strip()
    elif response.startswith("```"):
        response = response.replace("```", "").strip()

    return response

# Join hints for each MongoDB database
db_join_hints = {
    "ecommerce": "Join `users.user_id` with `transactions.user_id`. Do NOT use `_id` for joins.",
    "university": "Join `courses.professor_id` with `professors.professor_id`. Do NOT use `_id` for joins.",
    "clinic": "Join `visits.doctor_id` with `doctors.doctor_id`. Do NOT use `_id` for joins."
}

def nl_to_mongo(nl_query):
    db_name = st.session_state.get("current_mongo_db", "")
    if not db_name:
        raise ValueError("No current MongoDB database selected in session state.")

    client = MongoClient("mongodb://localhost:27017/")
    collections = client[db_name].list_collection_names()
    hint = db_join_hints.get(db_name, "")

    prompt = (
        f"The MongoDB database `{db_name}` has the following collections: {', '.join(collections)}.\n"
        f"{hint}\n"
        f"Convert the following natural language request into a MongoDB aggregation pipeline using pymongo syntax.\n"
        f"Return only a valid Python list like this: [{{'$match': ...}}, {{'$project': ...}}]\n"
        f"Do not use shell syntax. No explanations. No wrapper code.\n\n"
        f"Request: {nl_query}"
    )

    raw_response = model.generate_content(prompt).text.strip()

    if "```" in raw_response:
        raw_response = raw_response.split("```")[-2].strip()

    if raw_response.lower().startswith("python"):
        raw_response = raw_response[len("python"):].strip()

    mongo_ops = ['match', 'group', 'project', 'lookup', 'unwind', 'sort', 'limit']
    for op in mongo_ops:
        raw_response = re.sub(rf"'({op})'\\s*:", rf"'${op}':", raw_response)

    try:
        return ast.literal_eval(raw_response)
    except Exception as e:
        raise ValueError(f"Gemini returned invalid MongoDB query: {e}\n\nSanitized Output:\n{raw_response}")
