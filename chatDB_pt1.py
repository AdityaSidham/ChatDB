import streamlit as st
import mysql.connector
import google.generativeai as genai
import re

# ──────────────── Gemini Setup ────────────────
genai.configure(api_key="AIzaSyCjFhVxNVRXCYy29WvPI0VruJl-tst9rek")  # Replace with your actual API key
model = genai.GenerativeModel(model_name="models/gemini-1.5-pro-002")

# ──────────────── MySQL Connection ────────────────
def get_connection(db_name=None):
    return mysql.connector.connect(
        host="localhost",
        user="root",          # Update with your MySQL username
        password="password",      # Update with your MySQL password
        database=db_name if db_name else None
    )

# ──────────────── Streamlit UI ────────────────
st.title("💬 ChatDB (Powered by Gemini Pro)")
st.write("Ask your database questions in plain English!")

if "current_db" not in st.session_state:
    st.session_state.current_db = None

query = st.text_area("Enter your query:")

if st.button("Run Query"):
    if not query:
        st.warning("Please enter a query.")
    else:
        try:
            # Prompt to Gemini
            prompt = f"Convert the following natural language request into a MySQL query:\n{query}"
            response = model.generate_content(prompt)

            # Clean the response (remove markdown like ```sql)
            sql_query = re.sub(r"```(?:sql)?\s*|\s*```", "", response.text).strip()

            # Remove lines that are explanations, markdown, or commentary
            sql_query_lines = sql_query.splitlines()
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

            st.code(sql_query, language="sql")

            # Execute the query
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