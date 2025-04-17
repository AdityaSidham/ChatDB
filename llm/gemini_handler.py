import os
from google.generativeai import GenerativeModel, configure
import streamlit as st
from dotenv import load_dotenv
load_dotenv()

# Configure Gemini
configure(api_key=os.getenv("GEMINI_API_KEY"))
model = GenerativeModel(model_name="models/gemini-1.5-pro-002")

def nl_to_sql(nl_query):
    # Dynamically get current table name (fallback to default)
    table_name = st.session_state.get("current_table", "ecommercedataset")

    columns = [
    "Order_Date", "Time", "Aging", "Customer_Id", "Gender", "Device_Type",
    "Customer_Login_type", "Product_Category", "Product", "Sales",
    "Quantity", "Discount", "Profit", "Shipping_Cost", "Order_Priority", "Payment_method"
    ]

    prompt = (
        f"You are an AI that converts natural language to MySQL queries.\n"
        f"The table `{table_name}` has the following columns:\n"
        f"{', '.join(columns)}.\n"
        f"Use only these column names and respond with just the SQL query.\n\n"
        f"User: {nl_query}"
    )


    response = model.generate_content(prompt).text.strip()

    # Clean up ```sql or ``` wrapping
    if response.startswith("```sql"):
        response = response.replace("```sql", "").replace("```", "").strip()
    elif response.startswith("```"):
        response = response.replace("```", "").strip()

    return response

def nl_to_mongo(nl_query):
    prompt = f"Convert this into a MongoDB find/aggregate command:\n{nl_query}\nRespond with only the MongoDB query."
    return eval(model.generate_content(prompt).text.strip())  # CAUTION: validate this safely in production
