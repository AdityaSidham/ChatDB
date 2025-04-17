import os
from google.generativeai import GenerativeModel, configure
from dotenv import load_dotenv
load_dotenv()

configure(api_key=os.getenv("GEMINI_API_KEY"))
model = GenerativeModel("models/gemini-pro")  

def nl_to_sql(nl_query):
    prompt = f"Convert this into SQL:\n{nl_query}\nRespond with only the SQL query."
    return model.generate_content(prompt).text.strip()

def nl_to_mongo(nl_query):
    prompt = f"Convert this into a MongoDB find/aggregate command:\n{nl_query}\nRespond with only the MongoDB query."
    return eval(model.generate_content(prompt).text.strip())  # CAUTION: validate this safely in production
