from pymongo import MongoClient
import streamlit as st

client = MongoClient("mongodb://localhost:27017/")

def load_json_to_mongo(data, db_name, collection_name):
    if isinstance(data, dict):
        data = [data]
    db = client[db_name]
    coll = db[collection_name]
    coll.drop()
    coll.insert_many(data)

def run_mongo_query(query, db_name):
    db = client[db_name]
    try:
        if isinstance(query, list):  # Aggregation pipeline
            preferred_collection = None
            for stage in query:
                if "$lookup" in stage:
                    preferred_collection = stage["$lookup"].get("from")
                elif "$match" in stage and not preferred_collection:
                    field = list(stage["$match"].keys())[0]
                    preferred_collection = field.split(".")[0]
            collections = db.list_collection_names()
            ordered = [preferred_collection] + [c for c in collections if c != preferred_collection] if preferred_collection else collections
            for coll in ordered:
                try:
                    result = list(db[coll].aggregate(query))
                    if result:
                        return result
                except Exception:
                    continue
            return []
        else:  # Simple filter query
            results = []
            for coll_name in db.list_collection_names():
                coll = db[coll_name]
                matched = list(coll.find(query).limit(5))
                if matched:
                    results.extend(matched)
            return results
    except Exception as e:
        return [{"error": str(e)}]

def get_mongo_schema():
    schema = {}
    for name in client.list_database_names():
        db = client[name]
        schema[name] = {}
        for coll in db.list_collection_names():
            sample = db[coll].find_one()
            if sample:
                schema[name][coll] = list(sample.keys())
    return schema

