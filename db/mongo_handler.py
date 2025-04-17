from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["your_db"]

def load_json_to_mongo(data, collection_name):
    if isinstance(data, dict):
        data = [data]
    collection = db[collection_name]
    collection.drop()
    collection.insert_many(data)

def run_mongo_query(query):
    return list(db["ecommerce"].find(query).limit(5))
