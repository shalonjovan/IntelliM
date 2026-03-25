from pymongo import MongoClient

def get_db(mongo_uri: str):
    client = MongoClient(mongo_uri)
    return client["buildabot"]
