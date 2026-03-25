from dotenv import load_dotenv
import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# Load env
load_dotenv()

uri = os.getenv("MONGO_URI")
print("URI:", uri)

# Connect to Atlas
client = MongoClient(uri, server_api=ServerApi("1"))
print(client)
print("MongoClient created")
# Test connection
try:
    client.admin.command("ping")
    print("✅ Connected to MongoDB Atlas")
except Exception as e:
    print("❌ Connection failed")
    print(e)
    exit(1)

# Use a database + collection
db = client["buildabot"]
users = db["users"]

# Insert test data
result = users.insert_one({
    "name": "test-user",
    "role": "sanity-check"
})

print("Inserted ID:", result.inserted_id)

# Read it back
doc = users.find_one({"_id": result.inserted_id})
print("Fetched document:", doc)
