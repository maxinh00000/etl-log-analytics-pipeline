import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from pymongo import MongoClient
from parser.parser import parse_file

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["etl_project"]
collection = db["logs"]

# Clear old data (for testing)
collection.delete_many({})

# File path (use your correct path)
file_path = "E:/Acads/Sem 6/NoSQL/Project/etl-log-analytics-pipeline/data/sample/sample_log.txt"

# Parse data
data, malformed = parse_file(file_path)

# Insert into MongoDB
if data:
    collection.insert_many(data)

print(f"Inserted records: {len(data)}")
print(f"Malformed records: {malformed}")