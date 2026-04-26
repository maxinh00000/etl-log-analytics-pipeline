import time
from pymongo import MongoClient

start = time.time()

client = MongoClient("mongodb://localhost:27017/")
db = client["etl_project"]
collection = db["logs"]

pipeline = [
    {
        "$group": {
            "_id": {
                "log_date": "$log_date",
                "status": "$status"
            },
            "request_count": {"$sum": 1},
            "total_bytes": {"$sum": "$bytes"}
        }
    }
]

results = list(collection.aggregate(pipeline))

end = time.time()

print("Runtime:", end - start)

for r in results[:10]:
    print(r)