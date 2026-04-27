from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["etl_project"]
collection = db["logs"]

pipeline = [
    {
        "$group": {
            "_id": "$path",
            "request_count": {"$sum": 1},
            "total_bytes": {"$sum": "$bytes"},
            "hosts": {"$addToSet": "$host"}
        }
    },
    {
        "$project": {
            "resource_path": "$_id",
            "request_count": 1,
            "total_bytes": 1,
            "distinct_host_count": {"$size": "$hosts"}
        }
    },
    {
        "$sort": {"request_count": -1}
    },
    {
        "$limit": 20
    }
]

results = list(collection.aggregate(pipeline))

for r in results:
    print(r)