from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["etl_project"]
collection = db["logs"]

pipeline = [
    {
        "$group": {
            "_id": {
                "log_date": "$log_date",
                "log_hour": "$log_hour"
            },
            "total_requests": {"$sum": 1},
            "error_requests": {
                "$sum": {
                    "$cond": [
                        {"$and": [
                            {"$gte": ["$status", 400]},
                            {"$lte": ["$status", 599]}
                        ]},
                        1,
                        0
                    ]
                }
            },
            "error_hosts": {
                "$addToSet": {
                    "$cond": [
                        {"$and": [
                            {"$gte": ["$status", 400]},
                            {"$lte": ["$status", 599]}
                        ]},
                        "$host",
                        None
                    ]
                }
            }
        }
    },
    {
        "$project": {
            "log_date": "$_id.log_date",
            "log_hour": "$_id.log_hour",
            "total_requests": 1,
            "error_requests": 1,
            "error_rate": {
                "$cond": [
                    {"$eq": ["$total_requests", 0]},
                    0,
                    {"$divide": ["$error_requests", "$total_requests"]}
                ]
            },
            "distinct_error_hosts": {
                "$size": {
                    "$filter": {
                        "input": "$error_hosts",
                        "as": "h",
                        "cond": {"$ne": ["$$h", None]}
                    }
                }
            }
        }
    }
]

results = list(collection.aggregate(pipeline))

for r in results[:10]:
    print(r)