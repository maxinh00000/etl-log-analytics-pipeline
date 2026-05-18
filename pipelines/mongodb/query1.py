import time
from pymongo import MongoClient

def run_query1():
    start = time.time()

    client = MongoClient("mongodb://localhost:27017/")
    db = client["etl_project"]

    logs = db["logs"]
    result_collection = db["query1_results"]

    # clear old results
    result_collection.delete_many({})

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

    results = list(logs.aggregate(pipeline))

# format results (important for clean schema)
    formatted_results = []
    for r in results:
        formatted_results.append({
            "log_date": r["_id"]["log_date"],
            "status_code": r["_id"]["status"],
            "request_count": r["request_count"],
            "total_bytes": r["total_bytes"]
        })

    # insert into DB
    if formatted_results:
        result_collection.insert_many(formatted_results)
    # rows = []

    # for r in formatted_results:

    #     rows.append((
    #         r["log_date"],
    #         r["status_code"],
    #         r["request_count"],
    #         r["total_bytes"]
    #     ))

    return formatted_results
    # end = time.time()

# print("Runtime:", end - start)
# print("Stored Query1 results:", len(formatted_results))