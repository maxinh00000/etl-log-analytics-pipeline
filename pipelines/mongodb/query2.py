from pymongo import MongoClient

def run_query2():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["etl_project"]

    logs = db["logs"]
    result_collection = db["query2_results"]

    result_collection.delete_many({})

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
        {"$sort": {"request_count": -1}},
        {"$limit": 20}
    ]

    results = list(logs.aggregate(pipeline))

    formatted_results = []
    for r in results:
        formatted_results.append({
            "resource_path": r["resource_path"],
            "request_count": r["request_count"],
            "total_bytes": r["total_bytes"],
            "distinct_host_count": r["distinct_host_count"]
        })

    if formatted_results:
        result_collection.insert_many(formatted_results)
        rows = []

    # for r in formatted_results:

    #     rows.append((
    #         r["resource_path"],
    #         r["request_count"],
    #         r["total_bytes"],
    #         r["distinct_host_count"]
    #     ))

    return formatted_results

# print("Stored Query2 results:", len(formatted_results))