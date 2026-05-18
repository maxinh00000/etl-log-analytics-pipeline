from pymongo import MongoClient

def run_query3():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["etl_project"]
    
    logs = db["logs"]
    result_collection = db["query3_results"]
    
    result_collection.delete_many({})
    
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
    
    results = list(logs.aggregate(pipeline))
    
    formatted_results = []
    for r in results:
        formatted_results.append({
            "log_date": r["_id"]["log_date"],
            "log_hour": r["_id"]["log_hour"],
            "error_request_count": r["error_requests"],
            "total_request_count": r["total_requests"],
            "error_rate": r["error_rate"],
            "distinct_error_hosts": r["distinct_error_hosts"]
        })
    
    if formatted_results:
        result_collection.insert_many(formatted_results)
        rows = []

    # for r in formatted_results:

    #     rows.append((
    #         r["log_date"],
    #         r["log_hour"],
    #         r["error_requests"],
    #         r["total_requests"],
    #         r["error_rate"],
    #         r["distinct_error_hosts"]
    #     ))

    return formatted_results

# print("Stored Query3 results:", len(formatted_results))