from pipelines.mongodb.query1 import run_query1
from pipelines.mongodb.query2 import run_query2
from pipelines.mongodb.query3 import run_query3


class PipelineRunner:

    def run(
        self,
        records,
        query,
        run_id,
        batch_id
    ):

        result = {
            "records_processed": len(records),
            "malformed_records": 0,
            "query_results": {}
        }

        if query in ["q1", "all"]:

            result["query_results"]["q1"] = run_query1()

        if query in ["q2", "all"]:

            result["query_results"]["q2"] = run_query2()

        if query in ["q3", "all"]:

            result["query_results"]["q3"] = run_query3()

        return result