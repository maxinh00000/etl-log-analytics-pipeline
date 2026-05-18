import argparse, time, sys
from batching.split_batches import BatchSplitter
from reporting.load_to_pg  import load_results
from reporting.report      import generate_report
from datetime import datetime
from reporting.metadata import (
    write_run_metadata,
    write_batch_metadata
)

PIPELINE_RUNNERS = {
    "mongodb":   "pipelines.mongodb.runner",
    "hive":      "pipelines.hive.runner",
    "pig":       "pipelines.pig.runner",
    "mapreduce": "pipelines.mapreduce.runner",
}

def parse_args():
    p = argparse.ArgumentParser(
        description="ETL Log Analytics — multi-pipeline tool"
    )
    p.add_argument("--pipeline",
        choices=["mongodb", "hive", "pig", "mapreduce"],
        required=True)
    p.add_argument("--query",
        choices=["q1", "q2", "q3", "all"],
        default="all")
    p.add_argument("--batch-size",
        type=int, default=500_000)
    p.add_argument("--data-dir",
        default="data/raw")
    return p.parse_args()
def generate_run_id(pipeline):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{pipeline}_{ts}"
DIRECT_PG_PIPELINES = {"mongodb"}

def main():

    args = parse_args()

    splitter = BatchSplitter(
        args.data_dir,
        args.batch_size
    )

    batches = splitter.get_batches()

    import importlib

    runner_mod = importlib.import_module(
        PIPELINE_RUNNERS[args.pipeline]
    )

    runner = runner_mod.PipelineRunner()

    use_pg = args.pipeline in DIRECT_PG_PIPELINES

    t_start = time.time()

    run_id = generate_run_id(args.pipeline)

    total_records = 0
    malformed = 0

    # ==========================================
    # WRITE RUN METADATA FIRST
    # ==========================================

    if use_pg:

        write_run_metadata(
            run_id=run_id,
            pipeline_name=args.pipeline,
            query_name=args.query,
            batch_size=args.batch_size,
            avg_batch_size=splitter.avg_batch_size(batches),
            total_records=0,
            malformed_records=0,
            runtime_seconds=0,
        )

    # ==========================================
    # PROCESS BATCHES
    # ==========================================

    for batch_id, records in batches:

        result = runner.run(
            records=records,
            query=args.query,
            run_id=run_id,
            batch_id=batch_id,
        )

        total_records += result["records_processed"]

        malformed += result["malformed_records"]

        if use_pg:

            load_results(
                result,
                args.pipeline,
                run_id,
                batch_id
            )

            write_batch_metadata(
                run_id=run_id,
                batch_id=batch_id,
                batch_size=len(records),
                records_processed=result["records_processed"],
                malformed_records=result["malformed_records"],
            )

    runtime = time.time() - t_start

    # ==========================================
    # UPDATE FINAL RUN METADATA
    # ==========================================

    if use_pg:

        from reporting.metadata import update_run_metadata

        update_run_metadata(
            run_id=run_id,
            total_records=total_records,
            malformed_records=malformed,
            runtime_seconds=runtime,
        )

        generate_report(run_id)

if __name__ == "__main__":
    main()