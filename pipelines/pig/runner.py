import os
import shutil
import subprocess
import uuid

from reporting.load_to_pg import load_q1, load_q2, load_q3
# from reporting.metadata import (
#     write_run_metadata,
#     write_batch_metadata
# )

from reporting.load_to_pg import connect


class PigRunner:

    def __init__(self):

        self.pipeline_name = "pig"

    # =====================================================
    # RUN SINGLE QUERY
    # =====================================================

    def run_query(self, query_name, input_path):

        output_dir = f"pig_output/{query_name}"

        # ---------------------------------------------
        # Remove old output
        # ---------------------------------------------

        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)

        pig_script = f"pipelines/pig/{query_name}.pig"

        cmd = [
            "pig",
            "-x",
            "local",
            "-param",
            f"INPUT={input_path}",
            "-param",
            f"OUTPUT={output_dir}",
            pig_script
        ]

        print(f"\nRunning Pig {query_name}...")

        subprocess.run(cmd, check=True)

        return output_dir

    # =====================================================
    # LOAD RESULTS INTO POSTGRESQL
    # =====================================================

    def load_results(
        self,
        run_id,
        batch_id,
        q1_dir,
        q2_dir,
        q3_dir
    ):

        conn = connect()
        cur = conn.cursor()

        try:

            # -----------------------------------------
            # Q1
            # -----------------------------------------

            q1_path = os.path.join(q1_dir, "part-r-00000")

            if os.path.exists(q1_path):

                rows = []

                with open(q1_path) as f:

                    for line in f:

                        r = line.strip().split("\t")

                        rows.append(r)

                load_q1(
                    cur,
                    run_id,
                    batch_id,
                    rows
                )

            # -----------------------------------------
            # Q2
            # -----------------------------------------

            q2_path = os.path.join(q2_dir, "part-r-00000")

            if os.path.exists(q2_path):

                rows = []

                with open(q2_path) as f:

                    for line in f:

                        r = line.strip().split("\t")

                        rows.append(r)

                load_q2(
                    cur,
                    run_id,
                    batch_id,
                    rows
                )

            # -----------------------------------------
            # Q3
            # -----------------------------------------

            q3_path = os.path.join(q3_dir, "part-r-00000")

            if os.path.exists(q3_path):

                rows = []

                with open(q3_path) as f:

                    for line in f:

                        r = line.strip().split("\t")

                        rows.append(r)

                load_q3(
                    cur,
                    run_id,
                    batch_id,
                    rows
                )

            conn.commit()

        finally:

            cur.close()
            conn.close()

    # =====================================================
    # FULL PIPELINE
    # =====================================================

    def run_pipeline(self, input_path):

        run_id = str(uuid.uuid4())

        print("\n===================================")
        print("RUNNING PIG PIPELINE")
        print("===================================")

        # ---------------------------------------------
        # Run Queries
        # ---------------------------------------------

        q1_dir = self.run_query(
            "query1",
            input_path
        )

        q2_dir = self.run_query(
            "query2",
            input_path
        )

        q3_dir = self.run_query(
            "query3",
            input_path
        )

        # ---------------------------------------------
        # Metadata
        # ---------------------------------------------

        # write_run_metadata(
        #     run_id=run_id,
        #     pipeline_name="pig",
        #     query_name="all",
        #     batch_size=0,
        #     avg_batch_size=0,
        #     total_records=0,
        #     malformed_records=0,
        #     runtime_seconds=0
        # )

        # write_batch_metadata(
        #     run_id=run_id,
        #     batch_id=1,
        #     batch_size=0,
        #     records_processed=0,
        #     malformed_records=0
        # )

        # # ---------------------------------------------
        # # Load into PostgreSQL
        # # ---------------------------------------------

        # self.load_results(
        #     run_id,
        #     1,
        #     q1_dir,
        #     q2_dir,
        #     q3_dir
        # )

        print("\n===================================")
        print("PIG PIPELINE COMPLETE")
        print("Run ID:", run_id)
        print("===================================")

        return run_id


if __name__ == "__main__":

    runner = PigRunner()

    runner.run_pipeline(
        "data/raw/NASA_access_log_Aug95"
    )