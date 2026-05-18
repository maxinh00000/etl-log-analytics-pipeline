import os
import shutil
import subprocess
import tempfile
from pathlib import Path

from reporting.load_to_pg import (
    load_q1,
    load_q2,
    load_q3,
    connect
)

PIPELINE_NAME = "mapreduce"

SCRIPT_DIR = Path(__file__).resolve().parent


class PipelineRunner:

    def __init__(self):

        self.mappers = {
            "q1": SCRIPT_DIR / "mappers" / "mapper_q1.py",
            "q2": SCRIPT_DIR / "mappers" / "mapper_q2.py",
            "q3": SCRIPT_DIR / "mappers" / "mapper_q3.py",
        }

        self.reducers = {
            "q1": SCRIPT_DIR / "reducers" / "reducer_q1.py",
            "q2": SCRIPT_DIR / "reducers" / "reducer_q2.py",
            "q3": SCRIPT_DIR / "reducers" / "reducer_q3.py",
        }

    # =====================================================
    # RUN LOCAL MAPREDUCE
    # =====================================================

    def run_local_mapreduce(
        self,
        records,
        mapper_script,
        reducer_script
    ):

        temp_dir = tempfile.mkdtemp()

        input_file = os.path.join(temp_dir, "input.log")

        with open(input_file, "w", encoding="utf-8") as f:
            for line in records:
                f.write(line + "\n")

        output_file = os.path.join(temp_dir, "output.txt")

        try:

            with open(input_file, "r", encoding="utf-8") as inp:

                mapper = subprocess.Popen(
                    ["python3", str(mapper_script)],
                    stdin=inp,
                    stdout=subprocess.PIPE,
                    text=True
                )

                sorter = subprocess.Popen(
                    ["sort"],
                    stdin=mapper.stdout,
                    stdout=subprocess.PIPE,
                    text=True
                )

                with open(output_file, "w", encoding="utf-8") as out:

                    reducer = subprocess.Popen(
                        ["python3", str(reducer_script)],
                        stdin=sorter.stdout,
                        stdout=out,
                        text=True
                    )

                    reducer.communicate()

            rows = []

            malformed = 0

            with open(output_file, "r", encoding="utf-8") as f:

                for line in f:

                    line = line.strip()

                    if not line:
                        continue

                    parts = line.split("\t")

                    if parts[0] == "__MALFORMED__":

                        for p in parts[1:]:

                            try:
                                malformed += int(p)
                                break
                            except:
                                pass

                    else:
                        rows.append(parts)

            return rows, malformed

        finally:

            shutil.rmtree(temp_dir, ignore_errors=True)

    # =====================================================
    # MAIN PIPELINE RUN
    # =====================================================

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
            "q1_rows": [],
            "q2_rows": [],
            "q3_rows": [],
        }

        queries = ["q1", "q2", "q3"] if query == "all" else [query]

        total_malformed = 0

        for q in queries:

            rows, malformed = self.run_local_mapreduce(
                records,
                self.mappers[q],
                self.reducers[q]
            )

            total_malformed += malformed

            if q == "q1":
                result["q1_rows"] = rows

            elif q == "q2":
                result["q2_rows"] = rows

            elif q == "q3":
                result["q3_rows"] = rows

        result["malformed_records"] = total_malformed

        # ==========================================
        # SAVE OUTPUT FILES
        # ==========================================

        os.makedirs(
            "results/mapreduce",
            exist_ok=True
        )

        if result["q1_rows"]:

            with open(
                "results/mapreduce/q1_output.tsv",
                "a",
                encoding="utf-8"
            ) as f:

                for row in result["q1_rows"]:

                    f.write(
                        "\t".join(map(str, row)) + "\n"
                    )

        if result["q2_rows"]:

            with open(
                "results/mapreduce/q2_output.tsv",
                "a",
                encoding="utf-8"
            ) as f:

                for row in result["q2_rows"]:

                    f.write(
                        "\t".join(map(str, row)) + "\n"
                    )

        if result["q3_rows"]:

            with open(
                "results/mapreduce/q3_output.tsv",
                "a",
                encoding="utf-8"
            ) as f:

                for row in result["q3_rows"]:

                    f.write(
                        "\t".join(map(str, row)) + "\n"
                    )
        return result