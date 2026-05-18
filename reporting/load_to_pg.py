"""
load_to_pg.py

Unified PostgreSQL Result Loader
DAS 839 – NoSQL Systems

Supports:
- Hive TSV batch loading
- Unified in-memory loading for main.py
- Shared reporting schema across all pipelines
"""

import argparse
import glob
import os
import sys
from datetime import datetime

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    sys.exit(
        "[ERROR] psycopg2 not installed.\n"
        "Run:\n"
        "    pip install psycopg2-binary"
    )


# =========================================================
# DATABASE CONNECTION
# =========================================================

def connect(
    host="localhost",
    port=5432,
    db="etl_logs",
    user="postgres",
    password="postgres"
):
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=db,
        user=user,
        password=password
    )


# =========================================================
# TSV READER
# =========================================================

def read_tsv(directory):

    rows = []

    paths = (
        sorted(glob.glob(os.path.join(directory, "000*")))
        or
        sorted(glob.glob(os.path.join(directory, "*")))
    )

    for path in paths:

        if not os.path.isfile(path):
            continue

        with open(
            path,
            encoding="utf-8",
            errors="replace"
        ) as f:

            for line in f:

                line = line.rstrip("\n")

                if line:
                    rows.append(line.split("\t"))

    return rows


# =========================================================
# SAFE TYPE HELPERS
# =========================================================

def safe_int(v):

    try:
        return int(v)
    except:
        return None


def safe_bigint(v):

    try:
        return int(v)
    except:
        return 0


def safe_float(v):

    try:
        return float(v)
    except:
        return 0.0


# =========================================================
# QUERY LOADERS
# =========================================================

def load_q1(cur, run_id, batch_id, rows):

    sql = """
        INSERT INTO q1_daily_traffic (
            run_id,
            batch_id,
            log_date,
            status_code,
            request_count,
            total_bytes
        )
        VALUES (%s,%s,%s,%s,%s,%s)
    """

    data = []

    for r in rows:

        if len(r) < 4:
            continue

        data.append((
            run_id,
            batch_id,
            r[0] if r[0] != "UNKNOWN" else None,
            safe_int(r[1]),
            safe_bigint(r[2]),
            safe_bigint(r[3])
        ))

    if data:
        psycopg2.extras.execute_batch(cur, sql, data)

    return len(data)


def load_q2(cur, run_id, batch_id, rows):

    sql = """
        INSERT INTO q2_top_resources (
            run_id,
            batch_id,
            resource_path,
            request_count,
            total_bytes,
            distinct_host_count
        )
        VALUES (%s,%s,%s,%s,%s,%s)
    """

    data = []

    for r in rows:

        if len(r) < 4:
            continue

        data.append((
            run_id,
            batch_id,
            r[0] if r[0] != "UNKNOWN" else None,
            safe_bigint(r[1]),
            safe_bigint(r[2]),
            safe_bigint(r[3])
        ))

    if data:
        psycopg2.extras.execute_batch(cur, sql, data)

    return len(data)


def load_q3(cur, run_id, batch_id, rows):

    sql = """
        INSERT INTO q3_hourly_errors (
            run_id,
            batch_id,
            log_date,
            log_hour,
            error_request_count,
            total_request_count,
            error_rate,
            distinct_error_hosts
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """

    data = []

    for r in rows:

        if len(r) < 6:
            continue

        data.append((
            run_id,
            batch_id,
            r[0] if r[0] != "UNKNOWN" else None,
            safe_int(r[1]),
            safe_bigint(r[2]),
            safe_bigint(r[3]),
            safe_float(r[4]),
            safe_bigint(r[5])
        ))

    if data:
        psycopg2.extras.execute_batch(cur, sql, data)

    return len(data)


# =========================================================
# UNIFIED FRAMEWORK LOADER
# Used by main.py
# =========================================================

def load_results(result, pipeline, run_id, batch_id):

    conn = connect()
    cur = conn.cursor()

    try:

        query_results = result["query_results"]

        # -------------------------------------------------
        # QUERY 1
        # -------------------------------------------------

        for row in query_results.get("q1", []):

            cur.execute("""
                INSERT INTO q1_daily_traffic (
                    run_id,
                    batch_id,
                    log_date,
                    status_code,
                    request_count,
                    total_bytes
                )
                VALUES (%s,%s,%s,%s,%s,%s)
            """, (
                run_id,
                batch_id,
                row["log_date"],
                row["status_code"],
                row["request_count"],
                row["total_bytes"]
            ))

        # -------------------------------------------------
        # QUERY 2
        # -------------------------------------------------

        for row in query_results.get("q2", []):

            cur.execute("""
                INSERT INTO q2_top_resources (
                    run_id,
                    batch_id,
                    resource_path,
                    request_count,
                    total_bytes,
                    distinct_host_count
                )
                VALUES (%s,%s,%s,%s,%s,%s)
            """, (
                run_id,
                batch_id,
                row["resource_path"],
                row["request_count"],
                row["total_bytes"],
                row["distinct_host_count"]
            ))

        # -------------------------------------------------
        # QUERY 3
        # -------------------------------------------------

        for row in query_results.get("q3", []):

            cur.execute("""
                INSERT INTO q3_hourly_errors (
                    run_id,
                    batch_id,
                    log_date,
                    log_hour,
                    error_request_count,
                    total_request_count,
                    error_rate,
                    distinct_error_hosts
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                run_id,
                batch_id,
                row["log_date"],
                row["log_hour"],
                row["error_request_count"],
                row["total_request_count"],
                row["error_rate"],
                row["distinct_error_hosts"]
            ))

        conn.commit()

    except Exception as e:

        conn.rollback()
        print(f"[load_results] ERROR: {e}")
        raise

    finally:

        cur.close()
        conn.close()


# =========================================================
# CLI ARGUMENTS
# Used for Hive TSV batch loading
# =========================================================

def parse_args():

    p = argparse.ArgumentParser()

    p.add_argument("--run-id", required=True)

    p.add_argument(
        "--batch-id",
        required=True,
        type=int
    )

    p.add_argument(
        "--pipeline-name",
        default="hive"
    )

    p.add_argument(
        "--records-in-batch",
        required=True,
        type=int
    )

    p.add_argument(
        "--malformed-in-batch",
        required=True,
        type=int
    )

    p.add_argument(
        "--output-dir",
        required=True
    )

    p.add_argument(
        "--host",
        default="localhost"
    )

    p.add_argument(
        "--port",
        default=5432,
        type=int
    )

    p.add_argument(
        "--db",
        default="etl_logs"
    )

    p.add_argument(
        "--user",
        default="postgres"
    )

    p.add_argument(
        "--password",
        default="postgres"
    )

    return p.parse_args()


# =========================================================
# HIVE TSV LOADING MODE
# =========================================================

def main():

    args = parse_args()

    q1_rows = read_tsv(
        os.path.join(
            args.output_dir,
            "q1",
            f"batch_{args.batch_id}"
        )
    )

    q2_rows = read_tsv(
        os.path.join(
            args.output_dir,
            "q2",
            f"batch_{args.batch_id}"
        )
    )

    q3_rows = read_tsv(
        os.path.join(
            args.output_dir,
            "q3",
            f"batch_{args.batch_id}"
        )
    )

    print(
        f"[load_to_pg] "
        f"batch={args.batch_id} "
        f"q1={len(q1_rows)} "
        f"q2={len(q2_rows)} "
        f"q3={len(q3_rows)}"
    )

    conn = connect(
        args.host,
        args.port,
        args.db,
        args.user,
        args.password
    )

    cur = conn.cursor()

    try:

        n1 = load_q1(
            cur,
            args.run_id,
            args.batch_id,
            q1_rows
        )

        n2 = load_q2(
            cur,
            args.run_id,
            args.batch_id,
            q2_rows
        )

        n3 = load_q3(
            cur,
            args.run_id,
            args.batch_id,
            q3_rows
        )

        # ---------------------------------------------
        # BATCH METADATA
        # ---------------------------------------------

        cur.execute("""
            INSERT INTO batch_metadata (
                run_id,
                batch_id,
                batch_size,
                records_processed,
                malformed_records
            )
            VALUES (%s,%s,%s,%s,%s)

            ON CONFLICT (run_id, batch_id)
            DO UPDATE SET
                batch_size = EXCLUDED.batch_size,
                records_processed = EXCLUDED.records_processed,
                malformed_records = EXCLUDED.malformed_records
        """, (
            args.run_id,
            args.batch_id,
            args.records_in_batch,
            args.records_in_batch,
            args.malformed_in_batch
        ))

        conn.commit()

        print(
            f"[load_to_pg] "
            f"Committed "
            f"q1={n1} "
            f"q2={n2} "
            f"q3={n3}"
        )

    except Exception as e:

        conn.rollback()

        print(f"[load_to_pg] ERROR: {e}")

        raise

    finally:

        cur.close()
        conn.close()


if __name__ == "__main__":
    main()