#!/usr/bin/env python3
"""
report.py
DAS 839 – NoSQL Systems

Unified ETL Reporting Layer
Supports:
- MongoDB
- Hive
- Pig
- MapReduce

Reads all results ONLY from PostgreSQL and displays:
    - Run metadata
    - Batch metadata
    - Query 1 results
    - Query 2 results
    - Query 3 results

Usage:
    python3 report.py --run-id mongo_20260517_120000

    python3 report.py --pipeline hive
"""

import argparse
import sys

try:
    import psycopg2
except ImportError:
    sys.exit(
        "[ERROR] psycopg2 not installed.\n"
        "Run:\n"
        "    pip install psycopg2-binary"
    )


# =========================================================
# DATABASE CONNECTION
# =========================================================

def connect(host, port, db, user, password):
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=db,
        user=user,
        password=password or None
    )


# =========================================================
# FORMAT HELPERS
# =========================================================

def fmt_num(n):
    if n is None:
        return "NULL"
    return f"{n:,}"


def separator(width=100):
    print("-" * width)


def header(title, width=100):
    print()
    print("=" * width)
    print(f"  {title}")
    print("=" * width)


def row_fmt(cols, widths):
    parts = []

    for val, w in zip(cols, widths):
        s = str(val) if val is not None else "NULL"

        if len(s) > w:
            s = s[:w - 3] + "..."

        parts.append(s.ljust(w))

    print("  " + "  ".join(parts))


# =========================================================
# RUN METADATA
# =========================================================

def print_run_metadata(cursor, run_id):

    cursor.execute("""
        SELECT
            pipeline_name,
            query_name,
            batch_size,
            avg_batch_size,
            total_records,
            malformed_records,
            runtime_seconds,
            executed_at
        FROM run_metadata
        WHERE run_id = %s
    """, (run_id,))

    row = cursor.fetchone()

    if not row:
        print(f"[WARN] No run found for run_id='{run_id}'")
        return False

    (
        pipeline_name,
        query_name,
        batch_size,
        avg_batch_size,
        total_records,
        malformed_records,
        runtime_seconds,
        executed_at
    ) = row

    header("RUN METADATA")

    print(f"  Pipeline            : {pipeline_name}")
    print(f"  Run ID              : {run_id}")
    print(f"  Query               : {query_name}")
    print(f"  Batch Size          : {fmt_num(batch_size)}")
    print(f"  Average Batch Size  : {avg_batch_size:.2f}")
    print(f"  Total Records       : {fmt_num(total_records)}")
    print(f"  Malformed Records   : {fmt_num(malformed_records)}")
    print(f"  Runtime             : {runtime_seconds:.3f} seconds")
    print(f"  Executed At         : {executed_at}")

    return True


# =========================================================
# BATCH METADATA
# =========================================================

def print_batch_summary(cursor, run_id):

    cursor.execute("""
        SELECT
            batch_id,
            batch_size,
            records_processed,
            malformed_records
        FROM batch_metadata
        WHERE run_id = %s
        ORDER BY batch_id
    """, (run_id,))

    rows = cursor.fetchall()

    if not rows:
        return

    header("BATCH SUMMARY")

    cols = [
        "Batch ID",
        "Batch Size",
        "Processed",
        "Malformed"
    ]

    widths = [12, 16, 16, 16]

    row_fmt(cols, widths)
    separator()

    for r in rows:
        row_fmt([
            r[0],
            fmt_num(r[1]),
            fmt_num(r[2]),
            fmt_num(r[3])
        ], widths)


# =========================================================
# QUERY 1
# =========================================================

def print_query1(cursor, run_id):

    cursor.execute("""
        SELECT
            log_date,
            status_code,
            request_count,
            total_bytes
        FROM q1_daily_traffic
        WHERE run_id = %s
        ORDER BY log_date, status_code
        LIMIT 50
    """, (run_id,))

    rows = cursor.fetchall()

    header("QUERY 1 — DAILY TRAFFIC SUMMARY")

    cols = [
        "Date",
        "Status",
        "Requests",
        "Bytes"
    ]

    widths = [16, 12, 16, 18]

    row_fmt(cols, widths)
    separator()

    for r in rows:
        row_fmt([
            r[0],
            r[1],
            fmt_num(r[2]),
            fmt_num(r[3])
        ], widths)

    print()

    cursor.execute("""
        SELECT COUNT(*)
        FROM q1_daily_traffic
        WHERE run_id = %s
    """, (run_id,))

    total_rows = cursor.fetchone()[0]

    print(f"  Total Rows: {fmt_num(total_rows)}")


# =========================================================
# QUERY 2
# =========================================================

def print_query2(cursor, run_id):

    cursor.execute("""
        SELECT
            resource_path,
            request_count,
            total_bytes,
            distinct_host_count
        FROM q2_top_resources
        WHERE run_id = %s
        ORDER BY request_count DESC
        LIMIT 20
    """, (run_id,))

    rows = cursor.fetchall()

    header("QUERY 2 — TOP REQUESTED RESOURCES")

    cols = [
        "Resource Path",
        "Requests",
        "Bytes",
        "Distinct Hosts"
    ]

    widths = [50, 16, 16, 18]

    row_fmt(cols, widths)
    separator()

    for r in rows:
        row_fmt([
            r[0],
            fmt_num(r[1]),
            fmt_num(r[2]),
            fmt_num(r[3])
        ], widths)


# =========================================================
# QUERY 3
# =========================================================

def print_query3(cursor, run_id):

    cursor.execute("""
        SELECT
            log_date,
            log_hour,
            error_request_count,
            total_request_count,
            error_rate,
            distinct_error_hosts
        FROM q3_hourly_errors
        WHERE run_id = %s
        ORDER BY log_date, log_hour
        LIMIT 50
    """, (run_id,))

    rows = cursor.fetchall()

    header("QUERY 3 — HOURLY ERROR ANALYSIS")

    cols = [
        "Date",
        "Hour",
        "Error Requests",
        "Total Requests",
        "Error Rate",
        "Error Hosts"
    ]

    widths = [14, 8, 16, 16, 14, 16]

    row_fmt(cols, widths)
    separator()

    for r in rows:

        err_rate = (
            f"{float(r[4]):.6f}"
            if r[4] is not None
            else "NULL"
        )

        row_fmt([
            r[0],
            r[1],
            fmt_num(r[2]),
            fmt_num(r[3]),
            err_rate,
            fmt_num(r[5])
        ], widths)

    print()

    cursor.execute("""
        SELECT COUNT(*)
        FROM q3_hourly_errors
        WHERE run_id = %s
    """, (run_id,))

    total_rows = cursor.fetchone()[0]

    print(f"  Total Rows: {fmt_num(total_rows)}")


# =========================================================
# LIST AVAILABLE RUNS
# =========================================================

def list_runs(cursor, pipeline):

    cursor.execute("""
        SELECT
            run_id,
            pipeline_name,
            query_name,
            batch_size,
            total_records,
            runtime_seconds,
            executed_at
        FROM run_metadata
        WHERE pipeline_name = %s
        ORDER BY executed_at DESC
        LIMIT 20
    """, (pipeline,))

    rows = cursor.fetchall()

    header(f"AVAILABLE RUNS — PIPELINE={pipeline}")

    cols = [
        "Run ID",
        "Pipeline",
        "Query",
        "Batch Size",
        "Records",
        "Runtime(s)",
        "Executed At"
    ]

    widths = [30, 12, 10, 14, 14, 14, 24]

    row_fmt(cols, widths)
    separator()

    for r in rows:

        runtime = (
            f"{float(r[5]):.3f}"
            if r[5] is not None
            else "0.000"
        )

        row_fmt([
            r[0],
            r[1],
            r[2],
            fmt_num(r[3]),
            fmt_num(r[4]),
            runtime,
            r[6]
        ], widths)


# =========================================================
# ARGUMENTS
# =========================================================

def parse_args():

    p = argparse.ArgumentParser(
        description="Unified ETL Reporting Tool"
    )

    p.add_argument(
        "--run-id",
        help="Run identifier to display"
    )

    p.add_argument(
        "--pipeline",
        default="hive",
        help="Pipeline name when listing runs"
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

def generate_report(run_id):
    conn = connect(
        host="localhost",
        port=5432,
        db="etl_logs",
        user="postgres",
        password="postgres"
    )
    cursor = conn.cursor()
    try:
        found = print_run_metadata(cursor, run_id)

        if found:

            print_batch_summary(cursor, run_id)

            print_query1(cursor, run_id)

            print_query2(cursor, run_id)

            print_query3(cursor, run_id)
        print()

    finally:
        cursor.close()
        conn.close()


# =========================================================
# MAIN
# =========================================================

def main():

    args = parse_args()

    conn = connect(
        args.host,
        args.port,
        args.db,
        args.user,
        args.password
    )

    cursor = conn.cursor()

    try:

        if not args.run_id:
            list_runs(cursor, args.pipeline)

        else:

            found = print_run_metadata(cursor, args.run_id)

            if found:
                print_batch_summary(cursor, args.run_id)
                print_query1(cursor, args.run_id)
                print_query2(cursor, args.run_id)
                print_query3(cursor, args.run_id)

        print()

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()