#!/usr/bin/env python3
"""
report.py
DAS 839 – NoSQL Systems | Hive Pipeline

Reads results from MySQL and prints a formatted report showing:
  - Run-level execution metadata (pipeline, runtime, batches, avg batch size)
  - Query 1: Daily Traffic Summary
  - Query 2: Top 20 Requested Resources
  - Query 3: Hourly Error Analysis

Usage:
    python3 report.py \\
        --run-id hive_20250427_120000 \\
        [--host localhost] [--port 3306] [--db nasa_etl] \\
        [--user root] [--password ""]

    # Show all runs for a pipeline:
    python3 report.py --pipeline hive
"""

import argparse
import sys
from datetime import datetime

try:
    import psycopg2
except ImportError:
    sys.exit("[ERROR] psycopg2 not installed. Run: pip3 install psycopg2-binary")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def connect(host, port, db, user, password):
    return psycopg2.connect(host=host, port=port, dbname=db,
                            user=user, password=password or None)


def fmt_num(n):
    if n is None:
        return "NULL"
    return f"{n:,}"


def separator(width=90):
    print("-" * width)


def header(title, width=90):
    print()
    print("=" * width)
    print(f"  {title}")
    print("=" * width)


def row_fmt(cols, widths):
    parts = []
    for val, w in zip(cols, widths):
        s = str(val) if val is not None else "NULL"
        parts.append(s.ljust(w) if w > 0 else s)
    print("  " + "  ".join(parts))


# ---------------------------------------------------------------------------
# Report sections
# ---------------------------------------------------------------------------

def print_run_metadata(cursor, run_id):
    cursor.execute("""
        SELECT pipeline_name, batch_size, total_records, malformed_records,
               total_batches, avg_batch_size, runtime_seconds, started_at, completed_at
        FROM etl_runs
        WHERE run_id = %s
    """, (run_id,))
    row = cursor.fetchone()
    if not row:
        print(f"[WARN] No run found with run_id='{run_id}'")
        return

    (pipeline_name, batch_size, total_records, malformed_records,
     total_batches, avg_batch_size, runtime_seconds, started_at, completed_at) = row

    header("RUN METADATA")
    print(f"  Pipeline          : {pipeline_name}")
    print(f"  Run Identifier    : {run_id}")
    print(f"  Batch Size        : {fmt_num(batch_size)} records/batch")
    print(f"  Total Records     : {fmt_num(total_records)}")
    print(f"  Malformed Records : {fmt_num(malformed_records)}")
    print(f"  Total Batches     : {fmt_num(total_batches)}")
    print(f"  Avg Batch Size    : {avg_batch_size:.2f} records/batch")
    print(f"  Runtime           : {runtime_seconds:.3f} seconds")
    print(f"  Started At        : {started_at}")
    print(f"  Completed At      : {completed_at}")


def print_batch_summary(cursor, run_id):
    cursor.execute("""
        SELECT batch_id, records_in_batch, malformed_in_batch,
               batch_started_at, batch_ended_at
        FROM batch_metadata
        WHERE run_id = %s
        ORDER BY batch_id
    """, (run_id,))
    rows = cursor.fetchall()
    if not rows:
        return

    header("BATCH SUMMARY")
    cols = ["Batch", "Records", "Malformed", "Started", "Ended"]
    widths = [7, 12, 12, 22, 22]
    row_fmt(cols, widths)
    separator()
    for r in rows:
        row_fmt([r[0], fmt_num(r[1]), fmt_num(r[2]), r[3], r[4]], widths)


def print_query1(cursor, run_id):
    cursor.execute("""
        SELECT log_date, status_code,
               SUM(request_count) AS request_count,
               SUM(total_bytes)   AS total_bytes
        FROM q1_daily_traffic
        WHERE run_id = %s
        GROUP BY log_date, status_code
        ORDER BY log_date, status_code
        LIMIT 50
    """, (run_id,))
    rows = cursor.fetchall()

    header("QUERY 1: DAILY TRAFFIC SUMMARY  (first 50 rows)")
    cols   = ["log_date", "status_code", "request_count", "total_bytes"]
    widths = [14, 14, 16, 16]
    row_fmt(cols, widths)
    separator()
    for r in rows:
        row_fmt([r[0], r[1], fmt_num(r[2]), fmt_num(r[3])], widths)
    print(f"\n  Total rows in table: ", end="")
    cursor.execute("SELECT COUNT(*) FROM q1_daily_traffic WHERE run_id=%s", (run_id,))
    print(fmt_num(cursor.fetchone()[0]))


def print_query2(cursor, run_id):
    cursor.execute("""
        SELECT resource_path,
               SUM(request_count)       AS request_count,
               SUM(total_bytes)         AS total_bytes,
               SUM(distinct_host_count) AS distinct_host_count
        FROM q2_top_resources
        WHERE run_id = %s
        GROUP BY resource_path
        ORDER BY request_count DESC
        LIMIT 20
    """, (run_id,))
    rows = cursor.fetchall()

    header("QUERY 2: TOP 20 REQUESTED RESOURCES")
    cols   = ["resource_path", "request_count", "total_bytes", "distinct_hosts"]
    widths = [45, 16, 16, 16]
    row_fmt(cols, widths)
    separator()
    for r in rows:
        path = str(r[0])[:44] if r[0] else "NULL"
        row_fmt([path, fmt_num(r[1]), fmt_num(r[2]), fmt_num(r[3])], widths)


def print_query3(cursor, run_id):
    cursor.execute("""
        SELECT log_date, log_hour,
               SUM(error_request_count)  AS error_request_count,
               SUM(total_request_count)  AS total_request_count,
               ROUND(AVG(error_rate)::numeric, 6) AS error_rate,
               SUM(distinct_error_hosts) AS distinct_error_hosts
        FROM q3_hourly_errors
        WHERE run_id = %s
        GROUP BY log_date, log_hour
        ORDER BY log_date, log_hour
        LIMIT 50
    """, (run_id,))
    rows = cursor.fetchall()

    header("QUERY 3: HOURLY ERROR ANALYSIS  (first 50 rows)")
    cols   = ["log_date", "hour", "error_reqs", "total_reqs", "error_rate", "error_hosts"]
    widths = [14, 6, 12, 12, 12, 14]
    row_fmt(cols, widths)
    separator()
    for r in rows:
        row_fmt([r[0], r[1], fmt_num(r[2]), fmt_num(r[3]),
                 f"{r[4]:.4f}" if r[4] is not None else "NULL",
                 fmt_num(r[5])], widths)
    print(f"\n  Total rows in table: ", end="")
    cursor.execute("SELECT COUNT(*) FROM q3_hourly_errors WHERE run_id=%s", (run_id,))
    print(fmt_num(cursor.fetchone()[0]))


def list_runs(cursor, pipeline):
    cursor.execute("""
        SELECT run_id, pipeline_name, batch_size, total_records,
               total_batches, runtime_seconds, started_at
        FROM etl_runs
        WHERE pipeline_name = %s
        ORDER BY started_at DESC
        LIMIT 20
    """, (pipeline,))
    rows = cursor.fetchall()
    header(f"AVAILABLE RUNS  (pipeline={pipeline})")
    cols   = ["run_id", "pipeline", "batch_size", "total_records", "batches", "runtime_s", "started_at"]
    widths = [35, 10, 12, 14, 8, 12, 22]
    row_fmt(cols, widths)
    separator()
    for r in rows:
        row_fmt([r[0], r[1], fmt_num(r[2]), fmt_num(r[3]), r[4],
                 f"{r[5]:.2f}" if r[5] else "0.00", r[6]], widths)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Display Hive pipeline ETL report from MySQL")
    p.add_argument("--run-id",   help="Run identifier to report on")
    p.add_argument("--pipeline", default="hive",
                   help="Pipeline name to list runs for (used when --run-id is omitted)")
    p.add_argument("--host",     default="localhost")
    p.add_argument("--port",     default=3306, type=int)
    p.add_argument("--db",       default="nasa_etl")
    p.add_argument("--user",     default="root")
    p.add_argument("--password", default="")
    return p.parse_args()


def main():
    args   = parse_args()
    conn   = connect(args.host, args.port, args.db, args.user, args.password)
    cursor = conn.cursor()

    try:
        if not args.run_id:
            list_runs(cursor, args.pipeline)
        else:
            print_run_metadata(cursor, args.run_id)
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
