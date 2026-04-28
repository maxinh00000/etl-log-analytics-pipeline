#!/usr/bin/env python3
"""
load_to_mysql.py
DAS 839 – NoSQL Systems | Hive Pipeline

Reads query result files that Hive wrote via INSERT OVERWRITE LOCAL DIRECTORY
(tab-separated, one directory per query/batch) and loads them into MySQL.

Usage:
    python3 load_to_mysql.py \\
        --run-id  hive_20250427_120000 \\
        --batch-id 1 \\
        --records-in-batch 100000 \\
        --malformed-in-batch 42 \\
        --output-dir /tmp/nasa_hive_output \\
        --host localhost --port 3306 \\
        --db nasa_etl --user root --password ""
"""

import argparse
import glob
import os
import sys
from datetime import datetime

try:
    import mysql.connector
except ImportError:
    print("[ERROR] mysql-connector-python not installed. Run: pip3 install mysql-connector-python")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def connect(host, port, db, user, password):
    return mysql.connector.connect(
        host=host, port=port, database=db,
        user=user, password=password,
        autocommit=False
    )


def read_tsv_dir(directory):
    """Return rows from all part files in a Hive LOCAL DIRECTORY output."""
    rows = []
    part_files = sorted(glob.glob(os.path.join(directory, "000*")))
    if not part_files:
        # Hive sometimes uses a flat file name
        part_files = sorted(glob.glob(os.path.join(directory, "*")))
    for path in part_files:
        if not os.path.isfile(path):
            continue
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.rstrip("\n")
                if line:
                    rows.append(line.split("\t"))
    return rows


def safe_int(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def safe_bigint(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0


def safe_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def load_q1(cursor, run_id, batch_id, pipeline_name, ts, rows):
    """Query 1: Daily Traffic Summary."""
    sql = """
        INSERT INTO q1_daily_traffic
            (run_id, batch_id, pipeline_name, time_of_execution,
             log_date, status_code, request_count, total_bytes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    data = []
    for row in rows:
        if len(row) < 4:
            continue
        log_date    = row[0] if row[0] != 'UNKNOWN' else None
        status_code = safe_int(row[1])
        req_count   = safe_bigint(row[2])
        total_bytes = safe_bigint(row[3])
        data.append((run_id, batch_id, pipeline_name, ts,
                     log_date, status_code, req_count, total_bytes))
    if data:
        cursor.executemany(sql, data)
    return len(data)


def load_q2(cursor, run_id, batch_id, pipeline_name, ts, rows):
    """Query 2: Top 20 Requested Resources."""
    sql = """
        INSERT INTO q2_top_resources
            (run_id, batch_id, pipeline_name, time_of_execution,
             resource_path, request_count, total_bytes, distinct_host_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    data = []
    for row in rows:
        if len(row) < 4:
            continue
        resource_path       = row[0] if row[0] != 'UNKNOWN' else None
        req_count           = safe_bigint(row[1])
        total_bytes         = safe_bigint(row[2])
        distinct_host_count = safe_bigint(row[3])
        data.append((run_id, batch_id, pipeline_name, ts,
                     resource_path, req_count, total_bytes, distinct_host_count))
    if data:
        cursor.executemany(sql, data)
    return len(data)


def load_q3(cursor, run_id, batch_id, pipeline_name, ts, rows):
    """Query 3: Hourly Error Analysis."""
    sql = """
        INSERT INTO q3_hourly_errors
            (run_id, batch_id, pipeline_name, time_of_execution,
             log_date, log_hour, error_request_count, total_request_count,
             error_rate, distinct_error_hosts)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    data = []
    for row in rows:
        if len(row) < 6:
            continue
        log_date             = row[0] if row[0] != 'UNKNOWN' else None
        log_hour             = safe_int(row[1])
        error_request_count  = safe_bigint(row[2])
        total_request_count  = safe_bigint(row[3])
        error_rate           = safe_float(row[4])
        distinct_error_hosts = safe_bigint(row[5])
        data.append((run_id, batch_id, pipeline_name, ts,
                     log_date, log_hour,
                     error_request_count, total_request_count,
                     error_rate, distinct_error_hosts))
    if data:
        cursor.executemany(sql, data)
    return len(data)


def upsert_batch_metadata(cursor, run_id, batch_id, pipeline_name,
                          records_in_batch, malformed_in_batch):
    sql = """
        INSERT INTO batch_metadata
            (run_id, batch_id, pipeline_name, records_in_batch, malformed_in_batch, batch_ended_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE
            records_in_batch  = VALUES(records_in_batch),
            malformed_in_batch = VALUES(malformed_in_batch),
            batch_ended_at    = NOW()
    """
    cursor.execute(sql, (run_id, batch_id, pipeline_name,
                         records_in_batch, malformed_in_batch))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Load Hive query outputs to MySQL")
    p.add_argument("--run-id",             required=True)
    p.add_argument("--batch-id",           required=True, type=int)
    p.add_argument("--pipeline-name",      default="hive")
    p.add_argument("--records-in-batch",   required=True, type=int)
    p.add_argument("--malformed-in-batch", required=True, type=int)
    p.add_argument("--output-dir",         required=True,
                   help="Parent directory; expects q1/batch_N, q2/batch_N, q3/batch_N subdirs")
    p.add_argument("--host",     default="localhost")
    p.add_argument("--port",     default=3306, type=int)
    p.add_argument("--db",       default="nasa_etl")
    p.add_argument("--user",     default="root")
    p.add_argument("--password", default="")
    return p.parse_args()


def main():
    args = parse_args()
    ts   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    q1_dir = os.path.join(args.output_dir, "q1", f"batch_{args.batch_id}")
    q2_dir = os.path.join(args.output_dir, "q2", f"batch_{args.batch_id}")
    q3_dir = os.path.join(args.output_dir, "q3", f"batch_{args.batch_id}")

    q1_rows = read_tsv_dir(q1_dir)
    q2_rows = read_tsv_dir(q2_dir)
    q3_rows = read_tsv_dir(q3_dir)

    print(f"[load_to_mysql] batch={args.batch_id}  "
          f"q1={len(q1_rows)} rows  q2={len(q2_rows)} rows  q3={len(q3_rows)} rows")

    conn   = connect(args.host, args.port, args.db, args.user, args.password)
    cursor = conn.cursor()

    try:
        n1 = load_q1(cursor, args.run_id, args.batch_id, args.pipeline_name, ts, q1_rows)
        n2 = load_q2(cursor, args.run_id, args.batch_id, args.pipeline_name, ts, q2_rows)
        n3 = load_q3(cursor, args.run_id, args.batch_id, args.pipeline_name, ts, q3_rows)
        upsert_batch_metadata(cursor, args.run_id, args.batch_id, args.pipeline_name,
                              args.records_in_batch, args.malformed_in_batch)
        conn.commit()
        print(f"[load_to_mysql] Committed  q1={n1}  q2={n2}  q3={n3}")
    except Exception as e:
        conn.rollback()
        print(f"[load_to_mysql] ERROR – rolled back: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
