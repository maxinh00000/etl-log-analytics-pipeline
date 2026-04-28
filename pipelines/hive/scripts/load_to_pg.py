#!/usr/bin/env python3
"""
load_to_pg.py  –  Load Hive query TSV outputs into PostgreSQL
DAS 839 – NoSQL Systems | Hive Pipeline
"""
import argparse, glob, os, sys
from datetime import datetime

try:
    import psycopg2, psycopg2.extras
except ImportError:
    sys.exit("[ERROR] psycopg2 not installed. Run: pip3 install psycopg2-binary")

def connect(host, port, db, user, password):
    return psycopg2.connect(host=host, port=port, dbname=db,
                            user=user, password=password)

def read_tsv(directory):
    rows = []
    for path in sorted(glob.glob(os.path.join(directory, "000*"))) or \
                sorted(glob.glob(os.path.join(directory, "*"))):
        if not os.path.isfile(path): continue
        with open(path, encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.rstrip("\n")
                if line:
                    rows.append(line.split("\t"))
    return rows

def safe_int(v):
    try: return int(v)
    except: return None

def safe_bigint(v):
    try: return int(v)
    except: return 0

def safe_float(v):
    try: return float(v)
    except: return 0.0

def load_q1(cur, run_id, batch_id, pipeline, ts, rows):
    sql = """INSERT INTO q1_daily_traffic
             (run_id,batch_id,pipeline_name,time_of_execution,log_date,status_code,request_count,total_bytes)
             VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
    data = []
    for r in rows:
        if len(r) < 4: continue
        data.append((run_id, batch_id, pipeline, ts,
                     r[0] if r[0] != 'UNKNOWN' else None,
                     safe_int(r[1]), safe_bigint(r[2]), safe_bigint(r[3])))
    if data: psycopg2.extras.execute_batch(cur, sql, data)
    return len(data)

def load_q2(cur, run_id, batch_id, pipeline, ts, rows):
    sql = """INSERT INTO q2_top_resources
             (run_id,batch_id,pipeline_name,time_of_execution,resource_path,request_count,total_bytes,distinct_host_count)
             VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
    data = []
    for r in rows:
        if len(r) < 4: continue
        data.append((run_id, batch_id, pipeline, ts,
                     r[0] if r[0] != 'UNKNOWN' else None,
                     safe_bigint(r[1]), safe_bigint(r[2]), safe_bigint(r[3])))
    if data: psycopg2.extras.execute_batch(cur, sql, data)
    return len(data)

def load_q3(cur, run_id, batch_id, pipeline, ts, rows):
    sql = """INSERT INTO q3_hourly_errors
             (run_id,batch_id,pipeline_name,time_of_execution,log_date,log_hour,
              error_request_count,total_request_count,error_rate,distinct_error_hosts)
             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    data = []
    for r in rows:
        if len(r) < 6: continue
        data.append((run_id, batch_id, pipeline, ts,
                     r[0] if r[0] != 'UNKNOWN' else None,
                     safe_int(r[1]), safe_bigint(r[2]), safe_bigint(r[3]),
                     safe_float(r[4]), safe_bigint(r[5])))
    if data: psycopg2.extras.execute_batch(cur, sql, data)
    return len(data)

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--run-id",             required=True)
    p.add_argument("--batch-id",           required=True, type=int)
    p.add_argument("--pipeline-name",      default="hive")
    p.add_argument("--records-in-batch",   required=True, type=int)
    p.add_argument("--malformed-in-batch", required=True, type=int)
    p.add_argument("--output-dir",         required=True)
    p.add_argument("--host",     default="/home/rsvr_ind/pgdata_nasa/socket")
    p.add_argument("--port",     default=5433, type=int)
    p.add_argument("--db",       default="nasa_etl")
    p.add_argument("--user",     default="nasa_user")
    p.add_argument("--password", default="")
    return p.parse_args()

def main():
    args = parse_args()
    ts   = datetime.now()

    q1_rows = read_tsv(os.path.join(args.output_dir, "q1", f"batch_{args.batch_id}"))
    q2_rows = read_tsv(os.path.join(args.output_dir, "q2", f"batch_{args.batch_id}"))
    q3_rows = read_tsv(os.path.join(args.output_dir, "q3", f"batch_{args.batch_id}"))
    print(f"[load_to_pg] batch={args.batch_id}  q1={len(q1_rows)}  q2={len(q2_rows)}  q3={len(q3_rows)}")

    conn = connect(args.host, args.port, args.db, args.user, args.password)
    cur  = conn.cursor()
    try:
        n1 = load_q1(cur, args.run_id, args.batch_id, args.pipeline_name, ts, q1_rows)
        n2 = load_q2(cur, args.run_id, args.batch_id, args.pipeline_name, ts, q2_rows)
        n3 = load_q3(cur, args.run_id, args.batch_id, args.pipeline_name, ts, q3_rows)
        cur.execute("""
            INSERT INTO batch_metadata
              (run_id,batch_id,pipeline_name,records_in_batch,malformed_in_batch,batch_ended_at)
            VALUES (%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (run_id,batch_id) DO UPDATE
              SET records_in_batch=%s, malformed_in_batch=%s, batch_ended_at=NOW()
        """, (args.run_id, args.batch_id, args.pipeline_name,
              args.records_in_batch, args.malformed_in_batch,
              args.records_in_batch, args.malformed_in_batch))
        conn.commit()
        print(f"[load_to_pg] Committed  q1={n1}  q2={n2}  q3={n3}")
    except Exception as e:
        conn.rollback()
        print(f"[load_to_pg] ERROR: {e}"); raise
    finally:
        cur.close(); conn.close()

if __name__ == "__main__":
    main()
