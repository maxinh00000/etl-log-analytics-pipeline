import uuid

from reporting.load_to_pg import (
    connect,
    load_q1,
    load_q2,
    load_q3,
    read_tsv
)

from reporting.metadata import (
    write_run_metadata,
    write_batch_metadata
)

run_id = str(uuid.uuid4())

print("RUN ID:", run_id)

# ============================================
# WRITE METADATA
# ============================================

write_run_metadata(
    run_id=run_id,
    pipeline_name="pig",
    query_name="all",
    batch_size=0,
    avg_batch_size=0,
    total_records=0,
    malformed_records=0,
    runtime_seconds=0
)

write_batch_metadata(
    run_id=run_id,
    batch_id=1,
    batch_size=0,
    records_processed=0,
    malformed_records=0
)

# ============================================
# READ TSV OUTPUTS
# ============================================

q1_rows = read_tsv("pig_output/query1")
q2_rows = read_tsv("pig_output/query2")
q3_rows = read_tsv("pig_output/query3")

# ============================================
# LOAD INTO POSTGRESQL
# ============================================

conn = connect()
cur = conn.cursor()

load_q1(cur, run_id, 1, q1_rows)
load_q2(cur, run_id, 1, q2_rows)
load_q3(cur, run_id, 1, q3_rows)

conn.commit()

cur.close()
conn.close()

print("Pig results loaded successfully.")