#!/usr/bin/env bash
# reset_and_run.sh
# DAS 839 – NoSQL Systems | Hive Pipeline (LOCAL MODE — no HDFS)
#
# Wipes all state and runs the full pipeline from scratch.
#
# Usage:
#   bash reset_and_run.sh                          # Jul95, batch 500000
#   bash reset_and_run.sh Aug95                    # Aug95
#   bash reset_and_run.sh Jul95 300000             # custom batch size
#
# Args:
#   $1  dataset  — "Jul95" or "Aug95"  (default: Jul95)
#   $2  batch    — records per batch   (default: 500000)

set -euo pipefail

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DATASET="${1:-Jul95}"
BATCH_SIZE="${2:-500000}"

JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
HIVE_HOME=/home/rsvr_ind/hive
HADOOP_HOME=/home/rsvr_ind/hadoop-3.3.6
export JAVA_HOME HIVE_HOME HADOOP_HOME
export PATH=$JAVA_HOME/bin:$HIVE_HOME/bin:$HADOOP_HOME/bin:$PATH
export HADOOP_CLIENT_OPTS="-Xmx4g"
export HADOOP_HEAPSIZE=4096

PSQL="/usr/lib/postgresql/14/bin/psql -h /var/run/postgresql -p 5432 -U nasa_user"

LOG_FILE="/home/rsvr_ind/Music/NASA_access_log_${DATASET}"
HIVE_DB="nasa_logs"

# Local paths (no HDFS)
LOCAL_DATA="/home/rsvr_ind/nasa_etl_data"        # replaces HDFS
PLACEHOLDER="${LOCAL_DATA}/placeholder"
BATCHES_DIR="${LOCAL_DATA}/batches"

SCHEMA_SQL="/home/rsvr_ind/Music/hive_sreenivasa/hive_pipeline/sql/schema_pg.sql"
CREATE_HQL="/home/rsvr_ind/Music/hive_sreenivasa/hive_pipeline/hive/create_tables.hql"
PROCESS_HQL="/home/rsvr_ind/Music/hive_sreenivasa/hive_pipeline/hive/process_batch.hql"
LOAD_SCRIPT="/home/rsvr_ind/Music/hive_sreenivasa/hive_pipeline/scripts/load_to_pg.py"
REPORT_SCRIPT="/home/rsvr_ind/Music/hive_sreenivasa/hive_pipeline/scripts/report.py"

LOCAL_OUTPUT="/tmp/nasa_hive_output"
BATCH_DIR="/tmp/nasa_batches"

# Hive warehouse — keep under home so no permission issues
export HIVE_OPTS="-hiveconf hive.metastore.warehouse.dir=/home/rsvr_ind/hive_warehouse"

# ---------------------------------------------------------------------------
# Safety check
# ---------------------------------------------------------------------------
if [[ ! -f "$LOG_FILE" ]]; then
    echo "[ERROR] Log file not found: $LOG_FILE"
    exit 1
fi

echo ""
echo "========================================================================"
echo "  Hive Pipeline — Full Reset + Run  (LOCAL MODE)"
echo "  Dataset   : $LOG_FILE"
echo "  Batch size: $BATCH_SIZE"
echo "========================================================================"

# ---------------------------------------------------------------------------
# [1/6] Clean local temp files
# ---------------------------------------------------------------------------
echo ""
echo "--- [1/6] Cleaning local temp files ---"
rm -rf "$BATCH_DIR" "$LOCAL_OUTPUT" "$LOCAL_DATA"
rm -rf /home/rsvr_ind/hive_warehouse

# ---------------------------------------------------------------------------
# [2/6] Reset Hive metastore (Derby)
# ---------------------------------------------------------------------------
echo ""
echo "--- [2/6] Resetting Hive metastore ---"
cd $HOME
rm -rf metastore_db derby.log
$HIVE_HOME/bin/schematool -dbType derby -initSchema 2>&1 | tail -3

# ---------------------------------------------------------------------------
# [3/6] Reset PostgreSQL database
# ---------------------------------------------------------------------------
echo ""
echo "--- [3/6] Resetting PostgreSQL database ---"
$PSQL -d postgres -c "DROP DATABASE IF EXISTS nasa_etl;"
$PSQL -d postgres -c "CREATE DATABASE nasa_etl OWNER nasa_user;"
$PSQL -d nasa_etl -f "$SCHEMA_SQL"

# ---------------------------------------------------------------------------
# [4/6] Create local data directories
# ---------------------------------------------------------------------------
echo ""
echo "--- [4/6] Creating local data directories ---"
mkdir -p "$BATCHES_DIR"
mkdir -p "$PLACEHOLDER"
mkdir -p /tmp/hive
chmod 1777 /tmp/hive
mkdir -p /home/rsvr_ind/hive_warehouse

# ---------------------------------------------------------------------------
# [5/6] Create Hive tables
# ---------------------------------------------------------------------------
echo ""
echo "--- [5/6] Creating Hive tables ---"
cd $HOME
$HIVE_HOME/bin/hive \
    --hiveconf hive.metastore.warehouse.dir=/home/rsvr_ind/hive_warehouse \
    --hivevar hive_db="$HIVE_DB" \
    --hivevar hdfs_placeholder="$PLACEHOLDER" \
    -f "$CREATE_HQL"

# ---------------------------------------------------------------------------
# [6/6] Run the pipeline
# ---------------------------------------------------------------------------
echo ""
echo "========================================================================"
echo "  Starting pipeline run"
echo "========================================================================"

RUN_ID="hive_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BATCH_DIR" "$LOCAL_OUTPUT"

# Split into batches
echo ""
echo "--- Splitting $LOG_FILE into batches of $BATCH_SIZE ---"
split -l "$BATCH_SIZE" "$LOG_FILE" "$BATCH_DIR/batch_"

TOTAL=$(wc -l < "$LOG_FILE")
NUM_BATCHES=$(ls "$BATCH_DIR/batch_"* | wc -l)
AVG=$(echo "scale=2; $TOTAL / $NUM_BATCHES" | bc)
echo "Total: $TOTAL  |  Batches: $NUM_BATCHES  |  Avg: $AVG"

# Register run in PostgreSQL
STARTED_AT="$(date '+%Y-%m-%d %H:%M:%S')"
$PSQL -d nasa_etl -c "
    INSERT INTO etl_runs(run_id,pipeline_name,batch_size,total_records,
                         total_batches,avg_batch_size,runtime_seconds,started_at)
    VALUES ('$RUN_ID','hive',$BATCH_SIZE,$TOTAL,$NUM_BATCHES,$AVG,0,'$STARTED_AT');"

# Process batches
RUNTIME_START=$(date +%s)
TOTAL_MALFORMED=0
batch_id=1

for batch_file in $(ls "$BATCH_DIR/batch_"* | sort); do
    BATCH_RECORDS=$(wc -l < "$batch_file")
    LOCAL_BATCH="$BATCHES_DIR/batch_${batch_id}"
    echo ""
    echo "=== Batch $batch_id / $NUM_BATCHES  ($BATCH_RECORDS records) | $(date '+%H:%M:%S') ==="

    # Copy batch file to local data dir (replaces hdfs dfs -put)
    mkdir -p "$LOCAL_BATCH"
    cp "$batch_file" "$LOCAL_BATCH/data.log"

    # Register batch start
    $PSQL -d nasa_etl -c "
        INSERT INTO batch_metadata(run_id,batch_id,pipeline_name,records_in_batch,batch_started_at)
        VALUES ('$RUN_ID',$batch_id,'hive',$BATCH_RECORDS,NOW())
        ON CONFLICT(run_id,batch_id) DO UPDATE SET batch_started_at=NOW();"

    # Hive ETL + 3 queries
    cd $HOME
    $HIVE_HOME/bin/hive \
        --hiveconf hive.metastore.warehouse.dir=/home/rsvr_ind/hive_warehouse \
        --hivevar hive_db="$HIVE_DB" \
        --hivevar batch_id=$batch_id \
        --hivevar hdfs_batch_path="$LOCAL_BATCH" \
        --hivevar local_output="$LOCAL_OUTPUT" \
        -f "$PROCESS_HQL"

    # Count malformed records
    MALFORMED=$(cd $HOME && $HIVE_HOME/bin/hive \
        --hiveconf hive.metastore.warehouse.dir=/home/rsvr_ind/hive_warehouse \
        -e "SET mapreduce.framework.name=local;
            SELECT COUNT(*) FROM nasa_logs.parsed_logs
            WHERE batch_id=${batch_id} AND is_malformed=1;" \
        2>/dev/null | grep "^[0-9]" | tail -1)
    MALFORMED="${MALFORMED:-0}"
    TOTAL_MALFORMED=$((TOTAL_MALFORMED + MALFORMED))
    echo "  Malformed in batch: $MALFORMED"

    # Load results into PostgreSQL
    python3 "$LOAD_SCRIPT" \
        --run-id "$RUN_ID" \
        --batch-id $batch_id \
        --records-in-batch $BATCH_RECORDS \
        --malformed-in-batch "$MALFORMED" \
        --output-dir "$LOCAL_OUTPUT" \
        --host /var/run/postgresql \
        --port 5432 --db nasa_etl --user nasa_user --password ""

    batch_id=$((batch_id + 1))
done

# Finalise run
RUNTIME=$(($(date +%s) - RUNTIME_START))
$PSQL -d nasa_etl -c "
    UPDATE etl_runs
    SET malformed_records=$TOTAL_MALFORMED,
        runtime_seconds=$RUNTIME,
        completed_at=NOW()
    WHERE run_id='$RUN_ID';"

echo ""
echo "========================================================================"
echo "  Pipeline complete"
echo "  Run ID   : $RUN_ID"
echo "  Runtime  : ${RUNTIME}s"
echo "  Malformed: $TOTAL_MALFORMED"
echo "========================================================================"
echo ""

# Print report
python3 "$REPORT_SCRIPT" \
    --run-id "$RUN_ID" \
    --host /var/run/postgresql \
    --port 5432 --db nasa_etl --user nasa_user --password ""
