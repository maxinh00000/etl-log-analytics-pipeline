#!/usr/bin/env bash
# =============================================================================
# run_pipeline.sh  –  Hive ETL Pipeline
# DAS 839 – NoSQL Systems | Multi-Pipeline ETL for NASA HTTP Log Analytics
#
# This script orchestrates the complete Hive pipeline:
#   1. Combine and count input log files
#   2. Split into batches of BATCH_SIZE records
#   3. Upload each batch to HDFS
#   4. Run Hive ETL + 3 analytical queries per batch
#   5. Load query results into MySQL
#   6. Finalize run metadata in MySQL
#   7. Print report
#
# Usage:
#   ./run_pipeline.sh [OPTIONS]
#
# Options:
#   --batch-size N          Records per batch  (default: 100000)
#   --data-dir  /path       Directory with NASA log files  (default: ~/data/nasa)
#   --run-id    RUN_ID      Custom run identifier  (default: auto-generated)
#   --skip-setup            Skip HDFS/Hive table setup (tables already created)
#   --help
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.env"

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
SKIP_SETUP=false
RUN_ID=""

usage() {
    grep '^#' "$0" | grep -v '^#!/' | sed 's/^# \?//'
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --batch-size)  BATCH_SIZE="$2";  shift 2 ;;
        --data-dir)    DATA_DIR="$2";    shift 2 ;;
        --run-id)      RUN_ID="$2";      shift 2 ;;
        --skip-setup)  SKIP_SETUP=true;  shift   ;;
        --help|-h)     usage ;;
        *) echo "[ERROR] Unknown option: $1"; exit 1 ;;
    esac
done

RUN_ID="${RUN_ID:-hive_$(date +%Y%m%d_%H%M%S)}"

# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------
log()  { echo "[$(date '+%H:%M:%S')] $*"; }
die()  { echo "[ERROR] $*" >&2; exit 1; }

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------
require_cmd hive
require_cmd hdfs
require_cmd "${PYTHON}"

log "=== Hive ETL Pipeline ==="
log "Run ID      : ${RUN_ID}"
log "Batch Size  : ${BATCH_SIZE}"
log "Data Dir    : ${DATA_DIR}"

# ---------------------------------------------------------------------------
# Step 0 – MySQL setup (schema must already exist)
# ---------------------------------------------------------------------------
log "Initialising MySQL schema …"
mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" \
      ${MYSQL_PASSWORD:+-p"${MYSQL_PASSWORD}"} \
      < "${SCRIPT_DIR}/sql/schema.sql" || die "MySQL schema init failed"

# ---------------------------------------------------------------------------
# Step 1 – Prepare combined log file
# ---------------------------------------------------------------------------
COMBINED_LOG="/tmp/nasa_combined_${RUN_ID}.log"
BATCH_TMP_DIR="/tmp/nasa_batches_${RUN_ID}"

log "Combining log files from ${DATA_DIR} …"
mkdir -p "${BATCH_TMP_DIR}"

# Accept .gz or plain text NASA log files
shopt -s nullglob
LOG_FILES=("${DATA_DIR}"/*.log "${DATA_DIR}"/*_log_Jul95 "${DATA_DIR}"/*_log_Aug95
           "${DATA_DIR}"/NASA_access_log_Jul95 "${DATA_DIR}"/NASA_access_log_Aug95)

FOUND=0
> "${COMBINED_LOG}"   # create/truncate
for f in "${LOG_FILES[@]}"; do
    [[ -f "$f" ]] || continue
    if [[ "$f" == *.gz ]]; then
        zcat "$f" >> "${COMBINED_LOG}"
    else
        cat  "$f" >> "${COMBINED_LOG}"
    fi
    FOUND=$((FOUND + 1))
    log "  Added: $f"
done

[[ $FOUND -gt 0 ]] || die "No NASA log files found in ${DATA_DIR}. \
Download with: wget https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz"

TOTAL_RECORDS=$(wc -l < "${COMBINED_LOG}")
log "Total records in combined log: ${TOTAL_RECORDS}"

# ---------------------------------------------------------------------------
# Step 2 – Split into batches
# ---------------------------------------------------------------------------
log "Splitting into batches of ${BATCH_SIZE} records …"
split -l "${BATCH_SIZE}" "${COMBINED_LOG}" "${BATCH_TMP_DIR}/batch_"

BATCH_FILES=($(ls "${BATCH_TMP_DIR}"/batch_* 2>/dev/null | sort))
NUM_BATCHES="${#BATCH_FILES[@]}"
log "Created ${NUM_BATCHES} batch files"

# Average batch size = total_records / number of non-empty batches
NON_EMPTY_BATCHES=$(for f in "${BATCH_FILES[@]}"; do [[ -s "$f" ]] && echo 1; done | wc -l)
AVG_BATCH_SIZE=$(echo "scale=4; ${TOTAL_RECORDS} / ${NON_EMPTY_BATCHES}" | bc -l)

# ---------------------------------------------------------------------------
# Step 3 – HDFS and Hive table setup (once per run)
# ---------------------------------------------------------------------------
if [[ "${SKIP_SETUP}" == "false" ]]; then
    log "Setting up HDFS directories …"
    hdfs dfs -mkdir -p "${HDFS_BATCHES}"
    hdfs dfs -mkdir -p "${HDFS_PLACEHOLDER}"

    log "Creating Hive database and tables …"
    hive --hivevar hive_db="${HIVE_DB}" \
         --hivevar hdfs_placeholder="${HDFS_PLACEHOLDER}" \
         -f "${SCRIPT_DIR}/hive/create_tables.hql" \
        || die "Hive table creation failed"
fi

# ---------------------------------------------------------------------------
# Step 4 – Register run in MySQL (started_at, no completed_at yet)
# ---------------------------------------------------------------------------
STARTED_AT="$(date '+%Y-%m-%d %H:%M:%S')"

mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" \
      ${MYSQL_PASSWORD:+-p"${MYSQL_PASSWORD}"} "${MYSQL_DB}" \
      -e "INSERT INTO etl_runs
              (run_id, pipeline_name, batch_size, total_records,
               malformed_records, total_batches, avg_batch_size,
               runtime_seconds, started_at)
          VALUES
              ('${RUN_ID}', '${PIPELINE_NAME}', ${BATCH_SIZE}, ${TOTAL_RECORDS},
               0, ${NUM_BATCHES}, ${AVG_BATCH_SIZE}, 0, '${STARTED_AT}');" \
    || die "Failed to insert run metadata into MySQL"

# ---------------------------------------------------------------------------
# Step 5 – Process batches
# ---------------------------------------------------------------------------
# Runtime measurement starts here (not including setup above, per spec)
RUNTIME_START="$(date +%s%N)"

TOTAL_MALFORMED=0
mkdir -p "${LOCAL_OUTPUT}"

batch_id=1
for batch_file in "${BATCH_FILES[@]}"; do
    BATCH_RECORDS=$(wc -l < "${batch_file}")
    log "--- Batch ${batch_id}/${NUM_BATCHES}  (${BATCH_RECORDS} records) ---"

    HDFS_BATCH_PATH="${HDFS_BATCHES}/batch_${batch_id}"

    # 5a. Upload batch file to HDFS
    log "  Uploading to HDFS: ${HDFS_BATCH_PATH} …"
    hdfs dfs -mkdir -p "${HDFS_BATCH_PATH}"
    hdfs dfs -put -f "${batch_file}" "${HDFS_BATCH_PATH}/data.log"

    # 5b. Stamp batch start in MySQL
    mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" \
          ${MYSQL_PASSWORD:+-p"${MYSQL_PASSWORD}"} "${MYSQL_DB}" \
          -e "INSERT INTO batch_metadata
                  (run_id, batch_id, pipeline_name, records_in_batch,
                   malformed_in_batch, batch_started_at)
              VALUES
                  ('${RUN_ID}', ${batch_id}, '${PIPELINE_NAME}',
                   ${BATCH_RECORDS}, 0, NOW())
              ON DUPLICATE KEY UPDATE batch_started_at = NOW();" 2>/dev/null || true

    # 5c. Run Hive ETL + 3 queries
    log "  Running Hive ETL and queries …"
    hive \
        --hivevar hive_db="${HIVE_DB}" \
        --hivevar batch_id="${batch_id}" \
        --hivevar hdfs_batch_path="${HDFS_BATCH_PATH}" \
        --hivevar local_output="${LOCAL_OUTPUT}" \
        -f "${SCRIPT_DIR}/hive/process_batch.hql" \
        || die "Hive processing failed for batch ${batch_id}"

    # 5d. Count malformed records for this batch (Hive writes count to parsed_logs)
    MALFORMED=$(hive --hivevar hive_db="${HIVE_DB}" \
                     --hivevar batch_id="${batch_id}" \
                     -e "USE ${HIVE_DB};
                         SELECT COUNT(*) FROM parsed_logs
                         WHERE batch_id=${batch_id} AND is_malformed=1;" \
                2>/dev/null | tail -1)
    MALFORMED="${MALFORMED:-0}"
    TOTAL_MALFORMED=$((TOTAL_MALFORMED + MALFORMED))
    log "  Malformed records in batch ${batch_id}: ${MALFORMED}"

    # 5e. Load query results from local files into MySQL
    log "  Loading results into MySQL …"
    "${PYTHON}" "${SCRIPT_DIR}/scripts/load_to_mysql.py" \
        --run-id             "${RUN_ID}" \
        --batch-id           "${batch_id}" \
        --pipeline-name      "${PIPELINE_NAME}" \
        --records-in-batch   "${BATCH_RECORDS}" \
        --malformed-in-batch "${MALFORMED}" \
        --output-dir         "${LOCAL_OUTPUT}" \
        --host               "${MYSQL_HOST}" \
        --port               "${MYSQL_PORT}" \
        --db                 "${MYSQL_DB}" \
        --user               "${MYSQL_USER}" \
        --password           "${MYSQL_PASSWORD}" \
        || die "MySQL load failed for batch ${batch_id}"

    batch_id=$((batch_id + 1))
done

# ---------------------------------------------------------------------------
# Step 6 – Compute runtime and finalise run metadata
# ---------------------------------------------------------------------------
RUNTIME_END="$(date +%s%N)"
RUNTIME_SECONDS=$(echo "scale=6; (${RUNTIME_END} - ${RUNTIME_START}) / 1000000000" | bc -l)
COMPLETED_AT="$(date '+%Y-%m-%d %H:%M:%S')"

log "Total runtime: ${RUNTIME_SECONDS} seconds"

mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" \
      ${MYSQL_PASSWORD:+-p"${MYSQL_PASSWORD}"} "${MYSQL_DB}" \
      -e "UPDATE etl_runs SET
              malformed_records = ${TOTAL_MALFORMED},
              runtime_seconds   = ${RUNTIME_SECONDS},
              completed_at      = '${COMPLETED_AT}'
          WHERE run_id = '${RUN_ID}';" \
    || log "[WARN] Failed to update run completion metadata"

# ---------------------------------------------------------------------------
# Step 7 – Print report
# ---------------------------------------------------------------------------
log "Generating report …"
"${PYTHON}" "${SCRIPT_DIR}/scripts/report.py" \
    --run-id  "${RUN_ID}" \
    --host    "${MYSQL_HOST}" \
    --port    "${MYSQL_PORT}" \
    --db      "${MYSQL_DB}" \
    --user    "${MYSQL_USER}" \
    --password "${MYSQL_PASSWORD}"

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
log "Cleaning up temporary files …"
rm -f  "${COMBINED_LOG}"
rm -rf "${BATCH_TMP_DIR}"

log "=== Pipeline complete  |  Run ID: ${RUN_ID}  |  Runtime: ${RUNTIME_SECONDS}s ==="
