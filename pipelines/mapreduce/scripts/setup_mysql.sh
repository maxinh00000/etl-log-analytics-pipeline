#!/usr/bin/env bash
# setup_mysql.sh — One-time setup for MapReduce pipeline MySQL database
# DAS 839 – NoSQL Systems
#
# Usage:
#   bash setup_mysql.sh                        # defaults: root user, empty password
#   bash setup_mysql.sh myuser mypassword      # custom credentials
#
# What it does:
#   1. Creates the nasa_etl database if it doesn't exist
#   2. Creates all 5 reporting tables (matches Hive pipeline schema)
#   3. Prints a success message

MYSQL_USER="${1:-root}"
MYSQL_PASS="${2:-}"

MYSQL_CMD="mysql -u${MYSQL_USER}"
if [ -n "$MYSQL_PASS" ]; then
    MYSQL_CMD="$MYSQL_CMD -p${MYSQL_PASS}"
fi

echo ""
echo "══════════════════════════════════════════════════════════"
echo "  MapReduce Pipeline — MySQL Setup"
echo "  User: $MYSQL_USER"
echo "══════════════════════════════════════════════════════════"
echo ""

# Create DB
echo "--- Creating database nasa_etl (if not exists) ---"
$MYSQL_CMD -e "CREATE DATABASE IF NOT EXISTS nasa_etl CHARACTER SET utf8mb4;"
echo "✓ Database ready"

# Create tables
echo ""
echo "--- Creating tables ---"
$MYSQL_CMD nasa_etl << 'SQL'

CREATE TABLE IF NOT EXISTS etl_runs (
    run_id            VARCHAR(120) PRIMARY KEY,
    pipeline_name     VARCHAR(50)  NOT NULL,
    batch_size        INT          NOT NULL,
    total_records     BIGINT       NOT NULL DEFAULT 0,
    malformed_records BIGINT       NOT NULL DEFAULT 0,
    total_batches     INT          NOT NULL DEFAULT 0,
    avg_batch_size    DOUBLE       NOT NULL DEFAULT 0,
    runtime_seconds   DOUBLE       NOT NULL DEFAULT 0,
    started_at        DATETIME     NOT NULL,
    completed_at      DATETIME
);

CREATE TABLE IF NOT EXISTS batch_metadata (
    run_id             VARCHAR(120) NOT NULL,
    batch_id           INT          NOT NULL,
    pipeline_name      VARCHAR(50)  NOT NULL,
    records_in_batch   BIGINT       NOT NULL DEFAULT 0,
    malformed_in_batch BIGINT       NOT NULL DEFAULT 0,
    batch_started_at   DATETIME,
    batch_ended_at     DATETIME,
    PRIMARY KEY (run_id, batch_id)
);

CREATE TABLE IF NOT EXISTS q1_daily_traffic (
    id                BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id            VARCHAR(120) NOT NULL,
    batch_id          INT          NOT NULL,
    pipeline_name     VARCHAR(50)  NOT NULL,
    time_of_execution DATETIME     NOT NULL,
    log_date          VARCHAR(30),
    status_code       INT,
    request_count     BIGINT       NOT NULL DEFAULT 0,
    total_bytes       BIGINT       NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS q2_top_resources (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id              VARCHAR(120) NOT NULL,
    batch_id            INT          NOT NULL,
    pipeline_name       VARCHAR(50)  NOT NULL,
    time_of_execution   DATETIME     NOT NULL,
    resource_path       TEXT,
    request_count       BIGINT       NOT NULL DEFAULT 0,
    total_bytes         BIGINT       NOT NULL DEFAULT 0,
    distinct_host_count BIGINT       NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS q3_hourly_errors (
    id                   BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id               VARCHAR(120) NOT NULL,
    batch_id             INT          NOT NULL,
    pipeline_name        VARCHAR(50)  NOT NULL,
    time_of_execution    DATETIME     NOT NULL,
    log_date             VARCHAR(30),
    log_hour             INT,
    error_request_count  BIGINT       NOT NULL DEFAULT 0,
    total_request_count  BIGINT       NOT NULL DEFAULT 0,
    error_rate           DOUBLE       NOT NULL DEFAULT 0,
    distinct_error_hosts BIGINT       NOT NULL DEFAULT 0
);

SQL

echo "✓ Tables created"
echo ""
echo "Setup complete! Tables in nasa_etl:"
$MYSQL_CMD nasa_etl -e "SHOW TABLES;"
echo ""
echo "You can now run the pipeline:"
echo "  python3 pipelines/mapreduce/mr_runner.py \\"
echo "      --log-file /path/to/NASA_access_log_Jul95 \\"
echo "      --batch-size 500000 \\"
echo "      --mysql-user $MYSQL_USER"
echo ""
