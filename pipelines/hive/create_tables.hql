-- =============================================================================
-- Hive Table DDL
-- DAS 839 – NoSQL Systems | Hive Pipeline
--
-- Variables (passed via --hivevar):
--   hive_db        : Hive database name (e.g. nasa_logs)
--   hdfs_placeholder: HDFS path used as placeholder location for raw_input
-- =============================================================================

CREATE DATABASE IF NOT EXISTS ${hive_db};
USE ${hive_db};

-- ---------------------------------------------------------------------------
-- raw_input: external table pointing to one batch file at a time.
-- The orchestrator uses ALTER TABLE ... SET LOCATION before each batch.
-- EXTERNAL so that DROP TABLE never deletes HDFS data.
-- ---------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS raw_input (
    line STRING
)
STORED AS TEXTFILE
LOCATION 'file://${hdfs_placeholder}';

-- ---------------------------------------------------------------------------
-- parsed_logs: structured table partitioned by batch_id.
-- ORC + Snappy for efficient storage and query performance.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS parsed_logs;
CREATE TABLE parsed_logs (
    host              STRING,
    log_date          STRING,     -- YYYY-MM-DD
    log_hour          INT,        -- 0-23
    http_method       STRING,
    resource_path     STRING,
    protocol_version  STRING,
    status_code       INT,
    bytes_transferred BIGINT,
    is_malformed      INT         -- 1 = malformed line, 0 = valid
)
PARTITIONED BY (batch_id INT)
STORED AS ORC
TBLPROPERTIES ("orc.compress" = "SNAPPY");

-- ---------------------------------------------------------------------------
-- q1_result, q2_result, q3_result: Hive-side staging tables.
-- Partitioned by batch_id so each run's results are isolated.
-- Python loader reads these and pushes to MySQL.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS q1_result;
CREATE TABLE q1_result (
    log_date      STRING,
    status_code   INT,
    request_count BIGINT,
    total_bytes   BIGINT
)
PARTITIONED BY (batch_id INT)
STORED AS ORC;

DROP TABLE IF EXISTS q2_result;
CREATE TABLE q2_result (
    resource_path       STRING,
    request_count       BIGINT,
    total_bytes         BIGINT,
    distinct_host_count BIGINT
)
PARTITIONED BY (batch_id INT)
STORED AS ORC;

DROP TABLE IF EXISTS q3_result;
CREATE TABLE q3_result (
    log_date             STRING,
    log_hour             INT,
    error_request_count  BIGINT,
    total_request_count  BIGINT,
    error_rate           DOUBLE,
    distinct_error_hosts BIGINT
)
PARTITIONED BY (batch_id INT)
STORED AS ORC;
