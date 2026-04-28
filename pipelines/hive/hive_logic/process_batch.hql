-- =============================================================================
-- Hive Batch Processing: ETL + 3 Mandatory Queries
-- DAS 839 – NoSQL Systems | Hive Pipeline
--
-- Variables (passed via --hivevar):
--   hive_db         : Hive database name
--   batch_id        : Integer batch identifier (1-based)
--   hdfs_batch_path : HDFS path containing this batch's raw log file(s)
--   local_output    : Local filesystem path to write query result CSVs
--
-- NASA HTTP log line format:
--   host - - [DD/Mon/YYYY:HH:MM:SS ±HHMM] "METHOD /path HTTP/ver" STATUS BYTES
-- =============================================================================

-- Run in local mode: bypasses YARN, executes in the Hive client JVM (Java 8)
SET mapreduce.framework.name               = local;
SET hive.exec.mode.local.auto              = true;
SET hive.exec.mode.local.auto.inputbytes.max = 5368709120;
SET mapreduce.map.memory.mb                = 4096;
SET mapreduce.reduce.memory.mb             = 4096;
SET mapreduce.map.java.opts                = -Xmx3g;
SET mapreduce.reduce.java.opts             = -Xmx3g;

SET hive.exec.dynamic.partition       = true;
SET hive.exec.dynamic.partition.mode  = nonstrict;
SET hive.exec.max.dynamic.partitions  = 10000;
SET hive.exec.compress.output         = false;

USE ${hive_db};

-- ---------------------------------------------------------------------------
-- Step 1 – Point raw_input at this batch's HDFS directory
-- ---------------------------------------------------------------------------
ALTER TABLE raw_input SET LOCATION 'file://${hdfs_batch_path}';

-- ---------------------------------------------------------------------------
-- Step 2 – ETL: parse raw lines → structured parsed_logs partition
--
-- Regex groups used for timestamp extraction (single pattern, 4 groups):
--   Pattern : \[(\d{2})/(\w{3})/(\d{4}):(\d{2}):\d{2}:\d{2}
--   Group 1 : day (DD)
--   Group 2 : month abbreviation (Jan…Dec)
--   Group 3 : year (YYYY)
--   Group 4 : hour (HH)
--
-- Malformed detection: line does NOT match the expected overall structure.
-- Malformed records are kept in the table (is_malformed=1) so the count
-- can be reported; they are filtered out in the analytical queries.
-- ---------------------------------------------------------------------------
INSERT OVERWRITE TABLE parsed_logs PARTITION (batch_id = ${batch_id})
SELECT
    t.host,
    -- Build ISO date YYYY-MM-DD from the three extracted groups
    CASE
        WHEN t.yr != '' AND t.mon != '' AND t.dy != ''
        THEN CONCAT(t.yr, '-',
            CASE t.mon
                WHEN 'Jan' THEN '01' WHEN 'Feb' THEN '02' WHEN 'Mar' THEN '03'
                WHEN 'Apr' THEN '04' WHEN 'May' THEN '05' WHEN 'Jun' THEN '06'
                WHEN 'Jul' THEN '07' WHEN 'Aug' THEN '08' WHEN 'Sep' THEN '09'
                WHEN 'Oct' THEN '10' WHEN 'Nov' THEN '11' WHEN 'Dec' THEN '12'
                ELSE '00'
            END,
            '-', t.dy)
        ELSE NULL
    END                                                     AS log_date,
    CAST(t.hr AS INT)                                       AS log_hour,
    t.http_method,
    t.resource_path,
    NULLIF(t.protocol_version, '')                          AS protocol_version,
    CASE
        WHEN t.status_str RLIKE '^[0-9]+$' THEN CAST(t.status_str AS INT)
        ELSE NULL
    END                                                     AS status_code,
    CASE
        WHEN t.bytes_str IS NULL OR t.bytes_str = '' OR t.bytes_str = '-' THEN CAST(0 AS BIGINT)
        WHEN t.bytes_str RLIKE '^[0-9]+$'                                 THEN CAST(t.bytes_str AS BIGINT)
        ELSE CAST(0 AS BIGINT)
    END                                                     AS bytes_transferred,
    t.is_malformed
FROM (
    SELECT
        -- host: first non-space token
        regexp_extract(line, '^(\\S+)', 1)                                              AS host,

        -- timestamp day/month/year/hour via one pattern, four groups
        regexp_extract(line, '\\[(\\d{2})/(\\w{3})/(\\d{4}):(\\d{2}):\\d{2}:\\d{2}', 1) AS dy,
        regexp_extract(line, '\\[(\\d{2})/(\\w{3})/(\\d{4}):(\\d{2}):\\d{2}:\\d{2}', 2) AS mon,
        regexp_extract(line, '\\[(\\d{2})/(\\w{3})/(\\d{4}):(\\d{2}):\\d{2}:\\d{2}', 3) AS yr,
        regexp_extract(line, '\\[(\\d{2})/(\\w{3})/(\\d{4}):(\\d{2}):\\d{2}:\\d{2}', 4) AS hr,

        -- HTTP method: first word inside quotes
        regexp_extract(line, '"([A-Z]+)\\s',   1)   AS http_method,
        -- resource path: token after method, stops at whitespace or closing quote
        regexp_extract(line, '"[A-Z]+\\s+([^\\s"]+)', 1) AS resource_path,
        -- protocol version: optional third token
        regexp_extract(line, '"[A-Z]+\\s+\\S+\\s+(HTTP/[\\d.]+)"', 1) AS protocol_version,

        -- status code and bytes: two tokens after the closing quote
        regexp_extract(line, '"[^"]*"\\s+(\\S+)\\s+(\\S+)\\s*$', 1) AS status_str,
        regexp_extract(line, '"[^"]*"\\s+(\\S+)\\s+(\\S+)\\s*$', 2) AS bytes_str,

        -- malformed flag: line must broadly match the CLF structure
        CASE
            WHEN NOT (line RLIKE '^\\S+ \\S+ \\S+ \\[[^\\]]+\\] "[^"]*" \\S+ \\S+')
            THEN 1
            ELSE 0
        END AS is_malformed

    FROM raw_input
    WHERE line IS NOT NULL AND TRIM(line) != ''
) t;

-- ---------------------------------------------------------------------------
-- Step 3 – Query 1: Daily Traffic Summary
-- For each log_date and status_code: total request count and total bytes.
-- Only valid (non-malformed) records with a parseable status_code are included.
-- ---------------------------------------------------------------------------
INSERT OVERWRITE TABLE q1_result PARTITION (batch_id = ${batch_id})
SELECT
    COALESCE(log_date, 'UNKNOWN')   AS log_date,
    status_code,
    COUNT(*)                        AS request_count,
    COALESCE(SUM(bytes_transferred), 0) AS total_bytes
FROM parsed_logs
WHERE batch_id = ${batch_id}
  AND is_malformed = 0
  AND status_code IS NOT NULL
GROUP BY log_date, status_code;

-- Export Query 1 result to local filesystem (tab-separated, one file per batch)
INSERT OVERWRITE LOCAL DIRECTORY '${local_output}/q1/batch_${batch_id}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT
    COALESCE(log_date, 'UNKNOWN'),
    COALESCE(CAST(status_code AS STRING), 'NULL'),
    CAST(request_count AS STRING),
    CAST(total_bytes AS STRING)
FROM q1_result
WHERE batch_id = ${batch_id};

-- ---------------------------------------------------------------------------
-- Step 4 – Query 2: Top 20 Requested Resources
-- Rank resource paths by request count; report count, bytes, distinct hosts.
-- ---------------------------------------------------------------------------
INSERT OVERWRITE TABLE q2_result PARTITION (batch_id = ${batch_id})
SELECT
    resource_path,
    COUNT(*)                            AS request_count,
    COALESCE(SUM(bytes_transferred), 0) AS total_bytes,
    COUNT(DISTINCT host)                AS distinct_host_count
FROM parsed_logs
WHERE batch_id = ${batch_id}
  AND is_malformed = 0
  AND resource_path IS NOT NULL
  AND resource_path != ''
GROUP BY resource_path
ORDER BY request_count DESC
LIMIT 20;

-- Export Query 2 result
INSERT OVERWRITE LOCAL DIRECTORY '${local_output}/q2/batch_${batch_id}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT
    COALESCE(resource_path, 'UNKNOWN'),
    CAST(request_count AS STRING),
    CAST(total_bytes AS STRING),
    CAST(distinct_host_count AS STRING)
FROM q2_result
WHERE batch_id = ${batch_id};

-- ---------------------------------------------------------------------------
-- Step 5 – Query 3: Hourly Error Analysis
-- For each log_date and log_hour: error requests (status 400–599),
-- total requests, error rate (errors / total), distinct hosts with errors.
-- ---------------------------------------------------------------------------
INSERT OVERWRITE TABLE q3_result PARTITION (batch_id = ${batch_id})
SELECT
    COALESCE(log_date, 'UNKNOWN')    AS log_date,
    log_hour,
    SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END) AS error_request_count,
    COUNT(*)                                                           AS total_request_count,
    ROUND(
        SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END)
        / COUNT(*), 6)                                                 AS error_rate,
    COUNT(DISTINCT CASE WHEN status_code BETWEEN 400 AND 599 THEN host ELSE NULL END)
                                                                       AS distinct_error_hosts
FROM parsed_logs
WHERE batch_id = ${batch_id}
  AND is_malformed = 0
  AND log_date IS NOT NULL
  AND log_hour IS NOT NULL
GROUP BY log_date, log_hour;

-- Export Query 3 result
INSERT OVERWRITE LOCAL DIRECTORY '${local_output}/q3/batch_${batch_id}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT
    COALESCE(log_date, 'UNKNOWN'),
    COALESCE(CAST(log_hour AS STRING), 'NULL'),
    CAST(error_request_count AS STRING),
    CAST(total_request_count AS STRING),
    CAST(error_rate AS STRING),
    CAST(distinct_error_hosts AS STRING)
FROM q3_result
WHERE batch_id = ${batch_id};
