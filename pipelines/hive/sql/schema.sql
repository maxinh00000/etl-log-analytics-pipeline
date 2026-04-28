-- =============================================================================
-- MySQL Reporting Schema
-- DAS 839 – NoSQL Systems | Multi-Pipeline ETL for NASA HTTP Log Analytics
-- =============================================================================

CREATE DATABASE IF NOT EXISTS nasa_etl;
USE nasa_etl;

-- -----------------------------------------------------------------------
-- etl_runs: one row per pipeline invocation (run-level metadata)
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl_runs (
    run_id           VARCHAR(120) NOT NULL,
    pipeline_name    VARCHAR(50)  NOT NULL,
    batch_size       INT          NOT NULL,
    total_records    BIGINT       NOT NULL DEFAULT 0,
    malformed_records BIGINT      NOT NULL DEFAULT 0,
    total_batches    INT          NOT NULL DEFAULT 0,
    avg_batch_size   DOUBLE       NOT NULL DEFAULT 0,
    runtime_seconds  DOUBLE       NOT NULL DEFAULT 0,
    started_at       DATETIME     NOT NULL,
    completed_at     DATETIME,
    PRIMARY KEY (run_id)
) ENGINE=InnoDB;

-- -----------------------------------------------------------------------
-- batch_metadata: one row per (run, batch)
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS batch_metadata (
    run_id            VARCHAR(120) NOT NULL,
    batch_id          INT          NOT NULL,
    pipeline_name     VARCHAR(50)  NOT NULL,
    records_in_batch  BIGINT       NOT NULL DEFAULT 0,
    malformed_in_batch BIGINT      NOT NULL DEFAULT 0,
    batch_started_at  DATETIME,
    batch_ended_at    DATETIME,
    PRIMARY KEY (run_id, batch_id),
    FOREIGN KEY (run_id) REFERENCES etl_runs(run_id) ON DELETE CASCADE
) ENGINE=InnoDB;

-- -----------------------------------------------------------------------
-- Query 1: Daily Traffic Summary
-- Output: log_date, status_code, request_count, total_bytes
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS q1_daily_traffic (
    id               BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id           VARCHAR(120) NOT NULL,
    batch_id         INT          NOT NULL,
    pipeline_name    VARCHAR(50)  NOT NULL,
    time_of_execution DATETIME    NOT NULL,
    log_date         VARCHAR(20),
    status_code      INT,
    request_count    BIGINT       NOT NULL DEFAULT 0,
    total_bytes      BIGINT       NOT NULL DEFAULT 0,
    INDEX idx_run_batch (run_id, batch_id),
    FOREIGN KEY (run_id) REFERENCES etl_runs(run_id) ON DELETE CASCADE
) ENGINE=InnoDB;

-- -----------------------------------------------------------------------
-- Query 2: Top 20 Requested Resources
-- Output: resource_path, request_count, total_bytes, distinct_host_count
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS q2_top_resources (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id              VARCHAR(120) NOT NULL,
    batch_id            INT          NOT NULL,
    pipeline_name       VARCHAR(50)  NOT NULL,
    time_of_execution   DATETIME     NOT NULL,
    resource_path       VARCHAR(2048),
    request_count       BIGINT       NOT NULL DEFAULT 0,
    total_bytes         BIGINT       NOT NULL DEFAULT 0,
    distinct_host_count BIGINT       NOT NULL DEFAULT 0,
    INDEX idx_run_batch (run_id, batch_id),
    FOREIGN KEY (run_id) REFERENCES etl_runs(run_id) ON DELETE CASCADE
) ENGINE=InnoDB;

-- -----------------------------------------------------------------------
-- Query 3: Hourly Error Analysis
-- Output: log_date, log_hour, error_request_count, total_request_count,
--         error_rate, distinct_error_hosts
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS q3_hourly_errors (
    id                   BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id               VARCHAR(120) NOT NULL,
    batch_id             INT          NOT NULL,
    pipeline_name        VARCHAR(50)  NOT NULL,
    time_of_execution    DATETIME     NOT NULL,
    log_date             VARCHAR(20),
    log_hour             INT,
    error_request_count  BIGINT       NOT NULL DEFAULT 0,
    total_request_count  BIGINT       NOT NULL DEFAULT 0,
    error_rate           DOUBLE       NOT NULL DEFAULT 0,
    distinct_error_hosts BIGINT       NOT NULL DEFAULT 0,
    INDEX idx_run_batch (run_id, batch_id),
    FOREIGN KEY (run_id) REFERENCES etl_runs(run_id) ON DELETE CASCADE
) ENGINE=InnoDB;
