-- PostgreSQL schema for NASA ETL reporting
\c nasa_etl

CREATE TABLE IF NOT EXISTS etl_runs (
    run_id            VARCHAR(120) PRIMARY KEY,
    pipeline_name     VARCHAR(50)  NOT NULL,
    batch_size        INT          NOT NULL,
    total_records     BIGINT       NOT NULL DEFAULT 0,
    malformed_records BIGINT       NOT NULL DEFAULT 0,
    total_batches     INT          NOT NULL DEFAULT 0,
    avg_batch_size    DOUBLE PRECISION NOT NULL DEFAULT 0,
    runtime_seconds   DOUBLE PRECISION NOT NULL DEFAULT 0,
    started_at        TIMESTAMP    NOT NULL,
    completed_at      TIMESTAMP
);

CREATE TABLE IF NOT EXISTS batch_metadata (
    run_id             VARCHAR(120) NOT NULL REFERENCES etl_runs(run_id) ON DELETE CASCADE,
    batch_id           INT          NOT NULL,
    pipeline_name      VARCHAR(50)  NOT NULL,
    records_in_batch   BIGINT       NOT NULL DEFAULT 0,
    malformed_in_batch BIGINT       NOT NULL DEFAULT 0,
    batch_started_at   TIMESTAMP,
    batch_ended_at     TIMESTAMP,
    PRIMARY KEY (run_id, batch_id)
);

CREATE TABLE IF NOT EXISTS q1_daily_traffic (
    id                BIGSERIAL PRIMARY KEY,
    run_id            VARCHAR(120) NOT NULL REFERENCES etl_runs(run_id) ON DELETE CASCADE,
    batch_id          INT          NOT NULL,
    pipeline_name     VARCHAR(50)  NOT NULL,
    time_of_execution TIMESTAMP    NOT NULL,
    log_date          VARCHAR(20),
    status_code       INT,
    request_count     BIGINT       NOT NULL DEFAULT 0,
    total_bytes       BIGINT       NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS q2_top_resources (
    id                  BIGSERIAL PRIMARY KEY,
    run_id              VARCHAR(120) NOT NULL REFERENCES etl_runs(run_id) ON DELETE CASCADE,
    batch_id            INT          NOT NULL,
    pipeline_name       VARCHAR(50)  NOT NULL,
    time_of_execution   TIMESTAMP    NOT NULL,
    resource_path       TEXT,
    request_count       BIGINT       NOT NULL DEFAULT 0,
    total_bytes         BIGINT       NOT NULL DEFAULT 0,
    distinct_host_count BIGINT       NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS q3_hourly_errors (
    id                   BIGSERIAL PRIMARY KEY,
    run_id               VARCHAR(120) NOT NULL REFERENCES etl_runs(run_id) ON DELETE CASCADE,
    batch_id             INT          NOT NULL,
    pipeline_name        VARCHAR(50)  NOT NULL,
    time_of_execution    TIMESTAMP    NOT NULL,
    log_date             VARCHAR(20),
    log_hour             INT,
    error_request_count  BIGINT       NOT NULL DEFAULT 0,
    total_request_count  BIGINT       NOT NULL DEFAULT 0,
    error_rate           DOUBLE PRECISION NOT NULL DEFAULT 0,
    distinct_error_hosts BIGINT       NOT NULL DEFAULT 0
);
