-- =====================================================
-- PostgreSQL Schema
-- DAS 839 – NoSQL Systems
-- Unified ETL Reporting Framework
-- =====================================================

CREATE TABLE IF NOT EXISTS run_metadata (

    run_id             VARCHAR(64) PRIMARY KEY,

    pipeline_name      VARCHAR(20) NOT NULL,

    query_name         VARCHAR(10) NOT NULL,

    batch_size         INTEGER NOT NULL,

    avg_batch_size     NUMERIC(12,2),

    total_records      BIGINT NOT NULL DEFAULT 0,

    malformed_records  BIGINT NOT NULL DEFAULT 0,

    runtime_seconds    NUMERIC(12,3),

    executed_at        TIMESTAMP DEFAULT NOW()
);


-- =====================================================
-- BATCH METADATA
-- =====================================================

CREATE TABLE IF NOT EXISTS batch_metadata (

    id                  SERIAL PRIMARY KEY,

    run_id              VARCHAR(64)
                        REFERENCES run_metadata(run_id)
                        ON DELETE CASCADE,

    batch_id            INTEGER NOT NULL,

    batch_size          INTEGER,

    records_processed   INTEGER,

    malformed_records   INTEGER,

    UNIQUE(run_id, batch_id)
);


-- =====================================================
-- QUERY 1 RESULTS
-- =====================================================

CREATE TABLE IF NOT EXISTS q1_daily_traffic (

    id                BIGSERIAL PRIMARY KEY,

    run_id            VARCHAR(64)
                      REFERENCES run_metadata(run_id)
                      ON DELETE CASCADE,

    batch_id          INTEGER,

    log_date          VARCHAR(20),

    status_code       INTEGER,

    request_count     BIGINT DEFAULT 0,

    total_bytes       BIGINT DEFAULT 0
);


-- =====================================================
-- QUERY 2 RESULTS
-- =====================================================

CREATE TABLE IF NOT EXISTS q2_top_resources (

    id                    BIGSERIAL PRIMARY KEY,

    run_id                VARCHAR(64)
                          REFERENCES run_metadata(run_id)
                          ON DELETE CASCADE,

    batch_id              INTEGER,

    resource_path         TEXT,

    request_count         BIGINT DEFAULT 0,

    total_bytes           BIGINT DEFAULT 0,

    distinct_host_count   BIGINT DEFAULT 0
);


-- =====================================================
-- QUERY 3 RESULTS
-- =====================================================

CREATE TABLE IF NOT EXISTS q3_hourly_errors (

    id                     BIGSERIAL PRIMARY KEY,

    run_id                 VARCHAR(64)
                           REFERENCES run_metadata(run_id)
                           ON DELETE CASCADE,

    batch_id               INTEGER,

    log_date               VARCHAR(20),

    log_hour               INTEGER,

    error_request_count    BIGINT DEFAULT 0,

    total_request_count    BIGINT DEFAULT 0,

    error_rate             DOUBLE PRECISION DEFAULT 0,

    distinct_error_hosts   BIGINT DEFAULT 0
);