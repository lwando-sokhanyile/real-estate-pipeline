CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS monitoring.pipeline_runs (
    run_id UUID PRIMARY KEY,
    stage VARCHAR(50) NOT NULL,
    partition_name VARCHAR(100),
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    status VARCHAR(20) NOT NULL,
    rows_processed BIGINT,
    error_message TEXT
);
