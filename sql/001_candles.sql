CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS candles (
    symbol text NOT NULL,
    tf text NOT NULL,
    open_time timestamptz NOT NULL,
    open numeric NOT NULL,
    high numeric NOT NULL,
    low numeric NOT NULL,
    close numeric NOT NULL,
    tick_volume bigint NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, tf, open_time),
    CONSTRAINT candles_price_valid CHECK (
        low > 0
        AND low <= LEAST(open, close)
        AND high >= GREATEST(open, close)
    )
);

-- Split candles into monthly chucks by open_time (TimescaleDB)
SELECT create_hypertable(
    'candles', 'open_time',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

-- One row per ingest run (FR4)
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id bigserial PRIMARY KEY,
    mode text NOT NULL CHECK (mode IN ('incremental', 'full')),
    status text NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'success', 'partial', 'failed')),
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    files_ok int NOT NULL DEFAULT 0,
    files_failed int NOT NULL DEFAULT 0,
    rows_inserted bigint NOT NULL DEFAULT 0,
    rows_updated bigint NOT NULL DEFAULT 0
);

-- One row per file loaded in a run; also the watermark (file_mtime)
CREATE TABLE IF NOT EXISTS pipeline_run_items (
    run_id bigint NOT NULL REFERENCES pipeline_runs (run_id),
    file_path text NOT NULL,
    file_mtime timestamptz NOT NULL,
    symbol text NOT NULL,
    tf text NOT NULL,
    rows_read int NOT NULL DEFAULT 0,
    rows_inserted int NOT NULL DEFAULT 0,
    rows_updated int NOT NULL DEFAULT 0,
    status text NOT NULL CHECK (status IN ('success', 'failed')),
    error text,
    PRIMARY KEY (run_id, file_path)
);

-- Speeds up the watermark lookup: latest successful mtime per file
CREATE INDEX IF NOT EXISTS pipeline_run_items_watermark
    ON pipeline_run_items (file_path, file_mtime)
    WHERE status = 'success';