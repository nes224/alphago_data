"""Load candle CSV files into the candles table."""
import argparse
import os
import sys
from pathlib import Path

import polars as pl
import psycopg

from alphago_data.candle_files import CandleFile, discover, read, symbol_names
from alphago_data.db import connect, migrate

LOCK_ID = 224001

UPSERT_SQL = """
INSERT INTO candles (symbol, tf, open_time, open, high, low, close, tick_volume)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (symbol, tf, open_time) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    tick_volume = EXCLUDED.tick_volume,
    loaded_at = now()
"""

COUNT_SQL = "SELECT count(*) FROM candles WHERE symbol = %s AND tf = %s"

ITEM_SQL = """
INSERT INTO pipeline_run_items
    (run_id, file_path, file_mtime, symbol, tf,
    rows_read, rows_inserted, rows_updated, status, error)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

FINISH_SQL = """
UPDATE pipeline_runs
SET STATUS = %s, finished_at = now(),
    files_ok = %s, files_failed = %s, rows_inserted = %s, rows_updated = %s
WHERE run_id = %s
"""

WATERMARK_SQL = """
SELECT file_path, max(file_mtime) FROM pipeline_run_items WHERE status = 'success' GROUP BY file_path
"""

def changed(conn: psycopg.Connection, files: list[CandleFile]) -> list[CandleFile]:
    loaded = dict(conn.execute(WATERMARK_SQL).fetchall());
    return [
        f for f in files
        if str(f.path) not in loaded or f.mtime > loaded[str(f.path)]
    ]

def upsert(conn: psycopg.Connection, df: pl.DataFrame) -> tuple[int, int]:
    if df.height == 0:
        return 0, 0
    key = (df["symbol"][0], df["tf"][0])
    before = conn.execute(COUNT_SQL, key).fetchone()[0]
    with conn.cursor() as cur:
        cur.executemany(UPSERT_SQL, df.iter_rows())
    after = conn.execute(COUNT_SQL, key).fetchone()[0]
    inserted = after - before
    return inserted, df.height - inserted

def start_run(conn: psycopg.Connection, mode: str) -> int:
    with conn.transaction():
        row = conn.execute(
            "INSERT INTO pipeline_runs (mode) VALUES (%s) RETURNING run_id", (mode,)
        ).fetchone()
    return row[0]

def load_file(conn: psycopg.Connection, run_id: int, file: CandleFile) -> tuple[int, int]:
    try:
        df = read(file)
        with conn.transaction():
            inserted, updated = upsert(conn, df)
            conn.execute(ITEM_SQL, (run_id, str(file.path), file.mtime, file.symbol, file.tf,
                                    df.height, inserted, updated, "success", None))
        return inserted, updated
    except Exception as err:
        with conn.transaction():
            conn.execute(ITEM_SQL, (run_id, str(file.path), file.mtime, file.symbol, file.tf, 0, 0, 0, "failed", str(err)))
        raise

def run(conn: psycopg.Connection, files: list[CandleFile], mode: str) -> str:
    """Load every file, one transaction each. A failed file does not stop the run."""
    run_id = start_run(conn, mode)
    print(f"run {run_id} {mode}: {len(files)} files")

    ok = failed = inserted = updated = 0
    for file in files:
        try:
            ins, upd = load_file(conn, run_id, file)
        except Exception as err:
            failed += 1
            print(f" FAILED {file.path}: {err}")
            continue
        ok += 1
        inserted += ins
        updated += upd

    status = "success" if failed == 0 else "partial"
    with conn.transaction():
        conn.execute(FINISH_SQL, (status, ok, failed, inserted, updated, run_id))
    print(f"run {run_id} {status}: files ok={ok} failed={failed}"
          f"inserted={inserted} uodated={updated}")
    return status

def main() -> int:
    parser = argparse.ArgumentParser(
        prog="python -m alphago_data.ingest",
        description="Load gateway candle CSV files into Postgres."
    )
    parser.add_argument("--full", action="store_true", help="load every file, not onlu changed ones")
    parser.add_argument("--symbol", help="only this symbol, e.g. XAUUSDm")
    parser.add_argument("--tf", help="only this timeframe, e.g. H1")
    args = parser.parse_args();

    url = os.environ.get("DATABASE_URL");
    data_dir = os.environ.get("CANDLE_DATA_DIR");
    if not url or not data_dir:
        print("DATABASE_URL and CANDLE_DATA_DIR must be set", file=sys.stderr);
        return 2


    candle_dir = Path(data_dir);
    files = discover(candle_dir, symbol_names(candle_dir.parent / "symbols.json"));
    files = [
        f for f in files
        if (args.symbol is None or f.symbol == args.symbol)
        and (args.tf is None or f.tf == args.tf)
    ];

    try:
        conn = connect(url);
    except psycopg.OperationalError as err:
        print(f"cannot connect to database: {err}", file=sys.stderr);
        return 2
    with conn:
        got_lock = conn.execute("SELECT pg_try_advisory_lock(%s)", (LOCK_ID,)).fetchone()[0];
        if not got_lock:
            print("another ingest run is in progress", file=sys.stderr);
            return 2
        applied = migrate(conn);
        if applied:
            print("migrated:", ", ".join(applied));
        status = run(conn, files, "full");
    return 0 if status == "success" else 1

if __name__ == "__main__":
    sys.exit(main());

    