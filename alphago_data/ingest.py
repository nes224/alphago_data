"""Load candle CSV files into the candles table."""
import polars as pl
import psycopg
from alphago_data.candle_files import CandleFile, read

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

