import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import polars as pl

CSV_SCHEMA = {
    "time": pl.String,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "tick_volume": pl.Int64,
}

TF_BY_FOLDER = {
    "tf_m_1": "M1",
    "tf_m_5": "M5",
    "tf_m_15": "M15",
    "tf_m_30": "M30",
    "tf_m_60": "H1",
    "tf_m_240": "H4",
    "tf_m_1440": "D1",
}

@dataclass(frozen=True)
class CandleFile:
    path: Path
    symbol: str
    tf: str
    mtime: datetime

def symbol_names(symbols_json: Path) -> dict[str, str]:
    names = json.loads(symbols_json.read_text())
    return {name.lower(): name for name in names}

def discover(data_dir: Path, symbols: dict[str, str]) -> list[CandleFile]:
    files = []
    for path in sorted(data_dir.glob("*/*/*.csv")):
        symbol_dir, tf_dir = path.parent.parent.name, path.parent.name
        symbol = symbols.get(symbol_dir)
        tf = TF_BY_FOLDER.get(tf_dir)
        if symbol is None or tf is None:
            continue

        mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        files.append(CandleFile(path=path, symbol=symbol, tf=tf, mtime=mtime))

    return files

def read(file: CandleFile) -> pl.DataFrame:
    df = pl.read_csv(file.path, schema=CSV_SCHEMA)
    return df.select(
        pl.lit(file.symbol).alias("symbol"),
        pl.lit(file.tf).alias("tf"),
        pl.col("time")
        .str.to_datetime("%Y.%m.%d %H:%M:%S", time_unit="us")
        .dt.replace_time_zone("UTC")
        .alias("open_time"),
        "open", "high", "low", "close", "tick_volume",
    )
