from datetime import datetime, timezone
from alphago_data.candle_files import discover, read

def make_csv(root, symbol_dir, tf_dir, name, lines):
    folder = root / symbol_dir / tf_dir
    folder.mkdir(parents=True)
    path = folder / name
    path.write_text("time,open,high,low,close,tick_volume\n" + "\n".join(lines) + "\n")
    return path

def test_discover_maps_folders_and_skips_unknown(tmp_path):
    make_csv(tmp_path, "xauusdm", "tf_m_240", "2026-11.csv", [])
    make_csv(tmp_path, "oldpairm", "tf_m_60", "2026-11.csv", [])
    make_csv(tmp_path, "xauusdm", "tf_m_7", "2026-11.csv", [])

    files = discover(tmp_path, {"xauusdm": "XAUUSDm"})

    assert len(files) == 1
    assert files[0].symbol == "XAUUSDm"
    assert files[0].tf == "H4"
    assert files[0].mtime.tzinfo == timezone.utc

def test_read_gives_table_layout_in_utc(tmp_path):
    make_csv(tmp_path, "xauusdm", "tf_m_60", "2026-11.csv", [
        "2026.11.01 23:00:00,3885.185,3902.224,3882.915,3898.285,7640",
        "2026.11.02 00:00:00,3898.186,3903.781,3894.519,3901.705,10370",
    ])
    [file] = discover(tmp_path, {"xauusdm": "XAUUSDm"})

    df = read(file)

    assert df.columns == ["symbol", "tf", "open_time", "open", "high", "low", "close", "tick_volume"]
    assert df.height == 2
    assert df["symbol"][0] == "XAUUSDm"
    assert df["tf"][0] == "H1"
    assert df["open_time"][0] == datetime(2026,11,1,23,0,tzinfo=timezone.utc)