import os
import sys
from pathlib import Path
import psycopg

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"

def connect(url: str) -> psycopg.Connection:
    return psycopg.connect(url, autocommit=True)

def migrate(conn: psycopg.Connection) -> list[str]:
    with conn.transaction():
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version text PRIMARY KEY,
                applied_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )

    applied = {row[0] for row in conn.execute("SELECT version FROM schema_migrations")}

    ran = []
    for path in sorted(SQL_DIR.glob("*.sql")):
        if path.name in applied:
            continue
        with conn.transaction():
            conn.execute(path.read_text())
            conn.execute(
                "INSERT INTO schema_migrations (version) VALUES (%s)", (path.name,)
            )
        ran.append(path.name)
    return ran

def main() -> int:
    if sys.argv[1:] != ["migrate"]:
        print("usage: python -m alphago_data.db migrate", file=sys.stderr)
        return 2
    url = os.environ.get("DATABASE_URL")
    if not url:
        print("DATABSE_URL is not set", file=sys.stderr)
        return 2
    with connect(url) as conn:
        ran = migrate(conn)
    print("appled: " + ", ".join(ran) if ran else "nothing to apply")
    return 0

if __name__ == "__main__":
    sys.exit(main())