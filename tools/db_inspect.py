import os
import sqlite3
from pathlib import Path


def main() -> None:
    dbp = os.environ.get("DATABASE_PATH")
    if not dbp:
        dbp = str((Path.cwd() / "data" / "database" / "webapp.db").resolve())

    print("DATABASE_PATH =", dbp)
    if not Path(dbp).exists():
        print("ERROR: database file does not exist")
        return

    con = sqlite3.connect(dbp)
    cur = con.cursor()

    tables = [r[0] for r in cur.execute("select name from sqlite_master where type='table' order by name").fetchall()]
    print("table_count =", len(tables))
    print("tables =", tables)

    def has_table(name: str) -> bool:
        return bool(cur.execute("select 1 from sqlite_master where type='table' and name=?", (name,)).fetchone())

    for t in ["settings", "brands", "competitors", "competitor_intel", "connections"]:
        print(f"has_{t} =", has_table(t))

    openai_key = ""
    if has_table("settings"):
        row = cur.execute("select value from settings where key='openai_api_key'").fetchone()
        openai_key = (row[0] if row else "")
    print("openai_api_key =", "SET" if openai_key.strip() else "EMPTY")

    if has_table("brands"):
        brands_rows = cur.execute("select count(1) from brands").fetchone()[0]
        print("brands_rows =", brands_rows)
        sample = cur.execute(
            "select id, slug, display_name, competitors from brands order by id limit 3"
        ).fetchall()
        print("brands_sample =", [(r[0], r[1], r[2], (r[3] or "")[:60]) for r in sample])

    if has_table("competitor_intel"):
        total_intel_rows = cur.execute("select count(1) from competitor_intel").fetchone()[0]
        print("competitor_intel_rows =", total_intel_rows)

        by_type = cur.execute(
            "select intel_type, count(1) from competitor_intel group by intel_type order by intel_type"
        ).fetchall()
        print("competitor_intel_by_type =", [(r[0], r[1]) for r in by_type])

        research_rows = cur.execute(
            "select count(1) from competitor_intel where intel_type='research'"
        ).fetchone()[0]
        print("research_rows =", research_rows)

    if has_table("competitors"):
        competitor_rows = cur.execute("select count(1) from competitors").fetchone()[0]
        print("competitors_rows =", competitor_rows)
        sample = cur.execute(
            "select id, brand_id, name, website from competitors order by id limit 5"
        ).fetchall()
        print("competitors_sample =", [(r[0], r[1], r[2], r[3]) for r in sample])

    con.close()


if __name__ == "__main__":
    main()
