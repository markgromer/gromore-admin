import sqlite3, json

db = sqlite3.connect('data/database/gromore.db')
db.row_factory = sqlite3.Row

# All tables
tables = [r['name'] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]
print("=== ALL TABLES ===")
for t in tables:
    print(f"  {t}")

# Brands
print("\n=== BRANDS ===")
for r in db.execute("SELECT id, slug, display_name FROM brands").fetchall():
    print(dict(r))

# Hiring tables
for tbl in ['hiring_jobs', 'hiring_candidates', 'hiring_interviews', 'hiring_messages']:
    if tbl in tables:
        count = db.execute(f"SELECT count(*) as c FROM {tbl}").fetchone()['c']
        print(f"\n=== {tbl} ({count} rows) ===")
        for r in db.execute(f"SELECT * FROM {tbl} LIMIT 5").fetchall():
            print(dict(r))
    else:
        print(f"\n{tbl}: TABLE DOES NOT EXIST")

db.close()
