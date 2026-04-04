import sqlite3
import sys
sys.path.insert(0, ".")
from webapp.database import WebDB
db = WebDB("data/database/gromore.db")
db.init()
conn = sqlite3.connect("data/database/gromore.db")
cols = [r[1] for r in conn.execute("PRAGMA table_info(hiring_candidates)").fetchall()]
print("Columns:", cols)
print("social_scan present:", "social_scan" in cols)
conn.close()
