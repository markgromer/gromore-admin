"""
Database layer - SQLite storage for historical month-over-month tracking.
"""
import sqlite3
import json
import os
from pathlib import Path

# On Render (persistent disk at /data), store agency.db alongside webapp.db.
# Locally, use the project-relative data directory.
_on_render = os.environ.get("RENDER") or os.path.isdir("/data")
if _on_render:
    DB_PATH = Path("/data/database/agency.db")
else:
    DB_PATH = Path(__file__).parent.parent / "data" / "database" / "agency.db"


def get_connection():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_connection()
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS monthly_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id TEXT NOT NULL,
            month TEXT NOT NULL,
            source TEXT NOT NULL,
            data_json TEXT NOT NULL,
            imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(client_id, month, source)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS monthly_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id TEXT NOT NULL,
            month TEXT NOT NULL,
            summary_json TEXT NOT NULL,
            suggestions_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(client_id, month)
        )
    """)

    conn.commit()
    conn.close()


def store_monthly_data(client_id, month, source, data_dict):
    """Store parsed data for a specific client/month/source."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        INSERT OR REPLACE INTO monthly_data (client_id, month, source, data_json)
        VALUES (?, ?, ?, ?)
    """, (client_id, month, source, json.dumps(data_dict, default=str)))
    conn.commit()
    conn.close()


def get_monthly_data(client_id, month, source=None):
    """Retrieve stored data for a client/month, optionally filtered by source."""
    conn = get_connection()
    c = conn.cursor()
    if source:
        c.execute(
            "SELECT * FROM monthly_data WHERE client_id=? AND month=? AND source=?",
            (client_id, month, source)
        )
    else:
        c.execute(
            "SELECT * FROM monthly_data WHERE client_id=? AND month=?",
            (client_id, month)
        )
    rows = c.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_previous_month(month_str):
    """Given '2026-03', return '2026-02'."""
    year, month = map(int, month_str.split("-"))
    if month == 1:
        return f"{year - 1}-12"
    return f"{year}-{month - 1:02d}"


def get_historical_data(client_id, source, months_back=6):
    """Get data for the last N months for trend analysis."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        SELECT month, data_json FROM monthly_data
        WHERE client_id=? AND source=?
        ORDER BY month DESC
        LIMIT ?
    """, (client_id, source, months_back))
    rows = c.fetchall()
    conn.close()
    return [(r["month"], json.loads(r["data_json"])) for r in rows]


def store_monthly_summary(client_id, month, summary_dict, suggestions_list=None):
    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        INSERT OR REPLACE INTO monthly_summary (client_id, month, summary_json, suggestions_json)
        VALUES (?, ?, ?, ?)
    """, (
        client_id, month,
        json.dumps(summary_dict, default=str),
        json.dumps(suggestions_list, default=str) if suggestions_list else None
    ))
    conn.commit()
    conn.close()


def get_monthly_summary(client_id, month):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        "SELECT * FROM monthly_summary WHERE client_id=? AND month=?",
        (client_id, month)
    )
    row = c.fetchone()
    conn.close()
    if row:
        result = dict(row)
        result["summary"] = json.loads(result["summary_json"])
        if result["suggestions_json"]:
            result["suggestions"] = json.loads(result["suggestions_json"])
        return result
    return None


# Initialize on import
init_db()
