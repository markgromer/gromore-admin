"""
Web app database layer.

Extends the existing analytics SQLite with tables for brands, users,
contacts, connections (OAuth tokens), reports, and settings.
"""
import sqlite3
import json
from pathlib import Path
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash


class WebDB:
    def __init__(self, db_path):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def init(self):
        conn = self._conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                display_name TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS brands (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                slug TEXT UNIQUE NOT NULL,
                display_name TEXT NOT NULL,
                industry TEXT DEFAULT 'plumbing',
                monthly_budget REAL DEFAULT 0,
                website TEXT DEFAULT '',
                service_area TEXT DEFAULT '',
                primary_services TEXT DEFAULT '',
                goals TEXT DEFAULT '[]',
                ga4_property_id TEXT DEFAULT '',
                gsc_site_url TEXT DEFAULT '',
                meta_ad_account_id TEXT DEFAULT '',
                wp_category_id INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS connections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                platform TEXT NOT NULL,
                access_token TEXT DEFAULT '',
                refresh_token TEXT DEFAULT '',
                token_expiry TEXT DEFAULT '',
                account_id TEXT DEFAULT '',
                account_name TEXT DEFAULT '',
                scopes TEXT DEFAULT '',
                status TEXT DEFAULT 'disconnected',
                connected_at TEXT DEFAULT '',
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS contacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                role TEXT DEFAULT 'client',
                auto_send INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                month TEXT NOT NULL,
                internal_path TEXT DEFAULT '',
                client_path TEXT DEFAULT '',
                generated_at TEXT DEFAULT (datetime('now')),
                sent_at TEXT DEFAULT '',
                published_at TEXT DEFAULT '',
                published_url TEXT DEFAULT '',
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT DEFAULT ''
            );
        """)
        conn.commit()
        conn.close()

    # ── Users ──

    def get_users(self):
        conn = self._conn()
        rows = conn.execute("SELECT * FROM users").fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_user(self, user_id):
        conn = self._conn()
        row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def create_user(self, username, password, display_name):
        conn = self._conn()
        password_hash = generate_password_hash(password)
        conn.execute(
            "INSERT OR IGNORE INTO users (username, password_hash, display_name) VALUES (?, ?, ?)",
            (username, password_hash, display_name),
        )
        conn.commit()
        row = conn.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
        conn.close()
        return int(row["id"]) if row else None

    def authenticate(self, username, password):
        conn = self._conn()
        row = conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
        conn.close()
        if row and check_password_hash(row["password_hash"], password):
            return dict(row)
        return None

    def authenticate_by_id(self, user_id, password):
        conn = self._conn()
        row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        conn.close()
        if row and check_password_hash(row["password_hash"], password):
            return dict(row)
        return None

    def update_password(self, user_id, new_password):
        conn = self._conn()
        conn.execute(
            "UPDATE users SET password_hash = ? WHERE id = ?",
            (generate_password_hash(new_password), user_id),
        )
        conn.commit()
        conn.close()

    def update_password_by_username(self, username, new_password):
        conn = self._conn()
        conn.execute(
            "UPDATE users SET password_hash = ? WHERE username = ?",
            (generate_password_hash(new_password), username),
        )
        conn.commit()
        conn.close()

    # ── Brands ──

    def get_all_brands(self):
        conn = self._conn()
        rows = conn.execute("SELECT * FROM brands ORDER BY display_name").fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_brand(self, brand_id):
        conn = self._conn()
        row = conn.execute("SELECT * FROM brands WHERE id = ?", (brand_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def get_brand_by_slug(self, slug):
        conn = self._conn()
        row = conn.execute("SELECT * FROM brands WHERE slug = ?", (slug,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def create_brand(self, data):
        conn = self._conn()
        goals_json = json.dumps(data.get("goals", []))
        cur = conn.execute(
            """INSERT INTO brands (slug, display_name, industry, monthly_budget,
               website, service_area, primary_services, goals)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                data["slug"], data["display_name"], data.get("industry", "plumbing"),
                data.get("monthly_budget", 0), data.get("website", ""),
                data.get("service_area", ""), data.get("primary_services", ""),
                goals_json,
            ),
        )
        conn.commit()
        brand_id = cur.lastrowid
        conn.close()
        return brand_id

    def update_brand(self, brand_id, data):
        goals_json = json.dumps(data.get("goals", []))
        conn = self._conn()
        conn.execute(
            """UPDATE brands SET display_name=?, slug=?, industry=?, monthly_budget=?,
               website=?, service_area=?, primary_services=?, goals=?,
               updated_at=datetime('now')
               WHERE id=?""",
            (
                data["display_name"], data["slug"], data.get("industry", "plumbing"),
                data.get("monthly_budget", 0), data.get("website", ""),
                data.get("service_area", ""), data.get("primary_services", ""),
                goals_json, brand_id,
            ),
        )
        conn.commit()
        conn.close()

    def update_brand_api_field(self, brand_id, field, value):
        allowed = {"ga4_property_id", "gsc_site_url", "meta_ad_account_id", "wp_category_id"}
        if field not in allowed:
            raise ValueError(f"Cannot update field: {field}")
        conn = self._conn()
        conn.execute(f"UPDATE brands SET {field}=?, updated_at=datetime('now') WHERE id=?", (value, brand_id))
        conn.commit()
        conn.close()

    def delete_brand(self, brand_id):
        conn = self._conn()
        conn.execute("DELETE FROM brands WHERE id = ?", (brand_id,))
        conn.commit()
        conn.close()

    # ── Connections (OAuth tokens) ──

    def get_brand_connections(self, brand_id):
        conn = self._conn()
        rows = conn.execute("SELECT * FROM connections WHERE brand_id = ?", (brand_id,)).fetchall()
        conn.close()
        result = {}
        for r in rows:
            result[r["platform"]] = dict(r)
        return result

    def upsert_connection(self, brand_id, platform, token_data):
        conn = self._conn()
        existing = conn.execute(
            "SELECT id FROM connections WHERE brand_id = ? AND platform = ?",
            (brand_id, platform),
        ).fetchone()
        now = datetime.now().isoformat()
        if existing:
            conn.execute(
                """UPDATE connections SET access_token=?, refresh_token=?, token_expiry=?,
                   account_id=?, account_name=?, scopes=?, status='connected', connected_at=?
                   WHERE id=?""",
                (
                    token_data.get("access_token", ""),
                    token_data.get("refresh_token", ""),
                    token_data.get("token_expiry", ""),
                    token_data.get("account_id", ""),
                    token_data.get("account_name", ""),
                    token_data.get("scopes", ""),
                    now,
                    existing["id"],
                ),
            )
        else:
            conn.execute(
                """INSERT INTO connections (brand_id, platform, access_token, refresh_token,
                   token_expiry, account_id, account_name, scopes, status, connected_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'connected', ?)""",
                (
                    brand_id, platform,
                    token_data.get("access_token", ""),
                    token_data.get("refresh_token", ""),
                    token_data.get("token_expiry", ""),
                    token_data.get("account_id", ""),
                    token_data.get("account_name", ""),
                    token_data.get("scopes", ""),
                    now,
                ),
            )
        conn.commit()
        conn.close()

    def disconnect_platform(self, brand_id, platform):
        conn = self._conn()
        conn.execute(
            "UPDATE connections SET status='disconnected', access_token='', refresh_token='' WHERE brand_id=? AND platform=?",
            (brand_id, platform),
        )
        conn.commit()
        conn.close()

    # ── Contacts ──

    def get_brand_contacts(self, brand_id):
        conn = self._conn()
        rows = conn.execute("SELECT * FROM contacts WHERE brand_id = ? ORDER BY name", (brand_id,)).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_contact(self, contact_id):
        conn = self._conn()
        row = conn.execute("SELECT * FROM contacts WHERE id = ?", (contact_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def add_contact(self, brand_id, name, email, role="client", auto_send=False):
        conn = self._conn()
        conn.execute(
            "INSERT INTO contacts (brand_id, name, email, role, auto_send) VALUES (?, ?, ?, ?, ?)",
            (brand_id, name, email, role, 1 if auto_send else 0),
        )
        conn.commit()
        conn.close()

    def delete_contact(self, contact_id):
        conn = self._conn()
        conn.execute("DELETE FROM contacts WHERE id = ?", (contact_id,))
        conn.commit()
        conn.close()

    def toggle_contact_autosend(self, contact_id):
        conn = self._conn()
        conn.execute(
            "UPDATE contacts SET auto_send = CASE WHEN auto_send = 1 THEN 0 ELSE 1 END WHERE id = ?",
            (contact_id,),
        )
        conn.commit()
        conn.close()

    # ── Reports ──

    def get_brand_reports(self, brand_id, limit=12):
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM reports WHERE brand_id = ? ORDER BY month DESC LIMIT ?",
            (brand_id, limit),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_report(self, report_id):
        conn = self._conn()
        row = conn.execute("SELECT * FROM reports WHERE id = ?", (report_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def get_recent_reports(self, limit=10):
        conn = self._conn()
        rows = conn.execute(
            """SELECT r.*, b.display_name as brand_name, b.slug as brand_slug
               FROM reports r JOIN brands b ON r.brand_id = b.id
               ORDER BY r.generated_at DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def count_reports_for_month(self, month):
        conn = self._conn()
        row = conn.execute("SELECT COUNT(*) as cnt FROM reports WHERE month = ?", (month,)).fetchone()
        conn.close()
        return row["cnt"] if row else 0

    def create_report(self, brand_id, month, internal_path, client_path):
        conn = self._conn()
        cur = conn.execute(
            "INSERT INTO reports (brand_id, month, internal_path, client_path) VALUES (?, ?, ?, ?)",
            (brand_id, month, internal_path, client_path),
        )
        conn.commit()
        report_id = cur.lastrowid
        conn.close()
        return report_id

    def mark_report_sent(self, report_id):
        conn = self._conn()
        conn.execute(
            "UPDATE reports SET sent_at = datetime('now') WHERE id = ?", (report_id,)
        )
        conn.commit()
        conn.close()

    def mark_report_published(self, report_id, url):
        conn = self._conn()
        conn.execute(
            "UPDATE reports SET published_at = datetime('now'), published_url = ? WHERE id = ?",
            (url, report_id),
        )
        conn.commit()
        conn.close()

    # ── Settings ──

    def get_setting(self, key, default=""):
        conn = self._conn()
        row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        conn.close()
        return row["value"] if row else default

    def save_setting(self, key, value):
        conn = self._conn()
        conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=?",
            (key, value, value),
        )
        conn.commit()
        conn.close()
