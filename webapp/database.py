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
                google_ads_customer_id TEXT DEFAULT '',
                crm_last_webhook_at TEXT DEFAULT '',
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

            CREATE TABLE IF NOT EXISTS brand_month_finance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                month TEXT NOT NULL,
                closed_revenue REAL DEFAULT 0,
                closed_deals INTEGER DEFAULT 0,
                notes TEXT DEFAULT '',
                updated_at TEXT DEFAULT (datetime('now')),
                UNIQUE(brand_id, month),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS ai_briefs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                month TEXT NOT NULL,
                internal_json TEXT DEFAULT '',
                client_json TEXT DEFAULT '',
                model TEXT DEFAULT '',
                generated_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                UNIQUE(brand_id, month),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS ai_chat_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                month TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_ai_chat_brand_month_created
            ON ai_chat_messages(brand_id, month, created_at);

            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS client_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                display_name TEXT NOT NULL,
                is_active INTEGER DEFAULT 1,
                last_login_at TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS campaign_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                platform TEXT NOT NULL,
                campaign_id TEXT NOT NULL,
                campaign_name TEXT DEFAULT '',
                action TEXT NOT NULL,
                details TEXT DEFAULT '',
                changed_by TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_campaign_changes_brand
            ON campaign_changes(brand_id, created_at DESC);

            CREATE TABLE IF NOT EXISTS campaign_drafts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                platform TEXT NOT NULL,
                campaign_name TEXT DEFAULT '',
                plan_json TEXT NOT NULL,
                status TEXT DEFAULT 'draft',
                created_by TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            );
        """)
        conn.commit()

        # ── Migrations: add columns that may not exist on older DBs ──
        brand_columns = {r[1] for r in conn.execute("PRAGMA table_info(brands)").fetchall()}
        new_brand_cols = [
            ("brand_voice", "TEXT DEFAULT ''"),
            ("active_offers", "TEXT DEFAULT ''"),
            ("target_audience", "TEXT DEFAULT ''"),
            ("competitors", "TEXT DEFAULT ''"),
            ("reporting_notes", "TEXT DEFAULT ''"),
            ("kpi_target_cpa", "REAL DEFAULT 0"),
            ("kpi_target_leads", "INTEGER DEFAULT 0"),
            ("kpi_target_roas", "REAL DEFAULT 0"),
            ("call_tracking_number", "TEXT DEFAULT ''"),
            ("crm_type", "TEXT DEFAULT ''"),
            ("crm_api_key", "TEXT DEFAULT ''"),
            ("crm_webhook_url", "TEXT DEFAULT ''"),
            ("crm_pipeline_id", "TEXT DEFAULT ''"),
            ("crm_last_webhook_at", "TEXT DEFAULT ''"),
            ("google_ads_customer_id", "TEXT DEFAULT ''"),
            ("openai_api_key", "TEXT DEFAULT ''"),
            ("openai_model", "TEXT DEFAULT ''"),
            ("openai_model_chat", "TEXT DEFAULT ''"),
            ("openai_model_images", "TEXT DEFAULT ''"),
            ("openai_model_analysis", "TEXT DEFAULT ''"),
            ("openai_model_ads", "TEXT DEFAULT ''"),
            ("logo_path", "TEXT DEFAULT ''"),
            ("logo_variants", "TEXT DEFAULT '[]'"),
            ("brand_colors", "TEXT DEFAULT ''"),
            ("google_drive_folder_id", "TEXT DEFAULT ''"),
            ("google_drive_sheet_id", "TEXT DEFAULT ''"),
            ("facebook_page_id", "TEXT DEFAULT ''"),
        ]
        for col_name, col_def in new_brand_cols:
            if col_name not in brand_columns:
                conn.execute(f"ALTER TABLE brands ADD COLUMN {col_name} {col_def}")
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
        allowed = {"ga4_property_id", "gsc_site_url", "meta_ad_account_id", "google_ads_customer_id", "wp_category_id", "facebook_page_id"}
        if field not in allowed:
            raise ValueError(f"Cannot update field: {field}")
        conn = self._conn()
        conn.execute(f"UPDATE brands SET {field}=?, updated_at=datetime('now') WHERE id=?", (value, brand_id))
        conn.commit()
        conn.close()

    def update_brand_text_field(self, brand_id, field, value):
        allowed = {
            "brand_voice", "active_offers", "target_audience", "competitors",
            "reporting_notes", "call_tracking_number",
            "crm_type", "crm_api_key", "crm_webhook_url", "crm_pipeline_id",
            "openai_api_key", "openai_model",
            "openai_model_chat", "openai_model_images", "openai_model_analysis", "openai_model_ads",
            "logo_path", "logo_variants", "brand_colors",
            "google_drive_folder_id", "google_drive_sheet_id",
        }
        if field not in allowed:
            raise ValueError(f"Cannot update field: {field}")
        conn = self._conn()
        conn.execute(f"UPDATE brands SET {field}=?, updated_at=datetime('now') WHERE id=?", (value or "", brand_id))
        conn.commit()
        conn.close()

    def update_brand_number_field(self, brand_id, field, value):
        allowed = {"kpi_target_cpa", "kpi_target_leads", "kpi_target_roas"}
        if field not in allowed:
            raise ValueError(f"Cannot update field: {field}")
        try:
            num = float(value or 0)
        except (ValueError, TypeError):
            num = 0
        conn = self._conn()
        conn.execute(f"UPDATE brands SET {field}=?, updated_at=datetime('now') WHERE id=?", (num, brand_id))
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

    def get_expiring_connections(self, days=14):
        """Return connected tokens that expire within `days` days."""
        conn = self._conn()
        rows = conn.execute(
            """SELECT c.*, b.display_name as brand_name, b.id as brand_id
               FROM connections c JOIN brands b ON c.brand_id = b.id
               WHERE c.status = 'connected' AND c.token_expiry != ''
               ORDER BY c.token_expiry ASC""",
        ).fetchall()
        conn.close()
        results = []
        from datetime import datetime as _dt, timedelta as _td
        cutoff = (_dt.now() + _td(days=days)).isoformat()
        for r in rows:
            expiry = r["token_expiry"] or ""
            if expiry and expiry <= cutoff:
                results.append(dict(r))
        return results

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

    # ── Monthly Finance (CRM/offline revenue) ──

    def get_brand_month_finance(self, brand_id, month):
        conn = self._conn()
        row = conn.execute(
            "SELECT * FROM brand_month_finance WHERE brand_id = ? AND month = ?",
            (brand_id, month),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def upsert_brand_month_finance(self, brand_id, month, closed_revenue=0, closed_deals=0, notes=""):
        try:
            rev = float(closed_revenue or 0)
        except (TypeError, ValueError):
            rev = 0.0
        try:
            deals = int(float(closed_deals or 0))
        except (TypeError, ValueError):
            deals = 0

        conn = self._conn()
        conn.execute(
            """
            INSERT INTO brand_month_finance (brand_id, month, closed_revenue, closed_deals, notes, updated_at)
            VALUES (?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(brand_id, month) DO UPDATE SET
                closed_revenue = excluded.closed_revenue,
                closed_deals = excluded.closed_deals,
                notes = excluded.notes,
                updated_at = datetime('now')
            """,
            (brand_id, month, rev, deals, notes or ""),
        )
        conn.commit()
        conn.close()

    def mark_brand_webhook_received(self, brand_id):
        conn = self._conn()
        conn.execute(
            "UPDATE brands SET crm_last_webhook_at = datetime('now'), updated_at = datetime('now') WHERE id = ?",
            (brand_id,),
        )
        conn.commit()
        conn.close()

    # ── AI Briefs ──

    def get_ai_brief(self, brand_id, month):
        conn = self._conn()
        row = conn.execute(
            "SELECT * FROM ai_briefs WHERE brand_id = ? AND month = ?",
            (brand_id, month),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def upsert_ai_brief(self, brand_id, month, internal_json, client_json, model=""):
        conn = self._conn()
        conn.execute(
            """
            INSERT INTO ai_briefs (brand_id, month, internal_json, client_json, model, generated_at, updated_at)
            VALUES (?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            ON CONFLICT(brand_id, month) DO UPDATE SET
                internal_json = excluded.internal_json,
                client_json = excluded.client_json,
                model = excluded.model,
                updated_at = datetime('now')
            """,
            (brand_id, month, internal_json or "", client_json or "", model or ""),
        )
        conn.commit()
        conn.close()

    def get_recent_ai_briefs(self, limit=10):
        conn = self._conn()
        rows = conn.execute(
            """SELECT a.*, b.display_name as brand_name, b.slug as brand_slug
               FROM ai_briefs a JOIN brands b ON a.brand_id = b.id
               ORDER BY a.updated_at DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ── AI Chat ──

    def add_ai_chat_message(self, brand_id, month, role, content):
        role = (role or "").strip().lower()
        if role not in {"user", "assistant"}:
            raise ValueError("role must be 'user' or 'assistant'")
        conn = self._conn()
        conn.execute(
            "INSERT INTO ai_chat_messages (brand_id, month, role, content) VALUES (?, ?, ?, ?)",
            (brand_id, month, role, content or ""),
        )
        conn.commit()
        conn.close()

    def get_ai_chat_messages(self, brand_id, month, limit=30):
        conn = self._conn()
        rows = conn.execute(
            """SELECT * FROM ai_chat_messages
               WHERE brand_id = ? AND month = ?
               ORDER BY created_at ASC, id ASC
               LIMIT ?""",
            (brand_id, month, int(limit or 30)),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def clear_ai_chat_messages(self, brand_id, month):
        conn = self._conn()
        conn.execute(
            "DELETE FROM ai_chat_messages WHERE brand_id = ? AND month = ?",
            (brand_id, month),
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

    # ── Client Users ──

    def get_client_users_for_brand(self, brand_id):
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM client_users WHERE brand_id = ? ORDER BY display_name",
            (brand_id,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_client_user(self, client_user_id):
        conn = self._conn()
        row = conn.execute("SELECT * FROM client_users WHERE id = ?", (client_user_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def create_client_user(self, brand_id, email, password, display_name):
        conn = self._conn()
        password_hash = generate_password_hash(password)
        try:
            conn.execute(
                "INSERT INTO client_users (brand_id, email, password_hash, display_name) VALUES (?, ?, ?, ?)",
                (brand_id, email, password_hash, display_name),
            )
            conn.commit()
            row = conn.execute("SELECT id FROM client_users WHERE email = ?", (email,)).fetchone()
            conn.close()
            return int(row["id"]) if row else None
        except sqlite3.IntegrityError:
            conn.close()
            return None

    def authenticate_client(self, email, password):
        conn = self._conn()
        row = conn.execute(
            "SELECT cu.*, b.display_name AS brand_name, b.slug AS brand_slug "
            "FROM client_users cu JOIN brands b ON cu.brand_id = b.id "
            "WHERE cu.email = ? AND cu.is_active = 1",
            (email,),
        ).fetchone()
        conn.close()
        if row and check_password_hash(row["password_hash"], password):
            return dict(row)
        return None

    def update_client_user_login(self, client_user_id):
        conn = self._conn()
        conn.execute(
            "UPDATE client_users SET last_login_at = datetime('now') WHERE id = ?",
            (client_user_id,),
        )
        conn.commit()
        conn.close()

    def update_client_user_password(self, client_user_id, new_password):
        conn = self._conn()
        conn.execute(
            "UPDATE client_users SET password_hash = ? WHERE id = ?",
            (generate_password_hash(new_password), client_user_id),
        )
        conn.commit()
        conn.close()

    def toggle_client_user_active(self, client_user_id):
        conn = self._conn()
        conn.execute(
            "UPDATE client_users SET is_active = CASE WHEN is_active = 1 THEN 0 ELSE 1 END WHERE id = ?",
            (client_user_id,),
        )
        conn.commit()
        conn.close()

    def delete_client_user(self, client_user_id):
        conn = self._conn()
        conn.execute("DELETE FROM client_users WHERE id = ?", (client_user_id,))
        conn.commit()
        conn.close()

    # ── Campaign Changes Audit Log ──

    def log_campaign_change(self, brand_id, platform, campaign_id, campaign_name,
                            action, details, changed_by):
        conn = self._conn()
        conn.execute(
            "INSERT INTO campaign_changes (brand_id, platform, campaign_id, campaign_name, "
            "action, details, changed_by) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (brand_id, platform, campaign_id, campaign_name, action, details, changed_by),
        )
        conn.commit()
        conn.close()

    def get_campaign_changes(self, brand_id, limit=50):
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM campaign_changes WHERE brand_id = ? ORDER BY created_at DESC LIMIT ?",
            (brand_id, limit),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ── Campaign Drafts ──

    def save_campaign_draft(self, brand_id, platform, campaign_name, plan_json, created_by):
        conn = self._conn()
        conn.execute(
            "INSERT INTO campaign_drafts (brand_id, platform, campaign_name, plan_json, created_by) "
            "VALUES (?, ?, ?, ?, ?)",
            (brand_id, platform, campaign_name, plan_json, created_by),
        )
        conn.commit()
        draft_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.close()
        return draft_id

    def get_campaign_drafts(self, brand_id):
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM campaign_drafts WHERE brand_id = ? ORDER BY created_at DESC",
            (brand_id,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_campaign_draft(self, draft_id, brand_id):
        conn = self._conn()
        row = conn.execute(
            "SELECT * FROM campaign_drafts WHERE id = ? AND brand_id = ?",
            (draft_id, brand_id),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def delete_campaign_draft(self, draft_id, brand_id):
        conn = self._conn()
        conn.execute(
            "DELETE FROM campaign_drafts WHERE id = ? AND brand_id = ?",
            (draft_id, brand_id),
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

    # ── Aggregate Queries ──

    def get_report_for_brand_month(self, brand_id, month):
        """Get the most recent report for a brand/month combo."""
        conn = self._conn()
        row = conn.execute(
            "SELECT * FROM reports WHERE brand_id = ? AND month = ? ORDER BY generated_at DESC LIMIT 1",
            (brand_id, month),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def upsert_report(self, brand_id, month, internal_path, client_path):
        """Create or update report for a brand/month (avoids duplicates)."""
        conn = self._conn()
        existing = conn.execute(
            "SELECT id FROM reports WHERE brand_id = ? AND month = ? ORDER BY generated_at DESC LIMIT 1",
            (brand_id, month),
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE reports SET internal_path=?, client_path=?, generated_at=datetime('now'), sent_at='', published_at='', published_url='' WHERE id=?",
                (internal_path, client_path, existing["id"]),
            )
            conn.commit()
            report_id = existing["id"]
        else:
            cur = conn.execute(
                "INSERT INTO reports (brand_id, month, internal_path, client_path) VALUES (?, ?, ?, ?)",
                (brand_id, month, internal_path, client_path),
            )
            conn.commit()
            report_id = cur.lastrowid
        conn.close()
        return report_id
