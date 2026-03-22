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
            CREATE TABLE IF NOT EXISTS dismissed_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                month TEXT NOT NULL,
                action_key TEXT NOT NULL,
                dismissed_at TEXT DEFAULT (datetime('now')),
                UNIQUE(brand_id, month, action_key),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS heatmap_scans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                keyword TEXT NOT NULL,
                grid_size INTEGER DEFAULT 6,
                radius_miles REAL DEFAULT 5.0,
                center_lat REAL NOT NULL,
                center_lng REAL NOT NULL,
                results_json TEXT NOT NULL DEFAULT '[]',
                avg_rank REAL DEFAULT 0,
                scanned_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            );
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS warren_memories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                category TEXT NOT NULL DEFAULT 'insight',
                title TEXT NOT NULL DEFAULT '',
                content TEXT NOT NULL,
                embedding TEXT DEFAULT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            );
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS creative_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                name TEXT NOT NULL DEFAULT 'Untitled Template',
                ad_format TEXT NOT NULL DEFAULT 'facebook_feed',
                canvas_json TEXT NOT NULL,
                thumbnail TEXT DEFAULT '',
                canvas_width INTEGER DEFAULT 1200,
                canvas_height INTEGER DEFAULT 628,
                created_by TEXT NOT NULL DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            );
        """)

        # ── Ad Intelligence System ──
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ad_examples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL DEFAULT 'google',
                format TEXT NOT NULL DEFAULT 'search_rsa',
                industry TEXT DEFAULT '',
                headline TEXT DEFAULT '',
                description TEXT DEFAULT '',
                full_ad_json TEXT DEFAULT '{}',
                quality TEXT NOT NULL DEFAULT 'good',
                score INTEGER DEFAULT 0,
                analysis TEXT DEFAULT '',
                principles TEXT DEFAULT '[]',
                source TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ad_best_practices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL DEFAULT 'all',
                format TEXT DEFAULT '',
                category TEXT NOT NULL DEFAULT 'general',
                title TEXT NOT NULL DEFAULT '',
                content TEXT NOT NULL DEFAULT '',
                priority INTEGER DEFAULT 0,
                source TEXT DEFAULT '',
                is_active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ad_news_digests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                digest_date TEXT NOT NULL,
                platform TEXT NOT NULL DEFAULT 'all',
                raw_findings TEXT DEFAULT '[]',
                summary TEXT DEFAULT '',
                action_items TEXT DEFAULT '[]',
                prompt_updates TEXT DEFAULT '',
                status TEXT DEFAULT 'draft',
                created_at TEXT DEFAULT (datetime('now'))
            );
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ad_master_prompts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prompt_type TEXT NOT NULL DEFAULT 'ad_builder',
                platform TEXT DEFAULT 'all',
                format TEXT DEFAULT '',
                content TEXT NOT NULL DEFAULT '',
                version INTEGER DEFAULT 1,
                is_active INTEGER DEFAULT 1,
                generated_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_ad_examples_platform_format
            ON ad_examples(platform, format, quality);
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_ad_news_digests_date
            ON ad_news_digests(digest_date DESC);
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS ad_niche_prompts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                industry TEXT NOT NULL,
                title TEXT NOT NULL DEFAULT '',
                content TEXT NOT NULL DEFAULT '',
                is_active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );
        """)
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_ad_niche_industry
            ON ad_niche_prompts(industry) WHERE is_active = 1;
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS competitors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                website TEXT DEFAULT '',
                facebook_url TEXT DEFAULT '',
                google_maps_url TEXT DEFAULT '',
                yelp_url TEXT DEFAULT '',
                instagram_url TEXT DEFAULT '',
                notes TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (brand_id) REFERENCES brands(id)
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_competitors_brand
            ON competitors(brand_id);
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS competitor_intel (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                competitor_id INTEGER NOT NULL,
                brand_id INTEGER NOT NULL,
                intel_type TEXT NOT NULL,
                data_json TEXT DEFAULT '{}',
                fetched_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (competitor_id) REFERENCES competitors(id) ON DELETE CASCADE,
                FOREIGN KEY (brand_id) REFERENCES brands(id)
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_competitor_intel_lookup
            ON competitor_intel(competitor_id, intel_type);
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS campaign_strategies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_key TEXT NOT NULL UNIQUE,
                platform TEXT NOT NULL DEFAULT 'meta',
                name TEXT NOT NULL DEFAULT '',
                icon TEXT DEFAULT 'bi-megaphone-fill',
                color TEXT DEFAULT '#6366f1',
                tagline TEXT DEFAULT '',
                description TEXT DEFAULT '',
                best_for TEXT DEFAULT '',
                recommended_min INTEGER DEFAULT 200,
                objective TEXT DEFAULT '',
                is_active INTEGER DEFAULT 1,
                sort_order INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );
        """)

        conn.commit()
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
            ("business_lat", "REAL DEFAULT 0"),
            ("business_lng", "REAL DEFAULT 0"),
            ("google_place_id", "TEXT DEFAULT ''"),
            ("google_maps_api_key", "TEXT DEFAULT ''"),
        ]
        for col_name, col_def in new_brand_cols:
            if col_name not in brand_columns:
                conn.execute(f"ALTER TABLE brands ADD COLUMN {col_name} {col_def}")
        conn.commit()

        # ── campaign_strategies migrations ──
        cs_columns = {r[1] for r in conn.execute("PRAGMA table_info(campaign_strategies)").fetchall()}
        new_cs_cols = [
            ("blueprint", "TEXT DEFAULT ''"),
        ]
        for col_name, col_def in new_cs_cols:
            if col_name not in cs_columns:
                conn.execute(f"ALTER TABLE campaign_strategies ADD COLUMN {col_name} {col_def}")
        conn.commit()

        # ── Legacy migration: brands.competitors (text) -> competitors table ──
        # Older deployments stored competitor names in a free-form text field.
        # The client portal uses the structured competitors table.
        try:
            tables = {
                r[0]
                for r in conn.execute(
                    "select name from sqlite_master where type='table'"
                ).fetchall()
            }
            if "competitors" in tables and "brands" in tables:
                import re

                brand_rows = conn.execute(
                    "select id, competitors from brands"
                ).fetchall()
                for b in brand_rows:
                    brand_id = b["id"]
                    legacy = (b["competitors"] or "").strip()
                    if not legacy:
                        continue

                    has_any = conn.execute(
                        "select 1 from competitors where brand_id = ? limit 1",
                        (brand_id,),
                    ).fetchone()
                    if has_any:
                        continue

                    # Split on newlines/commas; keep it permissive.
                    parts = [p.strip() for p in re.split(r"[\n,]+", legacy) if p.strip()]
                    for name in parts:
                        conn.execute(
                            "insert into competitors (brand_id, name) values (?, ?)",
                            (brand_id, name),
                        )
                conn.commit()
        except Exception:
            # Best-effort migration; never block app startup.
            pass

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
            "google_maps_api_key", "google_place_id",
        }
        if field not in allowed:
            raise ValueError(f"Cannot update field: {field}")
        conn = self._conn()
        conn.execute(f"UPDATE brands SET {field}=?, updated_at=datetime('now') WHERE id=?", (value or "", brand_id))
        conn.commit()
        conn.close()

    def update_brand_number_field(self, brand_id, field, value):
        allowed = {"kpi_target_cpa", "kpi_target_leads", "kpi_target_roas",
                   "business_lat", "business_lng"}
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

    def update_campaign_draft(self, draft_id, brand_id, platform, campaign_name, plan_json):
        conn = self._conn()
        conn.execute(
            "UPDATE campaign_drafts SET platform = ?, campaign_name = ?, plan_json = ?, "
            "updated_at = CURRENT_TIMESTAMP WHERE id = ? AND brand_id = ?",
            (platform, campaign_name, plan_json, draft_id, brand_id),
        )
        conn.commit()
        conn.close()

    # ── Creative Templates ──

    def save_creative_template(self, brand_id, name, ad_format, canvas_json, thumbnail, canvas_width, canvas_height, created_by):
        conn = self._conn()
        conn.execute(
            "INSERT INTO creative_templates (brand_id, name, ad_format, canvas_json, thumbnail, canvas_width, canvas_height, created_by) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (brand_id, name, ad_format, canvas_json, thumbnail, canvas_width, canvas_height, created_by),
        )
        conn.commit()
        tid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.close()
        return tid

    def get_creative_templates(self, brand_id):
        conn = self._conn()
        rows = conn.execute(
            "SELECT id, name, ad_format, thumbnail, canvas_width, canvas_height, created_at "
            "FROM creative_templates WHERE brand_id = ? ORDER BY updated_at DESC",
            (brand_id,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_creative_template(self, template_id, brand_id):
        conn = self._conn()
        row = conn.execute(
            "SELECT * FROM creative_templates WHERE id = ? AND brand_id = ?",
            (template_id, brand_id),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def update_creative_template(self, template_id, brand_id, name, canvas_json, thumbnail, canvas_width, canvas_height):
        conn = self._conn()
        conn.execute(
            "UPDATE creative_templates SET name=?, canvas_json=?, thumbnail=?, canvas_width=?, canvas_height=?, updated_at=datetime('now') "
            "WHERE id=? AND brand_id=?",
            (name, canvas_json, thumbnail, canvas_width, canvas_height, template_id, brand_id),
        )
        conn.commit()
        conn.close()

    def delete_creative_template(self, template_id, brand_id):
        conn = self._conn()
        conn.execute(
            "DELETE FROM creative_templates WHERE id = ? AND brand_id = ?",
            (template_id, brand_id),
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

    # ── Dismissed Actions ──

    def get_dismissed_actions(self, brand_id, month):
        conn = self._conn()
        rows = conn.execute(
            "SELECT action_key FROM dismissed_actions WHERE brand_id = ? AND month = ?",
            (brand_id, month),
        ).fetchall()
        conn.close()
        return {r["action_key"] for r in rows}

    def dismiss_action(self, brand_id, month, action_key):
        conn = self._conn()
        conn.execute(
            "INSERT OR IGNORE INTO dismissed_actions (brand_id, month, action_key) VALUES (?, ?, ?)",
            (brand_id, month, action_key),
        )
        conn.commit()
        conn.close()

    def restore_action(self, brand_id, month, action_key):
        conn = self._conn()
        conn.execute(
            "DELETE FROM dismissed_actions WHERE brand_id = ? AND month = ? AND action_key = ?",
            (brand_id, month, action_key),
        )
        conn.commit()
        conn.close()

    def get_brand_briefs(self, brand_id, limit=12):
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM ai_briefs WHERE brand_id = ? ORDER BY month DESC LIMIT ?",
            (brand_id, limit),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ── Heatmap Scans ──

    def save_heatmap_scan(self, brand_id, keyword, grid_size, radius_miles,
                          center_lat, center_lng, results_json, avg_rank):
        conn = self._conn()
        conn.execute(
            """INSERT INTO heatmap_scans
               (brand_id, keyword, grid_size, radius_miles, center_lat, center_lng, results_json, avg_rank)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (brand_id, keyword, grid_size, radius_miles, center_lat, center_lng, results_json, avg_rank),
        )
        conn.commit()
        conn.close()

    def get_heatmap_scans(self, brand_id, limit=20):
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM heatmap_scans WHERE brand_id = ? ORDER BY scanned_at DESC LIMIT ?",
            (brand_id, limit),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_heatmap_scan(self, scan_id):
        conn = self._conn()
        row = conn.execute("SELECT * FROM heatmap_scans WHERE id = ?", (scan_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    # ── Warren Memories ──

    def add_warren_memory(self, brand_id, category, title, content, embedding=None):
        conn = self._conn()
        conn.execute(
            """INSERT INTO warren_memories (brand_id, category, title, content, embedding)
               VALUES (?, ?, ?, ?, ?)""",
            (brand_id, category, title, content, embedding),
        )
        conn.commit()
        conn.close()

    def get_warren_memories(self, brand_id, category=None, status="active", limit=50):
        conn = self._conn()
        if category:
            rows = conn.execute(
                "SELECT * FROM warren_memories WHERE brand_id = ? AND category = ? AND status = ? ORDER BY updated_at DESC LIMIT ?",
                (brand_id, category, status, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM warren_memories WHERE brand_id = ? AND status = ? ORDER BY updated_at DESC LIMIT ?",
                (brand_id, status, limit),
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def update_warren_memory(self, memory_id, content=None, status=None, title=None):
        conn = self._conn()
        updates = []
        params = []
        if content is not None:
            updates.append("content = ?")
            params.append(content)
        if status is not None:
            updates.append("status = ?")
            params.append(status)
        if title is not None:
            updates.append("title = ?")
            params.append(title)
        if updates:
            updates.append("updated_at = datetime('now')")
            params.append(memory_id)
            conn.execute(
                f"UPDATE warren_memories SET {', '.join(updates)} WHERE id = ?",
                params,
            )
            conn.commit()
        conn.close()

    def get_warren_memories_with_embeddings(self, brand_id, status="active"):
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM warren_memories WHERE brand_id = ? AND status = ? AND embedding IS NOT NULL ORDER BY updated_at DESC",
            (brand_id, status),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ── Ad Intelligence: Examples ──

    def add_ad_example(self, platform, fmt, industry, headline, description,
                       full_ad_json, quality, score, analysis, principles, source=""):
        conn = self._conn()
        conn.execute(
            """INSERT INTO ad_examples
               (platform, format, industry, headline, description, full_ad_json,
                quality, score, analysis, principles, source)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (platform, fmt, industry, headline, description, full_ad_json,
             quality, score, analysis, principles, source),
        )
        conn.commit()
        conn.close()

    def get_ad_examples(self, platform=None, fmt=None, quality=None, industry=None, limit=50):
        conn = self._conn()
        clauses, params = [], []
        if platform:
            clauses.append("platform = ?"); params.append(platform)
        if fmt:
            clauses.append("format = ?"); params.append(fmt)
        if quality:
            clauses.append("quality = ?"); params.append(quality)
        if industry:
            clauses.append("industry = ?"); params.append(industry)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        rows = conn.execute(
            f"SELECT * FROM ad_examples{where} ORDER BY score DESC, created_at DESC LIMIT ?",
            (*params, limit),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_ad_example(self, example_id):
        conn = self._conn()
        row = conn.execute("SELECT * FROM ad_examples WHERE id = ?", (example_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def delete_ad_example(self, example_id):
        conn = self._conn()
        conn.execute("DELETE FROM ad_examples WHERE id = ?", (example_id,))
        conn.commit()
        conn.close()

    # ── Ad Intelligence: Best Practices ──

    def add_ad_best_practice(self, platform, fmt, category, title, content, priority=0, source=""):
        conn = self._conn()
        conn.execute(
            """INSERT INTO ad_best_practices
               (platform, format, category, title, content, priority, source)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (platform, fmt, category, title, content, priority, source),
        )
        conn.commit()
        conn.close()

    def get_ad_best_practices(self, platform=None, fmt=None, category=None, active_only=True):
        conn = self._conn()
        clauses, params = [], []
        if active_only:
            clauses.append("is_active = 1")
        if platform:
            clauses.append("(platform = ? OR platform = 'all')"); params.append(platform)
        if fmt:
            clauses.append("(format = ? OR format = '')"); params.append(fmt)
        if category:
            clauses.append("category = ?"); params.append(category)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        rows = conn.execute(
            f"SELECT * FROM ad_best_practices{where} ORDER BY priority DESC, created_at DESC",
            params,
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def update_ad_best_practice(self, bp_id, **kwargs):
        conn = self._conn()
        sets = []
        params = []
        for k, v in kwargs.items():
            if k in ("title", "content", "platform", "format", "category", "priority", "source", "is_active"):
                sets.append(f"{k} = ?")
                params.append(v)
        if sets:
            sets.append("updated_at = datetime('now')")
            params.append(bp_id)
            conn.execute(f"UPDATE ad_best_practices SET {', '.join(sets)} WHERE id = ?", params)
            conn.commit()
        conn.close()

    def delete_ad_best_practice(self, bp_id):
        conn = self._conn()
        conn.execute("DELETE FROM ad_best_practices WHERE id = ?", (bp_id,))
        conn.commit()
        conn.close()

    # ── Ad Intelligence: News Digests ──

    def add_ad_news_digest(self, digest_date, platform, raw_findings, summary,
                           action_items, prompt_updates="", status="draft"):
        conn = self._conn()
        conn.execute(
            """INSERT INTO ad_news_digests
               (digest_date, platform, raw_findings, summary, action_items, prompt_updates, status)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (digest_date, platform, raw_findings, summary, action_items, prompt_updates, status),
        )
        conn.commit()
        conn.close()

    def get_ad_news_digests(self, limit=20):
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM ad_news_digests ORDER BY digest_date DESC, created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_ad_news_digest(self, digest_id):
        conn = self._conn()
        row = conn.execute("SELECT * FROM ad_news_digests WHERE id = ?", (digest_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def update_ad_news_digest(self, digest_id, **kwargs):
        conn = self._conn()
        sets, params = [], []
        for k, v in kwargs.items():
            if k in ("summary", "action_items", "prompt_updates", "status", "raw_findings"):
                sets.append(f"{k} = ?")
                params.append(v)
        if sets:
            params.append(digest_id)
            conn.execute(f"UPDATE ad_news_digests SET {', '.join(sets)} WHERE id = ?", params)
            conn.commit()
        conn.close()

    # ── Ad Intelligence: Master Prompts ──

    def get_active_master_prompt(self, prompt_type, platform="all", fmt=""):
        conn = self._conn()
        row = conn.execute(
            """SELECT * FROM ad_master_prompts
               WHERE prompt_type = ? AND platform = ? AND format = ? AND is_active = 1
               ORDER BY version DESC LIMIT 1""",
            (prompt_type, platform, fmt),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def save_master_prompt(self, prompt_type, platform, fmt, content):
        conn = self._conn()
        # Deactivate previous versions
        conn.execute(
            """UPDATE ad_master_prompts SET is_active = 0
               WHERE prompt_type = ? AND platform = ? AND format = ?""",
            (prompt_type, platform, fmt),
        )
        # Get next version number
        row = conn.execute(
            """SELECT COALESCE(MAX(version), 0) + 1 as next_v FROM ad_master_prompts
               WHERE prompt_type = ? AND platform = ? AND format = ?""",
            (prompt_type, platform, fmt),
        ).fetchone()
        version = row["next_v"] if row else 1
        conn.execute(
            """INSERT INTO ad_master_prompts
               (prompt_type, platform, format, content, version, is_active)
               VALUES (?, ?, ?, ?, ?, 1)""",
            (prompt_type, platform, fmt, content, version),
        )
        conn.commit()
        conn.close()

    def get_all_master_prompts(self, active_only=True):
        conn = self._conn()
        clause = " WHERE is_active = 1" if active_only else ""
        rows = conn.execute(
            f"SELECT * FROM ad_master_prompts{clause} ORDER BY prompt_type, platform, format, version DESC",
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ── Ad Intelligence: Niche Prompts ──

    def get_niche_prompt(self, industry):
        conn = self._conn()
        row = conn.execute(
            "SELECT * FROM ad_niche_prompts WHERE industry = ? AND is_active = 1",
            (industry,),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def get_all_niche_prompts(self):
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM ad_niche_prompts WHERE is_active = 1 ORDER BY industry",
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def save_niche_prompt(self, industry, title, content):
        conn = self._conn()
        existing = conn.execute(
            "SELECT id FROM ad_niche_prompts WHERE industry = ? AND is_active = 1",
            (industry,),
        ).fetchone()
        if existing:
            conn.execute(
                """UPDATE ad_niche_prompts
                   SET title = ?, content = ?, updated_at = datetime('now')
                   WHERE id = ?""",
                (title, content, existing["id"]),
            )
        else:
            conn.execute(
                """INSERT INTO ad_niche_prompts (industry, title, content)
                   VALUES (?, ?, ?)""",
                (industry, title, content),
            )
        conn.commit()
        conn.close()

    def delete_niche_prompt(self, niche_id):
        conn = self._conn()
        conn.execute("DELETE FROM ad_niche_prompts WHERE id = ?", (niche_id,))
        conn.commit()
        conn.close()

    # ── Campaign Strategies ──────────────────────────────────────

    def get_all_campaign_strategies(self, active_only=True):
        conn = self._conn()
        if active_only:
            rows = conn.execute(
                "SELECT * FROM campaign_strategies WHERE is_active = 1 ORDER BY sort_order, name",
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM campaign_strategies ORDER BY sort_order, name",
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_campaign_strategy(self, strategy_key):
        conn = self._conn()
        row = conn.execute(
            "SELECT * FROM campaign_strategies WHERE strategy_key = ?",
            (strategy_key,),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def save_campaign_strategy(self, strategy_key, platform="meta", name="",
                                icon="bi-megaphone-fill", color="#6366f1",
                                tagline="", description="", best_for="",
                                recommended_min=200, objective="",
                                is_active=1, sort_order=0, blueprint=""):
        conn = self._conn()
        existing = conn.execute(
            "SELECT id FROM campaign_strategies WHERE strategy_key = ?",
            (strategy_key,),
        ).fetchone()
        if existing:
            conn.execute(
                """UPDATE campaign_strategies
                   SET platform=?, name=?, icon=?, color=?, tagline=?,
                       description=?, best_for=?, recommended_min=?,
                       objective=?, is_active=?, sort_order=?, blueprint=?,
                       updated_at=datetime('now')
                   WHERE id=?""",
                (platform, name, icon, color, tagline, description,
                 best_for, recommended_min, objective, is_active,
                 sort_order, blueprint, existing["id"]),
            )
        else:
            conn.execute(
                """INSERT INTO campaign_strategies
                   (strategy_key, platform, name, icon, color, tagline,
                    description, best_for, recommended_min, objective,
                    is_active, sort_order, blueprint)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (strategy_key, platform, name, icon, color, tagline,
                 description, best_for, recommended_min, objective,
                 is_active, sort_order, blueprint),
            )
        conn.commit()
        conn.close()

    def delete_campaign_strategy(self, strategy_id):
        conn = self._conn()
        conn.execute("DELETE FROM campaign_strategies WHERE id = ?", (strategy_id,))
        conn.commit()
        conn.close()

    # ── Competitors (structured) ─────────────────────────────────

    def add_competitor(self, brand_id, name, website="", facebook_url="",
                       google_maps_url="", yelp_url="", instagram_url="",
                       notes=""):
        try:
            conn = self._conn()
            cur = conn.execute(
                """INSERT INTO competitors
                   (brand_id, name, website, facebook_url, google_maps_url,
                    yelp_url, instagram_url, notes)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (brand_id, name, website, facebook_url, google_maps_url,
                 yelp_url, instagram_url, notes),
            )
            conn.commit()
            new_id = cur.lastrowid
            conn.close()
            return new_id
        except sqlite3.OperationalError as exc:
            if "no such table: competitors" in str(exc).lower():
                self.init()
                return self.add_competitor(
                    brand_id,
                    name,
                    website=website,
                    facebook_url=facebook_url,
                    google_maps_url=google_maps_url,
                    yelp_url=yelp_url,
                    instagram_url=instagram_url,
                    notes=notes,
                )
            raise

    def get_competitors(self, brand_id):
        try:
            conn = self._conn()
            rows = conn.execute(
                "SELECT * FROM competitors WHERE brand_id = ? ORDER BY name",
                (brand_id,),
            ).fetchall()
            conn.close()
            return [dict(r) for r in rows]
        except sqlite3.OperationalError as exc:
            if "no such table: competitors" in str(exc).lower():
                self.init()
                return self.get_competitors(brand_id)
            raise

    def get_competitor(self, competitor_id, brand_id):
        try:
            conn = self._conn()
            row = conn.execute(
                "SELECT * FROM competitors WHERE id = ? AND brand_id = ?",
                (competitor_id, brand_id),
            ).fetchone()
            conn.close()
            return dict(row) if row else None
        except sqlite3.OperationalError as exc:
            if "no such table: competitors" in str(exc).lower():
                self.init()
                return self.get_competitor(competitor_id, brand_id)
            raise

    def update_competitor(self, competitor_id, brand_id, **kwargs):
        allowed = {"name", "website", "facebook_url", "google_maps_url",
                    "yelp_url", "instagram_url", "notes"}
        sets, params = [], []
        for k, v in kwargs.items():
            if k in allowed:
                sets.append(f"{k} = ?")
                params.append(v)
        if not sets:
            return
        sets.append("updated_at = datetime('now')")
        params.extend([competitor_id, brand_id])
        conn = self._conn()
        conn.execute(
            f"UPDATE competitors SET {', '.join(sets)} WHERE id = ? AND brand_id = ?",
            params,
        )
        conn.commit()
        conn.close()

    def delete_competitor(self, competitor_id, brand_id):
        try:
            conn = self._conn()
            conn.execute(
                "DELETE FROM competitors WHERE id = ? AND brand_id = ?",
                (competitor_id, brand_id),
            )
            conn.commit()
            conn.close()
        except sqlite3.OperationalError as exc:
            if "no such table: competitors" in str(exc).lower():
                self.init()
                return self.delete_competitor(competitor_id, brand_id)
            raise

    def replace_competitors_for_brand(self, brand_id, competitors):
        """Replace a brand's structured competitors list.

        competitors: list of dicts with keys: name (required), website (optional)
        """
        try:
            conn = self._conn()
            conn.execute("DELETE FROM competitors WHERE brand_id = ?", (brand_id,))
            for c in competitors or []:
                name = (c.get("name") or "").strip()
                if not name:
                    continue
                website = (c.get("website") or "").strip()
                conn.execute(
                    "INSERT INTO competitors (brand_id, name, website) VALUES (?, ?, ?)",
                    (brand_id, name, website),
                )
            conn.commit()
            conn.close()
        except sqlite3.OperationalError as exc:
            if "no such table: competitors" in str(exc).lower():
                self.init()
                return self.replace_competitors_for_brand(brand_id, competitors)
            raise

    # ── Competitor Intel (cached reports) ────────────────────────

    def upsert_competitor_intel(self, competitor_id, brand_id, intel_type, data_json):
        try:
            conn = self._conn()
            existing = conn.execute(
                "SELECT id FROM competitor_intel WHERE competitor_id = ? AND intel_type = ?",
                (competitor_id, intel_type),
            ).fetchone()
            if existing:
                conn.execute(
                    """UPDATE competitor_intel
                       SET data_json = ?, fetched_at = datetime('now')
                       WHERE id = ?""",
                    (data_json, existing["id"]),
                )
            else:
                conn.execute(
                    """INSERT INTO competitor_intel
                       (competitor_id, brand_id, intel_type, data_json)
                       VALUES (?, ?, ?, ?)""",
                    (competitor_id, brand_id, intel_type, data_json),
                )
            conn.commit()
            conn.close()
        except sqlite3.OperationalError as exc:
            if "no such table: competitor_intel" in str(exc).lower():
                self.init()
                return self.upsert_competitor_intel(competitor_id, brand_id, intel_type, data_json)
            raise

    def get_competitor_intel(self, competitor_id, intel_type=None):
        try:
            conn = self._conn()
            if intel_type:
                row = conn.execute(
                    "SELECT * FROM competitor_intel WHERE competitor_id = ? AND intel_type = ?",
                    (competitor_id, intel_type),
                ).fetchone()
                conn.close()
                return dict(row) if row else None
            rows = conn.execute(
                "SELECT * FROM competitor_intel WHERE competitor_id = ? ORDER BY intel_type",
                (competitor_id,),
            ).fetchall()
            conn.close()
            return [dict(r) for r in rows]
        except sqlite3.OperationalError as exc:
            if "no such table: competitor_intel" in str(exc).lower():
                self.init()
                return self.get_competitor_intel(competitor_id, intel_type=intel_type)
            raise

    def get_all_competitor_intel(self, brand_id):
        try:
            conn = self._conn()
            rows = conn.execute(
                "SELECT * FROM competitor_intel WHERE brand_id = ? ORDER BY competitor_id, intel_type",
                (brand_id,),
            ).fetchall()
            conn.close()
            return [dict(r) for r in rows]
        except sqlite3.OperationalError as exc:
            if "no such table: competitor_intel" in str(exc).lower():
                self.init()
                return self.get_all_competitor_intel(brand_id)
            raise
