"""
Web app database layer.

Extends the existing analytics SQLite with tables for brands, users,
contacts, connections (OAuth tokens), reports, and settings.
"""
import sqlite3
import json
import re
import secrets
from pathlib import Path
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse
from werkzeug.security import generate_password_hash, check_password_hash


class WebDB:
    def __init__(self, db_path):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _normalize_email(email):
        return (email or "").strip().lower()

    @classmethod
    def _parse_email_list(cls, raw_value):
        emails = []
        seen = set()
        for part in re.split(r"[;,\n]+", raw_value or ""):
            email = cls._normalize_email(part)
            if not email or "@" not in email or email in seen:
                continue
            seen.add(email)
            emails.append(email)
        return emails

    @staticmethod
    def _safe_json_object(raw_value):
        if isinstance(raw_value, dict):
            return dict(raw_value)
        if not raw_value:
            return {}
        try:
            parsed = json.loads(raw_value)
        except Exception:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}

    @staticmethod
    def _safe_json_list(raw_value):
        if isinstance(raw_value, list):
            return list(raw_value)
        if not raw_value:
            return []
        try:
            parsed = json.loads(raw_value)
        except Exception:
            return []
        return list(parsed) if isinstance(parsed, list) else []

    @staticmethod
    def _normalize_website(value):
        website = (value or "").strip()
        if not website:
            return ""
        parsed = urlparse(website if website.startswith(("http://", "https://")) else f"https://{website}")
        host = (parsed.netloc or parsed.path or "").strip().lower()
        path = parsed.path if parsed.netloc else ""
        normalized = f"{host}{path}".rstrip("/")
        if normalized.startswith("www."):
            normalized = normalized[4:]
        return normalized

    @staticmethod
    def _normalize_feature_state(state):
        value = (state or "on").strip().lower()
        if value not in {"on", "off", "upgrade"}:
            return "on"
        return value

    @staticmethod
    def _normalize_feature_access_level(level):
        value = (level or "all").strip().lower()
        if value not in {"all", "beta", "brand", "admin"}:
            return "all"
        return value

    def _conn(self):
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA busy_timeout = 5000")
        return conn

    def _safe_add_column(self, conn, table_name, col_name, col_def):
        try:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_def}")
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            if "duplicate column name" in message:
                return
            raise

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

            CREATE TABLE IF NOT EXISTS lead_threads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                lead_name TEXT DEFAULT '',
                lead_email TEXT DEFAULT '',
                lead_phone TEXT DEFAULT '',
                source TEXT DEFAULT '',
                channel TEXT NOT NULL DEFAULT 'sms',
                external_thread_id TEXT DEFAULT NULL,
                status TEXT NOT NULL DEFAULT 'new',
                quote_status TEXT NOT NULL DEFAULT 'not_started',
                assigned_to TEXT DEFAULT '',
                unread_count INTEGER DEFAULT 0,
                summary TEXT DEFAULT '',
                commercial_data_json TEXT DEFAULT '{}',
                last_message_at TEXT DEFAULT (datetime('now')),
                last_inbound_at TEXT DEFAULT '',
                last_outbound_at TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE,
                UNIQUE(brand_id, channel, external_thread_id)
            );

            CREATE INDEX IF NOT EXISTS idx_lead_threads_brand_updated
            ON lead_threads(brand_id, updated_at DESC, id DESC);

            CREATE TABLE IF NOT EXISTS lead_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id INTEGER NOT NULL,
                direction TEXT NOT NULL DEFAULT 'inbound',
                role TEXT NOT NULL DEFAULT 'lead',
                channel TEXT DEFAULT '',
                external_message_id TEXT DEFAULT '',
                content TEXT NOT NULL DEFAULT '',
                metadata_json TEXT DEFAULT '{}',
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (thread_id) REFERENCES lead_threads(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_lead_messages_thread_created
            ON lead_messages(thread_id, created_at ASC, id ASC);

            CREATE TABLE IF NOT EXISTS lead_quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                thread_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'draft',
                quote_mode TEXT NOT NULL DEFAULT 'hybrid',
                amount_low REAL DEFAULT 0,
                amount_high REAL DEFAULT 0,
                currency TEXT DEFAULT 'USD',
                line_items_json TEXT DEFAULT '[]',
                summary TEXT DEFAULT '',
                follow_up_text TEXT DEFAULT '',
                sent_at TEXT DEFAULT '',
                accepted_at TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE,
                FOREIGN KEY (thread_id) REFERENCES lead_threads(id) ON DELETE CASCADE,
                UNIQUE(thread_id)
            );

            CREATE INDEX IF NOT EXISTS idx_lead_quotes_brand_status
            ON lead_quotes(brand_id, status, updated_at DESC);

            CREATE TABLE IF NOT EXISTS lead_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                thread_id INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                event_value TEXT DEFAULT '',
                metadata_json TEXT DEFAULT '{}',
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE,
                FOREIGN KEY (thread_id) REFERENCES lead_threads(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_lead_events_thread_created
            ON lead_events(thread_id, created_at DESC, id DESC);

            CREATE TABLE IF NOT EXISTS commercial_service_visits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                thread_id INTEGER NOT NULL,
                service_date TEXT DEFAULT '',
                completed_at TEXT DEFAULT '',
                completed_by TEXT DEFAULT '',
                property_label TEXT DEFAULT '',
                summary TEXT DEFAULT '',
                waste_station_count_serviced INTEGER DEFAULT 0,
                bags_restocked INTEGER DEFAULT 0,
                gate_secured INTEGER DEFAULT 0,
                issues_json TEXT DEFAULT '[]',
                photos_json TEXT DEFAULT '[]',
                client_note TEXT DEFAULT '',
                internal_note TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE,
                FOREIGN KEY (thread_id) REFERENCES lead_threads(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_commercial_service_visits_thread_date
            ON commercial_service_visits(thread_id, service_date DESC, id DESC);

            CREATE TABLE IF NOT EXISTS lead_profile_overrides (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id INTEGER NOT NULL UNIQUE,
                dog_count INTEGER DEFAULT NULL,
                objections_text TEXT DEFAULT '',
                waiting_on_text TEXT DEFAULT '',
                closeability_pct INTEGER DEFAULT NULL,
                profile_notes TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (thread_id) REFERENCES lead_threads(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS sms_consent (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                phone TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'opted_in',
                opted_in_at TEXT DEFAULT (datetime('now')),
                opted_out_at TEXT DEFAULT '',
                opted_out_keyword TEXT DEFAULT '',
                opted_in_source TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE,
                UNIQUE(brand_id, phone)
            );

            CREATE INDEX IF NOT EXISTS idx_sms_consent_brand_phone
            ON sms_consent(brand_id, phone, status);

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

            CREATE TABLE IF NOT EXISTS client_billing_reminders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                external_client_id TEXT NOT NULL,
                due_date TEXT NOT NULL,
                channel TEXT NOT NULL,
                reminder_type TEXT NOT NULL DEFAULT 'payment_due',
                recipient TEXT DEFAULT '',
                status TEXT NOT NULL DEFAULT 'sent',
                detail TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                UNIQUE(brand_id, external_client_id, due_date, channel, reminder_type),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_client_billing_reminders_brand_due
            ON client_billing_reminders(brand_id, due_date, channel, reminder_type);

            CREATE TABLE IF NOT EXISTS sng_webhook_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                external_event_id TEXT NOT NULL,
                event_type TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'received',
                detail TEXT DEFAULT '',
                summary_json TEXT DEFAULT '',
                payload_json TEXT DEFAULT '',
                received_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                UNIQUE(brand_id, external_event_id),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_sng_webhook_events_brand_received
            ON sng_webhook_events(brand_id, received_at DESC);

            CREATE TABLE IF NOT EXISTS lead_webhook_deliveries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER DEFAULT NULL,
                brand_slug TEXT DEFAULT '',
                endpoint TEXT DEFAULT '',
                status TEXT NOT NULL DEFAULT 'received',
                http_status INTEGER DEFAULT 0,
                reason TEXT DEFAULT '',
                source TEXT DEFAULT '',
                lead_name TEXT DEFAULT '',
                lead_email TEXT DEFAULT '',
                lead_phone TEXT DEFAULT '',
                thread_id INTEGER DEFAULT NULL,
                signature_present INTEGER DEFAULT 0,
                payload_preview TEXT DEFAULT '',
                remote_addr TEXT DEFAULT '',
                received_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_lead_webhook_deliveries_brand_received
            ON lead_webhook_deliveries(brand_id, received_at DESC, id DESC);

            CREATE TABLE IF NOT EXISTS crm_event_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                source_event_id TEXT NOT NULL DEFAULT '',
                source_event_type TEXT NOT NULL DEFAULT '',
                rule_key TEXT NOT NULL DEFAULT '',
                action_kind TEXT NOT NULL DEFAULT 'client_message',
                channel TEXT NOT NULL DEFAULT 'sms',
                recipient TEXT DEFAULT '',
                client_id TEXT DEFAULT '',
                payment_id TEXT DEFAULT '',
                invoice_id TEXT DEFAULT '',
                subscription_id TEXT DEFAULT '',
                subject TEXT DEFAULT '',
                message_text TEXT DEFAULT '',
                status TEXT NOT NULL DEFAULT 'queued',
                attempt_number INTEGER NOT NULL DEFAULT 1,
                max_attempts INTEGER NOT NULL DEFAULT 1,
                scheduled_for TEXT DEFAULT (datetime('now')),
                sent_at TEXT DEFAULT '',
                resolved_at TEXT DEFAULT '',
                resolution_reason TEXT DEFAULT '',
                detail TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                UNIQUE(brand_id, source_event_id, rule_key, action_kind, channel, recipient, attempt_number),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_crm_event_actions_brand_schedule
            ON crm_event_actions(brand_id, status, scheduled_for, created_at DESC);

            CREATE TABLE IF NOT EXISTS appointment_reminder_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                target_date TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'completed',
                reason TEXT DEFAULT '',
                candidates INTEGER NOT NULL DEFAULT 0,
                sent INTEGER NOT NULL DEFAULT 0,
                failed INTEGER NOT NULL DEFAULT 0,
                skipped INTEGER NOT NULL DEFAULT 0,
                summary_json TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_appointment_reminder_runs_brand_created
            ON appointment_reminder_runs(brand_id, created_at DESC);

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

            CREATE TABLE IF NOT EXISTS meta_deletion_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                confirmation_code TEXT UNIQUE NOT NULL,
                meta_user_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'received',
                payload_json TEXT DEFAULT '{}',
                notes TEXT DEFAULT '',
                deleted_thread_count INTEGER DEFAULT 0,
                requested_at TEXT DEFAULT (datetime('now')),
                completed_at TEXT DEFAULT ''
            );

            CREATE INDEX IF NOT EXISTS idx_meta_deletion_requests_user
            ON meta_deletion_requests(meta_user_id, requested_at DESC);

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

            CREATE TABLE IF NOT EXISTS client_onboarding_progress (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                client_user_id INTEGER NOT NULL,
                item_key TEXT NOT NULL,
                is_completed INTEGER DEFAULT 0,
                is_dismissed INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                UNIQUE(brand_id, client_user_id, item_key),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE,
                FOREIGN KEY (client_user_id) REFERENCES client_users(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_client_onboarding_progress_user
            ON client_onboarding_progress(client_user_id, brand_id, item_key);

            CREATE TABLE IF NOT EXISTS client_onboarding_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                client_user_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                stage_key TEXT NOT NULL DEFAULT 'setup',
                current_question_key TEXT DEFAULT '',
                profile_json TEXT NOT NULL DEFAULT '{}',
                notes_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                UNIQUE(brand_id, client_user_id),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE,
                FOREIGN KEY (client_user_id) REFERENCES client_users(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_client_onboarding_sessions_user
            ON client_onboarding_sessions(client_user_id, brand_id, status);

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
                status TEXT NOT NULL DEFAULT 'complete',
                error_message TEXT NOT NULL DEFAULT '',
                debug_json TEXT NOT NULL DEFAULT '{}',
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
                gbp_cid TEXT DEFAULT '',
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

        conn.execute("""
            CREATE TABLE IF NOT EXISTS scheduled_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                platform TEXT NOT NULL DEFAULT 'facebook',
                post_type TEXT DEFAULT '',
                message TEXT NOT NULL DEFAULT '',
                image_url TEXT DEFAULT '',
                link_url TEXT DEFAULT '',
                scheduled_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                fb_post_id TEXT DEFAULT '',
                error_message TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                published_at TEXT DEFAULT NULL,
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_scheduled_posts_brand
            ON scheduled_posts(brand_id, scheduled_at)
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS beta_testers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                business_name TEXT DEFAULT '',
                website TEXT DEFAULT '',
                industry TEXT DEFAULT '',
                monthly_ad_spend TEXT DEFAULT '',
                platforms TEXT DEFAULT '',
                referral_source TEXT DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pending',
                brand_id INTEGER DEFAULT NULL,
                client_user_id INTEGER DEFAULT NULL,
                admin_notes TEXT DEFAULT '',
                invite_sent_at TEXT DEFAULT '',
                approved_at TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE SET NULL,
                FOREIGN KEY (client_user_id) REFERENCES client_users(id) ON DELETE SET NULL
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_beta_testers_brand
            ON beta_testers(brand_id);
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS beta_feedback (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                client_user_id INTEGER NOT NULL,
                category TEXT NOT NULL DEFAULT 'general',
                rating INTEGER DEFAULT 0 CHECK(rating BETWEEN 0 AND 5),
                message TEXT NOT NULL,
                page TEXT DEFAULT '',
                status TEXT NOT NULL DEFAULT 'new',
                admin_response TEXT DEFAULT '',
                responded_at TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE,
                FOREIGN KEY (client_user_id) REFERENCES client_users(id) ON DELETE CASCADE
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_beta_feedback_brand
            ON beta_feedback(brand_id, created_at);
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS feedback_ai_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scope_type TEXT NOT NULL DEFAULT 'status',
                scope_label TEXT DEFAULT '',
                feedback_ids_json TEXT DEFAULT '[]',
                filters_json TEXT DEFAULT '{}',
                model TEXT DEFAULT '',
                prompt_version TEXT DEFAULT '',
                summary_json TEXT DEFAULT '{}',
                dev_plan_json TEXT DEFAULT '{}',
                created_by INTEGER DEFAULT NULL,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_feedback_ai_runs_created
            ON feedback_ai_runs(created_at DESC, id DESC);
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS feedback_ai_drafts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                feedback_id INTEGER NOT NULL UNIQUE,
                run_id INTEGER DEFAULT NULL,
                reply_subject TEXT DEFAULT '',
                reply_draft TEXT DEFAULT '',
                internal_note TEXT DEFAULT '',
                recommended_status TEXT DEFAULT 'reviewed',
                confidence REAL DEFAULT 0,
                needs_manual_review INTEGER DEFAULT 0,
                approved INTEGER DEFAULT 0,
                approved_by INTEGER DEFAULT NULL,
                approved_at TEXT DEFAULT '',
                sent_at TEXT DEFAULT '',
                send_error TEXT DEFAULT '',
                generated_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (feedback_id) REFERENCES beta_feedback(id) ON DELETE CASCADE,
                FOREIGN KEY (run_id) REFERENCES feedback_ai_runs(id) ON DELETE SET NULL,
                FOREIGN KEY (approved_by) REFERENCES users(id) ON DELETE SET NULL
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_feedback_ai_drafts_run
            ON feedback_ai_drafts(run_id, updated_at DESC);
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS upgrade_considerations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                description TEXT DEFAULT '',
                category TEXT NOT NULL DEFAULT 'feature',
                source_feedback_ids TEXT DEFAULT '',
                request_count INTEGER DEFAULT 1,
                feasibility TEXT DEFAULT 'unknown',
                safety_risk TEXT DEFAULT 'low',
                priority TEXT DEFAULT 'medium',
                status TEXT NOT NULL DEFAULT 'proposed',
                decision_notes TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS blog_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                title TEXT NOT NULL DEFAULT '',
                content TEXT NOT NULL DEFAULT '',
                excerpt TEXT DEFAULT '',
                slug TEXT DEFAULT '',
                status TEXT NOT NULL DEFAULT 'draft',
                featured_image_url TEXT DEFAULT '',
                categories TEXT DEFAULT '',
                tags TEXT DEFAULT '',
                seo_title TEXT DEFAULT '',
                seo_description TEXT DEFAULT '',
                wp_post_id INTEGER DEFAULT 0,
                wp_post_url TEXT DEFAULT '',
                scheduled_at TEXT DEFAULT NULL,
                published_at TEXT DEFAULT NULL,
                created_by INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_blog_posts_brand
            ON blog_posts(brand_id, status);
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS site_builds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                model TEXT DEFAULT 'gpt-4o-mini',
                blueprint_json TEXT DEFAULT '[]',
                page_count INTEGER DEFAULT 0,
                pages_completed INTEGER DEFAULT 0,
                error_message TEXT DEFAULT '',
                created_by INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                completed_at TEXT DEFAULT '',
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_site_builds_brand
            ON site_builds(brand_id, status);
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS site_pages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                build_id INTEGER NOT NULL,
                brand_id INTEGER NOT NULL,
                page_type TEXT NOT NULL DEFAULT 'home',
                label TEXT NOT NULL DEFAULT '',
                slug TEXT DEFAULT '',
                title TEXT DEFAULT '',
                content TEXT DEFAULT '',
                excerpt TEXT DEFAULT '',
                seo_title TEXT DEFAULT '',
                seo_description TEXT DEFAULT '',
                primary_keyword TEXT DEFAULT '',
                secondary_keywords TEXT DEFAULT '',
                faq_items_json TEXT DEFAULT '[]',
                schema_json TEXT DEFAULT '[]',
                schema_html TEXT DEFAULT '',
                full_html TEXT DEFAULT '',
                wp_page_id INTEGER DEFAULT 0,
                wp_page_url TEXT DEFAULT '',
                status TEXT NOT NULL DEFAULT 'draft',
                published_at TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (build_id) REFERENCES site_builds(id) ON DELETE CASCADE,
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_site_pages_build
            ON site_pages(build_id, page_type);
        """)

        # ── Site Builder Admin: Templates ──
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sb_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                category TEXT NOT NULL DEFAULT 'section',
                page_types TEXT DEFAULT '',
                html_content TEXT DEFAULT '',
                css_content TEXT DEFAULT '',
                preview_image TEXT DEFAULT '',
                description TEXT DEFAULT '',
                sort_order INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_sb_templates_category
            ON sb_templates(category, is_active);
        """)

        # ── Site Builder Admin: Themes ──
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sb_themes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT DEFAULT '',
                primary_color TEXT DEFAULT '#2563eb',
                secondary_color TEXT DEFAULT '#1e40af',
                accent_color TEXT DEFAULT '#f59e0b',
                text_color TEXT DEFAULT '#1f2937',
                bg_color TEXT DEFAULT '#ffffff',
                font_heading TEXT DEFAULT 'Inter',
                font_body TEXT DEFAULT 'Inter',
                button_style TEXT DEFAULT 'rounded',
                layout_style TEXT DEFAULT 'modern',
                custom_css TEXT DEFAULT '',
                preview_image TEXT DEFAULT '',
                is_default INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );
        """)

        # ── Site Builder Admin: Full Site Templates ──
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sb_site_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                slug TEXT NOT NULL UNIQUE,
                description TEXT DEFAULT '',
                preview_image TEXT DEFAULT '',
                theme_id INTEGER DEFAULT 0,
                template_ids_json TEXT DEFAULT '[]',
                prompt_notes TEXT DEFAULT '',
                sort_order INTEGER DEFAULT 0,
                is_default INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );
        """)

        # ── Site Builder Admin: Prompt Overrides ──
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sb_prompt_overrides (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                page_type TEXT NOT NULL,
                section TEXT NOT NULL DEFAULT 'user_prompt',
                content TEXT DEFAULT '',
                is_active INTEGER DEFAULT 1,
                notes TEXT DEFAULT '',
                updated_by TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                UNIQUE(page_type, section)
            );
        """)

        # ── Site Builder Admin: Image Categories ──
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sb_image_categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                slug TEXT NOT NULL UNIQUE,
                description TEXT DEFAULT '',
                sort_order INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now'))
            );
        """)

        # ── Site Builder Admin: Image Library ──
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sb_images (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL,
                original_name TEXT DEFAULT '',
                file_path TEXT NOT NULL,
                file_size INTEGER DEFAULT 0,
                mime_type TEXT DEFAULT 'image/jpeg',
                width INTEGER DEFAULT 0,
                height INTEGER DEFAULT 0,
                alt_text TEXT DEFAULT '',
                title TEXT DEFAULT '',
                category_id INTEGER DEFAULT NULL,
                tags TEXT DEFAULT '',
                industry TEXT DEFAULT '',
                page_types TEXT DEFAULT '',
                source TEXT DEFAULT 'upload',
                drive_file_id TEXT DEFAULT '',
                wp_media_id INTEGER DEFAULT 0,
                wp_media_url TEXT DEFAULT '',
                is_active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (category_id) REFERENCES sb_image_categories(id) ON DELETE SET NULL
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_sb_images_category
            ON sb_images(category_id, is_active);
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_sb_images_industry
            ON sb_images(industry, is_active);
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS assessment_leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                business_name TEXT DEFAULT '',
                industry TEXT DEFAULT '',
                service_area TEXT DEFAULT '',
                website TEXT DEFAULT '',
                gmb_url TEXT DEFAULT '',
                facebook_url TEXT DEFAULT '',
                phone TEXT DEFAULT '',
                overall_score INTEGER DEFAULT 0,
                results_json TEXT DEFAULT '',
                converted_to_brand_id INTEGER DEFAULT NULL,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (converted_to_brand_id) REFERENCES brands(id) ON DELETE SET NULL
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_assessment_leads_email
            ON assessment_leads(email);
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS signup_leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                phone TEXT DEFAULT '',
                business_name TEXT DEFAULT '',
                website TEXT DEFAULT '',
                industry TEXT DEFAULT '',
                service_area TEXT DEFAULT '',
                primary_services TEXT DEFAULT '',
                monthly_budget TEXT DEFAULT '',
                platforms TEXT DEFAULT '',
                goals TEXT DEFAULT '',
                referral_source TEXT DEFAULT '',
                converted_to_brand_id INTEGER DEFAULT NULL,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (converted_to_brand_id) REFERENCES brands(id) ON DELETE SET NULL
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_signup_leads_email
            ON signup_leads(email);
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS feature_flags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                feature_key TEXT UNIQUE NOT NULL,
                label TEXT NOT NULL,
                description TEXT DEFAULT '',
                access_level TEXT NOT NULL DEFAULT 'all',
                enabled INTEGER DEFAULT 1,
                category TEXT DEFAULT 'general',
                sort_order INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now'))
            );
        """)

        # ── Drip campaign tables ──
        conn.execute("""
            CREATE TABLE IF NOT EXISTS drip_sequences (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT DEFAULT '',
                trigger TEXT NOT NULL DEFAULT 'assessment',
                is_active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now'))
            );
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS drip_steps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sequence_id INTEGER NOT NULL,
                step_order INTEGER NOT NULL DEFAULT 1,
                delay_days INTEGER NOT NULL DEFAULT 1,
                subject TEXT NOT NULL,
                body_html TEXT NOT NULL DEFAULT '',
                body_text TEXT NOT NULL DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (sequence_id) REFERENCES drip_sequences(id) ON DELETE CASCADE
            );
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS drip_enrollments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sequence_id INTEGER NOT NULL,
                email TEXT NOT NULL COLLATE NOCASE,
                name TEXT DEFAULT '',
                lead_source TEXT DEFAULT 'assessment',
                lead_id INTEGER DEFAULT NULL,
                current_step INTEGER DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'active',
                enrolled_at TEXT DEFAULT (datetime('now')),
                completed_at TEXT DEFAULT NULL,
                converted_at TEXT DEFAULT NULL,
                unsubscribed_at TEXT DEFAULT NULL,
                FOREIGN KEY (sequence_id) REFERENCES drip_sequences(id) ON DELETE CASCADE
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_drip_enrollments_status
            ON drip_enrollments(status);
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_drip_enrollments_email
            ON drip_enrollments(email);
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS drip_sends (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                enrollment_id INTEGER NOT NULL,
                step_id INTEGER NOT NULL,
                sent_at TEXT DEFAULT (datetime('now')),
                status TEXT NOT NULL DEFAULT 'sent',
                error TEXT DEFAULT '',
                FOREIGN KEY (enrollment_id) REFERENCES drip_enrollments(id) ON DELETE CASCADE,
                FOREIGN KEY (step_id) REFERENCES drip_steps(id) ON DELETE CASCADE
            );
        """)

        # ── Agency CRM (GroMore's own pipeline) ──
        conn.execute("""
            CREATE TABLE IF NOT EXISTS agency_prospects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT DEFAULT '',
                phone TEXT DEFAULT '',
                business_name TEXT DEFAULT '',
                website TEXT DEFAULT '',
                industry TEXT DEFAULT '',
                service_area TEXT DEFAULT '',
                source TEXT DEFAULT '',
                stage TEXT DEFAULT 'new',
                score INTEGER DEFAULT 0,
                monthly_budget TEXT DEFAULT '',
                notes TEXT DEFAULT '',
                assigned_to TEXT DEFAULT '',
                converted_brand_id INTEGER DEFAULT NULL,
                assessment_lead_id INTEGER DEFAULT NULL,
                signup_lead_id INTEGER DEFAULT NULL,
                last_contact_at TEXT DEFAULT '',
                next_follow_up TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (converted_brand_id) REFERENCES brands(id) ON DELETE SET NULL
            );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_agency_prospects_stage ON agency_prospects(stage);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_agency_prospects_email ON agency_prospects(email);")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS agency_prospect_notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prospect_id INTEGER NOT NULL,
                note_type TEXT DEFAULT 'note',
                content TEXT NOT NULL,
                created_by TEXT DEFAULT 'system',
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (prospect_id) REFERENCES agency_prospects(id) ON DELETE CASCADE
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS agency_prospect_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prospect_id INTEGER NOT NULL,
                direction TEXT NOT NULL DEFAULT 'outbound',
                channel TEXT DEFAULT 'email',
                subject TEXT DEFAULT '',
                content TEXT NOT NULL,
                status TEXT DEFAULT 'sent',
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (prospect_id) REFERENCES agency_prospects(id) ON DELETE CASCADE
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS agency_nurture_sequences (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT DEFAULT '',
                trigger_stage TEXT DEFAULT '',
                is_active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now'))
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS agency_nurture_steps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sequence_id INTEGER NOT NULL,
                step_order INTEGER DEFAULT 0,
                delay_days INTEGER DEFAULT 1,
                channel TEXT DEFAULT 'email',
                subject TEXT DEFAULT '',
                body_template TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (sequence_id) REFERENCES agency_nurture_sequences(id) ON DELETE CASCADE
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS agency_nurture_enrollments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prospect_id INTEGER NOT NULL,
                sequence_id INTEGER NOT NULL,
                current_step INTEGER DEFAULT 0,
                status TEXT DEFAULT 'active',
                enrolled_at TEXT DEFAULT (datetime('now')),
                completed_at TEXT DEFAULT '',
                FOREIGN KEY (prospect_id) REFERENCES agency_prospects(id) ON DELETE CASCADE,
                FOREIGN KEY (sequence_id) REFERENCES agency_nurture_sequences(id) ON DELETE CASCADE
            );
        """)

        # ── Stripe event log ──
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stripe_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT UNIQUE,
                event_type TEXT NOT NULL,
                brand_id INTEGER DEFAULT NULL,
                prospect_id INTEGER DEFAULT NULL,
                data_json TEXT DEFAULT '{}',
                created_at TEXT DEFAULT (datetime('now'))
            );
        """)

        # ── Email broadcast tracking ──
        conn.execute("""
            CREATE TABLE IF NOT EXISTS email_broadcasts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subject TEXT NOT NULL,
                body_text TEXT DEFAULT '',
                audience TEXT DEFAULT '',
                sent_by TEXT DEFAULT '',
                recipient_count INTEGER DEFAULT 0,
                open_count INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now'))
            );
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS email_broadcast_recipients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                broadcast_id INTEGER NOT NULL,
                email TEXT NOT NULL,
                name TEXT DEFAULT '',
                token TEXT UNIQUE NOT NULL,
                sent_at TEXT DEFAULT (datetime('now')),
                opened_at TEXT DEFAULT '',
                open_count INTEGER DEFAULT 0,
                FOREIGN KEY (broadcast_id) REFERENCES email_broadcasts(id) ON DELETE CASCADE
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_email_broadcast_recipients_token
            ON email_broadcast_recipients(token);
        """)

        # ── Warren bulk message history ──
        conn.execute("""
            CREATE TABLE IF NOT EXISTS warren_bulk_message_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                subject TEXT DEFAULT '',
                body_text TEXT DEFAULT '',
                groups_json TEXT DEFAULT '[]',
                channels_json TEXT DEFAULT '[]',
                sent_by TEXT DEFAULT '',
                recipient_count INTEGER DEFAULT 0,
                sent_sms INTEGER DEFAULT 0,
                sent_email INTEGER DEFAULT 0,
                skipped INTEGER DEFAULT 0,
                failed INTEGER DEFAULT 0,
                detail TEXT DEFAULT '',
                errors_json TEXT DEFAULT '{}',
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_warren_bulk_message_runs_brand_created
            ON warren_bulk_message_runs(brand_id, created_at DESC, id DESC);
        """)

        # ── AI Agent activity table ──
        conn.execute("""
            CREATE TABLE IF NOT EXISTS agent_activity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                agent_key TEXT NOT NULL,
                action TEXT NOT NULL,
                detail TEXT DEFAULT '',
                status TEXT NOT NULL DEFAULT 'completed',
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_agent_activity_brand
            ON agent_activity(brand_id, created_at DESC);
        """)

        # ── Agent findings table ──
        conn.execute("""
            CREATE TABLE IF NOT EXISTS agent_findings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                agent_key TEXT NOT NULL,
                month TEXT NOT NULL,
                severity TEXT NOT NULL DEFAULT 'info',
                title TEXT NOT NULL DEFAULT '',
                detail TEXT DEFAULT '',
                action TEXT DEFAULT '',
                extra_json TEXT DEFAULT '{}',
                dismissed INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_agent_findings_brand_month
            ON agent_findings(brand_id, month, dismissed);
        """)

        # ── Agent forecasts table (for backtesting and accuracy scoring) ──
        conn.execute("""
            CREATE TABLE IF NOT EXISTS agent_forecasts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                agent_key TEXT NOT NULL,
                created_month TEXT NOT NULL,
                target_month TEXT NOT NULL,
                forecast_json TEXT NOT NULL DEFAULT '{}',
                method TEXT NOT NULL DEFAULT 'seasonal_naive',
                features_json TEXT NOT NULL DEFAULT '{}',
                actual_json TEXT NOT NULL DEFAULT '{}',
                scored_at TEXT DEFAULT '',
                mae REAL DEFAULT NULL,
                mape REAL DEFAULT NULL,
                updated_at TEXT DEFAULT (datetime('now')),
                UNIQUE(brand_id, agent_key, target_month),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_agent_forecasts_brand_target
            ON agent_forecasts(brand_id, target_month);
        """)

        # ── Brand tasks table ──
        conn.execute("""
            CREATE TABLE IF NOT EXISTS brand_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                description TEXT DEFAULT '',
                steps_json TEXT DEFAULT '[]',
                status TEXT DEFAULT 'open',
                priority TEXT DEFAULT 'normal',
                source TEXT DEFAULT 'manual',
                source_ref TEXT DEFAULT '',
                assigned_to INTEGER DEFAULT NULL,
                created_by INTEGER DEFAULT NULL,
                due_date TEXT DEFAULT '',
                completed_at TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE,
                FOREIGN KEY (assigned_to) REFERENCES client_users(id) ON DELETE SET NULL,
                FOREIGN KEY (created_by) REFERENCES client_users(id) ON DELETE SET NULL
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_brand_tasks_brand
            ON brand_tasks(brand_id, status, assigned_to);
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS va_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                details TEXT DEFAULT '',
                specialty_key TEXT DEFAULT 'account_va',
                priority TEXT DEFAULT 'normal',
                status TEXT DEFAULT 'submitted',
                requested_by INTEGER DEFAULT NULL,
                estimated_tokens INTEGER DEFAULT 0,
                actual_tokens_used INTEGER DEFAULT 0,
                status_note TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                closed_at TEXT DEFAULT '',
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE,
                FOREIGN KEY (requested_by) REFERENCES client_users(id) ON DELETE SET NULL
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_va_requests_brand
            ON va_requests(brand_id, status, updated_at DESC, id DESC);
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS va_token_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                request_id INTEGER DEFAULT NULL,
                entry_type TEXT DEFAULT 'adjustment',
                specialty_key TEXT DEFAULT '',
                token_amount INTEGER NOT NULL DEFAULT 0,
                note TEXT DEFAULT '',
                created_by INTEGER DEFAULT NULL,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE,
                FOREIGN KEY (request_id) REFERENCES va_requests(id) ON DELETE SET NULL,
                FOREIGN KEY (created_by) REFERENCES client_users(id) ON DELETE SET NULL
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_va_token_ledger_brand
            ON va_token_ledger(brand_id, created_at DESC, id DESC);
        """)

        # ── Hiring tables ──
        conn.execute("""
            CREATE TABLE IF NOT EXISTS hiring_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                department TEXT DEFAULT '',
                job_type TEXT DEFAULT 'full-time',
                location TEXT DEFAULT '',
                remote TEXT DEFAULT 'no',
                description TEXT DEFAULT '',
                requirements TEXT DEFAULT '[]',
                nice_to_haves TEXT DEFAULT '[]',
                salary_min REAL DEFAULT 0,
                salary_max REAL DEFAULT 0,
                benefits TEXT DEFAULT '',
                screening_criteria TEXT DEFAULT '{}',
                scheduling_link TEXT DEFAULT '',
                status TEXT DEFAULT 'draft',
                generated_post TEXT DEFAULT '',
                created_by INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_hiring_jobs_brand
            ON hiring_jobs(brand_id, status);
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS hiring_candidates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                job_id INTEGER,
                name TEXT NOT NULL,
                email TEXT NOT NULL COLLATE NOCASE,
                phone TEXT DEFAULT '',
                source TEXT DEFAULT 'website',
                resume_text TEXT DEFAULT '',
                cover_letter TEXT DEFAULT '',
                status TEXT DEFAULT 'applied',
                ai_score INTEGER DEFAULT 0,
                score_breakdown TEXT DEFAULT '{}',
                ai_summary TEXT DEFAULT '',
                ai_recommendation TEXT DEFAULT '',
                interview_questions TEXT DEFAULT '[]',
                response_time_avg_sec INTEGER DEFAULT 0,
                applied_at TEXT DEFAULT (datetime('now')),
                screening_started_at TEXT DEFAULT '',
                screening_completed_at TEXT DEFAULT '',
                interview_scheduled_at TEXT DEFAULT '',
                hired_at TEXT DEFAULT '',
                notes TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE,
                FOREIGN KEY (job_id) REFERENCES hiring_jobs(id) ON DELETE SET NULL
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_hiring_candidates_brand
            ON hiring_candidates(brand_id, status, ai_score DESC);
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_hiring_candidates_email
            ON hiring_candidates(email);
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS hiring_interviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                candidate_id INTEGER NOT NULL,
                brand_id INTEGER NOT NULL,
                job_id INTEGER,
                token TEXT UNIQUE NOT NULL,
                status TEXT DEFAULT 'pending',
                current_question INTEGER DEFAULT 0,
                started_at TEXT DEFAULT '',
                completed_at TEXT DEFAULT '',
                expired_at TEXT DEFAULT '',
                total_score INTEGER DEFAULT 0,
                score_breakdown TEXT DEFAULT '{}',
                ai_evaluation TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (candidate_id) REFERENCES hiring_candidates(id) ON DELETE CASCADE,
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE,
                FOREIGN KEY (job_id) REFERENCES hiring_jobs(id) ON DELETE SET NULL
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_hiring_interviews_token
            ON hiring_interviews(token);
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_hiring_interviews_status
            ON hiring_interviews(candidate_id, status);
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS hiring_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                interview_id INTEGER NOT NULL,
                candidate_id INTEGER NOT NULL,
                direction TEXT NOT NULL,
                channel TEXT DEFAULT 'web_chat',
                content TEXT NOT NULL,
                is_question INTEGER DEFAULT 0,
                question_number INTEGER DEFAULT NULL,
                signal_scores TEXT DEFAULT '{}',
                response_time_sec INTEGER DEFAULT NULL,
                sent_at TEXT DEFAULT (datetime('now')),
                read_at TEXT DEFAULT '',
                FOREIGN KEY (interview_id) REFERENCES hiring_interviews(id) ON DELETE CASCADE,
                FOREIGN KEY (candidate_id) REFERENCES hiring_candidates(id) ON DELETE CASCADE
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_hiring_messages_interview
            ON hiring_messages(interview_id, sent_at);
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS hiring_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                template_type TEXT NOT NULL,
                name TEXT DEFAULT '',
                subject TEXT DEFAULT '',
                body TEXT DEFAULT '',
                is_default INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            );
        """)

        conn.commit()

        # ── Seed default feature flags ──
        DEFAULT_FLAGS = [
            ("dashboard",       "Overview",         "Main dashboard overview",           "all",   "main",     10),
            ("kpis",            "KPIs",             "Key performance indicators",        "all",   "main",     20),
            ("campaigns",       "Campaigns",        "Campaign list and management",      "all",   "main",     30),
            ("quick_launch",    "Quick Launch",     "One-click campaign launcher",       "all",   "main",     40),
            ("missions",        "Missions",         "Action items and tasks",            "all",   "main",     50),
            ("coaching",        "Coaching",         "AI coaching and learning",          "all",   "main",     60),
            ("ad_builder",      "Ad Builder",       "AI-powered ad copy generator",      "all",   "create",   70),
            ("creative",        "Creative",         "Image and creative generation",     "all",   "create",   80),
            ("blog",            "Blog",             "Blog post creation and publishing", "all",   "create",   90),
            ("my_business",     "My Business",      "Business profile and details",      "all",   "business", 100),
            ("crm",             "CRM",              "Customer relationship management",  "all",   "business", 110),
                ("commercial",      "Commercial",       "Commercial prospecting, proposals, and service follow-up", "all", "business", 110),
            ("warren_inbox",    "Lead Inbox",        "Warren AI lead inbox and pipeline",  "all",  "business", 111),
            ("va_services",     "VA Desk",           "Managed VA support, token packs, and work requests", "all", "business", 112),
            ("gbp",             "Google Profile",   "Google Business Profile manager",   "all",   "business", 120),
            ("post_scheduler",  "Post Scheduler",   "Social media post scheduling",      "all",   "business", 130),
            ("competitor_intel","Competitor Intel",  "Competitor analysis tools",         "all",   "business", 140),
            ("your_team",       "Your Team",         "AI agent team dashboard",           "all",   "business", 145),
            ("staff",           "Staff",             "Manage team members and roles",     "all",   "business", 146),
            ("tasks",           "Tasks",             "Task management and assignment",    "all",   "business", 147),
            ("hiring",          "Hiring Hub",        "AI-powered hiring and screening",   "beta",  "business", 148),
            ("site_builder",    "Site Builder",      "AI website generation and publishing", "all", "create",  95),
            ("connections",     "Connections",       "Platform connection settings",      "all",   "settings", 150),
            ("feedback",        "Feedback",          "Submit feedback to the team",       "all",   "settings", 160),
            ("help",            "Help",              "Help documentation and support",    "all",   "settings", 170),
        ]
        existing = {r[0] for r in conn.execute("SELECT feature_key FROM feature_flags").fetchall()}
        for key, label, desc, level, cat, sort in DEFAULT_FLAGS:
            if key not in existing:
                conn.execute(
                    "INSERT OR IGNORE INTO feature_flags (feature_key, label, description, access_level, category, sort_order) VALUES (?,?,?,?,?,?)",
                    (key, label, desc, level, cat, sort),
                )
        conn.commit()

        # ── Migrate existing beta/none flags to 'all' (one-time) ──
        _PROMOTE_TO_ALL = ("crm", "warren_inbox", "post_scheduler", "competitor_intel", "coaching")
        conn.execute(
            f"UPDATE feature_flags SET access_level = 'all' WHERE feature_key IN ({','.join('?' for _ in _PROMOTE_TO_ALL)}) AND access_level IN ('beta', 'none')",
            _PROMOTE_TO_ALL,
        )
        conn.commit()

        brand_columns = {r[1] for r in conn.execute("PRAGMA table_info(brands)").fetchall()}
        new_brand_cols = [
            ("brand_voice", "TEXT DEFAULT ''"),
            ("active_offers", "TEXT DEFAULT ''"),
            ("target_audience", "TEXT DEFAULT ''"),
            ("facebook_storytelling_strategy", "TEXT DEFAULT ''"),
            ("facebook_content_personality", "TEXT DEFAULT ''"),
            ("facebook_cta_style", "TEXT DEFAULT ''"),
            ("facebook_post_length", "TEXT DEFAULT ''"),
            ("facebook_storytelling_guardrails", "TEXT DEFAULT ''"),
            ("facebook_recurring_characters", "TEXT DEFAULT ''"),
            ("competitors", "TEXT DEFAULT ''"),
            ("reporting_notes", "TEXT DEFAULT ''"),
            ("feature_access_json", "TEXT DEFAULT '{}'"),
            ("upgrade_dev_email", "TEXT DEFAULT ''"),
            ("upgrade_contact_emails", "TEXT DEFAULT ''"),
            ("kpi_target_cpa", "REAL DEFAULT 0"),
            ("kpi_target_leads", "INTEGER DEFAULT 0"),
            ("kpi_target_roas", "REAL DEFAULT 0"),
            ("call_tracking_number", "TEXT DEFAULT ''"),
            ("crm_type", "TEXT DEFAULT ''"),
            ("crm_api_key", "TEXT DEFAULT ''"),
            ("crm_webhook_url", "TEXT DEFAULT ''"),
            ("crm_pipeline_id", "TEXT DEFAULT ''"),
            ("crm_server_url", "TEXT DEFAULT ''"),
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
            ("primary_color", "TEXT DEFAULT '#2563eb'"),
            ("accent_color", "TEXT DEFAULT '#f59e0b'"),
            ("font_heading", "TEXT DEFAULT 'Inter'"),
            ("font_body", "TEXT DEFAULT 'Inter'"),
            ("google_drive_folder_id", "TEXT DEFAULT ''"),
            ("google_drive_sheet_id", "TEXT DEFAULT ''"),
            ("facebook_page_id", "TEXT DEFAULT ''"),
            ("titan_snapshot_id", "TEXT DEFAULT ''"),
            ("titan_account_id", "TEXT DEFAULT ''"),
            ("titan_ghl_location_id", "TEXT DEFAULT ''"),
            ("titan_email", "TEXT DEFAULT ''"),
            ("business_lat", "REAL DEFAULT 0"),
            ("business_lng", "REAL DEFAULT 0"),
            ("google_place_id", "TEXT DEFAULT ''"),
            ("google_maps_api_key", "TEXT DEFAULT ''"),
            ("wp_site_url", "TEXT DEFAULT ''"),
            ("wp_username", "TEXT DEFAULT ''"),
            ("wp_app_password", "TEXT DEFAULT ''"),
            ("crm_avg_service_price", "REAL DEFAULT 0"),
            ("ai_quality_tier", "TEXT DEFAULT 'balanced'"),
            ("agent_context", "TEXT DEFAULT '{}'"),
            ("hired_agents", "TEXT DEFAULT '{}'"),
            ("quo_api_key", "TEXT DEFAULT ''"),
            ("quo_phone_number", "TEXT DEFAULT ''"),
            ("sales_bot_enabled", "INTEGER DEFAULT 0"),
            ("sales_bot_channels", "TEXT DEFAULT '[]'"),
            ("sales_bot_quote_mode", "TEXT DEFAULT 'hybrid'"),
            ("sales_bot_business_hours", "TEXT DEFAULT ''"),
            ("sales_bot_reply_tone", "TEXT DEFAULT ''"),
                ("sales_bot_reply_delay_seconds", "REAL DEFAULT 0"),
            ("sales_bot_service_menu", "TEXT DEFAULT ''"),
            ("sales_bot_pricing_notes", "TEXT DEFAULT ''"),
            ("sales_bot_guardrails", "TEXT DEFAULT ''"),
            ("sales_bot_example_language", "TEXT DEFAULT ''"),
            ("sales_bot_disallowed_language", "TEXT DEFAULT ''"),
            ("sales_bot_handoff_rules", "TEXT DEFAULT ''"),
            ("sales_bot_handoff_alert_phones", "TEXT DEFAULT ''"),
            ("sales_bot_quo_webhook_secret", "TEXT DEFAULT ''"),
            ("sales_bot_meta_webhook_secret", "TEXT DEFAULT ''"),
            ("sales_bot_incoming_webhook_secret", "TEXT DEFAULT ''"),
            ("sales_bot_lead_form_config", "TEXT DEFAULT '{}'"),
            ("sales_bot_transcript_export", "INTEGER DEFAULT 0"),
            ("sales_bot_meta_lead_forms", "INTEGER DEFAULT 0"),
            ("sales_bot_messenger_enabled", "INTEGER DEFAULT 0"),
            ("sales_bot_call_logging", "INTEGER DEFAULT 1"),
            ("sales_bot_auto_push_crm", "INTEGER DEFAULT 0"),
            ("sales_bot_nurture_enabled", "INTEGER DEFAULT 1"),
            ("sales_bot_nurture_hot_hours", "REAL DEFAULT 2"),
            ("sales_bot_nurture_hot_max", "INTEGER DEFAULT 3"),
            ("sales_bot_nurture_warm_hours", "REAL DEFAULT 24"),
            ("sales_bot_nurture_warm_max", "INTEGER DEFAULT 2"),
            ("sales_bot_nurture_cold_hours", "REAL DEFAULT 48"),
            ("sales_bot_nurture_cold_max", "INTEGER DEFAULT 2"),
            ("sales_bot_nurture_ghost_hours", "REAL DEFAULT 72"),
            ("sales_bot_dnd_enabled", "INTEGER DEFAULT 0"),
            ("sales_bot_dnd_start", "TEXT DEFAULT '21:00'"),
            ("sales_bot_dnd_end", "TEXT DEFAULT '08:00'"),
            ("sales_bot_dnd_timezone", "TEXT DEFAULT 'America/New_York'"),
            ("sales_bot_dnd_weekends", "INTEGER DEFAULT 0"),
            ("sales_bot_sms_opt_out_footer", "TEXT DEFAULT 'Reply STOP to opt out'"),
            ("sales_bot_objection_playbook", "TEXT DEFAULT ''"),
            ("sales_bot_message_templates", "TEXT DEFAULT ''"),
            ("sales_bot_collect_fields", "TEXT DEFAULT 'name,phone'"),
            ("sales_bot_closing_procedure", "TEXT DEFAULT ''"),
            ("sales_bot_booking_success_message", "TEXT DEFAULT ''"),
            ("sales_bot_service_area_schedule", "TEXT DEFAULT ''"),
            ("sales_bot_closing_action", "TEXT DEFAULT 'none'"),
            ("sales_bot_onboarding_link", "TEXT DEFAULT ''"),
            ("sales_bot_payment_reminders_enabled", "INTEGER DEFAULT 0"),
            ("sales_bot_payment_reminder_days_before", "INTEGER DEFAULT 3"),
            ("sales_bot_payment_reminder_billing_day", "INTEGER DEFAULT 1"),
            ("sales_bot_payment_reminder_channels", "TEXT DEFAULT 'email'"),
            ("sales_bot_payment_reminder_template", "TEXT DEFAULT ''"),
            ("sales_bot_appointment_reminders_enabled", "INTEGER DEFAULT 0"),
            ("sales_bot_appointment_reminder_send_time", "TEXT DEFAULT '17:00'"),
            ("sales_bot_appointment_reminder_timezone", "TEXT DEFAULT 'America/New_York'"),
            ("sales_bot_appointment_reminder_channels", "TEXT DEFAULT 'sms'"),
            ("sales_bot_appointment_reminder_template", "TEXT DEFAULT ''"),
            ("sales_bot_appointment_reminder_respect_client_channel", "INTEGER DEFAULT 1"),
            ("sales_bot_appointment_reminder_last_attempt_at", "TEXT DEFAULT ''"),
            ("sales_bot_sng_webhook_secret", "TEXT DEFAULT ''"),
            ("sales_bot_crm_event_rules", "TEXT DEFAULT '{}'"),
            ("sales_bot_crm_event_alert_emails", "TEXT DEFAULT ''"),
            ("hiring_design", "TEXT DEFAULT '{}'"),
            # Stripe billing
            ("stripe_customer_id", "TEXT DEFAULT ''"),
            ("stripe_subscription_id", "TEXT DEFAULT ''"),
            ("stripe_plan", "TEXT DEFAULT ''"),
            ("stripe_status", "TEXT DEFAULT ''"),
            ("stripe_mrr", "REAL DEFAULT 0"),
            ("stripe_trial_end", "TEXT DEFAULT ''"),
            ("stripe_next_invoice", "TEXT DEFAULT ''"),
            ("stripe_payment_method_last4", "TEXT DEFAULT ''"),
            ("onboarded_at", "TEXT DEFAULT ''"),
            ("churned_at", "TEXT DEFAULT ''"),
        ]
        for col_name, col_def in new_brand_cols:
            if col_name not in brand_columns:
                self._safe_add_column(conn, "brands", col_name, col_def)
        conn.commit()

        heatmap_columns = {r[1] for r in conn.execute("PRAGMA table_info(heatmap_scans)").fetchall()}
        new_heatmap_cols = [
            ("status", "TEXT NOT NULL DEFAULT 'complete'"),
            ("error_message", "TEXT NOT NULL DEFAULT ''"),
            ("debug_json", "TEXT NOT NULL DEFAULT '{}'"),
        ]
        for col_name, col_def in new_heatmap_cols:
            if col_name not in heatmap_columns:
                self._safe_add_column(conn, "heatmap_scans", col_name, col_def)
        conn.commit()

        # ── agent_findings migrations ──
        af_columns = {r[1] for r in conn.execute("PRAGMA table_info(agent_findings)").fetchall()}
        new_af_cols = [
            ("status", "TEXT DEFAULT 'new'"),           # new|acknowledged|in_progress|done|dismissed
            ("user_vote", "INTEGER DEFAULT 0"),          # 1=thumbs up, -1=thumbs down, 0=no vote
            ("user_feedback", "TEXT DEFAULT ''"),         # why they voted down
            ("outcome_note", "TEXT DEFAULT ''"),          # retrospective: what happened after acting
        ]
        for col_name, col_def in new_af_cols:
            if col_name not in af_columns:
                self._safe_add_column(conn, "agent_findings", col_name, col_def)
        conn.commit()

        # ── campaign_strategies migrations ──
        cs_columns = {r[1] for r in conn.execute("PRAGMA table_info(campaign_strategies)").fetchall()}
        new_cs_cols = [
            ("blueprint", "TEXT DEFAULT ''"),
        ]
        for col_name, col_def in new_cs_cols:
            if col_name not in cs_columns:
                self._safe_add_column(conn, "campaign_strategies", col_name, col_def)
        conn.commit()

        # ── beta_testers migrations ──
        bt_columns = {r[1] for r in conn.execute("PRAGMA table_info(beta_testers)").fetchall()}
        new_bt_cols = [
            ("facebook_page_id", "TEXT DEFAULT ''"),
            ("google_business_email", "TEXT DEFAULT ''"),
            ("meta_login_email", "TEXT DEFAULT ''"),
            ("onboarding_token", "TEXT DEFAULT ''"),
            ("onboarding_completed_at", "TEXT DEFAULT ''"),
            ("activated_at", "TEXT DEFAULT ''"),
            ("temp_password", "TEXT DEFAULT ''"),
        ]
        for col_name, col_def in new_bt_cols:
            if col_name not in bt_columns:
                self._safe_add_column(conn, "beta_testers", col_name, col_def)
        conn.commit()

        # ── feedback_ai_runs migrations ──
        fair_columns = {r[1] for r in conn.execute("PRAGMA table_info(feedback_ai_runs)").fetchall()}
        new_fair_cols = [
            ("scope_type", "TEXT NOT NULL DEFAULT 'status'"),
            ("scope_label", "TEXT DEFAULT ''"),
            ("feedback_ids_json", "TEXT DEFAULT '[]'"),
            ("filters_json", "TEXT DEFAULT '{}'"),
            ("model", "TEXT DEFAULT ''"),
            ("prompt_version", "TEXT DEFAULT ''"),
            ("summary_json", "TEXT DEFAULT '{}'"),
            ("dev_plan_json", "TEXT DEFAULT '{}'"),
            ("created_by", "INTEGER DEFAULT NULL"),
        ]
        for col_name, col_def in new_fair_cols:
            if col_name not in fair_columns:
                self._safe_add_column(conn, "feedback_ai_runs", col_name, col_def)
        conn.commit()

        competitor_columns = {r[1] for r in conn.execute("PRAGMA table_info(competitors)").fetchall()}
        new_competitor_cols = [
            ("gbp_cid", "TEXT DEFAULT ''"),
        ]
        for col_name, col_def in new_competitor_cols:
            if col_name not in competitor_columns:
                self._safe_add_column(conn, "competitors", col_name, col_def)
        conn.commit()

        scheduled_post_columns = {r[1] for r in conn.execute("PRAGMA table_info(scheduled_posts)").fetchall()}
        new_scheduled_post_cols = [
            ("post_type", "TEXT DEFAULT ''"),
        ]
        for col_name, col_def in new_scheduled_post_cols:
            if col_name not in scheduled_post_columns:
                self._safe_add_column(conn, "scheduled_posts", col_name, col_def)
        conn.commit()

        # ── feedback_ai_drafts migrations ──
        faid_columns = {r[1] for r in conn.execute("PRAGMA table_info(feedback_ai_drafts)").fetchall()}
        new_faid_cols = [
            ("run_id", "INTEGER DEFAULT NULL"),
            ("reply_subject", "TEXT DEFAULT ''"),
            ("reply_draft", "TEXT DEFAULT ''"),
            ("internal_note", "TEXT DEFAULT ''"),
            ("recommended_status", "TEXT DEFAULT 'reviewed'"),
            ("confidence", "REAL DEFAULT 0"),
            ("needs_manual_review", "INTEGER DEFAULT 0"),
            ("approved", "INTEGER DEFAULT 0"),
            ("approved_by", "INTEGER DEFAULT NULL"),
            ("approved_at", "TEXT DEFAULT ''"),
            ("sent_at", "TEXT DEFAULT ''"),
            ("send_error", "TEXT DEFAULT ''"),
            ("generated_at", "TEXT DEFAULT (datetime('now'))"),
            ("updated_at", "TEXT DEFAULT (datetime('now'))"),
        ]
        for col_name, col_def in new_faid_cols:
            if col_name not in faid_columns:
                self._safe_add_column(conn, "feedback_ai_drafts", col_name, col_def)
        conn.commit()

        # ── client_users migrations ──
        cu_columns = {r[1] for r in conn.execute("PRAGMA table_info(client_users)").fetchall()}
        new_cu_cols = [
            ("password_reset_token", "TEXT DEFAULT ''"),
            ("reset_token_expires", "TEXT DEFAULT ''"),
            ("role", "TEXT DEFAULT 'owner'"),
            ("invited_by", "INTEGER DEFAULT NULL"),
        ]
        for col_name, col_def in new_cu_cols:
            if col_name not in cu_columns:
                self._safe_add_column(conn, "client_users", col_name, col_def)
        conn.commit()

        # ── lead_threads migrations ──
        lt_columns = {r[1] for r in conn.execute("PRAGMA table_info(lead_threads)").fetchall()}
        new_lt_cols = [
            ("is_private", "INTEGER DEFAULT 0"),
            ("commercial_data_json", "TEXT DEFAULT '{}'"),
        ]
        for col_name, col_def in new_lt_cols:
            if col_name not in lt_columns:
                self._safe_add_column(conn, "lead_threads", col_name, col_def)
        conn.commit()

        # ── hiring_jobs migrations ──
        hj_columns = {r[1] for r in conn.execute("PRAGMA table_info(hiring_jobs)").fetchall()}
        new_hj_cols = [
            ("gate_questions", "TEXT DEFAULT '[]'"),
            ("auto_send_interview", "INTEGER DEFAULT 0"),
        ]
        for col_name, col_def in new_hj_cols:
            if col_name not in hj_columns:
                self._safe_add_column(conn, "hiring_jobs", col_name, col_def)
        conn.commit()

        # ── hiring_interviews migrations ──
        hi_columns = {r[1] for r in conn.execute("PRAGMA table_info(hiring_interviews)").fetchall()}
        new_hi_cols = [
            ("gate_answers", "TEXT DEFAULT '{}'"),
            ("gate_passed", "INTEGER DEFAULT 1"),
        ]
        for col_name, col_def in new_hi_cols:
            if col_name not in hi_columns:
                self._safe_add_column(conn, "hiring_interviews", col_name, col_def)
        conn.commit()

        # ── hiring_candidates migrations ──
        hc_columns = {r[1] for r in conn.execute("PRAGMA table_info(hiring_candidates)").fetchall()}
        new_hc_cols = [
            ("signal_reasoning", "TEXT DEFAULT '{}'"),
            ("key_moments", "TEXT DEFAULT '[]'"),
            ("social_scan", "TEXT DEFAULT '{}'"),
        ]
        for col_name, col_def in new_hc_cols:
            if col_name not in hc_columns:
                self._safe_add_column(conn, "hiring_candidates", col_name, col_def)
        conn.commit()

        # ── agency_prospects migrations ──
        ap_columns = {r[1] for r in conn.execute("PRAGMA table_info(agency_prospects)").fetchall()}
        new_ap_cols = [
            ("account_type", "TEXT DEFAULT ''"),
            ("decision_maker_role", "TEXT DEFAULT ''"),
            ("property_count", "TEXT DEFAULT ''"),
            ("current_vendor_status", "TEXT DEFAULT ''"),
            ("outreach_angle", "TEXT DEFAULT ''"),
            ("proposal_status", "TEXT DEFAULT 'not_ready'"),
            ("next_action", "TEXT DEFAULT ''"),
            ("source_details_json", "TEXT DEFAULT '{}'"),
            ("audit_snapshot_json", "TEXT DEFAULT '{}'"),
            ("pain_points_json", "TEXT DEFAULT '[]'"),
            ("qualification_answers_json", "TEXT DEFAULT '{}'"),
        ]
        for col_name, col_def in new_ap_cols:
            if col_name not in ap_columns:
                self._safe_add_column(conn, "agency_prospects", col_name, col_def)
        conn.commit()

        # ── site_builds migrations (intake data) ──
        sb_columns = {r[1] for r in conn.execute("PRAGMA table_info(site_builds)").fetchall()}
        new_sb_cols = [
            ("intake_json", "TEXT DEFAULT '{}'"),
        ]
        for col_name, col_def in new_sb_cols:
            if col_name not in sb_columns:
                self._safe_add_column(conn, "site_builds", col_name, col_def)
        conn.commit()

        # ── site_pages migrations (visual editor) ──
        sp_columns = {r[1] for r in conn.execute("PRAGMA table_info(site_pages)").fetchall()}
        new_sp_cols = [
            ("editor_json", "TEXT DEFAULT ''"),
            ("page_css", "TEXT DEFAULT ''"),
        ]
        for col_name, col_def in new_sp_cols:
            if col_name not in sp_columns:
                self._safe_add_column(conn, "site_pages", col_name, col_def)
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

        # ── Dashboard Snapshots (Phase 1 cache layer) ──
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dashboard_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand_id INTEGER NOT NULL,
                month TEXT NOT NULL,
                snapshot_json TEXT NOT NULL DEFAULT '{}',
                source TEXT NOT NULL DEFAULT 'auto',
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(brand_id, month),
                FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_dashboard_snapshots_brand_month
            ON dashboard_snapshots(brand_id, month);
        """)
        conn.commit()

        conn.close()

    # ── Users ──

    def get_users(self):
        conn = self._conn()
        rows = conn.execute("SELECT * FROM users").fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_users_with_email(self):
        recipients = []
        for user in self.get_users():
            email = self._normalize_email(user.get("username"))
            if "@" not in email:
                continue
            recipients.append({
                **user,
                "email": email,
                "recipient_name": user.get("display_name") or email,
            })
        return recipients

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
            "website",
            "brand_voice", "active_offers", "target_audience", "competitors",
            "facebook_storytelling_strategy", "facebook_content_personality", "facebook_cta_style",
            "facebook_post_length",
            "facebook_storytelling_guardrails", "facebook_recurring_characters",
            "feature_access_json", "upgrade_dev_email", "upgrade_contact_emails",
            "reporting_notes", "call_tracking_number",
            "crm_type", "crm_api_key", "crm_webhook_url", "crm_pipeline_id", "crm_server_url",
            "openai_api_key", "openai_model",
            "openai_model_chat", "openai_model_images", "openai_model_analysis", "openai_model_ads",
            "ai_quality_tier",
            "display_name", "industry", "service_area", "primary_services",
            "logo_path", "logo_variants", "brand_colors", "primary_color", "accent_color",
            "font_heading", "font_body",
            "google_drive_folder_id", "google_drive_sheet_id",
            "google_maps_api_key", "google_place_id",
            "titan_snapshot_id", "titan_account_id", "titan_ghl_location_id", "titan_email",
            "wp_site_url", "wp_username", "wp_app_password",
            "hired_agents", "agent_context",
            "quo_api_key", "quo_phone_number",
            "sales_bot_channels", "sales_bot_quote_mode", "sales_bot_business_hours",
            "sales_bot_reply_tone", "sales_bot_service_menu", "sales_bot_pricing_notes",
            "sales_bot_guardrails", "sales_bot_example_language",
            "sales_bot_disallowed_language", "sales_bot_handoff_rules",
            "sales_bot_handoff_alert_phones",
            "sales_bot_quo_webhook_secret", "sales_bot_meta_webhook_secret",
            "sales_bot_incoming_webhook_secret", "sales_bot_sng_webhook_secret", "sales_bot_lead_form_config",
            "sales_bot_objection_playbook", "sales_bot_message_templates", "sales_bot_collect_fields",
            "sales_bot_closing_procedure", "sales_bot_booking_success_message",
            "sales_bot_service_area_schedule", "sales_bot_closing_action", "sales_bot_onboarding_link",
            "sales_bot_payment_reminder_channels", "sales_bot_payment_reminder_template",
            "sales_bot_appointment_reminder_send_time", "sales_bot_appointment_reminder_timezone",
            "sales_bot_appointment_reminder_channels", "sales_bot_appointment_reminder_template",
            "sales_bot_appointment_reminder_last_attempt_at",
            "sales_bot_crm_event_rules", "sales_bot_crm_event_alert_emails",
            "sales_bot_dnd_start", "sales_bot_dnd_end", "sales_bot_dnd_timezone",
            "sales_bot_sms_opt_out_footer",
            "hiring_design",
        }
        if field not in allowed:
            raise ValueError(f"Cannot update field: {field}")
        conn = self._conn()
        conn.execute(f"UPDATE brands SET {field}=?, updated_at=datetime('now') WHERE id=?", (value or "", brand_id))
        conn.commit()
        conn.close()

    def get_brand_feature_access(self, brand_id):
        brand = self.get_brand(brand_id) or {}
        try:
            raw = json.loads(brand.get("feature_access_json") or "{}")
        except Exception:
            raw = {}
        if not isinstance(raw, dict):
            return {}
        return {
            str(key).strip(): self._normalize_feature_state(value)
            for key, value in raw.items()
            if str(key).strip()
        }

    def update_brand_feature_access(self, brand_id, feature_access):
        normalized = {}
        for key, value in (feature_access or {}).items():
            feature_key = str(key or "").strip()
            if not feature_key:
                continue
            normalized[feature_key] = self._normalize_feature_state(value)
        conn = self._conn()
        conn.execute(
            "UPDATE brands SET feature_access_json=?, updated_at=datetime('now') WHERE id=?",
            (json.dumps(normalized, separators=(",", ":")), brand_id),
        )
        conn.commit()
        conn.close()

    def get_brand_upgrade_contacts(self, brand_id):
        brand = self.get_brand(brand_id) or {}
        recipients = []
        seen = set()

        def _append(email, name, role):
            normalized = self._normalize_email(email)
            if "@" not in normalized or normalized in seen:
                return
            seen.add(normalized)
            recipients.append({
                "email": normalized,
                "name": name or normalized,
                "role": role,
            })

        _append(brand.get("upgrade_dev_email"), "Developer Contact", "developer")
        for email in self._parse_email_list(brand.get("upgrade_contact_emails")):
            _append(email, email, "contact")
        return recipients

    def update_brand_number_field(self, brand_id, field, value):
        allowed = {
            "kpi_target_cpa", "kpi_target_leads", "kpi_target_roas",
            "business_lat", "business_lng", "crm_avg_service_price",
            "sales_bot_enabled", "sales_bot_transcript_export", "sales_bot_meta_lead_forms",
            "sales_bot_messenger_enabled", "sales_bot_call_logging", "sales_bot_auto_push_crm",
            "sales_bot_reply_delay_seconds",
            "sales_bot_payment_reminders_enabled", "sales_bot_payment_reminder_days_before",
            "sales_bot_payment_reminder_billing_day",
            "sales_bot_appointment_reminders_enabled", "sales_bot_appointment_reminder_respect_client_channel",
            "sales_bot_nurture_enabled", "sales_bot_nurture_hot_hours", "sales_bot_nurture_hot_max",
            "sales_bot_nurture_warm_hours", "sales_bot_nurture_warm_max",
            "sales_bot_nurture_cold_hours", "sales_bot_nurture_cold_max",
            "sales_bot_nurture_ghost_hours", "sales_bot_dnd_enabled", "sales_bot_dnd_weekends",
        }
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

    # ── Leads Inbox ──

    def create_lead_thread(self, brand_id, data):
        conn = self._conn()
        cur = conn.execute(
            """
            INSERT INTO lead_threads (
                brand_id, lead_name, lead_email, lead_phone, source, channel,
                external_thread_id, status, quote_status, assigned_to, summary, commercial_data_json,
                last_message_at, last_inbound_at, last_outbound_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?, ?, datetime('now'))
            """,
            (
                brand_id,
                (data.get("lead_name") or "").strip(),
                (data.get("lead_email") or "").strip().lower(),
                (data.get("lead_phone") or "").strip(),
                (data.get("source") or "").strip(),
                (data.get("channel") or "sms").strip() or "sms",
                ((data.get("external_thread_id") or "").strip() or None),
                (data.get("status") or "new").strip() or "new",
                (data.get("quote_status") or "not_started").strip() or "not_started",
                (data.get("assigned_to") or "").strip(),
                (data.get("summary") or "").strip(),
                (data.get("commercial_data_json") or "{}").strip() or "{}",
                (data.get("last_inbound_at") or "").strip(),
                (data.get("last_outbound_at") or "").strip(),
            ),
        )
        conn.commit()
        thread_id = cur.lastrowid
        conn.close()
        return thread_id

    def get_lead_thread(self, thread_id, brand_id=None):
        conn = self._conn()
        if brand_id is None:
            row = conn.execute("SELECT * FROM lead_threads WHERE id = ?", (thread_id,)).fetchone()
        else:
            row = conn.execute(
                "SELECT * FROM lead_threads WHERE id = ? AND brand_id = ?",
                (thread_id, brand_id),
            ).fetchone()
        conn.close()
        return dict(row) if row else None

    def get_lead_threads(self, brand_id, status=None, limit=100):
        conn = self._conn()
        if status:
            rows = conn.execute(
                """
                SELECT * FROM lead_threads
                WHERE brand_id = ? AND status = ?
                ORDER BY last_message_at DESC, updated_at DESC, id DESC
                LIMIT ?
                """,
                (brand_id, status, int(limit or 100)),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT * FROM lead_threads
                WHERE brand_id = ?
                ORDER BY last_message_at DESC, updated_at DESC, id DESC
                LIMIT ?
                """,
                (brand_id, int(limit or 100)),
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_active_lead_contacts(self, brand_id, limit=500):
        conn = self._conn()
        rows = conn.execute(
            """
            SELECT * FROM lead_threads
            WHERE brand_id = ? AND status NOT IN ('won', 'lost')
            ORDER BY updated_at DESC, last_message_at DESC, id DESC
            LIMIT ?
            """,
            (brand_id, int(limit or 500)),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def find_brand_lead_thread(self, brand_id, *, email="", lead_name="", source="", website=""):
        conn = self._conn()
        normalized_email = self._normalize_email(email)
        normalized_name = (lead_name or "").strip().lower()
        normalized_source = (source or "").strip().lower()
        normalized_website = self._normalize_website(website)
        row = None
        if normalized_email:
            if normalized_source:
                row = conn.execute(
                    "SELECT * FROM lead_threads WHERE brand_id = ? AND lower(lead_email) = ? AND lower(source) = ? ORDER BY id DESC LIMIT 1",
                    (brand_id, normalized_email, normalized_source),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM lead_threads WHERE brand_id = ? AND lower(lead_email) = ? ORDER BY id DESC LIMIT 1",
                    (brand_id, normalized_email),
                ).fetchone()
        if not row and normalized_website:
            if normalized_source:
                rows = conn.execute(
                    "SELECT * FROM lead_threads WHERE brand_id = ? AND lower(source) = ? ORDER BY id DESC LIMIT 250",
                    (brand_id, normalized_source),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM lead_threads WHERE brand_id = ? ORDER BY id DESC LIMIT 250",
                    (brand_id,),
                ).fetchall()
            for candidate in rows:
                payload = self._safe_json_object(candidate["commercial_data_json"])
                source_details = self._safe_json_object(payload.get("source_details_json"))
                candidate_websites = {
                    self._normalize_website(payload.get("website")),
                    self._normalize_website(source_details.get("website")),
                }
                candidate_websites.discard("")
                if normalized_website in candidate_websites:
                    row = candidate
                    break
        if not row and normalized_name:
            if normalized_source:
                row = conn.execute(
                    "SELECT * FROM lead_threads WHERE brand_id = ? AND lower(lead_name) = ? AND lower(source) = ? ORDER BY id DESC LIMIT 1",
                    (brand_id, normalized_name, normalized_source),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM lead_threads WHERE brand_id = ? AND lower(lead_name) = ? ORDER BY id DESC LIMIT 1",
                    (brand_id, normalized_name),
                ).fetchone()
        conn.close()
        return dict(row) if row else None

    def upsert_lead_thread(self, brand_id, channel, external_thread_id, data=None):
        data = data or {}
        normalized_channel = (channel or "sms").strip() or "sms"
        normalized_external_id = (external_thread_id or "").strip()
        if not normalized_external_id:
            return self.create_lead_thread(
                brand_id,
                {
                    **data,
                    "channel": normalized_channel,
                    "external_thread_id": None,
                },
            )

        conn = self._conn()
        row = conn.execute(
            "SELECT id FROM lead_threads WHERE brand_id = ? AND channel = ? AND external_thread_id = ?",
            (brand_id, normalized_channel, normalized_external_id),
        ).fetchone()
        if row:
            updates = []
            values = []
            for field in ("lead_name", "lead_email", "lead_phone", "source", "status", "quote_status", "assigned_to", "summary", "commercial_data_json"):
                if field in data and data.get(field) is not None:
                    updates.append(f"{field} = ?")
                    value = data.get(field) or ""
                    if field == "lead_email":
                        value = value.strip().lower()
                    elif isinstance(value, str):
                        value = value.strip()
                    values.append(value)
            updates.append("updated_at = datetime('now')")
            values.extend([brand_id, normalized_channel, normalized_external_id])
            if updates:
                conn.execute(
                    f"UPDATE lead_threads SET {', '.join(updates)} WHERE brand_id = ? AND channel = ? AND external_thread_id = ?",
                    values,
                )
                conn.commit()
            thread_id = row["id"]
        else:
            cur = conn.execute(
                """
                INSERT INTO lead_threads (
                    brand_id, lead_name, lead_email, lead_phone, source, channel,
                    external_thread_id, status, quote_status, assigned_to, summary, commercial_data_json,
                    last_message_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
                """,
                (
                    brand_id,
                    (data.get("lead_name") or "").strip(),
                    (data.get("lead_email") or "").strip().lower(),
                    (data.get("lead_phone") or "").strip(),
                    (data.get("source") or "").strip(),
                    normalized_channel,
                    normalized_external_id,
                    (data.get("status") or "new").strip() or "new",
                    (data.get("quote_status") or "not_started").strip() or "not_started",
                    (data.get("assigned_to") or "").strip(),
                    (data.get("summary") or "").strip(),
                    (data.get("commercial_data_json") or "{}").strip() or "{}",
                ),
            )
            conn.commit()
            thread_id = cur.lastrowid
        conn.close()
        return thread_id

    def update_lead_thread_status(self, thread_id, *, status=None, quote_status=None, assigned_to=None, summary=None):
        updates = []
        values = []
        if status is not None:
            updates.append("status = ?")
            values.append((status or "new").strip() or "new")
        if quote_status is not None:
            updates.append("quote_status = ?")
            values.append((quote_status or "not_started").strip() or "not_started")
        if assigned_to is not None:
            updates.append("assigned_to = ?")
            values.append((assigned_to or "").strip())
        if summary is not None:
            updates.append("summary = ?")
            values.append((summary or "").strip())
        if not updates:
            return
        updates.append("updated_at = datetime('now')")
        values.append(thread_id)
        conn = self._conn()
        conn.execute(
            f"UPDATE lead_threads SET {', '.join(updates)} WHERE id = ?",
            values,
        )
        conn.commit()
        conn.close()

    def update_lead_thread_profile_fields(self, thread_id, brand_id, **fields):
        allowed_fields = {"lead_name", "lead_phone", "lead_email", "summary"}
        updates = []
        values = []
        for field, value in fields.items():
            if field not in allowed_fields or value is None:
                continue
            cleaned = value.strip() if isinstance(value, str) else value
            if field == "lead_email" and isinstance(cleaned, str):
                cleaned = cleaned.lower()
            updates.append(f"{field} = ?")
            values.append(cleaned)
        if not updates:
            return
        updates.append("updated_at = datetime('now')")
        values.extend([thread_id, brand_id])
        conn = self._conn()
        conn.execute(
            f"UPDATE lead_threads SET {', '.join(updates)} WHERE id = ? AND brand_id = ?",
            values,
        )
        conn.commit()
        conn.close()

    def update_lead_thread_commercial_data(self, thread_id, brand_id, commercial_data_json):
        conn = self._conn()
        conn.execute(
            "UPDATE lead_threads SET commercial_data_json = ?, updated_at = datetime('now') WHERE id = ? AND brand_id = ?",
            ((commercial_data_json or "{}").strip() or "{}", thread_id, brand_id),
        )
        conn.commit()
        conn.close()

    def mark_lead_thread_read(self, thread_id):
        conn = self._conn()
        conn.execute(
            "UPDATE lead_threads SET unread_count = 0, updated_at = datetime('now') WHERE id = ?",
            (thread_id,),
        )
        conn.commit()
        conn.close()

    def toggle_lead_thread_private(self, thread_id, brand_id):
        """Toggle the is_private flag on a lead thread. Returns the new value."""
        conn = self._conn()
        row = conn.execute(
            "SELECT is_private FROM lead_threads WHERE id = ? AND brand_id = ?",
            (thread_id, brand_id),
        ).fetchone()
        if not row:
            conn.close()
            return None
        new_val = 0 if row["is_private"] else 1
        conn.execute(
            "UPDATE lead_threads SET is_private = ?, updated_at = datetime('now') WHERE id = ? AND brand_id = ?",
            (new_val, thread_id, brand_id),
        )
        conn.commit()
        conn.close()
        return new_val

    def delete_lead_thread(self, thread_id, brand_id):
        """Delete a lead thread and all related data (messages, events, quotes cascade)."""
        conn = self._conn()
        conn.execute(
            "DELETE FROM lead_threads WHERE id = ? AND brand_id = ?",
            (thread_id, brand_id),
        )
        conn.commit()
        conn.close()

    def add_lead_message(self, thread_id, direction, role, content, channel="", external_message_id="", metadata=None):
        normalized_direction = (direction or "inbound").strip().lower()
        if normalized_direction not in {"inbound", "outbound"}:
            raise ValueError("direction must be 'inbound' or 'outbound'")
        normalized_role = (role or "lead").strip().lower()
        if normalized_role not in {"lead", "assistant", "user", "system"}:
            raise ValueError("role must be one of: lead, assistant, user, system")

        conn = self._conn()
        cur = conn.execute(
            """
            INSERT INTO lead_messages (
                thread_id, direction, role, channel, external_message_id, content, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                thread_id,
                normalized_direction,
                normalized_role,
                (channel or "").strip(),
                (external_message_id or "").strip(),
                content or "",
                json.dumps(metadata or {}),
            ),
        )
        timestamp_field = "last_inbound_at" if normalized_direction == "inbound" else "last_outbound_at"
        unread_sql = ", unread_count = unread_count + 1" if normalized_direction == "inbound" else ""
        conn.execute(
            f"""
            UPDATE lead_threads
            SET last_message_at = datetime('now'),
                {timestamp_field} = datetime('now'),
                updated_at = datetime('now')
                {unread_sql}
            WHERE id = ?
            """,
            (thread_id,),
        )
        conn.commit()
        message_id = cur.lastrowid
        conn.close()
        return message_id

    def get_lead_messages(self, thread_id, limit=200):
        conn = self._conn()
        rows = conn.execute(
            """
            SELECT * FROM lead_messages
            WHERE thread_id = ?
            ORDER BY created_at ASC, id ASC
            LIMIT ?
            """,
            (thread_id, int(limit or 200)),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def update_lead_message_metadata(self, message_id, metadata_updates):
        if not message_id or not isinstance(metadata_updates, dict):
            return False
        conn = self._conn()
        row = conn.execute(
            "SELECT metadata_json FROM lead_messages WHERE id = ?",
            (message_id,),
        ).fetchone()
        if not row:
            conn.close()
            return False
        try:
            metadata = json.loads(row["metadata_json"] or "{}")
            if not isinstance(metadata, dict):
                metadata = {}
        except Exception:
            metadata = {}
        metadata.update(metadata_updates)
        conn.execute(
            "UPDATE lead_messages SET metadata_json = ? WHERE id = ?",
            (json.dumps(metadata), message_id),
        )
        conn.commit()
        conn.close()
        return True

    def add_lead_event(self, brand_id, thread_id, event_type, event_value="", metadata=None):
        conn = self._conn()
        cur = conn.execute(
            "INSERT INTO lead_events (brand_id, thread_id, event_type, event_value, metadata_json) VALUES (?, ?, ?, ?, ?)",
            (brand_id, thread_id, (event_type or "").strip(), event_value or "", json.dumps(metadata or {})),
        )
        conn.commit()
        event_id = cur.lastrowid
        conn.close()
        return event_id

    def get_lead_events(self, brand_id_or_thread=None, thread_id=None, event_type=None, limit=100):
        """Get lead events. Supports old call style (thread_id) and new style (brand_id, thread_id, event_type)."""
        # Handle old-style call: get_lead_events(thread_id)
        if thread_id is None and brand_id_or_thread is not None:
            thread_id = brand_id_or_thread

        conn = self._conn()
        sql = "SELECT * FROM lead_events WHERE thread_id = ?"
        params = [thread_id]
        if event_type:
            sql += " AND event_type = ?"
            params.append(event_type)
        sql += " ORDER BY created_at DESC, id DESC LIMIT ?"
        params.append(int(limit or 100))
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def add_commercial_service_visit(
        self,
        brand_id,
        thread_id,
        *,
        service_date="",
        completed_at="",
        completed_by="",
        property_label="",
        summary="",
        waste_station_count_serviced=0,
        bags_restocked=False,
        gate_secured=False,
        issues=None,
        photos=None,
        client_note="",
        internal_note="",
    ):
        conn = self._conn()
        cur = conn.execute(
            """
            INSERT INTO commercial_service_visits (
                brand_id, thread_id, service_date, completed_at, completed_by, property_label, summary,
                waste_station_count_serviced, bags_restocked, gate_secured, issues_json, photos_json,
                client_note, internal_note, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            """,
            (
                brand_id,
                thread_id,
                (service_date or "").strip(),
                (completed_at or "").strip(),
                (completed_by or "").strip(),
                (property_label or "").strip(),
                (summary or "").strip(),
                max(0, int(waste_station_count_serviced or 0)),
                1 if bags_restocked else 0,
                1 if gate_secured else 0,
                json.dumps(issues or []),
                json.dumps(photos or []),
                (client_note or "").strip(),
                (internal_note or "").strip(),
            ),
        )
        conn.commit()
        visit_id = cur.lastrowid
        row = conn.execute(
            "SELECT * FROM commercial_service_visits WHERE id = ?",
            (visit_id,),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def get_commercial_service_visits(self, thread_id, limit=25):
        conn = self._conn()
        rows = conn.execute(
            """
            SELECT * FROM commercial_service_visits
            WHERE thread_id = ?
            ORDER BY COALESCE(NULLIF(service_date, ''), created_at) DESC, id DESC
            LIMIT ?
            """,
            (thread_id, int(limit or 25)),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_lead_quote_for_thread(self, thread_id):
        conn = self._conn()
        row = conn.execute("SELECT * FROM lead_quotes WHERE thread_id = ?", (thread_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def get_lead_profile_override(self, thread_id):
        conn = self._conn()
        row = conn.execute(
            "SELECT * FROM lead_profile_overrides WHERE thread_id = ?",
            (thread_id,),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def save_lead_profile_override(self, thread_id, **fields):
        dog_count = fields.get("dog_count")
        closeability_pct = fields.get("closeability_pct")
        if dog_count is not None:
            try:
                dog_count = int(dog_count)
            except (TypeError, ValueError):
                dog_count = None
        if closeability_pct is not None:
            try:
                closeability_pct = int(closeability_pct)
            except (TypeError, ValueError):
                closeability_pct = None
        conn = self._conn()
        conn.execute(
            """
            INSERT INTO lead_profile_overrides (
                thread_id, dog_count, objections_text, waiting_on_text, closeability_pct, profile_notes, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            ON CONFLICT(thread_id) DO UPDATE SET
                dog_count = excluded.dog_count,
                objections_text = excluded.objections_text,
                waiting_on_text = excluded.waiting_on_text,
                closeability_pct = excluded.closeability_pct,
                profile_notes = excluded.profile_notes,
                updated_at = datetime('now')
            """,
            (
                thread_id,
                dog_count,
                (fields.get("objections_text") or "").strip(),
                (fields.get("waiting_on_text") or "").strip(),
                closeability_pct,
                (fields.get("profile_notes") or "").strip(),
            ),
        )
        conn.commit()
        conn.close()

    def upsert_lead_quote(
        self,
        brand_id,
        thread_id,
        *,
        status="draft",
        quote_mode="hybrid",
        amount_low=0,
        amount_high=0,
        currency="USD",
        line_items=None,
        summary="",
        follow_up_text="",
        sent_at="",
        accepted_at="",
    ):
        try:
            low_value = float(amount_low or 0)
        except (TypeError, ValueError):
            low_value = 0.0
        try:
            high_value = float(amount_high or 0)
        except (TypeError, ValueError):
            high_value = 0.0
        conn = self._conn()
        conn.execute(
            """
            INSERT INTO lead_quotes (
                brand_id, thread_id, status, quote_mode, amount_low, amount_high, currency,
                line_items_json, summary, follow_up_text, sent_at, accepted_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            ON CONFLICT(thread_id) DO UPDATE SET
                status = excluded.status,
                quote_mode = excluded.quote_mode,
                amount_low = excluded.amount_low,
                amount_high = excluded.amount_high,
                currency = excluded.currency,
                line_items_json = excluded.line_items_json,
                summary = excluded.summary,
                follow_up_text = excluded.follow_up_text,
                sent_at = excluded.sent_at,
                accepted_at = excluded.accepted_at,
                updated_at = datetime('now')
            """,
            (
                brand_id,
                thread_id,
                (status or "draft").strip() or "draft",
                (quote_mode or "hybrid").strip() or "hybrid",
                low_value,
                high_value,
                (currency or "USD").strip() or "USD",
                json.dumps(line_items or []),
                summary or "",
                follow_up_text or "",
                sent_at or "",
                accepted_at or "",
            ),
        )
        conn.execute(
            "UPDATE lead_threads SET quote_status = ?, updated_at = datetime('now') WHERE id = ?",
            ((status or "draft").strip() or "draft", thread_id),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM lead_quotes WHERE thread_id = ?", (thread_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    # ── SMS Consent / A2P Opt-Out ──

    def get_sms_consent(self, brand_id, phone):
        conn = self._conn()
        row = conn.execute(
            "SELECT * FROM sms_consent WHERE brand_id = ? AND phone = ?",
            (brand_id, phone),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def is_opted_out(self, brand_id, phone):
        record = self.get_sms_consent(brand_id, phone)
        if not record:
            return False
        return record.get("status") == "opted_out"

    def record_opt_out(self, brand_id, phone, keyword="STOP"):
        conn = self._conn()
        conn.execute(
            """
            INSERT INTO sms_consent (brand_id, phone, status, opted_out_at, opted_out_keyword, updated_at)
            VALUES (?, ?, 'opted_out', datetime('now'), ?, datetime('now'))
            ON CONFLICT(brand_id, phone) DO UPDATE SET
                status = 'opted_out',
                opted_out_at = datetime('now'),
                opted_out_keyword = excluded.opted_out_keyword,
                updated_at = datetime('now')
            """,
            (brand_id, phone, keyword.upper()),
        )
        conn.commit()
        conn.close()

    def record_opt_in(self, brand_id, phone, source="START"):
        conn = self._conn()
        conn.execute(
            """
            INSERT INTO sms_consent (brand_id, phone, status, opted_in_at, opted_in_source, updated_at)
            VALUES (?, ?, 'opted_in', datetime('now'), ?, datetime('now'))
            ON CONFLICT(brand_id, phone) DO UPDATE SET
                status = 'opted_in',
                opted_in_at = datetime('now'),
                opted_in_source = excluded.opted_in_source,
                updated_at = datetime('now')
            """,
            (brand_id, phone, source),
        )
        conn.commit()
        conn.close()

    def get_opted_out_phones(self, brand_id):
        conn = self._conn()
        rows = conn.execute(
            "SELECT phone, opted_out_at FROM sms_consent WHERE brand_id = ? AND status = 'opted_out' ORDER BY opted_out_at DESC",
            (brand_id,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ── Client Billing Reminders ──

    def get_client_billing_reminder(self, brand_id, external_client_id, due_date, channel, reminder_type="payment_due"):
        conn = self._conn()
        row = conn.execute(
            """
            SELECT * FROM client_billing_reminders
            WHERE brand_id = ? AND external_client_id = ? AND due_date = ?
              AND channel = ? AND reminder_type = ?
            """,
            (brand_id, external_client_id, due_date, channel, reminder_type),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def has_sent_client_billing_reminder(self, brand_id, external_client_id, due_date, channel, reminder_type="payment_due"):
        reminder = self.get_client_billing_reminder(brand_id, external_client_id, due_date, channel, reminder_type)
        return bool(reminder and reminder.get("status") == "sent")

    def try_claim_client_billing_reminder(
        self,
        brand_id,
        external_client_id,
        due_date,
        channel,
        recipient="",
        detail="",
        reminder_type="payment_due",
    ):
        """Atomically reserve a reminder row before sending.

        Returns True only for the caller that successfully claims the reminder.
        Existing sent or pending rows are treated as already in-flight/done.
        Failed rows can be reclaimed for a retry.
        """
        conn = self._conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT id, status FROM client_billing_reminders
                WHERE brand_id = ? AND external_client_id = ? AND due_date = ?
                  AND channel = ? AND reminder_type = ?
                """,
                (brand_id, external_client_id, due_date, channel, reminder_type),
            ).fetchone()

            existing_status = str((row["status"] if row else "") or "").strip().lower()
            if existing_status in {"sent", "pending"}:
                conn.rollback()
                return False

            if row:
                conn.execute(
                    """
                    UPDATE client_billing_reminders
                    SET recipient = ?, status = 'pending', detail = ?, updated_at = datetime('now')
                    WHERE id = ?
                    """,
                    (recipient or "", detail or "", row["id"]),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO client_billing_reminders (
                        brand_id, external_client_id, due_date, channel,
                        reminder_type, recipient, status, detail, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, datetime('now'))
                    """,
                    (
                        brand_id,
                        external_client_id,
                        due_date,
                        channel,
                        reminder_type,
                        recipient or "",
                        detail or "",
                    ),
                )

            conn.commit()
            return True
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def record_client_billing_reminder(
        self,
        brand_id,
        external_client_id,
        due_date,
        channel,
        recipient="",
        status="sent",
        detail="",
        reminder_type="payment_due",
    ):
        conn = self._conn()
        conn.execute(
            """
            INSERT INTO client_billing_reminders (
                brand_id, external_client_id, due_date, channel,
                reminder_type, recipient, status, detail, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(brand_id, external_client_id, due_date, channel, reminder_type)
            DO UPDATE SET
                recipient = excluded.recipient,
                status = excluded.status,
                detail = excluded.detail,
                updated_at = datetime('now')
            """,
            (
                brand_id,
                external_client_id,
                due_date,
                channel,
                reminder_type,
                recipient or "",
                status or "sent",
                detail or "",
            ),
        )
        conn.commit()
        conn.close()

    def get_brand_client_billing_reminders(self, brand_id, reminder_type=None, limit=20):
        conn = self._conn()
        if reminder_type:
            rows = conn.execute(
                """
                SELECT * FROM client_billing_reminders
                WHERE brand_id = ? AND reminder_type = ?
                ORDER BY updated_at DESC, id DESC
                LIMIT ?
                """,
                (brand_id, reminder_type, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT * FROM client_billing_reminders
                WHERE brand_id = ?
                ORDER BY updated_at DESC, id DESC
                LIMIT ?
                """,
                (brand_id, limit),
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def ensure_brand_sng_webhook_secret(self, brand_id):
        brand = self.get_brand(brand_id)
        existing = ((brand or {}).get("sales_bot_sng_webhook_secret") or "").strip()
        if existing:
            return existing

        secret_value = secrets.token_urlsafe(24)
        conn = self._conn()
        conn.execute(
            "UPDATE brands SET sales_bot_sng_webhook_secret = ?, updated_at = datetime('now') WHERE id = ?",
            (secret_value, brand_id),
        )
        conn.commit()
        conn.close()
        return secret_value

    def record_sng_webhook_event(
        self,
        brand_id,
        external_event_id,
        event_type="",
        status="received",
        detail="",
        summary=None,
        payload=None,
    ):
        summary = summary if isinstance(summary, dict) else {"value": summary}
        payload = payload if isinstance(payload, dict) else {"value": payload}

        summary_json = json.dumps(summary or {}, separators=(",", ":"))
        if len(summary_json) > 2000:
            summary_json = json.dumps(
                {
                    "truncated": True,
                    "preview": summary_json[:1900],
                },
                separators=(",", ":"),
            )

        payload_json = json.dumps(payload or {}, separators=(",", ":"))
        if len(payload_json) > 20000:
            payload_json = json.dumps(
                {
                    "truncated": True,
                    "preview": payload_json[:19800],
                },
                separators=(",", ":"),
            )

        conn = self._conn()
        conn.execute(
            """
            INSERT INTO sng_webhook_events (
                brand_id, external_event_id, event_type, status,
                detail, summary_json, payload_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(brand_id, external_event_id)
            DO UPDATE SET
                event_type = excluded.event_type,
                status = excluded.status,
                detail = excluded.detail,
                summary_json = excluded.summary_json,
                payload_json = excluded.payload_json,
                updated_at = datetime('now')
            """,
            (
                brand_id,
                str(external_event_id or "")[:255],
                str(event_type or "")[:255],
                str(status or "received")[:50],
                str(detail or "")[:1000],
                summary_json,
                payload_json,
            ),
        )
        conn.commit()
        conn.close()

    def update_sng_webhook_event(self, brand_id, external_event_id, *, status=None, detail=None, summary=None, payload=None):
        parts = ["updated_at = datetime('now')"]
        values = []

        if status is not None:
            parts.append("status = ?")
            values.append(str(status or "received")[:50])
        if detail is not None:
            parts.append("detail = ?")
            values.append(str(detail or "")[:1000])
        if summary is not None:
            summary_json = json.dumps(summary or {}, separators=(",", ":"))[:2000]
            parts.append("summary_json = ?")
            values.append(summary_json)
        if payload is not None:
            payload_json = json.dumps(payload or {}, separators=(",", ":"))[:20000]
            parts.append("payload_json = ?")
            values.append(payload_json)

        values.extend([brand_id, str(external_event_id or "")[:255]])
        conn = self._conn()
        conn.execute(
            f"UPDATE sng_webhook_events SET {', '.join(parts)} WHERE brand_id = ? AND external_event_id = ?",
            values,
        )
        conn.commit()
        conn.close()

    def get_sng_webhook_event_by_external_id(self, brand_id, external_event_id):
        conn = self._conn()
        row = conn.execute(
            "SELECT * FROM sng_webhook_events WHERE brand_id = ? AND external_event_id = ?",
            (brand_id, str(external_event_id or "")[:255]),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def get_sng_webhook_events(self, brand_id, limit=20):
        conn = self._conn()
        rows = conn.execute(
            """
            SELECT * FROM sng_webhook_events
            WHERE brand_id = ?
            ORDER BY received_at DESC, id DESC
            LIMIT ?
            """,
            (brand_id, limit),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def record_lead_webhook_delivery(
        self,
        brand_id=None,
        *,
        brand_slug="",
        endpoint="",
        status="received",
        http_status=0,
        reason="",
        source="",
        lead_name="",
        lead_email="",
        lead_phone="",
        thread_id=None,
        signature_present=False,
        payload_preview="",
        remote_addr="",
    ):
        conn = self._conn()
        cur = conn.execute(
            """
            INSERT INTO lead_webhook_deliveries (
                brand_id, brand_slug, endpoint, status, http_status, reason, source,
                lead_name, lead_email, lead_phone, thread_id, signature_present,
                payload_preview, remote_addr
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                brand_id,
                str(brand_slug or "")[:120],
                str(endpoint or "")[:255],
                str(status or "received")[:50],
                int(http_status or 0),
                str(reason or "")[:500],
                str(source or "")[:120],
                str(lead_name or "")[:200],
                str(lead_email or "")[:255],
                str(lead_phone or "")[:100],
                thread_id,
                1 if signature_present else 0,
                str(payload_preview or "")[:2000],
                str(remote_addr or "")[:120],
            ),
        )
        conn.commit()
        row_id = cur.lastrowid
        conn.close()
        return row_id

    def get_lead_webhook_deliveries(self, brand_id, limit=20):
        conn = self._conn()
        rows = conn.execute(
            """
            SELECT * FROM lead_webhook_deliveries
            WHERE brand_id = ?
            ORDER BY received_at DESC, id DESC
            LIMIT ?
            """,
            (brand_id, int(limit or 20)),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def queue_crm_event_action(
        self,
        brand_id,
        *,
        source_event_id,
        source_event_type,
        rule_key,
        action_kind,
        channel,
        recipient="",
        client_id="",
        payment_id="",
        invoice_id="",
        subscription_id="",
        subject="",
        message_text="",
        attempt_number=1,
        max_attempts=1,
        scheduled_for="",
        detail="",
    ):
        conn = self._conn()
        cur = conn.execute(
            """
            INSERT INTO crm_event_actions (
                brand_id, source_event_id, source_event_type, rule_key,
                action_kind, channel, recipient, client_id, payment_id,
                invoice_id, subscription_id, subject, message_text,
                attempt_number, max_attempts, scheduled_for, detail, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(brand_id, source_event_id, rule_key, action_kind, channel, recipient, attempt_number)
            DO NOTHING
            """,
            (
                brand_id,
                str(source_event_id or "")[:255],
                str(source_event_type or "")[:255],
                str(rule_key or "")[:100],
                str(action_kind or "client_message")[:50],
                str(channel or "sms")[:50],
                str(recipient or "")[:255],
                str(client_id or "")[:255],
                str(payment_id or "")[:255],
                str(invoice_id or "")[:255],
                str(subscription_id or "")[:255],
                str(subject or "")[:255],
                str(message_text or "")[:4000],
                max(1, int(attempt_number or 1)),
                max(1, int(max_attempts or 1)),
                str(scheduled_for or datetime.now(UTC).isoformat())[:32],
                str(detail or "")[:1000],
            ),
        )
        conn.commit()
        action_id = cur.lastrowid if (cur.rowcount or 0) > 0 else 0
        conn.close()
        return action_id

    def get_crm_event_actions(self, brand_id, limit=50):
        conn = self._conn()
        rows = conn.execute(
            """
            SELECT * FROM crm_event_actions
            WHERE brand_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (brand_id, int(limit or 50)),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_due_crm_event_actions(self, now_iso=None, brand_id=None, limit=100):
        now_iso = str(now_iso or datetime.now(UTC).isoformat())[:32]
        conn = self._conn()
        if brand_id is None:
            rows = conn.execute(
                """
                SELECT * FROM crm_event_actions
                WHERE status = 'queued' AND scheduled_for <= ?
                ORDER BY scheduled_for ASC, id ASC
                LIMIT ?
                """,
                (now_iso, int(limit or 100)),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT * FROM crm_event_actions
                WHERE brand_id = ? AND status = 'queued' AND scheduled_for <= ?
                ORDER BY scheduled_for ASC, id ASC
                LIMIT ?
                """,
                (brand_id, now_iso, int(limit or 100)),
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def update_crm_event_action(self, action_id, **fields):
        allowed = {
            "status", "recipient", "subject", "message_text", "scheduled_for",
            "sent_at", "resolved_at", "resolution_reason", "detail", "updated_at",
        }
        parts = ["updated_at = datetime('now')"]
        values = []
        for key, value in fields.items():
            if key not in allowed:
                continue
            parts.append(f"{key} = ?")
            values.append(str(value or "")[:4000] if key in {"message_text", "detail"} else str(value or "")[:255])
        values.append(action_id)
        conn = self._conn()
        conn.execute(f"UPDATE crm_event_actions SET {', '.join(parts)} WHERE id = ?", values)
        conn.commit()
        conn.close()

    def resolve_crm_event_actions(self, brand_id, rule_key, *, client_id="", payment_id="", invoice_id="", subscription_id="", reason=""):
        clauses = ["brand_id = ?", "rule_key = ?", "status = 'queued'"]
        values = [datetime.now(UTC).isoformat(), str(reason or "")[:255], brand_id, str(rule_key or "")[:100]]
        identifiers = []
        if payment_id:
            identifiers.append(("payment_id = ?", str(payment_id)[:255]))
        if subscription_id:
            identifiers.append(("subscription_id = ?", str(subscription_id)[:255]))
        if invoice_id:
            identifiers.append(("invoice_id = ?", str(invoice_id)[:255]))
        if client_id:
            identifiers.append(("client_id = ?", str(client_id)[:255]))
        if not identifiers:
            return 0

        clauses.append("(" + " OR ".join(part for part, _ in identifiers) + ")")
        values.extend(value for _, value in identifiers)

        conn = self._conn()
        cur = conn.execute(
            f"""
            UPDATE crm_event_actions
            SET status = 'resolved', resolved_at = ?, resolution_reason = ?, updated_at = datetime('now')
            WHERE {' AND '.join(clauses)}
            """,
            values,
        )
        conn.commit()
        count = cur.rowcount or 0
        conn.close()
        return count

    def record_appointment_reminder_run(
        self,
        brand_id,
        target_date,
        status="completed",
        reason="",
        candidates=0,
        sent=0,
        failed=0,
        skipped=0,
        summary=None,
    ):
        conn = self._conn()
        conn.execute(
            """
            INSERT INTO appointment_reminder_runs (
                brand_id, target_date, status, reason,
                candidates, sent, failed, skipped, summary_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                brand_id,
                str(target_date or ""),
                (status or "completed").strip() or "completed",
                (reason or "").strip()[:500],
                int(candidates or 0),
                int(sent or 0),
                int(failed or 0),
                int(skipped or 0),
                json.dumps(summary or {}, separators=(",", ":"))[:2000],
            ),
        )
        conn.commit()
        conn.close()

    def get_appointment_reminder_runs(self, brand_id, limit=10, min_sent=None):
        conn = self._conn()
        clauses = ["brand_id = ?"]
        values = [brand_id]
        if min_sent is not None:
            clauses.append("sent >= ?")
            values.append(int(min_sent))
        values.append(limit)
        rows = conn.execute(
            f"""
            SELECT * FROM appointment_reminder_runs
            WHERE {' AND '.join(clauses)}
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            values,
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

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

    def get_brand_usage_pulse(self):
        """Return usage metrics for every brand in one shot.

        Returns dict keyed by brand_id with counts + timestamps:
          lead_threads_total, leads_30d, messages_30d, blog_posts_total,
          blogs_30d, reports_total, last_client_login, last_lead_at,
          last_blog_at, warren_enabled
        """
        conn = self._conn()
        cutoff_30d = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")

        pulse = {}

        # Lead threads total + 30d
        for row in conn.execute("""
            SELECT brand_id,
                   COUNT(*) as total,
                   SUM(CASE WHEN created_at >= ? THEN 1 ELSE 0 END) as recent,
                   MAX(last_message_at) as last_at
            FROM lead_threads GROUP BY brand_id
        """, (cutoff_30d,)).fetchall():
            bid = row["brand_id"]
            pulse.setdefault(bid, {})
            pulse[bid]["lead_threads_total"] = row["total"]
            pulse[bid]["leads_30d"] = row["recent"]
            pulse[bid]["last_lead_at"] = row["last_at"] or ""

        # Messages 30d (across all threads per brand)
        for row in conn.execute("""
            SELECT lt.brand_id, COUNT(*) as cnt
            FROM lead_messages lm
            JOIN lead_threads lt ON lt.id = lm.thread_id
            WHERE lm.created_at >= ?
            GROUP BY lt.brand_id
        """, (cutoff_30d,)).fetchall():
            bid = row["brand_id"]
            pulse.setdefault(bid, {})
            pulse[bid]["messages_30d"] = row["cnt"]

        # Blog posts total + 30d
        for row in conn.execute("""
            SELECT brand_id,
                   COUNT(*) as total,
                   SUM(CASE WHEN created_at >= ? THEN 1 ELSE 0 END) as recent,
                   MAX(created_at) as last_at
            FROM blog_posts GROUP BY brand_id
        """, (cutoff_30d,)).fetchall():
            bid = row["brand_id"]
            pulse.setdefault(bid, {})
            pulse[bid]["blog_posts_total"] = row["total"]
            pulse[bid]["blogs_30d"] = row["recent"]
            pulse[bid]["last_blog_at"] = row["last_at"] or ""

        # Reports total
        for row in conn.execute("""
            SELECT brand_id, COUNT(*) as total
            FROM reports GROUP BY brand_id
        """).fetchall():
            bid = row["brand_id"]
            pulse.setdefault(bid, {})
            pulse[bid]["reports_total"] = row["total"]

        # Last client login
        for row in conn.execute("""
            SELECT brand_id, MAX(last_login_at) as last_login
            FROM client_users
            WHERE is_active = 1
            GROUP BY brand_id
        """).fetchall():
            bid = row["brand_id"]
            pulse.setdefault(bid, {})
            pulse[bid]["last_client_login"] = row["last_login"] or ""

        conn.close()
        return pulse

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

    def get_client_users(self, active_only=True):
        conn = self._conn()
        sql = (
            "SELECT cu.*, b.display_name AS brand_name "
            "FROM client_users cu "
            "LEFT JOIN brands b ON cu.brand_id = b.id"
        )
        if active_only:
            sql += " WHERE cu.is_active = 1"
        sql += " ORDER BY cu.display_name, cu.email"
        rows = conn.execute(sql).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_client_user(self, client_user_id):
        conn = self._conn()
        row = conn.execute("SELECT * FROM client_users WHERE id = ?", (client_user_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def get_client_onboarding_progress(self, brand_id, client_user_id):
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM client_onboarding_progress WHERE brand_id = ? AND client_user_id = ?",
            (brand_id, client_user_id),
        ).fetchall()
        conn.close()
        return {
            row["item_key"]: {
                "is_completed": bool(row["is_completed"]),
                "is_dismissed": bool(row["is_dismissed"]),
                "updated_at": row["updated_at"],
            }
            for row in rows
        }

    def save_client_onboarding_progress(self, brand_id, client_user_id, item_key, *, is_completed=None, is_dismissed=None):
        existing = self.get_client_onboarding_progress(brand_id, client_user_id).get(item_key, {})
        completed_value = existing.get("is_completed", False) if is_completed is None else bool(is_completed)
        dismissed_value = existing.get("is_dismissed", False) if is_dismissed is None else bool(is_dismissed)

        conn = self._conn()
        conn.execute(
            """
            INSERT INTO client_onboarding_progress (
                brand_id, client_user_id, item_key, is_completed, is_dismissed, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            ON CONFLICT(brand_id, client_user_id, item_key) DO UPDATE SET
                is_completed = excluded.is_completed,
                is_dismissed = excluded.is_dismissed,
                updated_at = datetime('now')
            """,
            (brand_id, client_user_id, (item_key or "").strip(), 1 if completed_value else 0, 1 if dismissed_value else 0),
        )
        conn.commit()
        conn.close()

    def get_client_onboarding_session(self, brand_id, client_user_id):
        conn = self._conn()
        row = conn.execute(
            "SELECT * FROM client_onboarding_sessions WHERE brand_id = ? AND client_user_id = ?",
            (brand_id, client_user_id),
        ).fetchone()
        conn.close()
        if not row:
            return None
        payload = dict(row)
        payload["profile"] = self._safe_json_object(payload.get("profile_json"))
        payload["notes"] = self._safe_json_object(payload.get("notes_json"))
        return payload

    def save_client_onboarding_session(
        self,
        brand_id,
        client_user_id,
        *,
        status=None,
        stage_key=None,
        current_question_key=None,
        profile=None,
        notes=None,
    ):
        existing = self.get_client_onboarding_session(brand_id, client_user_id) or {}
        next_status = (status or existing.get("status") or "active").strip() or "active"
        next_stage_key = (stage_key or existing.get("stage_key") or "setup").strip() or "setup"
        if current_question_key is None:
            next_question_key = (existing.get("current_question_key") or "").strip()
        else:
            next_question_key = str(current_question_key or "").strip()
        next_profile = profile if isinstance(profile, dict) else existing.get("profile") or {}
        next_notes = notes if isinstance(notes, dict) else existing.get("notes") or {}

        conn = self._conn()
        conn.execute(
            """
            INSERT INTO client_onboarding_sessions (
                brand_id, client_user_id, status, stage_key, current_question_key,
                profile_json, notes_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            ON CONFLICT(brand_id, client_user_id) DO UPDATE SET
                status = excluded.status,
                stage_key = excluded.stage_key,
                current_question_key = excluded.current_question_key,
                profile_json = excluded.profile_json,
                notes_json = excluded.notes_json,
                updated_at = datetime('now')
            """,
            (
                brand_id,
                client_user_id,
                next_status,
                next_stage_key,
                next_question_key,
                json.dumps(next_profile, separators=(",", ":")),
                json.dumps(next_notes, separators=(",", ":")),
            ),
        )
        conn.commit()
        conn.close()

    def reset_client_onboarding_session(self, brand_id, client_user_id):
        conn = self._conn()
        conn.execute(
            "DELETE FROM client_onboarding_sessions WHERE brand_id = ? AND client_user_id = ?",
            (brand_id, client_user_id),
        )
        conn.commit()
        conn.close()

    def create_client_user(self, brand_id, email, password, display_name, role="owner", invited_by=None):
        email = self._normalize_email(email)
        conn = self._conn()
        password_hash = generate_password_hash(password)
        try:
            conn.execute(
                "INSERT INTO client_users (brand_id, email, password_hash, display_name, role, invited_by) VALUES (?, ?, ?, ?, ?, ?)",
                (brand_id, email, password_hash, display_name, role, invited_by),
            )
            conn.commit()
            row = conn.execute("SELECT id FROM client_users WHERE lower(email) = ?", (email,)).fetchone()
            conn.close()
            # Auto-remove from drip campaigns on conversion
            try:
                self.convert_drip_by_email(email.lower())
            except Exception:
                pass
            return int(row["id"]) if row else None
        except sqlite3.IntegrityError:
            conn.close()
            return None

    def authenticate_client(self, email, password):
        email = self._normalize_email(email)
        conn = self._conn()
        row = conn.execute(
            "SELECT cu.*, b.display_name AS brand_name, b.slug AS brand_slug "
            "FROM client_users cu JOIN brands b ON cu.brand_id = b.id "
            "WHERE lower(cu.email) = ? AND cu.is_active = 1",
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

    def get_client_user_by_email(self, email):
        email = self._normalize_email(email)
        conn = self._conn()
        row = conn.execute(
            "SELECT * FROM client_users WHERE lower(email) = ? AND is_active = 1",
            (email,),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def set_password_reset_token(self, client_user_id, token, expires):
        conn = self._conn()
        conn.execute(
            "UPDATE client_users SET password_reset_token = ?, reset_token_expires = ? WHERE id = ?",
            (token, expires, client_user_id),
        )
        conn.commit()
        conn.close()

    def validate_password_reset_token(self, token):
        conn = self._conn()
        row = conn.execute(
            "SELECT * FROM client_users WHERE password_reset_token = ? AND is_active = 1",
            (token,),
        ).fetchone()
        conn.close()
        if not row:
            return None
        user = dict(row)
        expires = user.get("reset_token_expires", "")
        if expires and expires < datetime.now().strftime("%Y-%m-%d %H:%M:%S"):
            return None
        return user

    def clear_password_reset_token(self, client_user_id):
        conn = self._conn()
        conn.execute(
            "UPDATE client_users SET password_reset_token = '', reset_token_expires = '' WHERE id = ?",
            (client_user_id,),
        )
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

    def create_meta_deletion_request(self, meta_user_id, payload_json="{}"):
        conn = self._conn()
        confirmation_code = f"GRO-{secrets.token_hex(8).upper()}"
        conn.execute(
            """
            INSERT INTO meta_deletion_requests (
                confirmation_code, meta_user_id, status, payload_json
            ) VALUES (?, ?, 'received', ?)
            """,
            (confirmation_code, str(meta_user_id or "").strip(), payload_json or "{}"),
        )
        conn.commit()
        row = conn.execute(
            "SELECT * FROM meta_deletion_requests WHERE confirmation_code = ?",
            (confirmation_code,),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def get_meta_deletion_request(self, confirmation_code):
        conn = self._conn()
        row = conn.execute(
            "SELECT * FROM meta_deletion_requests WHERE confirmation_code = ?",
            (confirmation_code,),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def process_meta_deletion_request(self, confirmation_code):
        conn = self._conn()
        row = conn.execute(
            "SELECT * FROM meta_deletion_requests WHERE confirmation_code = ?",
            (confirmation_code,),
        ).fetchone()
        if not row:
            conn.close()
            return None

        meta_user_id = (row["meta_user_id"] or "").strip()
        deleted_thread_count = 0
        notes = "No directly keyed in-app Meta thread records were found."

        if meta_user_id:
            deleted_thread_count = conn.execute(
                "SELECT COUNT(1) FROM lead_threads WHERE channel = 'messenger' AND external_thread_id = ?",
                (meta_user_id,),
            ).fetchone()[0]
            conn.execute(
                "DELETE FROM lead_threads WHERE channel = 'messenger' AND external_thread_id = ?",
                (meta_user_id,),
            )
            if deleted_thread_count:
                notes = (
                    "Removed in-app Messenger thread records keyed by the Meta user identifier. "
                    "Other records may require manual review if they are not stored against that identifier."
                )

        conn.execute(
            """
            UPDATE meta_deletion_requests
               SET status = 'completed',
                   deleted_thread_count = ?,
                   notes = ?,
                   completed_at = datetime('now')
             WHERE confirmation_code = ?
            """,
            (deleted_thread_count, notes, confirmation_code),
        )
        conn.commit()
        updated = conn.execute(
            "SELECT * FROM meta_deletion_requests WHERE confirmation_code = ?",
            (confirmation_code,),
        ).fetchone()
        conn.close()
        return dict(updated) if updated else None

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
                          center_lat, center_lng, results_json, avg_rank,
                          status="complete", error_message="", debug_json="{}"):
        conn = self._conn()
        cursor = conn.execute(
            """INSERT INTO heatmap_scans
               (brand_id, keyword, grid_size, radius_miles, center_lat, center_lng, results_json, avg_rank, status, error_message, debug_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (brand_id, keyword, grid_size, radius_miles, center_lat, center_lng, results_json, avg_rank, status, error_message, debug_json),
        )
        conn.commit()
        scan_id = cursor.lastrowid
        conn.close()
        return scan_id

    def update_heatmap_scan(self, scan_id, brand_id, *, results_json=None, avg_rank=None,
                            status=None, error_message=None, debug_json=None):
        fields = []
        values = []
        updates = {
            "results_json": results_json,
            "avg_rank": avg_rank,
            "status": status,
            "error_message": error_message,
            "debug_json": debug_json,
        }
        for field_name, value in updates.items():
            if value is not None:
                fields.append(f"{field_name} = ?")
                values.append(value)
        if not fields:
            return

        conn = self._conn()
        conn.execute(
            f"UPDATE heatmap_scans SET {', '.join(fields)} WHERE id = ? AND brand_id = ?",
            (*values, scan_id, brand_id),
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

    def delete_heatmap_scan(self, scan_id, brand_id):
        conn = self._conn()
        conn.execute("DELETE FROM heatmap_scans WHERE id = ? AND brand_id = ?",
                     (scan_id, brand_id))
        conn.commit()
        conn.close()

    def delete_all_heatmap_scans(self, brand_id):
        conn = self._conn()
        conn.execute("DELETE FROM heatmap_scans WHERE brand_id = ?", (brand_id,))
        conn.commit()
        conn.close()

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

    def toggle_campaign_strategy_active(self, strategy_id):
        conn = self._conn()
        conn.execute(
            "UPDATE campaign_strategies SET is_active = CASE WHEN is_active = 1 THEN 0 ELSE 1 END, updated_at = datetime('now') WHERE id = ?",
            (strategy_id,),
        )
        conn.commit()
        conn.close()

    # ── Competitors (structured) ─────────────────────────────────

    def add_competitor(self, brand_id, name, website="", facebook_url="",
                       google_maps_url="", gbp_cid="", yelp_url="", instagram_url="",
                       notes=""):
        try:
            conn = self._conn()
            cur = conn.execute(
                """INSERT INTO competitors
                   (brand_id, name, website, facebook_url, google_maps_url,
                    gbp_cid, yelp_url, instagram_url, notes)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (brand_id, name, website, facebook_url, google_maps_url,
                 gbp_cid, yelp_url, instagram_url, notes),
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
                    gbp_cid=gbp_cid,
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
        allowed = {"name", "website", "facebook_url", "google_maps_url", "gbp_cid",
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

    # ── Scheduled Posts ─────────────────────────────────────────────

    def save_scheduled_post(self, brand_id, platform, message, scheduled_at,
                            image_url="", link_url="", post_type=""):
        conn = self._conn()
        conn.execute(
            """INSERT INTO scheduled_posts
               (brand_id, platform, post_type, message, image_url, link_url, scheduled_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (brand_id, platform, post_type, message, image_url, link_url, scheduled_at),
        )
        conn.commit()
        row_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.close()
        return row_id

    def save_scheduled_posts_bulk(self, posts):
        """Insert multiple posts. Each item: dict with brand_id, platform,
        message, scheduled_at, image_url, link_url, post_type."""
        conn = self._conn()
        for p in posts:
            conn.execute(
                """INSERT INTO scheduled_posts
                   (brand_id, platform, post_type, message, image_url, link_url, scheduled_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (p["brand_id"], p.get("platform", "facebook"), p.get("post_type", ""), p["message"],
                 p.get("image_url", ""), p.get("link_url", ""), p["scheduled_at"]),
            )
        conn.commit()
        conn.close()

    def get_scheduled_posts(self, brand_id, status=None, limit=100):
        conn = self._conn()
        if status:
            rows = conn.execute(
                "SELECT * FROM scheduled_posts WHERE brand_id = ? AND status = ? ORDER BY scheduled_at",
                (brand_id, status),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM scheduled_posts WHERE brand_id = ? ORDER BY scheduled_at DESC LIMIT ?",
                (brand_id, limit),
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_scheduled_post(self, post_id):
        conn = self._conn()
        row = conn.execute("SELECT * FROM scheduled_posts WHERE id = ?", (post_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def update_scheduled_post_status(self, post_id, status, fb_post_id="", error_message=""):
        conn = self._conn()
        if status == "published":
            conn.execute(
                """UPDATE scheduled_posts SET status = ?, fb_post_id = ?,
                   published_at = datetime('now') WHERE id = ?""",
                (status, fb_post_id, post_id),
            )
        elif status == "failed":
            conn.execute(
                "UPDATE scheduled_posts SET status = ?, error_message = ? WHERE id = ?",
                (status, error_message, post_id),
            )
        else:
            conn.execute(
                "UPDATE scheduled_posts SET status = ? WHERE id = ?",
                (status, post_id),
            )
        conn.commit()
        conn.close()

    def delete_scheduled_post(self, post_id, brand_id):
        conn = self._conn()
        conn.execute("DELETE FROM scheduled_posts WHERE id = ? AND brand_id = ?",
                     (post_id, brand_id))
        conn.commit()
        conn.close()

    # ── Beta Testers ──

    def create_beta_tester(self, data):
        conn = self._conn()
        try:
            conn.execute(
                "INSERT INTO beta_testers (name, email, business_name, website, industry, "
                "monthly_ad_spend, platforms, referral_source, meta_login_email, google_business_email, facebook_page_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    data["name"], data["email"], data.get("business_name", ""),
                    data.get("website", ""), data.get("industry", ""),
                    data.get("monthly_ad_spend", ""), data.get("platforms", ""),
                    data.get("referral_source", ""),
                    data.get("meta_login_email", ""), data.get("google_business_email", ""),
                    data.get("facebook_page_id", ""),
                ),
            )
            conn.commit()
            row = conn.execute("SELECT id FROM beta_testers WHERE email = ?", (data["email"],)).fetchone()
            conn.close()
            return int(row["id"]) if row else None
        except sqlite3.IntegrityError:
            conn.close()
            return None

    def get_beta_testers(self, status=None):
        conn = self._conn()
        if status:
            rows = conn.execute(
                "SELECT * FROM beta_testers WHERE status = ? ORDER BY created_at DESC", (status,)
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM beta_testers ORDER BY created_at DESC").fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_beta_testers_for_broadcast(self):
        recipients = []
        seen = set()
        for tester in self.get_beta_testers():
            if tester.get("status") not in {"pending", "approved"}:
                continue
            email = self._normalize_email(tester.get("email"))
            if "@" not in email or email in seen:
                continue
            seen.add(email)
            recipients.append({
                **tester,
                "email": email,
                "recipient_name": tester.get("name") or email,
            })
        return recipients

    def get_beta_tester(self, tester_id):
        conn = self._conn()
        row = conn.execute("SELECT * FROM beta_testers WHERE id = ?", (tester_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def get_beta_tester_by_email(self, email):
        conn = self._conn()
        row = conn.execute("SELECT * FROM beta_testers WHERE email = ?", (email,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def update_beta_tester_status(self, tester_id, status, **kwargs):
        conn = self._conn()
        sets = ["status = ?"]
        params = [status]
        allowed = ("brand_id", "client_user_id", "admin_notes", "invite_sent_at",
                   "approved_at", "onboarding_token", "temp_password", "activated_at")
        for k, v in kwargs.items():
            if k in allowed:
                sets.append(f"{k} = ?")
                params.append(v)
        params.append(tester_id)
        conn.execute(f"UPDATE beta_testers SET {', '.join(sets)} WHERE id = ?", params)
        conn.commit()
        conn.close()

    def get_beta_tester_by_token(self, token):
        if not token:
            return None
        conn = self._conn()
        row = conn.execute("SELECT * FROM beta_testers WHERE onboarding_token = ?", (token,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def update_beta_tester_onboarding(self, tester_id, facebook_page_id, google_business_email, meta_login_email):
        conn = self._conn()
        conn.execute(
            "UPDATE beta_testers SET facebook_page_id = ?, google_business_email = ?, meta_login_email = ?, onboarding_completed_at = datetime('now') WHERE id = ?",
            (facebook_page_id, google_business_email, meta_login_email, tester_id),
        )
        conn.commit()
        conn.close()

    def deactivate_beta_tester(self, tester_id):
        conn = self._conn()
        row = conn.execute("SELECT client_user_id FROM beta_testers WHERE id = ?", (tester_id,)).fetchone()
        if row and row["client_user_id"]:
            conn.execute("UPDATE client_users SET is_active = 0 WHERE id = ?", (row["client_user_id"],))
        conn.execute("UPDATE beta_testers SET status = 'removed' WHERE id = ?", (tester_id,))
        conn.commit()
        conn.close()

    # ── Blog Posts ──────────────────────────────────────────────────

    def save_blog_post(self, brand_id, title, content, excerpt="",
                       slug="", status="draft", featured_image_url="",
                       categories="", tags="", seo_title="",
                       seo_description="", scheduled_at=None, created_by=0):
        conn = self._conn()
        conn.execute(
            """INSERT INTO blog_posts
               (brand_id, title, content, excerpt, slug, status,
                featured_image_url, categories, tags, seo_title,
                seo_description, scheduled_at, created_by)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (brand_id, title, content, excerpt, slug, status,
             featured_image_url, categories, tags, seo_title,
             seo_description, scheduled_at, created_by),
        )
        conn.commit()
        row_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.close()
        return row_id

    def update_blog_post(self, post_id, **kwargs):
        conn = self._conn()
        allowed = ("title", "content", "excerpt", "slug", "status",
                   "featured_image_url", "categories", "tags",
                   "seo_title", "seo_description", "scheduled_at",
                   "wp_post_id", "wp_post_url", "published_at")
        sets = ["updated_at = datetime('now')"]
        params = []
        for k, v in kwargs.items():
            if k in allowed:
                sets.append(f"{k} = ?")
                params.append(v)
        if len(params) == 0:
            conn.close()
            return
        params.append(post_id)
        conn.execute(f"UPDATE blog_posts SET {', '.join(sets)} WHERE id = ?", params)
        conn.commit()
        conn.close()

    def get_blog_posts(self, brand_id, status=None, limit=50):
        conn = self._conn()
        if status:
            rows = conn.execute(
                "SELECT * FROM blog_posts WHERE brand_id = ? AND status = ? ORDER BY updated_at DESC LIMIT ?",
                (brand_id, status, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM blog_posts WHERE brand_id = ? ORDER BY updated_at DESC LIMIT ?",
                (brand_id, limit),
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_blog_post(self, post_id):
        conn = self._conn()
        row = conn.execute("SELECT * FROM blog_posts WHERE id = ?", (post_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def delete_blog_post(self, post_id):
        conn = self._conn()
        conn.execute("DELETE FROM blog_posts WHERE id = ?", (post_id,))
        conn.commit()
        conn.close()

    def get_due_blog_posts(self, brand_id=None):
        """Return blog posts that are scheduled and past due."""
        conn = self._conn()
        sql = "SELECT * FROM blog_posts WHERE status = 'scheduled' AND datetime(scheduled_at) <= datetime('now')"
        params = []
        if brand_id is not None:
            sql += " AND brand_id = ?"
            params.append(brand_id)
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_beta_stats(self):
        conn = self._conn()
        total = conn.execute("SELECT COUNT(*) as c FROM beta_testers").fetchone()["c"]
        pending = conn.execute("SELECT COUNT(*) as c FROM beta_testers WHERE status='pending'").fetchone()["c"]
        approved = conn.execute("SELECT COUNT(*) as c FROM beta_testers WHERE status='approved'").fetchone()["c"]
        active = conn.execute("SELECT COUNT(*) as c FROM beta_testers WHERE status='approved' AND activated_at != ''").fetchone()["c"]
        removed = conn.execute("SELECT COUNT(*) as c FROM beta_testers WHERE status='removed'").fetchone()["c"]
        onboarding_done = conn.execute("SELECT COUNT(*) as c FROM beta_testers WHERE onboarding_completed_at != '' AND onboarding_completed_at IS NOT NULL").fetchone()["c"]
        feedback_count = conn.execute("SELECT COUNT(*) as c FROM beta_feedback").fetchone()["c"]
        new_feedback = conn.execute("SELECT COUNT(*) as c FROM beta_feedback WHERE status='new'").fetchone()["c"]
        conn.close()
        return {
            "total": total, "pending": pending, "approved": approved,
            "active": active, "removed": removed,
            "onboarding_done": onboarding_done,
            "feedback_count": feedback_count,
            "new_feedback": new_feedback,
        }

    # ── Beta Feedback ──

    def create_beta_feedback(self, brand_id, client_user_id, category, rating, message, page=""):
        conn = self._conn()
        conn.execute(
            "INSERT INTO beta_feedback (brand_id, client_user_id, category, rating, message, page) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (brand_id, client_user_id, category, rating, message, page),
        )
        conn.commit()
        conn.close()

    def get_beta_feedback(self, status=None, limit=100):
        conn = self._conn()
        if status:
            rows = conn.execute(
                "SELECT bf.*, COALESCE(bt.name, cu.display_name, '') as tester_name, "
                "COALESCE(bt.email, cu.email, '') as tester_email, "
                "b.display_name as brand_name "
                "FROM beta_feedback bf "
                "LEFT JOIN beta_testers bt ON bt.client_user_id = bf.client_user_id "
                "LEFT JOIN client_users cu ON cu.id = bf.client_user_id "
                "LEFT JOIN brands b ON b.id = bf.brand_id "
                "WHERE bf.status = ? ORDER BY bf.created_at DESC LIMIT ?",
                (status, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT bf.*, COALESCE(bt.name, cu.display_name, '') as tester_name, "
                "COALESCE(bt.email, cu.email, '') as tester_email, "
                "b.display_name as brand_name "
                "FROM beta_feedback bf "
                "LEFT JOIN beta_testers bt ON bt.client_user_id = bf.client_user_id "
                "LEFT JOIN client_users cu ON cu.id = bf.client_user_id "
                "LEFT JOIN brands b ON b.id = bf.brand_id "
                "ORDER BY bf.created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_beta_feedback_by_ids(self, feedback_ids):
        ids = [int(fid) for fid in (feedback_ids or []) if str(fid).strip()]
        if not ids:
            return []
        conn = self._conn()
        placeholders = ",".join("?" for _ in ids)
        rows = conn.execute(
            "SELECT bf.*, COALESCE(bt.name, cu.display_name, '') as tester_name, "
            "COALESCE(bt.email, cu.email, '') as tester_email, "
            "b.display_name as brand_name "
            "FROM beta_feedback bf "
            "LEFT JOIN beta_testers bt ON bt.client_user_id = bf.client_user_id "
            "LEFT JOIN client_users cu ON cu.id = bf.client_user_id "
            "LEFT JOIN brands b ON b.id = bf.brand_id "
            f"WHERE bf.id IN ({placeholders}) ORDER BY bf.created_at DESC",
            ids,
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_beta_feedback_filtered(self, status=None, category=None, limit=100, feedback_ids=None):
        clauses = []
        params = []
        if status:
            clauses.append("bf.status = ?")
            params.append(status)
        if category:
            clauses.append("bf.category = ?")
            params.append(category)
        ids = [int(fid) for fid in (feedback_ids or []) if str(fid).strip()]
        if ids:
            placeholders = ",".join("?" for _ in ids)
            clauses.append(f"bf.id IN ({placeholders})")
            params.extend(ids)

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        conn = self._conn()
        rows = conn.execute(
            "SELECT bf.*, COALESCE(bt.name, cu.display_name, '') as tester_name, "
            "COALESCE(bt.email, cu.email, '') as tester_email, "
            "b.display_name as brand_name "
            "FROM beta_feedback bf "
            "LEFT JOIN beta_testers bt ON bt.client_user_id = bf.client_user_id "
            "LEFT JOIN client_users cu ON cu.id = bf.client_user_id "
            "LEFT JOIN brands b ON b.id = bf.brand_id "
            f"{where_sql} ORDER BY bf.created_at DESC LIMIT ?",
            tuple(params + [limit]),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_beta_feedback_for_brand(self, brand_id):
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM beta_feedback WHERE brand_id = ? ORDER BY created_at DESC",
            (brand_id,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def update_beta_feedback_status(self, feedback_id, status, admin_response=""):
        conn = self._conn()
        conn.execute(
            "UPDATE beta_feedback SET status = ?, admin_response = ?, responded_at = datetime('now') WHERE id = ?",
            (status, admin_response, feedback_id),
        )
        conn.commit()
        conn.close()

    def get_beta_feedback_summary(self):
        """Aggregate feedback by category for the admin dashboard."""
        conn = self._conn()
        rows = conn.execute(
            "SELECT category, COUNT(*) as count, AVG(rating) as avg_rating "
            "FROM beta_feedback GROUP BY category ORDER BY count DESC"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_feedback_themes(self):
        """Group feedback by similar messages to surface recurring themes."""
        conn = self._conn()
        rows = conn.execute(
            "SELECT bf.id, bf.category, bf.message, bf.rating, bf.status, bf.created_at, "
            "bt.name as tester_name "
            "FROM beta_feedback bf "
            "LEFT JOIN beta_testers bt ON bt.client_user_id = bf.client_user_id "
            "WHERE bf.category IN ('feature_request', 'bug', 'ui_ux', 'dislike') "
            "ORDER BY bf.created_at DESC"
        ).fetchall()
        conn.close()
        items = [dict(r) for r in rows]

        # Simple keyword-based clustering
        themes = {}
        for item in items:
            msg = (item.get("message") or "").lower()
            words = set(w for w in msg.split() if len(w) > 3)
            matched = False
            for key in themes:
                overlap = words & themes[key]["keywords"]
                if len(overlap) >= 2:
                    themes[key]["items"].append(item)
                    themes[key]["keywords"] |= words
                    matched = True
                    break
            if not matched:
                themes[item["id"]] = {"keywords": words, "items": [item]}

        result = []
        for _key, group in themes.items():
            if len(group["items"]) >= 1:
                result.append({
                    "count": len(group["items"]),
                    "category": group["items"][0]["category"],
                    "sample": group["items"][0]["message"],
                    "feedback_ids": [i["id"] for i in group["items"]],
                    "testers": list(set(i.get("tester_name") or "Unknown" for i in group["items"])),
                })
        result.sort(key=lambda x: x["count"], reverse=True)
        return result

    def create_feedback_ai_run(self, data):
        conn = self._conn()
        cur = conn.execute(
            "INSERT INTO feedback_ai_runs "
            "(scope_type, scope_label, feedback_ids_json, filters_json, model, prompt_version, "
            "summary_json, dev_plan_json, created_by) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                data.get("scope_type", "status"),
                data.get("scope_label", ""),
                json.dumps(data.get("feedback_ids") or []),
                json.dumps(data.get("filters") or {}),
                data.get("model", ""),
                data.get("prompt_version", ""),
                json.dumps(data.get("summary") or {}),
                json.dumps(data.get("dev_plan") or {}),
                data.get("created_by"),
            ),
        )
        conn.commit()
        run_id = cur.lastrowid
        conn.close()
        return run_id

    def get_feedback_ai_run(self, run_id):
        conn = self._conn()
        row = conn.execute("SELECT * FROM feedback_ai_runs WHERE id = ?", (run_id,)).fetchone()
        conn.close()
        if not row:
            return None
        item = dict(row)
        item["feedback_ids"] = self._safe_json_list(item.get("feedback_ids_json"))
        item["filters"] = self._safe_json_object(item.get("filters_json"))
        item["summary"] = self._safe_json_object(item.get("summary_json"))
        item["dev_plan"] = self._safe_json_object(item.get("dev_plan_json"))
        return item

    def get_latest_feedback_ai_run(self):
        conn = self._conn()
        row = conn.execute("SELECT * FROM feedback_ai_runs ORDER BY id DESC LIMIT 1").fetchone()
        conn.close()
        if not row:
            return None
        item = dict(row)
        item["feedback_ids"] = self._safe_json_list(item.get("feedback_ids_json"))
        item["filters"] = self._safe_json_object(item.get("filters_json"))
        item["summary"] = self._safe_json_object(item.get("summary_json"))
        item["dev_plan"] = self._safe_json_object(item.get("dev_plan_json"))
        return item

    def save_feedback_ai_draft(self, data):
        conn = self._conn()
        conn.execute(
            "INSERT INTO feedback_ai_drafts "
            "(feedback_id, run_id, reply_subject, reply_draft, internal_note, recommended_status, "
            "confidence, needs_manual_review, approved, approved_by, approved_at, sent_at, send_error, generated_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now')) "
            "ON CONFLICT(feedback_id) DO UPDATE SET "
            "run_id=excluded.run_id, reply_subject=excluded.reply_subject, reply_draft=excluded.reply_draft, "
            "internal_note=excluded.internal_note, recommended_status=excluded.recommended_status, "
            "confidence=excluded.confidence, needs_manual_review=excluded.needs_manual_review, approved=0, "
            "approved_by=NULL, approved_at='', sent_at='', send_error='', generated_at=datetime('now'), updated_at=datetime('now')",
            (
                data.get("feedback_id"),
                data.get("run_id"),
                data.get("reply_subject", ""),
                data.get("reply_draft", ""),
                data.get("internal_note", ""),
                data.get("recommended_status", "reviewed"),
                float(data.get("confidence") or 0),
                1 if data.get("needs_manual_review") else 0,
                1 if data.get("approved") else 0,
                data.get("approved_by"),
                data.get("approved_at", ""),
                data.get("sent_at", ""),
                data.get("send_error", ""),
            ),
        )
        conn.commit()
        conn.close()

    def get_feedback_ai_drafts(self, feedback_ids=None):
        clauses = []
        params = []
        ids = [int(fid) for fid in (feedback_ids or []) if str(fid).strip()]
        if ids:
            placeholders = ",".join("?" for _ in ids)
            clauses.append(f"d.feedback_id IN ({placeholders})")
            params.extend(ids)
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        conn = self._conn()
        rows = conn.execute(
            "SELECT d.* FROM feedback_ai_drafts d "
            f"{where_sql} ORDER BY d.updated_at DESC, d.id DESC",
            tuple(params),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_feedback_ai_draft(self, feedback_id):
        conn = self._conn()
        row = conn.execute(
            "SELECT * FROM feedback_ai_drafts WHERE feedback_id = ?",
            (feedback_id,),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def approve_feedback_ai_draft(self, feedback_id, approved_by):
        conn = self._conn()
        conn.execute(
            "UPDATE feedback_ai_drafts SET approved = 1, approved_by = ?, approved_at = datetime('now'), updated_at = datetime('now') WHERE feedback_id = ?",
            (approved_by, feedback_id),
        )
        conn.commit()
        conn.close()

    def mark_feedback_ai_draft_sent(self, feedback_id, send_error=""):
        conn = self._conn()
        if send_error:
            conn.execute(
                "UPDATE feedback_ai_drafts SET send_error = ?, updated_at = datetime('now') WHERE feedback_id = ?",
                (send_error, feedback_id),
            )
        else:
            conn.execute(
                "UPDATE feedback_ai_drafts SET sent_at = datetime('now'), send_error = '', updated_at = datetime('now') WHERE feedback_id = ?",
                (feedback_id,),
            )
        conn.commit()
        conn.close()

    # ── Site Builder ──

    def create_site_build(self, brand_id, blueprint, model="gpt-4o-mini", created_by=0, intake=None):
        conn = self._conn()
        cur = conn.execute(
            "INSERT INTO site_builds (brand_id, status, model, blueprint_json, page_count, created_by, intake_json) "
            "VALUES (?, 'pending', ?, ?, ?, ?, ?)",
            (brand_id, model, json.dumps(blueprint), len(blueprint), created_by, json.dumps(intake or {})),
        )
        build_id = cur.lastrowid
        conn.commit()
        conn.close()
        return build_id

    def get_site_build(self, build_id):
        conn = self._conn()
        row = conn.execute("SELECT * FROM site_builds WHERE id = ?", (build_id,)).fetchone()
        conn.close()
        if not row:
            return None
        item = dict(row)
        item["blueprint"] = self._safe_json_list(item.get("blueprint_json"))
        try:
            item["intake"] = json.loads(item.get("intake_json") or "{}")
        except (json.JSONDecodeError, TypeError):
            item["intake"] = {}
        return item

    def get_site_builds(self, brand_id, limit=20):
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM site_builds WHERE brand_id = ? ORDER BY id DESC LIMIT ?",
            (brand_id, limit),
        ).fetchall()
        conn.close()
        result = []
        for row in rows:
            item = dict(row)
            item["blueprint"] = self._safe_json_list(item.get("blueprint_json"))
            result.append(item)
        return result

    def delete_site_build(self, build_id, brand_id=None):
        conn = self._conn()
        if brand_id is None:
            cur = conn.execute("DELETE FROM site_builds WHERE id = ?", (build_id,))
        else:
            cur = conn.execute(
                "DELETE FROM site_builds WHERE id = ? AND brand_id = ?",
                (build_id, brand_id),
            )
        conn.commit()
        deleted = cur.rowcount or 0
        conn.close()
        return deleted > 0

    def update_site_build_status(self, build_id, status, pages_completed=None, error_message=None):
        conn = self._conn()
        sets = ["status = ?"]
        params = [status]
        if pages_completed is not None:
            sets.append("pages_completed = ?")
            params.append(pages_completed)
        if error_message is not None:
            sets.append("error_message = ?")
            params.append(error_message)
        if status in ("completed", "failed"):
            sets.append("completed_at = datetime('now')")
        params.append(build_id)
        conn.execute(f"UPDATE site_builds SET {', '.join(sets)} WHERE id = ?", tuple(params))
        conn.commit()
        conn.close()

    def save_site_page(self, data):
        conn = self._conn()
        cur = conn.execute(
            "INSERT INTO site_pages "
            "(build_id, brand_id, page_type, label, slug, title, content, excerpt, "
            "seo_title, seo_description, primary_keyword, secondary_keywords, "
            "faq_items_json, schema_json, schema_html, full_html, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                data.get("build_id"),
                data.get("brand_id"),
                data.get("page_type", "home"),
                data.get("label", ""),
                data.get("slug", ""),
                data.get("title", ""),
                data.get("content", ""),
                data.get("excerpt", ""),
                data.get("seo_title", ""),
                data.get("seo_description", ""),
                data.get("primary_keyword", ""),
                data.get("secondary_keywords", ""),
                json.dumps(data.get("faq_items") or []),
                json.dumps(data.get("schemas") or []),
                data.get("schema_html", ""),
                data.get("full_html", ""),
                data.get("status", "draft"),
            ),
        )
        page_id = cur.lastrowid
        conn.commit()
        conn.close()
        return page_id

    def get_site_pages(self, build_id):
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM site_pages WHERE build_id = ? ORDER BY id",
            (build_id,),
        ).fetchall()
        conn.close()
        result = []
        for row in rows:
            item = dict(row)
            item["faq_items"] = self._safe_json_list(item.get("faq_items_json"))
            item["schemas"] = self._safe_json_list(item.get("schema_json"))
            result.append(item)
        return result

    def get_site_page(self, page_id):
        conn = self._conn()
        row = conn.execute("SELECT * FROM site_pages WHERE id = ?", (page_id,)).fetchone()
        conn.close()
        if not row:
            return None
        item = dict(row)
        item["faq_items"] = self._safe_json_list(item.get("faq_items_json"))
        item["schemas"] = self._safe_json_list(item.get("schema_json"))
        return item

    def update_site_page_wp(self, page_id, wp_page_id, wp_page_url):
        conn = self._conn()
        conn.execute(
            "UPDATE site_pages SET wp_page_id = ?, wp_page_url = ?, status = 'published', "
            "published_at = datetime('now'), updated_at = datetime('now') WHERE id = ?",
            (wp_page_id, wp_page_url, page_id),
        )
        conn.commit()
        conn.close()

    def update_site_page_content(self, page_id, data):
        """Update content, SEO fields, editor data, and/or CSS for a site page."""
        conn = self._conn()
        allowed = {
            "title", "content", "excerpt", "seo_title", "seo_description",
            "primary_keyword", "secondary_keywords", "faq_items_json",
            "schema_json", "schema_html", "full_html", "editor_json", "page_css",
        }
        sets = []
        params = []
        for key, val in data.items():
            if key in allowed:
                sets.append(f"{key} = ?")
                params.append(val)
        if not sets:
            conn.close()
            return
        sets.append("updated_at = datetime('now')")
        params.append(page_id)
        conn.execute(f"UPDATE site_pages SET {', '.join(sets)} WHERE id = ?", tuple(params))
        conn.commit()
        conn.close()

    # ── Site Builder Admin: Templates ──

    def create_sb_template(self, data):
        conn = self._conn()
        cur = conn.execute(
            "INSERT INTO sb_templates (name, category, page_types, html_content, css_content, "
            "preview_image, description, sort_order, is_active) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                data.get("name", ""),
                data.get("category", "section"),
                data.get("page_types", ""),
                data.get("html_content", ""),
                data.get("css_content", ""),
                data.get("preview_image", ""),
                data.get("description", ""),
                data.get("sort_order", 0),
                data.get("is_active", 1),
            ),
        )
        template_id = cur.lastrowid
        conn.commit()
        conn.close()
        return template_id

    def get_sb_template(self, template_id):
        conn = self._conn()
        row = conn.execute("SELECT * FROM sb_templates WHERE id = ?", (template_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def get_sb_templates(self, category=None, active_only=True):
        conn = self._conn()
        sql = "SELECT * FROM sb_templates"
        params = []
        clauses = []
        if category:
            clauses.append("category = ?")
            params.append(category)
        if active_only:
            clauses.append("is_active = 1")
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY sort_order, name"
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def update_sb_template(self, template_id, data):
        conn = self._conn()
        fields = []
        params = []
        for key in ("name", "category", "page_types", "html_content", "css_content",
                     "preview_image", "description", "sort_order", "is_active"):
            if key in data:
                fields.append(f"{key} = ?")
                params.append(data[key])
        if not fields:
            conn.close()
            return
        fields.append("updated_at = datetime('now')")
        params.append(template_id)
        conn.execute(f"UPDATE sb_templates SET {', '.join(fields)} WHERE id = ?", params)
        conn.commit()
        conn.close()

    def delete_sb_template(self, template_id):
        conn = self._conn()
        conn.execute("DELETE FROM sb_templates WHERE id = ?", (template_id,))
        conn.commit()
        conn.close()

    # ── Site Builder Admin: Themes ──

    def create_sb_theme(self, data):
        conn = self._conn()
        cur = conn.execute(
            "INSERT INTO sb_themes (name, description, primary_color, secondary_color, "
            "accent_color, text_color, bg_color, font_heading, font_body, button_style, "
            "layout_style, custom_css, preview_image, is_default, is_active) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                data.get("name", ""),
                data.get("description", ""),
                data.get("primary_color", "#2563eb"),
                data.get("secondary_color", "#1e40af"),
                data.get("accent_color", "#f59e0b"),
                data.get("text_color", "#1f2937"),
                data.get("bg_color", "#ffffff"),
                data.get("font_heading", "Inter"),
                data.get("font_body", "Inter"),
                data.get("button_style", "rounded"),
                data.get("layout_style", "modern"),
                data.get("custom_css", ""),
                data.get("preview_image", ""),
                data.get("is_default", 0),
                data.get("is_active", 1),
            ),
        )
        theme_id = cur.lastrowid
        conn.commit()
        conn.close()
        return theme_id

    def get_sb_theme(self, theme_id):
        conn = self._conn()
        row = conn.execute("SELECT * FROM sb_themes WHERE id = ?", (theme_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def get_sb_themes(self, active_only=True):
        conn = self._conn()
        sql = "SELECT * FROM sb_themes"
        if active_only:
            sql += " WHERE is_active = 1"
        sql += " ORDER BY is_default DESC, name"
        rows = conn.execute(sql).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_sb_default_theme(self):
        conn = self._conn()
        row = conn.execute(
            "SELECT * FROM sb_themes WHERE is_default = 1 AND is_active = 1 LIMIT 1"
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def update_sb_theme(self, theme_id, data):
        conn = self._conn()
        fields = []
        params = []
        for key in ("name", "description", "primary_color", "secondary_color",
                     "accent_color", "text_color", "bg_color", "font_heading",
                     "font_body", "button_style", "layout_style", "custom_css",
                     "preview_image", "is_default", "is_active"):
            if key in data:
                fields.append(f"{key} = ?")
                params.append(data[key])
        if not fields:
            conn.close()
            return
        fields.append("updated_at = datetime('now')")
        params.append(theme_id)
        # If setting as default, clear other defaults first
        if data.get("is_default"):
            conn.execute("UPDATE sb_themes SET is_default = 0")
        conn.execute(f"UPDATE sb_themes SET {', '.join(fields)} WHERE id = ?", params)
        conn.commit()
        conn.close()

    def delete_sb_theme(self, theme_id):
        conn = self._conn()
        conn.execute("DELETE FROM sb_themes WHERE id = ?", (theme_id,))
        conn.commit()
        conn.close()

    def _upsert_sb_theme_by_name(self, conn, data):
        name = str(data.get("name") or "").strip()
        row = conn.execute("SELECT id FROM sb_themes WHERE name = ? LIMIT 1", (name,)).fetchone()
        if data.get("is_default"):
            conn.execute("UPDATE sb_themes SET is_default = 0")

        params = (
            name,
            data.get("description", ""),
            data.get("primary_color", "#2563eb"),
            data.get("secondary_color", "#1e40af"),
            data.get("accent_color", "#f59e0b"),
            data.get("text_color", "#1f2937"),
            data.get("bg_color", "#ffffff"),
            data.get("font_heading", "Inter"),
            data.get("font_body", "Inter"),
            data.get("button_style", "rounded"),
            data.get("layout_style", "modern"),
            data.get("custom_css", ""),
            data.get("preview_image", ""),
            data.get("is_default", 0),
            data.get("is_active", 1),
        )
        if row:
            conn.execute(
                "UPDATE sb_themes SET name = ?, description = ?, primary_color = ?, secondary_color = ?, "
                "accent_color = ?, text_color = ?, bg_color = ?, font_heading = ?, font_body = ?, "
                "button_style = ?, layout_style = ?, custom_css = ?, preview_image = ?, is_default = ?, "
                "is_active = ?, updated_at = datetime('now') WHERE id = ?",
                params + (row["id"],),
            )
            return row["id"], False

        cur = conn.execute(
            "INSERT INTO sb_themes (name, description, primary_color, secondary_color, accent_color, text_color, "
            "bg_color, font_heading, font_body, button_style, layout_style, custom_css, preview_image, is_default, is_active) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params,
        )
        return cur.lastrowid, True

    def _upsert_sb_template_by_name(self, conn, data):
        name = str(data.get("name") or "").strip()
        row = conn.execute("SELECT id FROM sb_templates WHERE name = ? LIMIT 1", (name,)).fetchone()
        params = (
            name,
            data.get("category", "section"),
            data.get("page_types", ""),
            data.get("html_content", ""),
            data.get("css_content", ""),
            data.get("preview_image", ""),
            data.get("description", ""),
            data.get("sort_order", 0),
            data.get("is_active", 1),
        )
        if row:
            conn.execute(
                "UPDATE sb_templates SET name = ?, category = ?, page_types = ?, html_content = ?, css_content = ?, "
                "preview_image = ?, description = ?, sort_order = ?, is_active = ?, updated_at = datetime('now') WHERE id = ?",
                params + (row["id"],),
            )
            return row["id"], False

        cur = conn.execute(
            "INSERT INTO sb_templates (name, category, page_types, html_content, css_content, preview_image, description, sort_order, is_active) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params,
        )
        return cur.lastrowid, True

    def _upsert_sb_site_template_by_slug(self, conn, data):
        slug = str(data.get("slug") or "").strip()
        row = conn.execute("SELECT id FROM sb_site_templates WHERE slug = ? LIMIT 1", (slug,)).fetchone()
        if data.get("is_default"):
            conn.execute("UPDATE sb_site_templates SET is_default = 0")

        params = (
            data.get("name", ""),
            slug,
            data.get("description", ""),
            data.get("preview_image", ""),
            int(data.get("theme_id") or 0),
            json.dumps(list(data.get("template_ids") or [])),
            data.get("prompt_notes", ""),
            data.get("sort_order", 0),
            data.get("is_default", 0),
            data.get("is_active", 1),
        )
        if row:
            conn.execute(
                "UPDATE sb_site_templates SET name = ?, slug = ?, description = ?, preview_image = ?, theme_id = ?, "
                "template_ids_json = ?, prompt_notes = ?, sort_order = ?, is_default = ?, is_active = ?, "
                "updated_at = datetime('now') WHERE id = ?",
                params + (row["id"],),
            )
            return row["id"], False

        cur = conn.execute(
            "INSERT INTO sb_site_templates (name, slug, description, preview_image, theme_id, template_ids_json, prompt_notes, sort_order, is_default, is_active) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params,
        )
        return cur.lastrowid, True

    def seed_default_site_builder_kits(self):
        from webapp.site_builder_kits import get_production_site_kit_definitions

        conn = self._conn()
        counts = {
            "themes_created": 0,
            "themes_updated": 0,
            "templates_created": 0,
            "templates_updated": 0,
            "site_templates_created": 0,
            "site_templates_updated": 0,
            "kits": 0,
        }
        try:
            for kit in get_production_site_kit_definitions():
                theme_id, theme_created = self._upsert_sb_theme_by_name(conn, kit.get("theme") or {})
                counts["themes_created" if theme_created else "themes_updated"] += 1

                template_ids = []
                for template in kit.get("templates") or []:
                    template_id, template_created = self._upsert_sb_template_by_name(conn, template)
                    template_ids.append(template_id)
                    counts["templates_created" if template_created else "templates_updated"] += 1

                site_template = dict(kit.get("site_template") or {})
                site_template["theme_id"] = theme_id
                site_template["template_ids"] = template_ids
                _, site_template_created = self._upsert_sb_site_template_by_slug(conn, site_template)
                counts["site_templates_created" if site_template_created else "site_templates_updated"] += 1
                counts["kits"] += 1

            conn.commit()
        finally:
            conn.close()
        return counts

    def ensure_default_site_builder_kits(self):
        existing = self.get_sb_site_templates(active_only=False)
        if existing:
            return {
                "seeded": False,
                "kits": len(existing),
            }
        result = self.seed_default_site_builder_kits()
        result["seeded"] = True
        return result

    # ── Site Builder Admin: Full Site Templates ──

    def _decorate_sb_site_template(self, item):
        item = dict(item or {})
        item["template_ids"] = self._safe_json_list(item.get("template_ids_json"))
        item["template_count"] = len(item["template_ids"])
        theme_id = int(item.get("theme_id") or 0)
        item["theme"] = self.get_sb_theme(theme_id) if theme_id else None
        item["theme_name"] = (item.get("theme") or {}).get("name") or ""
        return item

    def create_sb_site_template(self, data):
        conn = self._conn()
        if data.get("is_default"):
            conn.execute("UPDATE sb_site_templates SET is_default = 0")
        cur = conn.execute(
            "INSERT INTO sb_site_templates (name, slug, description, preview_image, theme_id, "
            "template_ids_json, prompt_notes, sort_order, is_default, is_active) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                data.get("name", ""),
                data.get("slug", ""),
                data.get("description", ""),
                data.get("preview_image", ""),
                int(data.get("theme_id") or 0),
                json.dumps(list(data.get("template_ids") or [])),
                data.get("prompt_notes", ""),
                data.get("sort_order", 0),
                data.get("is_default", 0),
                data.get("is_active", 1),
            ),
        )
        site_template_id = cur.lastrowid
        conn.commit()
        conn.close()
        return site_template_id

    def get_sb_site_template(self, site_template_id):
        conn = self._conn()
        row = conn.execute("SELECT * FROM sb_site_templates WHERE id = ?", (site_template_id,)).fetchone()
        conn.close()
        return self._decorate_sb_site_template(row) if row else None

    def get_sb_site_templates(self, active_only=True):
        conn = self._conn()
        sql = "SELECT * FROM sb_site_templates"
        if active_only:
            sql += " WHERE is_active = 1"
        sql += " ORDER BY is_default DESC, sort_order, name"
        rows = conn.execute(sql).fetchall()
        conn.close()
        return [self._decorate_sb_site_template(row) for row in rows]

    def get_sb_default_site_template(self):
        conn = self._conn()
        row = conn.execute(
            "SELECT * FROM sb_site_templates WHERE is_default = 1 AND is_active = 1 LIMIT 1"
        ).fetchone()
        conn.close()
        return self._decorate_sb_site_template(row) if row else None

    def update_sb_site_template(self, site_template_id, data):
        conn = self._conn()
        fields = []
        params = []
        for key in (
            "name", "slug", "description", "preview_image", "theme_id",
            "prompt_notes", "sort_order", "is_default", "is_active",
        ):
            if key in data:
                value = data[key]
                if key == "theme_id":
                    value = int(value or 0)
                fields.append(f"{key} = ?")
                params.append(value)
        if "template_ids" in data:
            fields.append("template_ids_json = ?")
            params.append(json.dumps(list(data.get("template_ids") or [])))
        if not fields:
            conn.close()
            return
        fields.append("updated_at = datetime('now')")
        params.append(site_template_id)
        if data.get("is_default"):
            conn.execute("UPDATE sb_site_templates SET is_default = 0")
        conn.execute(f"UPDATE sb_site_templates SET {', '.join(fields)} WHERE id = ?", params)
        conn.commit()
        conn.close()

    def delete_sb_site_template(self, site_template_id):
        conn = self._conn()
        conn.execute("DELETE FROM sb_site_templates WHERE id = ?", (site_template_id,))
        conn.commit()
        conn.close()

    # ── Site Builder Admin: Prompt Overrides ──

    def save_sb_prompt_override(self, page_type, section, content, notes="", updated_by=""):
        conn = self._conn()
        conn.execute(
            "INSERT INTO sb_prompt_overrides (page_type, section, content, notes, updated_by) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(page_type, section) DO UPDATE SET "
            "content = excluded.content, notes = excluded.notes, "
            "updated_by = excluded.updated_by, updated_at = datetime('now')",
            (page_type, section, content, notes, updated_by),
        )
        conn.commit()
        conn.close()

    def get_sb_prompt_override(self, page_type, section="user_prompt"):
        conn = self._conn()
        row = conn.execute(
            "SELECT * FROM sb_prompt_overrides WHERE page_type = ? AND section = ?",
            (page_type, section),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def get_sb_prompt_overrides(self):
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM sb_prompt_overrides ORDER BY page_type, section"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def toggle_sb_prompt_override(self, override_id, is_active):
        conn = self._conn()
        conn.execute(
            "UPDATE sb_prompt_overrides SET is_active = ?, updated_at = datetime('now') WHERE id = ?",
            (is_active, override_id),
        )
        conn.commit()
        conn.close()

    def delete_sb_prompt_override(self, override_id):
        conn = self._conn()
        conn.execute("DELETE FROM sb_prompt_overrides WHERE id = ?", (override_id,))
        conn.commit()
        conn.close()

    # ── Site Builder Admin: Image Categories ──

    def create_sb_image_category(self, name, slug, description=""):
        conn = self._conn()
        cur = conn.execute(
            "INSERT INTO sb_image_categories (name, slug, description) VALUES (?, ?, ?)",
            (name, slug, description),
        )
        cat_id = cur.lastrowid
        conn.commit()
        conn.close()
        return cat_id

    def get_sb_image_categories(self):
        conn = self._conn()
        rows = conn.execute(
            "SELECT c.*, COUNT(i.id) as image_count "
            "FROM sb_image_categories c "
            "LEFT JOIN sb_images i ON i.category_id = c.id AND i.is_active = 1 "
            "GROUP BY c.id ORDER BY c.sort_order, c.name"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def update_sb_image_category(self, cat_id, name, slug, description=""):
        conn = self._conn()
        conn.execute(
            "UPDATE sb_image_categories SET name = ?, slug = ?, description = ? WHERE id = ?",
            (name, slug, description, cat_id),
        )
        conn.commit()
        conn.close()

    def delete_sb_image_category(self, cat_id):
        conn = self._conn()
        # Unlink images, don't delete them
        conn.execute("UPDATE sb_images SET category_id = NULL WHERE category_id = ?", (cat_id,))
        conn.execute("DELETE FROM sb_image_categories WHERE id = ?", (cat_id,))
        conn.commit()
        conn.close()

    # ── Site Builder Admin: Images ──

    def create_sb_image(self, data):
        conn = self._conn()
        cur = conn.execute(
            "INSERT INTO sb_images (filename, original_name, file_path, file_size, mime_type, "
            "width, height, alt_text, title, category_id, tags, industry, page_types, "
            "source, drive_file_id, wp_media_id, wp_media_url) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                data.get("filename", ""),
                data.get("original_name", ""),
                data.get("file_path", ""),
                data.get("file_size", 0),
                data.get("mime_type", "image/jpeg"),
                data.get("width", 0),
                data.get("height", 0),
                data.get("alt_text", ""),
                data.get("title", ""),
                data.get("category_id"),
                data.get("tags", ""),
                data.get("industry", ""),
                data.get("page_types", ""),
                data.get("source", "upload"),
                data.get("drive_file_id", ""),
                data.get("wp_media_id", 0),
                data.get("wp_media_url", ""),
            ),
        )
        image_id = cur.lastrowid
        conn.commit()
        conn.close()
        return image_id

    def get_sb_image(self, image_id):
        conn = self._conn()
        row = conn.execute("SELECT * FROM sb_images WHERE id = ?", (image_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def get_sb_images(self, category_id=None, industry=None, tags=None, limit=100, offset=0):
        conn = self._conn()
        sql = "SELECT * FROM sb_images WHERE is_active = 1"
        params = []
        if category_id:
            sql += " AND category_id = ?"
            params.append(category_id)
        if industry:
            sql += " AND industry = ?"
            params.append(industry)
        if tags:
            # Search tags field (comma-separated) for any matching tag
            tag_clauses = []
            for tag in tags.split(","):
                tag = tag.strip()
                if tag:
                    tag_clauses.append("tags LIKE ?")
                    params.append(f"%{tag}%")
            if tag_clauses:
                sql += " AND (" + " OR ".join(tag_clauses) + ")"
        sql += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def count_sb_images(self, category_id=None, industry=None):
        conn = self._conn()
        sql = "SELECT COUNT(*) FROM sb_images WHERE is_active = 1"
        params = []
        if category_id:
            sql += " AND category_id = ?"
            params.append(category_id)
        if industry:
            sql += " AND industry = ?"
            params.append(industry)
        count = conn.execute(sql, params).fetchone()[0]
        conn.close()
        return count

    def update_sb_image(self, image_id, data):
        conn = self._conn()
        fields = []
        params = []
        for key in ("alt_text", "title", "category_id", "tags", "industry",
                     "page_types", "wp_media_id", "wp_media_url", "is_active"):
            if key in data:
                fields.append(f"{key} = ?")
                params.append(data[key])
        if not fields:
            conn.close()
            return
        params.append(image_id)
        conn.execute(f"UPDATE sb_images SET {', '.join(fields)} WHERE id = ?", params)
        conn.commit()
        conn.close()

    def delete_sb_image(self, image_id):
        conn = self._conn()
        conn.execute("UPDATE sb_images SET is_active = 0 WHERE id = ?", (image_id,))
        conn.commit()
        conn.close()

    def bulk_update_sb_images(self, image_ids, data):
        if not image_ids:
            return
        conn = self._conn()
        fields = []
        params = []
        for key in ("category_id", "tags", "industry", "page_types"):
            if key in data:
                fields.append(f"{key} = ?")
                params.append(data[key])
        if not fields:
            conn.close()
            return
        placeholders = ",".join("?" * len(image_ids))
        params.extend(image_ids)
        conn.execute(
            f"UPDATE sb_images SET {', '.join(fields)} WHERE id IN ({placeholders})",
            params,
        )
        conn.commit()
        conn.close()

    # ── Upgrade Considerations ──

    def create_upgrade_consideration(self, data):
        conn = self._conn()
        conn.execute(
            "INSERT INTO upgrade_considerations "
            "(title, description, category, source_feedback_ids, request_count, "
            "feasibility, safety_risk, priority, status, decision_notes) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                data.get("title", ""),
                data.get("description", ""),
                data.get("category", "feature"),
                data.get("source_feedback_ids", ""),
                data.get("request_count", 1),
                data.get("feasibility", "unknown"),
                data.get("safety_risk", "low"),
                data.get("priority", "medium"),
                data.get("status", "proposed"),
                data.get("decision_notes", ""),
            ),
        )
        conn.commit()
        conn.close()

    def get_upgrade_considerations(self, status=None):
        conn = self._conn()
        if status:
            rows = conn.execute(
                "SELECT * FROM upgrade_considerations WHERE status = ? ORDER BY "
                "CASE priority WHEN 'critical' THEN 0 WHEN 'high' THEN 1 "
                "WHEN 'medium' THEN 2 WHEN 'low' THEN 3 ELSE 4 END, created_at DESC",
                (status,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM upgrade_considerations ORDER BY "
                "CASE priority WHEN 'critical' THEN 0 WHEN 'high' THEN 1 "
                "WHEN 'medium' THEN 2 WHEN 'low' THEN 3 ELSE 4 END, created_at DESC"
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def update_upgrade_consideration(self, consideration_id, data):
        conn = self._conn()
        conn.execute(
            "UPDATE upgrade_considerations SET title=?, description=?, category=?, "
            "feasibility=?, safety_risk=?, priority=?, status=?, decision_notes=?, "
            "request_count=?, updated_at=datetime('now') WHERE id=?",
            (
                data.get("title", ""),
                data.get("description", ""),
                data.get("category", "feature"),
                data.get("feasibility", "unknown"),
                data.get("safety_risk", "low"),
                data.get("priority", "medium"),
                data.get("status", "proposed"),
                data.get("decision_notes", ""),
                data.get("request_count", 1),
                consideration_id,
            ),
        )
        conn.commit()
        conn.close()

    def delete_upgrade_consideration(self, consideration_id):
        conn = self._conn()
        conn.execute("DELETE FROM upgrade_considerations WHERE id = ?", (consideration_id,))
        conn.commit()
        conn.close()

    def get_upgrade_stats(self):
        conn = self._conn()
        total = conn.execute("SELECT COUNT(*) as c FROM upgrade_considerations").fetchone()["c"]
        proposed = conn.execute("SELECT COUNT(*) as c FROM upgrade_considerations WHERE status='proposed'").fetchone()["c"]
        approved = conn.execute("SELECT COUNT(*) as c FROM upgrade_considerations WHERE status='approved'").fetchone()["c"]
        building = conn.execute("SELECT COUNT(*) as c FROM upgrade_considerations WHERE status='building'").fetchone()["c"]
        shipped = conn.execute("SELECT COUNT(*) as c FROM upgrade_considerations WHERE status='shipped'").fetchone()["c"]
        rejected = conn.execute("SELECT COUNT(*) as c FROM upgrade_considerations WHERE status='rejected'").fetchone()["c"]
        conn.close()
        return {"total": total, "proposed": proposed, "approved": approved,
                "building": building, "shipped": shipped, "rejected": rejected}

    # ── Feature Flags ──

    def get_feature_flags(self):
        conn = self._conn()
        rows = conn.execute("SELECT * FROM feature_flags ORDER BY sort_order, id").fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_feature_flag(self, feature_key):
        conn = self._conn()
        row = conn.execute("SELECT * FROM feature_flags WHERE feature_key = ?", (feature_key,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def update_feature_flag(self, feature_key, access_level, enabled):
        conn = self._conn()
        conn.execute(
            "UPDATE feature_flags SET access_level = ?, enabled = ? WHERE feature_key = ?",
            (self._normalize_feature_access_level(access_level), 1 if enabled else 0, feature_key),
        )
        conn.commit()
        conn.close()

    def is_beta_brand(self, brand_id):
        conn = self._conn()
        row = conn.execute(
            "SELECT id FROM beta_testers WHERE brand_id = ? AND status IN ('approved', 'activated') LIMIT 1",
            (brand_id,),
        ).fetchone()
        conn.close()
        return row is not None

    # ── Drip Campaigns ──

    def get_drip_sequences(self):
        conn = self._conn()
        rows = conn.execute("SELECT * FROM drip_sequences ORDER BY id").fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_drip_sequence(self, seq_id):
        conn = self._conn()
        row = conn.execute("SELECT * FROM drip_sequences WHERE id = ?", (seq_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def create_drip_sequence(self, name, description="", trigger="assessment"):
        conn = self._conn()
        conn.execute(
            "INSERT INTO drip_sequences (name, description, trigger) VALUES (?, ?, ?)",
            (name, description, trigger),
        )
        conn.commit()
        row = conn.execute("SELECT last_insert_rowid() AS id").fetchone()
        seq_id = row["id"]
        conn.close()
        return seq_id

    def update_drip_sequence(self, seq_id, name, description, is_active):
        conn = self._conn()
        conn.execute(
            "UPDATE drip_sequences SET name = ?, description = ?, is_active = ? WHERE id = ?",
            (name, description, 1 if is_active else 0, seq_id),
        )
        conn.commit()
        conn.close()

    def delete_drip_sequence(self, seq_id):
        conn = self._conn()
        conn.execute("DELETE FROM drip_sequences WHERE id = ?", (seq_id,))
        conn.commit()
        conn.close()

    def get_drip_steps(self, sequence_id):
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM drip_steps WHERE sequence_id = ? ORDER BY step_order", (sequence_id,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_drip_step(self, step_id):
        conn = self._conn()
        row = conn.execute("SELECT * FROM drip_steps WHERE id = ?", (step_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def create_drip_step(self, sequence_id, step_order, delay_days, subject, body_html, body_text=""):
        conn = self._conn()
        conn.execute(
            "INSERT INTO drip_steps (sequence_id, step_order, delay_days, subject, body_html, body_text) VALUES (?, ?, ?, ?, ?, ?)",
            (sequence_id, step_order, delay_days, subject, body_html, body_text),
        )
        conn.commit()
        conn.close()

    def update_drip_step(self, step_id, step_order, delay_days, subject, body_html, body_text=""):
        conn = self._conn()
        conn.execute(
            "UPDATE drip_steps SET step_order = ?, delay_days = ?, subject = ?, body_html = ?, body_text = ? WHERE id = ?",
            (step_order, delay_days, subject, body_html, body_text, step_id),
        )
        conn.commit()
        conn.close()

    def delete_drip_step(self, step_id):
        conn = self._conn()
        conn.execute("DELETE FROM drip_steps WHERE id = ?", (step_id,))
        conn.commit()
        conn.close()

    # Enrollments

    def enroll_in_drip(self, sequence_id, email, name="", lead_source="assessment", lead_id=None):
        """Enroll a lead unless already active in this sequence."""
        email = (email or "").strip().lower()
        if not email:
            return None
        conn = self._conn()
        existing = conn.execute(
            "SELECT id FROM drip_enrollments WHERE sequence_id = ? AND LOWER(email) = ? AND status = 'active'",
            (sequence_id, email),
        ).fetchone()
        if existing:
            conn.close()
            return None
        conn.execute(
            "INSERT INTO drip_enrollments (sequence_id, email, name, lead_source, lead_id, status) VALUES (?, ?, ?, ?, ?, 'active')",
            (sequence_id, email, name, lead_source, lead_id),
        )
        conn.commit()
        row = conn.execute("SELECT last_insert_rowid() AS id").fetchone()
        eid = row["id"]
        conn.close()
        return eid

    def get_drip_enrollments(self, sequence_id=None, status=None, limit=200):
        conn = self._conn()
        sql = "SELECT * FROM drip_enrollments WHERE 1=1"
        params = []
        if sequence_id:
            sql += " AND sequence_id = ?"
            params.append(sequence_id)
        if status:
            sql += " AND status = ?"
            params.append(status)
        sql += " ORDER BY enrolled_at DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_drip_enrollment(self, enrollment_id):
        conn = self._conn()
        row = conn.execute("SELECT * FROM drip_enrollments WHERE id = ?", (enrollment_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def get_lead_drip_enrollments(self, lead_source, lead_id, status=None, limit=20):
        conn = self._conn()
        sql = """
            SELECT e.*, seq.name AS sequence_name, seq.trigger AS sequence_trigger, seq.is_active AS sequence_active
            FROM drip_enrollments e
            JOIN drip_sequences seq ON seq.id = e.sequence_id
            WHERE e.lead_source = ? AND e.lead_id = ?
        """
        params = [(lead_source or "").strip(), lead_id]
        if status:
            sql += " AND e.status = ?"
            params.append((status or "").strip())
        sql += " ORDER BY e.enrolled_at DESC, e.id DESC LIMIT ?"
        params.append(int(limit or 20))
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_pending_drip_sends(self):
        """Return active enrollments that have a next step due based on delay_days."""
        conn = self._conn()
        rows = conn.execute("""
            SELECT e.id AS enrollment_id, e.sequence_id, e.email, e.name,
                   e.current_step, e.enrolled_at, e.lead_source,
                   e.lead_id,
                   seq.name AS sequence_name,
                   s.id AS step_id, s.step_order, s.delay_days,
                   s.subject, s.body_html, s.body_text
            FROM drip_enrollments e
            JOIN drip_steps s ON s.sequence_id = e.sequence_id
                AND s.step_order = e.current_step + 1
            JOIN drip_sequences seq ON seq.id = e.sequence_id AND seq.is_active = 1
            WHERE e.status = 'active'
              AND datetime(e.enrolled_at, '+' || (
                  SELECT COALESCE(SUM(ds.delay_days), 0)
                  FROM drip_steps ds
                  WHERE ds.sequence_id = e.sequence_id
                    AND ds.step_order <= e.current_step + 1
              ) || ' days') <= datetime('now')
        """).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def record_drip_send(self, enrollment_id, step_id, expected_step, status="sent", error=""):
        """Record a send and advance step only if current_step matches expected_step (race guard)."""
        conn = self._conn()
        conn.execute(
            "INSERT INTO drip_sends (enrollment_id, step_id, status, error) VALUES (?, ?, ?, ?)",
            (enrollment_id, step_id, status, error),
        )
        if status == "sent":
            conn.execute(
                "UPDATE drip_enrollments SET current_step = current_step + 1 WHERE id = ? AND current_step = ?",
                (enrollment_id, expected_step),
            )
        conn.commit()
        conn.close()

    def complete_drip_enrollment(self, enrollment_id, reason="completed"):
        conn = self._conn()
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if reason == "converted":
            conn.execute(
                "UPDATE drip_enrollments SET status = 'converted', converted_at = ? WHERE id = ?",
                (ts, enrollment_id),
            )
        elif reason == "unsubscribed":
            conn.execute(
                "UPDATE drip_enrollments SET status = 'unsubscribed', unsubscribed_at = ? WHERE id = ?",
                (ts, enrollment_id),
            )
        else:
            conn.execute(
                "UPDATE drip_enrollments SET status = 'completed', completed_at = ? WHERE id = ?",
                (ts, enrollment_id),
            )
        conn.commit()
        conn.close()

    def check_and_complete_finished_enrollments(self):
        conn = self._conn()
        conn.execute("""
            UPDATE drip_enrollments SET status = 'completed', completed_at = datetime('now')
            WHERE status = 'active'
              AND current_step >= (
                  SELECT MAX(step_order) FROM drip_steps
                  WHERE drip_steps.sequence_id = drip_enrollments.sequence_id
              )
        """)
        conn.commit()
        conn.close()

    def convert_drip_by_email(self, email):
        """When a lead converts (signs up as client), remove them from all active drips."""
        conn = self._conn()
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            "UPDATE drip_enrollments SET status = 'converted', converted_at = ? WHERE email = ? AND status = 'active'",
            (ts, email),
        )
        conn.execute(
            "UPDATE assessment_leads SET converted_to_brand_id = -1 WHERE email = ? AND converted_to_brand_id IS NULL",
            (email,),
        )
        conn.execute(
            "UPDATE signup_leads SET converted_to_brand_id = -1 WHERE email = ? AND converted_to_brand_id IS NULL",
            (email,),
        )
        conn.commit()
        conn.close()

    def get_drip_sends(self, enrollment_id=None, limit=100):
        conn = self._conn()
        if enrollment_id:
            rows = conn.execute(
                "SELECT * FROM drip_sends WHERE enrollment_id = ? ORDER BY sent_at DESC LIMIT ?",
                (enrollment_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM drip_sends ORDER BY sent_at DESC LIMIT ?", (limit,)
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_drip_stats(self):
        conn = self._conn()
        total = conn.execute("SELECT COUNT(*) FROM drip_enrollments").fetchone()[0]
        active = conn.execute("SELECT COUNT(*) FROM drip_enrollments WHERE status = 'active'").fetchone()[0]
        completed = conn.execute("SELECT COUNT(*) FROM drip_enrollments WHERE status = 'completed'").fetchone()[0]
        converted = conn.execute("SELECT COUNT(*) FROM drip_enrollments WHERE status = 'converted'").fetchone()[0]
        unsubscribed = conn.execute("SELECT COUNT(*) FROM drip_enrollments WHERE status = 'unsubscribed'").fetchone()[0]
        emails_sent = conn.execute("SELECT COUNT(*) FROM drip_sends WHERE status = 'sent'").fetchone()[0]
        conn.close()
        return {
            "total": total, "active": active, "completed": completed,
            "converted": converted, "unsubscribed": unsubscribed,
            "emails_sent": emails_sent,
        }

    def get_assessment_leads(self, limit=200):
        conn = self._conn()
        rows = conn.execute("SELECT * FROM assessment_leads ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_signup_leads(self, limit=200):
        conn = self._conn()
        rows = conn.execute("SELECT * FROM signup_leads ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_active_drip_sequence_for_trigger(self, trigger):
        conn = self._conn()
        row = conn.execute(
            "SELECT * FROM drip_sequences WHERE trigger = ? AND is_active = 1 ORDER BY id LIMIT 1",
            (trigger,),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    # ── Agent activity helpers ──

    def log_agent_activity(self, brand_id, agent_key, action, detail="", status="completed"):
        conn = self._conn()
        conn.execute(
            "INSERT INTO agent_activity (brand_id, agent_key, action, detail, status) VALUES (?,?,?,?,?)",
            (brand_id, agent_key, action, detail, status),
        )
        conn.commit()
        conn.close()

    def get_agent_activity(self, brand_id, limit=50, agent_key=None):
        conn = self._conn()
        if agent_key:
            rows = conn.execute(
                "SELECT * FROM agent_activity WHERE brand_id = ? AND agent_key = ? ORDER BY created_at DESC LIMIT ?",
                (brand_id, agent_key, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM agent_activity WHERE brand_id = ? ORDER BY created_at DESC LIMIT ?",
                (brand_id, limit),
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_agent_latest(self, brand_id):
        """Get the most recent activity per agent for a brand."""
        conn = self._conn()
        rows = conn.execute("""
            SELECT a.* FROM agent_activity a
            INNER JOIN (
                SELECT agent_key, MAX(id) as max_id
                FROM agent_activity WHERE brand_id = ?
                GROUP BY agent_key
            ) latest ON a.id = latest.max_id
            ORDER BY a.created_at DESC
        """, (brand_id,)).fetchall()
        conn.close()
        return {r["agent_key"]: dict(r) for r in rows}

    # ── Agent findings helpers ──

    def save_agent_finding(self, brand_id, agent_key, month, severity, title,
                           detail="", action="", extra_json="{}"):
        conn = self._conn()
        conn.execute(
            """INSERT INTO agent_findings
               (brand_id, agent_key, month, severity, title, detail, action, extra_json)
               VALUES (?,?,?,?,?,?,?,?)""",
            (brand_id, agent_key, month, severity, title, detail, action, extra_json),
        )
        conn.commit()
        conn.close()

    def get_agent_findings(self, brand_id, month=None, agent_key=None,
                           severity=None, dismissed=False, limit=50):
        conn = self._conn()
        sql = "SELECT * FROM agent_findings WHERE brand_id = ? AND dismissed = ?"
        params = [brand_id, 1 if dismissed else 0]
        if month:
            sql += " AND month = ?"
            params.append(month)
        if agent_key:
            sql += " AND agent_key = ?"
            params.append(agent_key)
        if severity:
            sql += " AND severity = ?"
            params.append(severity)
        sql += " ORDER BY CASE severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 WHEN 'positive' THEN 2 ELSE 3 END, created_at DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def dismiss_agent_finding(self, finding_id, brand_id):
        conn = self._conn()
        conn.execute(
            "UPDATE agent_findings SET dismissed = 1 WHERE id = ? AND brand_id = ?",
            (finding_id, brand_id),
        )
        conn.commit()
        conn.close()

    def clear_agent_findings(self, brand_id, month, agent_key=None):
        """Clear old findings before a fresh agent run.
        Preserves findings the user has interacted with (voted, acknowledged, in progress, done)."""
        conn = self._conn()
        preserve = "AND (user_vote = 0 OR user_vote IS NULL) AND (status IS NULL OR status = 'new')"
        if agent_key:
            conn.execute(
                f"DELETE FROM agent_findings WHERE brand_id = ? AND month = ? AND agent_key = ? AND dismissed = 0 {preserve}",
                (brand_id, month, agent_key),
            )
        else:
            conn.execute(
                f"DELETE FROM agent_findings WHERE brand_id = ? AND month = ? AND dismissed = 0 {preserve}",
                (brand_id, month),
            )
        conn.commit()
        conn.close()

    def vote_agent_finding(self, finding_id, brand_id, vote, feedback=""):
        """Record a thumbs-up (+1) or thumbs-down (-1) on a finding."""
        conn = self._conn()
        conn.execute(
            "UPDATE agent_findings SET user_vote = ?, user_feedback = ? WHERE id = ? AND brand_id = ?",
            (vote, (feedback or "")[:500], finding_id, brand_id),
        )
        conn.commit()
        conn.close()

    def update_finding_status(self, finding_id, brand_id, status):
        """Move a finding through its lifecycle: new -> acknowledged -> in_progress -> done -> dismissed."""
        conn = self._conn()
        conn.execute(
            "UPDATE agent_findings SET status = ? WHERE id = ? AND brand_id = ?",
            (status, finding_id, brand_id),
        )
        if status == "dismissed":
            conn.execute(
                "UPDATE agent_findings SET dismissed = 1 WHERE id = ? AND brand_id = ?",
                (finding_id, brand_id),
            )
        conn.commit()
        conn.close()

    def save_finding_outcome(self, finding_id, brand_id, outcome_note):
        """Store what happened after a finding was acted on (retrospective)."""
        conn = self._conn()
        conn.execute(
            "UPDATE agent_findings SET outcome_note = ? WHERE id = ? AND brand_id = ?",
            ((outcome_note or "")[:1000], finding_id, brand_id),
        )
        conn.commit()
        conn.close()

    def get_finding_feedback_for_agent(self, brand_id, agent_key, limit=20):
        """Get recent user feedback (thumbs down + reasons) for an agent, used in next run."""
        conn = self._conn()
        rows = conn.execute(
            """SELECT title, user_feedback, outcome_note, user_vote
               FROM agent_findings
               WHERE brand_id = ? AND agent_key = ? AND user_vote != 0
               ORDER BY created_at DESC LIMIT ?""",
            (brand_id, agent_key, limit),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_acted_findings_for_retrospective(self, brand_id, month):
        """Get findings that were acted on (done/acknowledged) for retrospective analysis."""
        conn = self._conn()
        rows = conn.execute(
            """SELECT * FROM agent_findings
               WHERE brand_id = ? AND month = ? AND status IN ('done', 'in_progress')
               ORDER BY agent_key, created_at""",
            (brand_id, month),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ── Agent forecast helpers ──

    def upsert_agent_forecast(
        self,
        *,
        brand_id: int,
        agent_key: str,
        created_month: str,
        target_month: str,
        forecast_json: str,
        method: str = "seasonal_naive",
        features_json: str = "{}",
    ):
        conn = self._conn()
        conn.execute(
            """INSERT INTO agent_forecasts
               (brand_id, agent_key, created_month, target_month, forecast_json, method, features_json, updated_at)
               VALUES (?,?,?,?,?,?,?, datetime('now'))
               ON CONFLICT(brand_id, agent_key, target_month)
               DO UPDATE SET
                   created_month=excluded.created_month,
                   forecast_json=excluded.forecast_json,
                   method=excluded.method,
                   features_json=excluded.features_json,
                   updated_at=datetime('now')""",
            (brand_id, agent_key, created_month, target_month, forecast_json, method, features_json),
        )
        conn.commit()
        conn.close()

    def get_agent_forecast(self, *, brand_id: int, agent_key: str, target_month: str):
        conn = self._conn()
        row = conn.execute(
            """SELECT * FROM agent_forecasts
               WHERE brand_id = ? AND agent_key = ? AND target_month = ?""",
            (brand_id, agent_key, target_month),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def score_agent_forecast(
        self,
        *,
        forecast_id: int,
        actual_json: str,
        mae: float = None,
        mape: float = None,
    ):
        conn = self._conn()
        conn.execute(
            """UPDATE agent_forecasts
               SET actual_json = ?, mae = ?, mape = ?, scored_at = datetime('now'), updated_at = datetime('now')
               WHERE id = ?""",
            (actual_json, mae, mape, forecast_id),
        )
        conn.commit()
        conn.close()

    # ── Client User Roles ──

    def update_client_user_role(self, client_user_id, role):
        conn = self._conn()
        conn.execute(
            "UPDATE client_users SET role = ? WHERE id = ?",
            (role, client_user_id),
        )
        conn.commit()
        conn.close()

    def update_client_user_profile(self, client_user_id, display_name, email):
        conn = self._conn()
        conn.execute(
            "UPDATE client_users SET display_name = ?, email = ? WHERE id = ?",
            (display_name, email, client_user_id),
        )
        conn.commit()
        conn.close()

    # ── Brand Tasks ──

    def create_brand_task(self, brand_id, title, description="", steps_json="[]",
                          priority="normal", source="manual", source_ref="",
                          assigned_to=None, created_by=None, due_date=""):
        conn = self._conn()
        conn.execute(
            """INSERT INTO brand_tasks
               (brand_id, title, description, steps_json, priority, source,
                source_ref, assigned_to, created_by, due_date)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (brand_id, title, description, steps_json, priority, source,
             source_ref, assigned_to, created_by, due_date),
        )
        conn.commit()
        task_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.close()
        return task_id

    def get_brand_tasks(self, brand_id, status=None, assigned_to=None, limit=100):
        conn = self._conn()
        sql = "SELECT t.*, u.display_name AS assignee_name FROM brand_tasks t LEFT JOIN client_users u ON t.assigned_to = u.id WHERE t.brand_id = ?"
        params = [brand_id]
        if status:
            sql += " AND t.status = ?"
            params.append(status)
        if assigned_to is not None:
            sql += " AND t.assigned_to = ?"
            params.append(assigned_to)
        sql += " ORDER BY CASE t.priority WHEN 'urgent' THEN 0 WHEN 'high' THEN 1 WHEN 'normal' THEN 2 ELSE 3 END, t.created_at DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_brand_task(self, task_id, brand_id):
        conn = self._conn()
        row = conn.execute(
            "SELECT t.*, u.display_name AS assignee_name FROM brand_tasks t LEFT JOIN client_users u ON t.assigned_to = u.id WHERE t.id = ? AND t.brand_id = ?",
            (task_id, brand_id),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def update_brand_task(self, task_id, brand_id, **fields):
        conn = self._conn()
        allowed = {"title", "description", "steps_json", "status", "priority",
                    "assigned_to", "due_date", "completed_at"}
        sets = []
        params = []
        for k, v in fields.items():
            if k in allowed:
                sets.append(f"{k} = ?")
                params.append(v)
        if not sets:
            conn.close()
            return
        sets.append("updated_at = datetime('now')")
        params.extend([task_id, brand_id])
        conn.execute(
            f"UPDATE brand_tasks SET {', '.join(sets)} WHERE id = ? AND brand_id = ?",
            params,
        )
        conn.commit()
        conn.close()

    def update_task_steps(self, task_id, brand_id, steps_json):
        conn = self._conn()
        conn.execute(
            "UPDATE brand_tasks SET steps_json = ?, updated_at = datetime('now') WHERE id = ? AND brand_id = ?",
            (steps_json, task_id, brand_id),
        )
        conn.commit()
        conn.close()

    def delete_brand_task(self, task_id, brand_id):
        conn = self._conn()
        conn.execute("DELETE FROM brand_tasks WHERE id = ? AND brand_id = ?", (task_id, brand_id))
        conn.commit()
        conn.close()

    # ── VA Desk ──

    def create_va_request(self, brand_id, title, details="", specialty_key="account_va",
                          priority="normal", requested_by=None, estimated_tokens=0,
                          status="submitted", status_note=""):
        conn = self._conn()
        conn.execute(
            """INSERT INTO va_requests
               (brand_id, title, details, specialty_key, priority, status,
                requested_by, estimated_tokens, status_note)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                brand_id,
                title,
                details,
                specialty_key,
                priority,
                status,
                requested_by,
                int(estimated_tokens or 0),
                status_note,
            ),
        )
        conn.commit()
        request_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.close()
        return request_id

    def get_va_requests(self, brand_id, status=None, limit=100):
        conn = self._conn()
        sql = (
            "SELECT vr.*, cu.display_name AS requested_by_name "
            "FROM va_requests vr "
            "LEFT JOIN client_users cu ON vr.requested_by = cu.id "
            "WHERE vr.brand_id = ?"
        )
        params = [brand_id]
        if status:
            sql += " AND vr.status = ?"
            params.append(status)
        sql += " ORDER BY CASE vr.status "
        sql += "WHEN 'in_progress' THEN 0 WHEN 'review' THEN 1 WHEN 'queued' THEN 2 "
        sql += "WHEN 'submitted' THEN 3 WHEN 'scoped' THEN 4 WHEN 'completed' THEN 5 ELSE 6 END, "
        sql += "vr.updated_at DESC, vr.id DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_va_request(self, request_id, brand_id):
        conn = self._conn()
        row = conn.execute(
            """SELECT vr.*, cu.display_name AS requested_by_name
               FROM va_requests vr
               LEFT JOIN client_users cu ON vr.requested_by = cu.id
               WHERE vr.id = ? AND vr.brand_id = ?""",
            (request_id, brand_id),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def update_va_request(self, request_id, brand_id, **fields):
        conn = self._conn()
        allowed = {
            "title", "details", "specialty_key", "priority", "status",
            "estimated_tokens", "actual_tokens_used", "status_note", "closed_at",
        }
        sets = []
        params = []
        for key, value in fields.items():
            if key not in allowed:
                continue
            sets.append(f"{key} = ?")
            if key in {"estimated_tokens", "actual_tokens_used"}:
                params.append(int(value or 0))
            else:
                params.append(value)
        if not sets:
            conn.close()
            return
        sets.append("updated_at = datetime('now')")
        params.extend([request_id, brand_id])
        conn.execute(
            f"UPDATE va_requests SET {', '.join(sets)} WHERE id = ? AND brand_id = ?",
            params,
        )
        conn.commit()
        conn.close()

    def create_va_token_entry(self, brand_id, token_amount, entry_type="adjustment",
                              specialty_key="", note="", request_id=None, created_by=None):
        conn = self._conn()
        conn.execute(
            """INSERT INTO va_token_ledger
               (brand_id, request_id, entry_type, specialty_key, token_amount, note, created_by)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                brand_id,
                request_id,
                entry_type,
                specialty_key,
                int(token_amount or 0),
                note,
                created_by,
            ),
        )
        conn.commit()
        entry_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.close()
        return entry_id

    def get_va_token_entries(self, brand_id, limit=20):
        conn = self._conn()
        rows = conn.execute(
            """SELECT vtl.*, cu.display_name AS created_by_name
               FROM va_token_ledger vtl
               LEFT JOIN client_users cu ON vtl.created_by = cu.id
               WHERE vtl.brand_id = ?
               ORDER BY vtl.created_at DESC, vtl.id DESC
               LIMIT ?""",
            (brand_id, int(limit or 20)),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_va_token_balance(self, brand_id):
        conn = self._conn()
        row = conn.execute(
            "SELECT COALESCE(SUM(token_amount), 0) AS balance FROM va_token_ledger WHERE brand_id = ?",
            (brand_id,),
        ).fetchone()
        conn.close()
        return int((row["balance"] if row else 0) or 0)

    # ── Hiring: Jobs ──

    def create_hiring_job(self, brand_id, title, department="", job_type="full-time",
                          location="", remote="no", description="", requirements="[]",
                          nice_to_haves="[]", salary_min=0, salary_max=0, benefits="",
                          screening_criteria="{}", scheduling_link="", status="draft",
                          generated_post="", created_by=0):
        conn = self._conn()
        conn.execute(
            """INSERT INTO hiring_jobs
               (brand_id, title, department, job_type, location, remote, description,
                requirements, nice_to_haves, salary_min, salary_max, benefits,
                screening_criteria, scheduling_link, status, generated_post, created_by)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (brand_id, title, department, job_type, location, remote, description,
             requirements, nice_to_haves, salary_min, salary_max, benefits,
             screening_criteria, scheduling_link, status, generated_post, created_by),
        )
        job_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.commit()
        conn.close()
        return job_id

    def get_hiring_job(self, job_id):
        conn = self._conn()
        row = conn.execute("SELECT * FROM hiring_jobs WHERE id = ?", (job_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def get_hiring_jobs(self, brand_id, status=None):
        conn = self._conn()
        if status:
            rows = conn.execute(
                "SELECT * FROM hiring_jobs WHERE brand_id = ? AND status = ? ORDER BY created_at DESC",
                (brand_id, status),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM hiring_jobs WHERE brand_id = ? ORDER BY created_at DESC",
                (brand_id,),
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def update_hiring_job(self, job_id, **fields):
        if not fields:
            return
        fields["updated_at"] = "datetime('now')"
        parts, vals = [], []
        for k, v in fields.items():
            if v == "datetime('now')":
                parts.append(f"{k} = datetime('now')")
            else:
                parts.append(f"{k} = ?")
                vals.append(v)
        vals.append(job_id)
        conn = self._conn()
        conn.execute(f"UPDATE hiring_jobs SET {', '.join(parts)} WHERE id = ?", vals)
        conn.commit()
        conn.close()

    def delete_hiring_job(self, job_id):
        conn = self._conn()
        # Cascade: messages -> interviews -> candidates -> job
        conn.execute(
            "DELETE FROM hiring_messages WHERE interview_id IN "
            "(SELECT id FROM hiring_interviews WHERE job_id = ?)", (job_id,)
        )
        conn.execute("DELETE FROM hiring_interviews WHERE job_id = ?", (job_id,))
        conn.execute("DELETE FROM hiring_candidates WHERE job_id = ?", (job_id,))
        conn.execute("DELETE FROM hiring_jobs WHERE id = ?", (job_id,))
        conn.commit()
        conn.close()

    def count_candidates_for_job(self, job_id):
        conn = self._conn()
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM hiring_candidates WHERE job_id = ?", (job_id,)
        ).fetchone()
        conn.close()
        return row["cnt"] if row else 0

    # ── Hiring: Candidates ──

    def create_hiring_candidate(self, brand_id, job_id, name, email, phone="",
                                source="website", resume_text="", cover_letter=""):
        conn = self._conn()
        conn.execute(
            """INSERT INTO hiring_candidates
               (brand_id, job_id, name, email, phone, source, resume_text, cover_letter)
               VALUES (?,?,?,?,?,?,?,?)""",
            (brand_id, job_id, name, email, phone, source, resume_text, cover_letter),
        )
        cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.commit()
        conn.close()
        return cid

    def get_hiring_candidate(self, candidate_id):
        conn = self._conn()
        row = conn.execute("SELECT * FROM hiring_candidates WHERE id = ?", (candidate_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def get_hiring_candidates(self, brand_id, job_id=None, status=None, sort="ai_score"):
        conn = self._conn()
        sql = "SELECT * FROM hiring_candidates WHERE brand_id = ?"
        params = [brand_id]
        if job_id:
            sql += " AND job_id = ?"
            params.append(job_id)
        if status:
            sql += " AND status = ?"
            params.append(status)
        order = "ai_score DESC" if sort == "ai_score" else "created_at DESC"
        sql += f" ORDER BY {order}"
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def update_hiring_candidate(self, candidate_id, **fields):
        if not fields:
            return
        parts, vals = [], []
        for k, v in fields.items():
            parts.append(f"{k} = ?")
            vals.append(v)
        vals.append(candidate_id)
        conn = self._conn()
        conn.execute(f"UPDATE hiring_candidates SET {', '.join(parts)} WHERE id = ?", vals)
        conn.commit()
        conn.close()

    def search_hiring_candidates(self, brand_id, query):
        conn = self._conn()
        q = f"%{query}%"
        rows = conn.execute(
            """SELECT * FROM hiring_candidates
               WHERE brand_id = ? AND (name LIKE ? OR email LIKE ? OR notes LIKE ?)
               ORDER BY ai_score DESC""",
            (brand_id, q, q, q),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_candidate_by_email_and_job(self, email, job_id):
        conn = self._conn()
        row = conn.execute(
            "SELECT * FROM hiring_candidates WHERE email = ? AND job_id = ?",
            (email.lower().strip(), job_id),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    # ── Hiring: Interviews ──

    def create_hiring_interview(self, candidate_id, brand_id, job_id):
        import uuid
        token = uuid.uuid4().hex
        conn = self._conn()
        conn.execute(
            """INSERT INTO hiring_interviews
               (candidate_id, brand_id, job_id, token)
               VALUES (?,?,?,?)""",
            (candidate_id, brand_id, job_id, token),
        )
        iid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.commit()
        conn.close()
        return iid, token

    def get_hiring_interview_by_token(self, token):
        conn = self._conn()
        row = conn.execute(
            """SELECT hi.*, hc.name as candidate_name, hc.email as candidate_email,
                      hc.phone as candidate_phone, hc.cover_letter,
                      hj.title as job_title, hj.description as job_description,
                      hj.requirements as job_requirements, hj.screening_criteria,
                      hj.scheduling_link
               FROM hiring_interviews hi
               JOIN hiring_candidates hc ON hi.candidate_id = hc.id
               LEFT JOIN hiring_jobs hj ON hi.job_id = hj.id
               WHERE hi.token = ?""",
            (token,),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def get_hiring_interview(self, interview_id):
        conn = self._conn()
        row = conn.execute("SELECT * FROM hiring_interviews WHERE id = ?", (interview_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def get_hiring_interviews_for_candidate(self, candidate_id):
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM hiring_interviews WHERE candidate_id = ? ORDER BY created_at DESC",
            (candidate_id,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def update_hiring_interview(self, interview_id, **fields):
        if not fields:
            return
        parts, vals = [], []
        for k, v in fields.items():
            parts.append(f"{k} = ?")
            vals.append(v)
        vals.append(interview_id)
        conn = self._conn()
        conn.execute(f"UPDATE hiring_interviews SET {', '.join(parts)} WHERE id = ?", vals)
        conn.commit()
        conn.close()

    def get_expired_hiring_interviews(self, pending_hours=48, active_hours=72):
        conn = self._conn()
        rows = conn.execute(
            """SELECT * FROM hiring_interviews
               WHERE (status = 'pending' AND created_at < datetime('now', ?))
                  OR (status = 'in_progress' AND started_at < datetime('now', ?))""",
            (f"-{pending_hours} hours", f"-{active_hours} hours"),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ── Hiring: Messages ──

    def add_hiring_message(self, interview_id, candidate_id, direction, channel,
                           content, is_question=0, question_number=None,
                           signal_scores="{}", response_time_sec=None):
        conn = self._conn()
        conn.execute(
            """INSERT INTO hiring_messages
               (interview_id, candidate_id, direction, channel, content,
                is_question, question_number, signal_scores, response_time_sec)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (interview_id, candidate_id, direction, channel, content,
             is_question, question_number, signal_scores, response_time_sec),
        )
        mid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.commit()
        conn.close()
        return mid

    def get_hiring_messages(self, interview_id):
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM hiring_messages WHERE interview_id = ? ORDER BY sent_at ASC",
            (interview_id,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ── Hiring: Templates ──

    def get_hiring_templates(self, brand_id, template_type=None):
        conn = self._conn()
        if template_type:
            rows = conn.execute(
                "SELECT * FROM hiring_templates WHERE brand_id = ? AND template_type = ? ORDER BY is_default DESC",
                (brand_id, template_type),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM hiring_templates WHERE brand_id = ? ORDER BY template_type, is_default DESC",
                (brand_id,),
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def upsert_hiring_template(self, brand_id, template_type, name, subject, body, is_default=0):
        conn = self._conn()
        existing = conn.execute(
            "SELECT id FROM hiring_templates WHERE brand_id = ? AND template_type = ? AND name = ?",
            (brand_id, template_type, name),
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE hiring_templates SET subject=?, body=?, is_default=? WHERE id=?",
                (subject, body, is_default, existing["id"]),
            )
        else:
            conn.execute(
                """INSERT INTO hiring_templates (brand_id, template_type, name, subject, body, is_default)
                   VALUES (?,?,?,?,?,?)""",
                (brand_id, template_type, name, subject, body, is_default),
            )
        conn.commit()
        conn.close()

    # ── Dashboard Snapshots ──

    def upsert_dashboard_snapshot(self, brand_id, month, snapshot_json, source="auto"):
        conn = self._conn()
        conn.execute(
            """INSERT INTO dashboard_snapshots (brand_id, month, snapshot_json, source, created_at)
               VALUES (?, ?, ?, ?, datetime('now'))
               ON CONFLICT(brand_id, month) DO UPDATE SET
                 snapshot_json = excluded.snapshot_json,
                 source = excluded.source,
                 created_at = datetime('now')""",
            (brand_id, month, snapshot_json, source),
        )
        conn.commit()
        conn.close()

    def get_dashboard_snapshot(self, brand_id, month, max_age_hours=168):
        conn = self._conn()
        row = conn.execute(
            """SELECT * FROM dashboard_snapshots
               WHERE brand_id = ? AND month = ?
                 AND created_at > datetime('now', ?)""",
            (brand_id, month, f"-{max_age_hours} hours"),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def get_available_dashboard_months(self, brand_id, limit=24):
        conn = self._conn()
        rows = conn.execute(
            """SELECT month FROM (
                   SELECT month FROM reports WHERE brand_id = ?
                   UNION
                   SELECT month FROM dashboard_snapshots WHERE brand_id = ?
               ) months
               ORDER BY month DESC
               LIMIT ?""",
            (brand_id, brand_id, limit),
        ).fetchall()
        conn.close()
        months = {r["month"] for r in rows if r["month"]}

        # Also check the analytics DB (monthly_summary / monthly_data)
        # which may have data even when no report or snapshot row exists.
        try:
            brand = self.get_brand(brand_id)
            slug = brand.get("slug") if brand else None
            if slug:
                from src.database import get_connection as get_analytics_conn
                aconn = get_analytics_conn()
                for tbl in ("monthly_summary", "monthly_data"):
                    try:
                        arows = aconn.execute(
                            f"SELECT DISTINCT month FROM {tbl} WHERE client_id = ?",
                            (slug,),
                        ).fetchall()
                        months.update(r["month"] for r in arows if r["month"])
                    except Exception:
                        pass
                aconn.close()
        except Exception:
            pass

        sorted_months = sorted(months, reverse=True)[:limit]
        return sorted_months

    def get_latest_dashboard_month(self, brand_id):
        months = self.get_available_dashboard_months(brand_id, limit=1)
        return months[0] if months else None

    def get_stale_dashboard_brands(self, month, max_age_hours=20):
        """Return brand IDs whose snapshot is missing or older than max_age_hours."""
        conn = self._conn()
        rows = conn.execute(
            """SELECT b.id FROM brands b
               LEFT JOIN dashboard_snapshots ds
                 ON ds.brand_id = b.id AND ds.month = ?
               WHERE ds.id IS NULL
                  OR ds.created_at <= datetime('now', ?)""",
            (month, f"-{max_age_hours} hours"),
        ).fetchall()
        conn.close()
        return [r["id"] for r in rows]

    # ── Agency CRM CRUD ──

    def get_agency_prospects(self, stage=None, limit=200):
        conn = self._conn()
        if stage:
            rows = conn.execute(
                "SELECT * FROM agency_prospects WHERE stage = ? ORDER BY updated_at DESC LIMIT ?",
                (stage, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM agency_prospects ORDER BY updated_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_agency_prospect(self, prospect_id):
        conn = self._conn()
        row = conn.execute("SELECT * FROM agency_prospects WHERE id = ?", (prospect_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def find_agency_prospect(self, *, email="", website="", business_name=""):
        conn = self._conn()
        normalized_email = self._normalize_email(email)
        normalized_website = (website or "").strip().lower().rstrip("/")
        normalized_name = (business_name or "").strip().lower()
        row = None
        if normalized_email:
            row = conn.execute(
                "SELECT * FROM agency_prospects WHERE lower(email) = ? ORDER BY id DESC LIMIT 1",
                (normalized_email,),
            ).fetchone()
        if not row and normalized_website:
            row = conn.execute(
                "SELECT * FROM agency_prospects WHERE lower(rtrim(website, '/')) = ? ORDER BY id DESC LIMIT 1",
                (normalized_website,),
            ).fetchone()
        if not row and normalized_name:
            row = conn.execute(
                "SELECT * FROM agency_prospects WHERE lower(coalesce(business_name, name)) = ? ORDER BY id DESC LIMIT 1",
                (normalized_name,),
            ).fetchone()
        conn.close()
        return dict(row) if row else None

    def create_agency_prospect(self, **fields):
        conn = self._conn()
        allowed = {
            "name", "email", "phone", "business_name", "website", "industry",
            "service_area", "source", "stage", "score", "monthly_budget",
            "notes", "assigned_to", "assessment_lead_id", "signup_lead_id",
            "next_follow_up", "account_type", "decision_maker_role",
            "property_count", "current_vendor_status", "outreach_angle",
            "proposal_status", "next_action", "source_details_json",
            "audit_snapshot_json", "pain_points_json", "qualification_answers_json",
        }
        data = {k: v for k, v in fields.items() if k in allowed and v}
        cols = ", ".join(data.keys())
        placeholders = ", ".join("?" for _ in data)
        cur = conn.execute(
            f"INSERT INTO agency_prospects ({cols}) VALUES ({placeholders})",
            list(data.values()),
        )
        conn.commit()
        pid = cur.lastrowid
        conn.close()
        return pid

    def update_agency_prospect(self, prospect_id, **fields):
        conn = self._conn()
        allowed = {
            "name", "email", "phone", "business_name", "website", "industry",
            "service_area", "source", "stage", "score", "monthly_budget",
            "notes", "assigned_to", "converted_brand_id", "last_contact_at",
            "next_follow_up", "account_type", "decision_maker_role",
            "property_count", "current_vendor_status", "outreach_angle",
            "proposal_status", "next_action", "source_details_json",
            "audit_snapshot_json", "pain_points_json", "qualification_answers_json",
        }
        sets, params = [], []
        for k, v in fields.items():
            if k in allowed:
                sets.append(f"{k} = ?")
                params.append(v)
        if not sets:
            conn.close()
            return
        sets.append("updated_at = datetime('now')")
        params.append(prospect_id)
        conn.execute(f"UPDATE agency_prospects SET {', '.join(sets)} WHERE id = ?", params)
        conn.commit()
        conn.close()

    def delete_agency_prospect(self, prospect_id):
        conn = self._conn()
        conn.execute("DELETE FROM agency_prospects WHERE id = ?", (prospect_id,))
        conn.commit()
        conn.close()

    def add_agency_prospect_note(self, prospect_id, content, note_type="note", created_by="admin"):
        conn = self._conn()
        conn.execute(
            "INSERT INTO agency_prospect_notes (prospect_id, note_type, content, created_by) VALUES (?, ?, ?, ?)",
            (prospect_id, note_type, content, created_by),
        )
        conn.commit()
        conn.close()

    def get_agency_prospect_notes(self, prospect_id):
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM agency_prospect_notes WHERE prospect_id = ? ORDER BY created_at DESC",
            (prospect_id,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def add_agency_prospect_message(self, prospect_id, content, direction="outbound",
                                     channel="email", subject="", status="sent"):
        conn = self._conn()
        conn.execute(
            """INSERT INTO agency_prospect_messages
               (prospect_id, direction, channel, subject, content, status)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (prospect_id, direction, channel, subject, content, status),
        )
        conn.commit()
        conn.close()

    def get_agency_prospect_messages(self, prospect_id):
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM agency_prospect_messages WHERE prospect_id = ? ORDER BY created_at DESC",
            (prospect_id,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_agency_pipeline_counts(self):
        """Return {stage: count} for the pipeline board."""
        conn = self._conn()
        rows = conn.execute(
            "SELECT stage, COUNT(*) as cnt FROM agency_prospects GROUP BY stage"
        ).fetchall()
        conn.close()
        return {r["stage"]: r["cnt"] for r in rows}

    def import_assessment_leads_to_crm(self):
        """Import unconverted assessment_leads that aren't already in agency_prospects."""
        conn = self._conn()
        rows = conn.execute("""
            SELECT al.* FROM assessment_leads al
            LEFT JOIN agency_prospects ap ON ap.assessment_lead_id = al.id
            WHERE ap.id IS NULL AND al.converted_to_brand_id IS NULL
        """).fetchall()
        imported = 0
        for r in rows:
            conn.execute(
                """INSERT INTO agency_prospects
                   (name, email, phone, business_name, website, industry, service_area,
                    source, stage, score, assessment_lead_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'assessment', 'new', ?, ?)""",
                (r["name"], r["email"], r["phone"] if "phone" in r.keys() else "",
                 r["business_name"], r["website"], r["industry"], r["service_area"],
                 r["overall_score"] or 0, r["id"]),
            )
            imported += 1
        if imported:
            conn.commit()
        conn.close()
        return imported

    def import_signup_leads_to_crm(self):
        """Import unconverted signup_leads that aren't already in agency_prospects."""
        conn = self._conn()
        rows = conn.execute("""
            SELECT sl.* FROM signup_leads sl
            LEFT JOIN agency_prospects ap ON ap.signup_lead_id = sl.id
            WHERE ap.id IS NULL AND sl.converted_to_brand_id IS NULL
        """).fetchall()
        imported = 0
        for r in rows:
            conn.execute(
                """INSERT INTO agency_prospects
                   (name, email, phone, business_name, website, industry, service_area,
                    source, stage, monthly_budget)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'signup', 'new', ?)""",
                (r["name"], r["email"], r["phone"], r["business_name"],
                 r["website"], r["industry"], r["service_area"], r["monthly_budget"]),
            )
            imported += 1
        if imported:
            conn.commit()
        conn.close()
        return imported

    # ── Stripe Billing Helpers ──

    def log_stripe_event(self, event_id, event_type, brand_id=None, prospect_id=None, data=None):
        conn = self._conn()
        import json as _json
        conn.execute(
            """INSERT OR IGNORE INTO stripe_events
               (event_id, event_type, brand_id, prospect_id, data_json)
               VALUES (?, ?, ?, ?, ?)""",
            (event_id, event_type, brand_id, prospect_id, _json.dumps(data or {})),
        )
        conn.commit()
        conn.close()

    def update_brand_stripe(self, brand_id, **fields):
        """Update Stripe-related fields on a brand."""
        conn = self._conn()
        allowed = {
            "stripe_customer_id", "stripe_subscription_id", "stripe_plan",
            "stripe_status", "stripe_mrr", "stripe_trial_end",
            "stripe_next_invoice", "stripe_payment_method_last4",
            "onboarded_at", "churned_at",
        }
        sets, params = [], []
        for k, v in fields.items():
            if k in allowed:
                sets.append(f"{k} = ?")
                params.append(v)
        if not sets:
            conn.close()
            return
        params.append(brand_id)
        conn.execute(f"UPDATE brands SET {', '.join(sets)} WHERE id = ?", params)
        conn.commit()
        conn.close()

    def get_stripe_revenue_summary(self):
        """Return total MRR, active count, trialing count, churned count."""
        conn = self._conn()
        row = conn.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN stripe_status = 'active' THEN stripe_mrr ELSE 0 END), 0) as total_mrr,
                COALESCE(SUM(CASE WHEN stripe_status = 'active' THEN 1 ELSE 0 END), 0) as active_count,
                COALESCE(SUM(CASE WHEN stripe_status = 'trialing' THEN 1 ELSE 0 END), 0) as trialing_count,
                COALESCE(SUM(CASE WHEN stripe_status = 'canceled' THEN 1 ELSE 0 END), 0) as churned_count
            FROM brands WHERE stripe_customer_id != ''
        """).fetchone()
        conn.close()
        return dict(row) if row else {"total_mrr": 0, "active_count": 0, "trialing_count": 0, "churned_count": 0}

    # ── Email Broadcast Tracking ──

    def create_email_broadcast(self, subject, body_text, audience, sent_by, recipients):
        """Log a broadcast and its recipients. Returns broadcast_id and list of tokens."""
        import secrets
        conn = self._conn()
        cur = conn.execute(
            "INSERT INTO email_broadcasts (subject, body_text, audience, sent_by, recipient_count) VALUES (?, ?, ?, ?, ?)",
            (subject, body_text, audience, sent_by, len(recipients)),
        )
        broadcast_id = cur.lastrowid
        tokens = []
        for r in recipients:
            email = r.get("email") if isinstance(r, dict) else r
            name = r.get("name", "") if isinstance(r, dict) else ""
            token = secrets.token_urlsafe(16)
            conn.execute(
                "INSERT INTO email_broadcast_recipients (broadcast_id, email, name, token) VALUES (?, ?, ?, ?)",
                (broadcast_id, email, name, token),
            )
            tokens.append({"email": email, "token": token})
        conn.commit()
        conn.close()
        return broadcast_id, tokens

    def record_email_open(self, token):
        """Record an email open by tracking pixel token."""
        conn = self._conn()
        row = conn.execute("SELECT id, broadcast_id, opened_at FROM email_broadcast_recipients WHERE token = ?", (token,)).fetchone()
        if not row:
            conn.close()
            return
        conn.execute(
            "UPDATE email_broadcast_recipients SET open_count = open_count + 1, opened_at = COALESCE(NULLIF(opened_at, ''), datetime('now')) WHERE id = ?",
            (row["id"],),
        )
        conn.execute(
            "UPDATE email_broadcasts SET open_count = (SELECT COUNT(*) FROM email_broadcast_recipients WHERE broadcast_id = ? AND opened_at != '') WHERE id = ?",
            (row["broadcast_id"], row["broadcast_id"]),
        )
        conn.commit()
        conn.close()

    def get_email_broadcasts(self, limit=50):
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM email_broadcasts ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_email_broadcast_recipients(self, broadcast_id):
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM email_broadcast_recipients WHERE broadcast_id = ? ORDER BY opened_at DESC, email ASC",
            (broadcast_id,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ── Warren Bulk Messages ──

    def record_warren_bulk_message_run(
        self,
        brand_id,
        *,
        subject="",
        body_text="",
        groups=None,
        channels=None,
        sent_by="",
        recipient_count=0,
        sent_sms=0,
        sent_email=0,
        skipped=0,
        failed=0,
        detail="",
        errors=None,
    ):
        conn = self._conn()
        cur = conn.execute(
            """
            INSERT INTO warren_bulk_message_runs (
                brand_id, subject, body_text, groups_json, channels_json, sent_by,
                recipient_count, sent_sms, sent_email, skipped, failed, detail, errors_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                brand_id,
                subject or "",
                body_text or "",
                json.dumps(groups or []),
                json.dumps(channels or []),
                sent_by or "",
                int(recipient_count or 0),
                int(sent_sms or 0),
                int(sent_email or 0),
                int(skipped or 0),
                int(failed or 0),
                detail or "",
                json.dumps(errors or {}),
            ),
        )
        conn.commit()
        run_id = cur.lastrowid
        conn.close()
        return run_id

    def get_warren_bulk_message_runs(self, brand_id, limit=20):
        conn = self._conn()
        rows = conn.execute(
            """
            SELECT * FROM warren_bulk_message_runs
            WHERE brand_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (brand_id, int(limit or 20)),
        ).fetchall()
        conn.close()
        runs = []
        for row in rows:
            item = dict(row)
            item["groups"] = self._safe_json_list(item.get("groups_json"))
            item["channels"] = self._safe_json_list(item.get("channels_json"))
            item["errors"] = self._safe_json_object(item.get("errors_json"))
            runs.append(item)
        return runs
