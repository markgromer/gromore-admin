"""
Ad Agency Analytics - Web Admin Application

Flask web app for managing client brands, API connections,
report generation, email delivery, and WordPress publishing.
"""
import os
import json
import base64
import hashlib
import hmac
import logging
import re
import secrets
from pathlib import Path
from datetime import datetime, timedelta
from functools import wraps

from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, session, jsonify, send_file, abort, current_app
)
from flask_wtf.csrf import CSRFProtect
from werkzeug.security import generate_password_hash, check_password_hash

from webapp.database import WebDB
from webapp.oauth_google import google_bp
from webapp.oauth_meta import meta_bp
from webapp.client_oauth_google import client_google_bp
from webapp.client_oauth_meta import client_meta_bp
from webapp.jobs import jobs_bp
from webapp import client_portal as client_portal_module
from webapp.client_portal import client_bp, client_public_bp
from webapp.hiring import hiring_bp
from webapp.warren_webhooks import webhooks_bp
from webapp.appointment_runner import start_background_appointment_runner

BASE_DIR = Path(__file__).parent.parent
logger = logging.getLogger(__name__)


def _base64_url_decode(value):
    raw = (value or "").encode("utf-8")
    padding = b"=" * (-len(raw) % 4)
    return base64.urlsafe_b64decode(raw + padding)


def _decode_meta_signed_request(signed_request, app_secret):
    try:
        encoded_sig, encoded_payload = (signed_request or "").split(".", 1)
    except ValueError as exc:
        raise ValueError("Malformed signed_request") from exc

    signature = _base64_url_decode(encoded_sig)
    payload_bytes = _base64_url_decode(encoded_payload)

    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Invalid signed_request payload") from exc

    algorithm = (payload.get("algorithm") or "").upper()
    if algorithm != "HMAC-SHA256":
        raise ValueError("Unsupported signed_request algorithm")

    expected = hmac.new(
        (app_secret or "").encode("utf-8"),
        encoded_payload.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    if not hmac.compare_digest(expected, signature):
        raise ValueError("Invalid signed_request signature")

    return payload


def create_app():
    app = Flask(
        __name__,
        template_folder=str(BASE_DIR / "webapp" / "templates"),
        static_folder=str(BASE_DIR / "webapp" / "static"),
    )

    # Trust reverse proxy headers (Render, etc.) for correct HTTPS scheme
    from werkzeug.middleware.proxy_fix import ProxyFix
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

    # Config from environment (Render-friendly)
    env_secret_key = os.environ.get("SECRET_KEY")
    # Use a temporary key until DB-backed config is loaded.
    app.secret_key = env_secret_key or "dev-temp"

    # Detect Render: if RENDER env var is set OR /data mount exists, use persistent paths
    on_render = os.environ.get("RENDER") or os.path.isdir("/data")
    default_db = "/data/database/webapp.db" if on_render else str(BASE_DIR / "data" / "database" / "webapp.db")
    app.config["DATABASE_PATH"] = os.environ.get("DATABASE_PATH", default_db)

    # Initialize DB first so we can load settings from it
    db = WebDB(app.config["DATABASE_PATH"])
    db.init()
    app.db = db

    # Jinja filters
    import json as _json

    def _safe_from_json(s):
        if not s:
            return []
        try:
            return _json.loads(s)
        except (ValueError, TypeError):
            return {}

    app.jinja_env.filters["from_json"] = _safe_from_json

    # Storage paths for uploads + generated reports
    # Local/dev uses repo folders. Render uses persisted disk under /data.
    if on_render:
        app.config["IMPORTS_DIR"] = os.environ.get("IMPORTS_DIR", "/data/imports")
        app.config["REPORTS_DIR"] = os.environ.get("REPORTS_DIR", "/data/reports")
        app.config["UPLOADS_DIR"] = os.environ.get("UPLOADS_DIR", "/data/uploads")
    else:
        app.config["IMPORTS_DIR"] = os.environ.get("IMPORTS_DIR", str(BASE_DIR / "data" / "imports"))
        app.config["REPORTS_DIR"] = os.environ.get("REPORTS_DIR", str(BASE_DIR / "reports"))
        app.config["UPLOADS_DIR"] = os.environ.get("UPLOADS_DIR", str(BASE_DIR / "data" / "uploads"))

    # Make available to helper modules (report_runner reads these env vars)
    os.environ.setdefault("IMPORTS_DIR", app.config["IMPORTS_DIR"])
    os.environ.setdefault("REPORTS_DIR", app.config["REPORTS_DIR"])

    # Log paths at startup for debugging persistence issues
    print(f"[startup] DATABASE_PATH = {app.config['DATABASE_PATH']}")
    print(f"[startup] IMPORTS_DIR   = {app.config['IMPORTS_DIR']}")
    print(f"[startup] REPORTS_DIR   = {app.config['REPORTS_DIR']}")
    print(f"[startup] on_render     = {on_render}")

    # Competitor tables diagnostic (helps debug "no research" on Render)
    try:
        _diag_conn = db._conn()
        _tables = {r[0] for r in _diag_conn.execute("select name from sqlite_master where type='table'").fetchall()}
        _has_comp = "competitors" in _tables
        _has_intel = "competitor_intel" in _tables
        _comp_count = _diag_conn.execute("select count(1) from competitors").fetchone()[0] if _has_comp else 0
        _intel_count = _diag_conn.execute("select count(1) from competitor_intel").fetchone()[0] if _has_intel else 0
        _research_count = _diag_conn.execute("select count(1) from competitor_intel where intel_type='research'").fetchone()[0] if _has_intel else 0
        _has_oai = bool((db.get_setting("openai_api_key", "") or "").strip())
        _diag_conn.close()
        print(f"[startup] competitors_table={_has_comp} intel_table={_has_intel} "
              f"competitors={_comp_count} intel_rows={_intel_count} research_rows={_research_count} "
              f"openai_key={'SET' if _has_oai else 'MISSING'}")
    except Exception as _diag_err:
        print(f"[startup] competitor diagnostic failed: {_diag_err}")

    # ── One-time: seed analytics DB from dashboard snapshots ──
    # When agency.db is empty (first deploy after persistence fix), extract
    # raw analysis data from existing snapshots into monthly_data so the
    # report pipeline can generate data-specific suggestions.
    try:
        from src.database import init_db as _init_analytics, get_monthly_data as _get_md, store_monthly_data as _store_md
        import json as _mig_json
        _init_analytics()
        _diag_conn2 = db._conn()
        _all_snaps = _diag_conn2.execute(
            "SELECT brand_id, month, snapshot_json FROM dashboard_snapshots"
        ).fetchall()
        _seeded = 0
        for _snap_row in _all_snaps:
            _bid = _snap_row[0]
            _smonth = _snap_row[1]
            _brand_row = db.get_brand(_bid)
            if not _brand_row:
                continue
            _slug = _brand_row.get("slug")
            if not _slug:
                continue
            existing = _get_md(_slug, _smonth)
            if existing:
                continue  # already seeded
            try:
                _sdata = _mig_json.loads(_snap_row[2])
                _analysis = _sdata.get("_analysis")
                if not _analysis or not isinstance(_analysis, dict):
                    continue
                for _src_key in ("google_analytics", "meta_business", "search_console", "google_ads", "facebook_organic"):
                    _src_val = _analysis.get(_src_key)
                    if _src_val and isinstance(_src_val, dict):
                        _store_md(_slug, _smonth, _src_key, _src_val)
                        _seeded += 1
            except Exception:
                pass
        _diag_conn2.close()
        if _seeded:
            print(f"[startup] Seeded {_seeded} monthly_data rows from dashboard snapshots")
    except Exception as _seed_err:
        print(f"[startup] analytics seed skipped: {_seed_err}")

    # Stabilize SECRET_KEY across restarts in local/dev (unless provided via env)
    if not env_secret_key:
        persisted = db.get_setting("secret_key", "")
        if not persisted:
            persisted = secrets.token_hex(32)
            db.save_setting("secret_key", persisted)
        app.secret_key = persisted

    def _cfg(key, env_key, default=""):
        """Load config: DB setting wins, then env var, then default."""
        val = db.get_setting(key, "")
        if val:
            return val
        return os.environ.get(env_key, default)

    # Google OAuth
    app.config["GOOGLE_CLIENT_ID"] = _cfg("google_client_id", "GOOGLE_CLIENT_ID")
    app.config["GOOGLE_CLIENT_SECRET"] = _cfg("google_client_secret", "GOOGLE_CLIENT_SECRET")
    app.config["GOOGLE_ADS_DEVELOPER_TOKEN"] = _cfg("google_ads_developer_token", "GOOGLE_ADS_DEVELOPER_TOKEN")
    app.config["GOOGLE_ADS_LOGIN_CUSTOMER_ID"] = _cfg("google_ads_login_customer_id", "GOOGLE_ADS_LOGIN_CUSTOMER_ID")

    # Meta OAuth
    app.config["META_APP_ID"] = _cfg("meta_app_id", "META_APP_ID")
    app.config["META_APP_SECRET"] = _cfg("meta_app_secret", "META_APP_SECRET")

    # Email (SMTP)
    app.config["SMTP_HOST"] = _cfg("smtp_host", "SMTP_HOST", "smtp.gmail.com")
    app.config["SMTP_PORT"] = int(_cfg("smtp_port", "SMTP_PORT", "587"))
    app.config["SMTP_USER"] = _cfg("smtp_user", "SMTP_USER")
    app.config["SMTP_PASSWORD"] = _cfg("smtp_password", "SMTP_PASSWORD")
    app.config["SMTP_FROM_NAME"] = _cfg("smtp_from_name", "SMTP_FROM_NAME", "Agency Reports")
    app.config["SMTP_FROM_EMAIL"] = _cfg("smtp_from_email", "SMTP_FROM_EMAIL")

    # App URL (for OAuth callbacks)
    app.config["APP_URL"] = _cfg("app_url", "APP_URL", "http://localhost:5000")

    # Optional AI config
    app.config["OPENAI_API_KEY"] = _cfg("openai_api_key", "OPENAI_API_KEY")
    app.config["OPENAI_MODEL"] = _cfg("openai_model", "OPENAI_MODEL", "gpt-4o-mini")
    from webapp.ai_assistant import DEFAULT_CHAT_SYSTEM_PROMPT
    app.config["AI_CHAT_SYSTEM_PROMPT"] = _cfg("ai_chat_system_prompt", "AI_CHAT_SYSTEM_PROMPT", "") or DEFAULT_CHAT_SYSTEM_PROMPT

    # Stripe billing
    app.config["STRIPE_SECRET_KEY"] = _cfg("stripe_secret_key", "STRIPE_SECRET_KEY")
    app.config["STRIPE_WEBHOOK_SECRET"] = _cfg("stripe_webhook_secret", "STRIPE_WEBHOOK_SECRET")

    # Square OAuth / payments
    app.config["SQUARE_APP_ID"] = _cfg("square_app_id", "SQUARE_APP_ID")
    app.config["SQUARE_APP_SECRET"] = _cfg("square_app_secret", "SQUARE_APP_SECRET")
    app.config["SQUARE_API_VERSION"] = _cfg("square_api_version", "SQUARE_API_VERSION", "2026-01-22")

    # Ensure admin user exists and password stays in sync with env var
    admin_pw = os.environ.get("ADMIN_PASSWORD", "").strip()
    if not db.get_users():
        pw = admin_pw or "changeme123"
        db.create_user("admin", pw, "Admin")
        print(f"Created default admin user (username: admin)")
    elif admin_pw:
        # Always sync admin password from ADMIN_PASSWORD env var so
        # changing the env var on Render takes effect on next restart.
        db.create_user("admin", admin_pw, "Admin")  # no-op if exists
        db.update_password_by_username("admin", admin_pw)
        print("Admin password synced from ADMIN_PASSWORD env var")

    # Optional bootstrap user (useful for first-time access or recovery)
    bootstrap_username = os.environ.get("BOOTSTRAP_USERNAME", "").strip()
    bootstrap_password = os.environ.get("BOOTSTRAP_PASSWORD", "").strip()
    bootstrap_display_name = os.environ.get("BOOTSTRAP_DISPLAY_NAME", "Bootstrap Admin").strip() or "Bootstrap Admin"
    reset_bootstrap = os.environ.get("RESET_BOOTSTRAP_PASSWORD", "").strip().lower() in ("1", "true", "yes")

    if bootstrap_username and bootstrap_password:
        db.create_user(bootstrap_username, bootstrap_password, bootstrap_display_name)
        if reset_bootstrap:
            db.update_password_by_username(bootstrap_username, bootstrap_password)
            print(f"Bootstrap user password reset via RESET_BOOTSTRAP_PASSWORD (username: {bootstrap_username})")
        else:
            print(f"Bootstrap user ensured (username: {bootstrap_username})")

    # CSRF protection
    csrf = CSRFProtect(app)

    # Register blueprints
    app.register_blueprint(google_bp, url_prefix="/oauth/google")
    app.register_blueprint(meta_bp, url_prefix="/oauth/meta")
    app.register_blueprint(client_google_bp, url_prefix="/client/oauth/google")
    app.register_blueprint(client_meta_bp, url_prefix="/client/oauth/meta")
    app.register_blueprint(jobs_bp, url_prefix="/jobs")
    app.register_blueprint(hiring_bp, url_prefix="/client/hiring")
    csrf.exempt(hiring_bp)  # Public apply + interview endpoints
    app.register_blueprint(webhooks_bp, url_prefix="/webhooks")
    csrf.exempt(webhooks_bp)  # Public webhook endpoints (verified by signature)
    app.register_blueprint(client_bp)
    app.register_blueprint(client_public_bp)

    # ── Static asset cache headers ──
    @app.after_request
    def _set_cache_headers(response):
        if request.path.startswith('/static/'):
            response.headers['Cache-Control'] = 'public, max-age=86400'
        return response

    # Global 500 handler that includes CORS headers for cross-origin API callers
    @app.errorhandler(500)
    def _handle_500(e):
        import traceback, logging
        logging.getLogger(__name__).error("[500] %s", traceback.format_exc())
        if request.accept_mimetypes.best == 'application/json' or request.is_json:
            resp = jsonify({"ok": False, "error": f"Server error: {str(e)[:200]}"})
            resp.status_code = 500
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp
        try:
            flash(f"Something went wrong: {str(e)[:200]}", "error")
            if "client_user_id" in session and not session.get("client_admin_impersonating"):
                return redirect(url_for("client.client_dashboard"))
            return redirect(request.referrer or url_for("dashboard"))
        except Exception:
            return "Internal server error", 500

    # Exempt OAuth callback routes from CSRF (external redirects have no token)
    csrf.exempt(google_bp)
    csrf.exempt(meta_bp)
    csrf.exempt(client_google_bp)
    csrf.exempt(client_meta_bp)

    # Exempt public cross-origin API endpoints from CSRF
    csrf.exempt("webapp.client_portal.public_signup")
    csrf.exempt("webapp.client_portal.public_assess")
    csrf.exempt("client.public_signup")
    csrf.exempt("client.public_assess")
    csrf.exempt("client_public.public_lead_form")
    csrf.exempt("webapp.client_portal.public_lead_form")
    csrf.exempt("client_public.wordpress_pull_next_post")
    csrf.exempt("webapp.client_portal.wordpress_pull_next_post")
    csrf.exempt("client_public.wordpress_pull_complete")
    csrf.exempt("webapp.client_portal.wordpress_pull_complete")

    # Exempt React SPA JSON API endpoints from CSRF
    csrf.exempt("client.api_login")
    csrf.exempt("client.api_logout")

    # ── Auth decorator ──
    def login_required(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if "user_id" not in session:
                return redirect(url_for("login"))
            return f(*args, **kwargs)
        return decorated

    def partner_login_required(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if "partner_user_id" not in session:
                return redirect(url_for("partner_login"))
            partner_user = db.get_partner_user(session["partner_user_id"])
            if not partner_user or partner_user.get("status") != "active" or partner_user.get("partner_status") != "active":
                session.pop("partner_user_id", None)
                session.pop("partner_id", None)
                flash("Partner session expired. Please sign in again.", "error")
                return redirect(url_for("partner_login"))
            return f(*args, **kwargs)
        return decorated

    # Make it available to blueprints
    app.login_required = login_required
    app.csrf = csrf

    # ── Auto-detect APP_URL on first real request if not configured ──
    @app.before_request
    def _auto_detect_app_url():
        # Use X-Forwarded-Proto (set by Render/load balancers) for correct scheme
        scheme = request.headers.get("X-Forwarded-Proto", request.scheme)
        current_url = app.config.get("APP_URL", "").rstrip("/")
        # Fix any previously saved http:// URL that should be https://
        if current_url and current_url.startswith("http://") and scheme == "https":
            fixed = current_url.replace("http://", "https://", 1)
            app.config["APP_URL"] = fixed
            db.save_setting("app_url", fixed)
            return
        if current_url and "localhost" not in current_url:
            return  # already configured
        if request.host and "localhost" not in request.host:
            detected = scheme + "://" + request.host
            app.config["APP_URL"] = detected
            db.save_setting("app_url", detected)

    def _brand_feature_state(brand_id, feature_key):
        flag = db.get_feature_flag(feature_key)
        if not flag or not flag.get("enabled"):
            return "off"

        level = flag.get("access_level") or "all"
        if level == "admin" and "user_id" not in session:
            return "off"
        if level == "beta" and not (brand_id and db.is_beta_brand(brand_id)):
            return "off"
        if level == "brand" and not brand_id:
            return "off"

        if brand_id:
            overrides = db.get_brand_feature_access(brand_id)
            if level == "brand":
                return overrides.get(feature_key, "off")
            return overrides.get(feature_key, "on")
        return "on"

    def _collect_broadcast_recipients(audience, single_email=""):
        recipients = []
        seen = set()

        def _append(email, name, source):
            normalized = db._normalize_email(email)
            if "@" not in normalized or normalized in seen:
                return
            seen.add(normalized)
            recipients.append({
                "email": normalized,
                "name": (name or normalized).strip(),
                "source": source,
            })

        audience_key = (audience or "").strip().lower()
        if audience_key == "admins":
            for user in db.get_users_with_email():
                _append(user.get("email"), user.get("recipient_name") or user.get("display_name"), "admin")
        elif audience_key == "beta_users":
            for tester in db.get_beta_testers_for_broadcast():
                _append(tester.get("email"), tester.get("recipient_name") or tester.get("name"), "beta")
        elif audience_key == "all_users":
            for user in db.get_users_with_email():
                _append(user.get("email"), user.get("recipient_name") or user.get("display_name"), "admin")
            for tester in db.get_beta_testers_for_broadcast():
                _append(tester.get("email"), tester.get("recipient_name") or tester.get("name"), "beta")
            for client_user in db.get_client_users(active_only=True):
                _append(client_user.get("email"), client_user.get("display_name"), "client")
        elif audience_key == "single_user":
            _append(single_email, single_email, "single")

        return recipients

    # ── Context processor ──
    @app.context_processor
    def inject_globals():
        user = None
        if "user_id" in session:
            user = db.get_user(session["user_id"])

        # Build feature-flag lookup for templates
        _flags = {f["feature_key"]: f for f in db.get_feature_flags()}

        def feature_on(key):
            """Return True if the feature should be visible to the current viewer."""
            flag = _flags.get(key)
            if not flag or not flag["enabled"]:
                return False
            # Admin session sees everything
            if "user_id" in session:
                return True
            brand_id = session.get("client_brand_id")
            return _brand_feature_state(brand_id, key) in {"on", "upgrade"}

        return {
            "current_user": user,
            "now": datetime.now(),
            "app_name": "Agency Analytics",
            "feature_on": feature_on,
        }

    # ── Auth Routes ──
    @app.route("/login", methods=["GET", "POST"])
    def login():
        if request.method == "POST":
            username = request.form.get("username", "").strip()
            password = request.form.get("password", "")
            user = db.authenticate(username, password)
            if user:
                session["user_id"] = user["id"]
                session["user_name"] = user["display_name"]
                return redirect(url_for("dashboard"))
            flash("Invalid credentials", "error")
        return render_template("login.html")

    @app.route("/logout")
    def logout():
        session.clear()
        return redirect(url_for("login"))

    def _current_partner_user():
        if "partner_user_id" not in session:
            return None
        return db.get_partner_user(session["partner_user_id"])

    def _build_partner_demo_snapshot(data):
        services = (data.get("primary_services") or "service calls, estimates, follow-up work").strip()
        service_list = [item.strip() for item in services.split(",") if item.strip()] or ["service calls", "estimates", "follow-up work"]
        area = (data.get("service_area") or "the local service area").strip()
        industry = (data.get("industry") or "home services").replace("_", " ").title()
        avg_job = float(data.get("avg_job_value") or 450)
        monthly_leads = int(data.get("monthly_leads") or 35)
        recovered = max(3, round(monthly_leads * 0.18))
        projected_revenue = round(recovered * avg_job, 2)
        lead_sources = (data.get("lead_sources") or "Facebook lead forms, website forms, missed calls").strip()
        crm_used = (data.get("crm_used") or "current CRM").strip()
        owner = data.get("owner_intake") or {}
        business_name = data.get("business_name") or "this business"
        profitable = owner.get("profitable_services") or services
        good_lead = owner.get("good_lead_definition") or f"Someone in {area} asking about {service_list[0]}"
        handoff = owner.get("handoff_rules") or "Urgent jobs, angry customers, unusual pricing, out-of-area work, or anything that needs owner judgment."

        return {
            "demo_mode": True,
            "industry": industry,
            "headline": f"WARREN operating system for {business_name}",
            "metrics": {
                "monthly_leads": monthly_leads,
                "estimated_unfollowed_leads": recovered,
                "avg_job_value": avg_job,
                "projected_recovered_revenue": projected_revenue,
                "speed_to_lead_target": "under 60 seconds",
                "demo_close_rate_lift": "18-32%",
                "automation_readiness": 82,
            },
            "connection_plan": [
                {"name": "Meta Lead Forms", "status": "ready", "impact": "New Facebook and Instagram form submissions become WARREN lead threads instantly."},
                {"name": "Hosted WARREN Form", "status": "ready", "impact": "A public form and iframe can capture website leads with SMS consent and structured service fields."},
                {"name": "OpenPhone / Quo SMS", "status": "activation", "impact": "Turns demo replies into live two-way SMS with STOP/START/HELP and opt-out handling."},
                {"name": crm_used, "status": "activation", "impact": "Pushes qualified or won leads into the CRM with contact details, quote context, and transcript notes."},
                {"name": "Calendar + Appointment Reminders", "status": "activation", "impact": "Sends day-ahead reminders with dedupe protection and local-time windows once scheduling data is connected."},
                {"name": "Payments", "status": "activation", "impact": "Enables upcoming billing reminders by SMS or email when payment data is connected."},
                {"name": "Google Drive + Creative", "status": "optional", "impact": "Stores generated ads, images, and campaign assets in organized client folders."},
            ],
            "workspace_modules": [
                {"key": "capture", "icon": "bi-broadcast-pin", "title": "Lead Capture Hub", "value": lead_sources, "detail": "Meta forms, hosted forms, missed-call follow-up, Messenger, and web leads route into one WARREN inbox."},
                {"key": "inbox", "icon": "bi-inbox", "title": "AI Lead Inbox", "value": "Live thread workspace", "detail": "Conversation history, stage badges, private lock, handoff, draft reply, and pipeline movement."},
                {"key": "brain", "icon": "bi-stars", "title": "WARREN Brain", "value": good_lead, "detail": "Uses owner rules, service area, profitable services, tone, guardrails, pricing notes, and objection playbook."},
                {"key": "quote", "icon": "bi-cash-coin", "title": "Quote + Close Engine", "value": "Hybrid quote mode", "detail": "Collects missing details, explains ranges, sets the next booking step, and asks for owner review when needed."},
                {"key": "nurture", "icon": "bi-arrow-repeat", "title": "Nurture Automation", "value": "Hot / warm / cold follow-up", "detail": "Detects spouse checks, soft closes, ghosting, objections, and runs follow-up without pestering active clients."},
                {"key": "crm", "icon": "bi-diagram-3", "title": "CRM Handoff", "value": crm_used, "detail": "Qualified leads, won jobs, notes, quote status, and transcript context are ready for CRM push after activation."},
                {"key": "commercial", "icon": "bi-buildings", "title": "Commercial Accounts", "value": "Search, qualify, quote", "detail": "Commercial target search, walkthrough capture, itemized proposal builder, drip nurture, and service proof recaps."},
                {"key": "growth", "icon": "bi-graph-up-arrow", "title": "Growth Tools", "value": "Ads, SEO, creative, reviews", "detail": "Campaign builder, missions, competitor intel, heatmaps, post scheduler, image creator, and design studio sit beside WARREN."},
            ],
            "automation_timeline": [
                {"time": "0:00", "title": "Lead arrives", "detail": f"WARREN captures the lead from {lead_sources.split(',')[0].strip() if lead_sources else 'a lead source'} and creates a thread."},
                {"time": "0:08", "title": "Context matched", "detail": f"Checks service area, {profitable}, existing rules, and missing fields."},
                {"time": "0:18", "title": "Reply drafted", "detail": "Prepares an owner-safe reply with one question at a time and clear next step."},
                {"time": "0:45", "title": "Lead routed", "detail": "Qualified, needs-owner, nurture, or lost status is set automatically in the pipeline."},
                {"time": "After", "title": "Follow-up runs", "detail": "Hot, warm, and cold follow-up timers continue until the lead books, opts out, or needs a human."},
            ],
            "sample_leads": [
                {
                    "name": "Morgan Taylor",
                    "source": "Facebook Lead Form",
                    "need": service_list[0],
                    "stage": "Hot",
                    "value": avg_job,
                    "next_step": "Offer two booking windows and push to CRM after activation.",
                    "warren_action": f"Confirmed {area}, asked for the missing job detail, and prepared a booking reply.",
                    "messages": [
                        {"direction": "inbound", "content": f"I saw your ad. Can someone help with {service_list[0]} this week?"},
                        {"direction": "outbound", "content": f"Yes. We serve {area}. What is the service address and is this urgent for today or flexible this week?"},
                        {"direction": "inbound", "content": "Flexible this week. I just want the quote and next opening."},
                        {"direction": "outbound", "content": "WARREN draft: I can help get that moving. Based on what you shared, the next step is a quick estimate and the team can confirm the first available opening."},
                    ],
                },
                {
                    "name": "Casey Jordan",
                    "source": "Missed Call",
                    "need": "urgent quote",
                    "stage": "Needs owner review",
                    "value": round(avg_job * 1.4, 2),
                    "next_step": "Owner handoff because urgency and pricing sensitivity are both present.",
                    "warren_action": "Logged the missed call, sent a polite follow-up draft, and flagged it for same-day owner review.",
                    "messages": [
                        {"direction": "inbound", "content": "Missed call from a new lead. Voicemail says they need a price today."},
                        {"direction": "outbound", "content": "WARREN draft: Sorry we missed you. I can get the right details over to the owner. What service do you need and what city are you in?"},
                        {"direction": "inbound", "content": "Need a fast quote. I am comparing another company."},
                        {"direction": "outbound", "content": f"Handoff triggered: competitor quote plus urgency. Rule: {handoff}"},
                    ],
                },
                {
                    "name": "Riley Parker",
                    "source": "Website Form",
                    "need": service_list[-1],
                    "stage": "Nurture",
                    "value": round(avg_job * 0.8, 2),
                    "next_step": "Warm follow-up tomorrow, then soft close if no response.",
                    "warren_action": "Answered the first pricing question and scheduled a follow-up reminder.",
                    "messages": [
                        {"direction": "inbound", "content": f"Can you send information about {service_list[-1]}? I may need it next month."},
                        {"direction": "outbound", "content": "WARREN draft: Absolutely. I can give you a practical range and what affects price. Is this for a home or business?"},
                        {"direction": "inbound", "content": "Probably home, still deciding."},
                        {"direction": "outbound", "content": "Nurture scheduled: warm lead, follow up in 24 hours without pushing too hard."},
                    ],
                },
                {
                    "name": "Avery Commercial",
                    "source": "Commercial Prospecting",
                    "need": f"multi-location {service_list[0]}",
                    "stage": "Proposal",
                    "value": round(avg_job * 3.2, 2),
                    "next_step": "Build commercial scope, walkthrough checklist, and proposal package.",
                    "warren_action": "Created a commercial account, captured buying criteria, and prepared proposal fields.",
                    "messages": [
                        {"direction": "inbound", "content": "We manage several properties and need recurring service pricing."},
                        {"direction": "outbound", "content": "WARREN draft: I can help scope that. How many locations, what areas, and who approves the service agreement?"},
                        {"direction": "inbound", "content": "Three locations. Property manager approves."},
                        {"direction": "outbound", "content": "Commercial workflow started: decision maker, property count, walkthrough, and package comparison."},
                    ],
                },
            ],
            "owner_recap": [
                f"WARREN would watch {lead_sources} and centralize every lead before opportunities get lost.",
                f"Demo data estimates {recovered} recoverable leads per month, worth roughly ${projected_revenue:,.0f} in recovered opportunity.",
                f"The demo has already loaded good-lead rules, profitable services, and handoff rules for {business_name}.",
                "Live connections replace demo records after onboarding without losing the setup captured here.",
            ],
        }

    def _demo_shared_token_status():
        def _setting(*keys):
            for key in keys:
                try:
                    value = (db.get_setting(key) or "").strip()
                except Exception:
                    value = ""
                if value:
                    return True
            return False

        ai_ready = any([
            bool(os.environ.get("OPENAI_API_KEY")),
            bool(os.environ.get("OPENROUTER_API_KEY")),
            bool(os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")),
            bool(os.environ.get("XAI_API_KEY")),
            _setting("openai_api_key", "openrouter_api_key", "gemini_api_key", "google_api_key", "xai_api_key"),
        ])
        maps_ready = bool(os.environ.get("GOOGLE_MAPS_API_KEY")) or _setting("google_maps_api_key")
        return {
            "ai_ready": bool(ai_ready),
            "maps_ready": bool(maps_ready),
            "mode": "shared_demo_infrastructure",
            "note": "Demo brands use app-level AI and Maps credentials when brand-level tokens are blank.",
        }

    def _demo_location_defaults(service_area):
        text = (service_area or "").lower()
        known = [
            ("tucson", 32.2226, -110.9747),
            ("phoenix", 33.4484, -112.0740),
            ("mesa", 33.4152, -111.8315),
            ("scottsdale", 33.4942, -111.9261),
            ("tempe", 33.4255, -111.9400),
            ("chandler", 33.3062, -111.8413),
            ("gilbert", 33.3528, -111.7890),
            ("glendale", 33.5387, -112.1860),
            ("peoria", 33.5806, -112.2374),
            ("queen creek", 33.2487, -111.6343),
            ("surprise", 33.6292, -112.3679),
        ]
        for key, lat, lng in known:
            if key in text:
                return {"lat": lat, "lng": lng, "label": key.title()}
        return {"lat": 33.4484, "lng": -112.0740, "label": "Phoenix"}

    def _demo_dashboard_payload(data, brand, snapshot):
        metrics = snapshot.get("metrics") or {}
        business_name = data.get("business_name") or brand.get("display_name") or "Demo Business"
        services = data.get("primary_services") or brand.get("primary_services") or "core services"
        area = data.get("service_area") or brand.get("service_area") or "the service area"
        monthly_leads = int(metrics.get("monthly_leads") or data.get("monthly_leads") or 35)
        recoverable = int(metrics.get("estimated_unfollowed_leads") or max(3, round(monthly_leads * 0.18)))
        recovered_revenue = float(metrics.get("projected_recovered_revenue") or 0)
        token_status = snapshot.get("shared_tokens") or _demo_shared_token_status()
        action_title = "Work the demo lead inbox before activation"
        action_detail = (
            f"WARREN has {recoverable} recoverable sample opportunities for {business_name}. "
            "Use the seeded inbox, proposal, nurture, creative, and local-rank tools exactly like the live portal."
        )
        return {
            "demo_mode": True,
            "health": {"grade": "B", "score": 82, "label": "Demo-ready"},
            "health_summary": {
                "summary": f"{business_name} is set up in demo mode with lead flow, nurture rules, growth data, and activation gaps visible.",
                "status": "attention",
            },
            "health_cluster": {
                "cards": [
                    {
                        "label": "Lead system",
                        "grade": "B+",
                        "title": "Seeded and ready to test",
                        "detail": f"{monthly_leads} monthly leads modeled, {recoverable} recoverable opportunities.",
                    },
                    {
                        "label": "AI",
                        "grade": "A" if token_status.get("ai_ready") else "Setup",
                        "title": "Shared demo AI" if token_status.get("ai_ready") else "Connect shared AI token",
                        "detail": "Brand token is blank; app-level demo credential is used." if token_status.get("ai_ready") else "Add the global AI key once for all demos.",
                    },
                    {
                        "label": "Maps",
                        "grade": "A" if token_status.get("maps_ready") else "Setup",
                        "title": "Shared Maps ready" if token_status.get("maps_ready") else "Connect shared Maps token",
                        "detail": "Heatmap and commercial discovery can use the app-level Maps key." if token_status.get("maps_ready") else "Add the global Maps key once for all demos.",
                    },
                ],
            },
            "channels": {
                "website": {
                    "title": "Website and Lead Capture",
                    "icon": "bi-globe",
                    "cards": [
                        {"metric": "Modeled visitors", "value": f"{monthly_leads * 42:,}", "status": "neutral", "explanation": f"Demo traffic is modeled from {area} so WARREN has realistic operating context before live analytics are connected."},
                        {"metric": "Lead conversion", "value": f"{round(monthly_leads / max(monthly_leads * 42, 1) * 100, 1)}%", "status": "warning", "explanation": "WARREN highlights the first practical bottleneck: faster replies and better qualification before spending more."},
                    ],
                },
                "facebook_ads": {
                    "title": "Facebook and Instagram Leads",
                    "icon": "bi-meta",
                    "cards": [
                        {"metric": "Lead forms", "value": "Ready", "status": "good", "explanation": "The demo shows how Meta lead forms become WARREN lead threads. Activation swaps demo data for the real form connection."},
                        {"metric": "Follow-up risk", "value": str(recoverable), "status": "warning", "explanation": "These are modeled opportunities that WARREN would recover with fast reply, nurture, and owner handoff rules."},
                    ],
                },
                "seo": {
                    "title": "Local Search",
                    "icon": "bi-search",
                    "cards": [
                        {"metric": "Heatmap keyword", "value": services.split(",")[0].strip()[:42], "status": "info", "explanation": "A demo local-rank scan is seeded so the owner can inspect what Maps intelligence will look like after activation."},
                    ],
                },
            },
            "kpi_status": [
                {"label": "Monthly lead target", "actual": monthly_leads, "target": max(monthly_leads + recoverable, monthly_leads), "status": "attention"},
                {"label": "Recoverable revenue", "actual": recovered_revenue, "target": recovered_revenue, "status": "good"},
            ],
            "actions": [
                {
                    "key": "demo_work_real_warren",
                    "title": action_title,
                    "mission_name": action_title,
                    "priority": "Do This Now",
                    "priority_class": "danger",
                    "category": "Growth Strategy",
                    "why": action_detail,
                    "what": "Open the seeded lead threads, let WARREN draft replies, inspect the commercial/proposal flow, then review activation setup.",
                    "data_point": f"{monthly_leads} modeled monthly leads; {recoverable} recoverable.",
                    "reward": "The prospect sees the same WARREN portal they get after activation, with demo records replacing live connections.",
                    "steps": [
                        "Open the WARREN inbox and pick the hottest demo lead.",
                        "Use the draft/review flow to show how WARREN qualifies and routes the conversation.",
                        "Open the activation setup after the owner understands the operating flow.",
                    ],
                    "impact": "Makes the demo feel like a working client instance instead of a slideshow.",
                    "time": "10 minutes",
                    "icon": "bi-stars",
                    "source": "Affiliate demo workspace",
                }
            ],
            "highlights": [
                "Owner intake is stored on the demo brand.",
                "Lead, nurture, growth, creative, commercial, and local search surfaces are seeded.",
                "Activation keeps the setup and replaces demo records with live connections.",
            ],
            "concerns": [
                "Live sends, publishing, billing, OAuth connects, and external pushes stay blocked until activation.",
            ],
            "warren_briefing": {
                "total_findings": 3,
                "critical_count": 1,
                "warning_count": 1,
                "positive_count": 1,
                "top_critical": [{"title": "Lead speed is the first bottleneck", "detail": f"{recoverable} demo opportunities are recoverable if follow-up happens immediately.", "agent": "lead_command"}],
                "top_warnings": [{"title": "Activation still needed", "detail": "Real CRM, SMS, billing, and lead-source credentials are not connected in demo mode.", "agent": "activation"}],
                "top_wins": [{"title": "Owner rules captured", "detail": "Good-lead, profitable-service, and handoff details are already loaded.", "agent": "warren_brain"}],
            },
            "_analysis": {"demo": True, "business_name": business_name, "monthly_leads": monthly_leads, "recoverable": recoverable},
            "_suggestions": [],
        }

    def _demo_heatmap_results(location, business_name):
        lat = float(location.get("lat") or 33.4484)
        lng = float(location.get("lng") or -112.0740)
        competitors = [
            ("Prime Local Services", "demo-place-1"),
            ("Rapid Response Pros", "demo-place-2"),
            ("Trusted Neighborhood Co", "demo-place-3"),
            (business_name, "demo-target"),
        ]
        cells = []
        ranks = [4, 3, 5, 2, 3, 4, 1, 2, 3]
        idx = 0
        for row in range(3):
            for col in range(3):
                rank = ranks[idx]
                pack = []
                for pos, (name, place_id) in enumerate(competitors, start=1):
                    adjusted = pos
                    if name == business_name:
                        adjusted = rank
                    elif pos >= rank:
                        adjusted = min(pos + 1, 5)
                    pack.append({
                        "rank": adjusted,
                        "name": name,
                        "place_id": place_id,
                        "address": f"{location.get('label') or 'Local'} demo market",
                        "is_target": name == business_name,
                    })
                pack.sort(key=lambda item: item["rank"])
                cells.append({
                    "row": row,
                    "col": col,
                    "lat": round(lat + (row - 1) * 0.018, 6),
                    "lng": round(lng + (col - 1) * 0.018, 6),
                    "rank": rank,
                    "competitors": pack,
                })
                idx += 1
        return cells

    def _seed_demo_operating_data(brand_id, data, snapshot):
        brand = db.get_brand(brand_id) or {}
        now = datetime.now()
        month = now.strftime("%Y-%m")
        business_name = data.get("business_name") or brand.get("display_name") or "Demo Business"
        services = data.get("primary_services") or brand.get("primary_services") or "service calls"
        service = services.split(",")[0].strip() or "service calls"
        area = data.get("service_area") or brand.get("service_area") or ""
        location = _demo_location_defaults(area)
        token_status = _demo_shared_token_status()
        plan = list(snapshot.get("connection_plan") or [])
        plan_names = {str(item.get("name") or "").lower() for item in plan if isinstance(item, dict)}
        if "shared ai token" not in plan_names:
            plan.insert(0, {
                "name": "Shared AI Token",
                "status": "ready" if token_status.get("ai_ready") else "needs_setup",
                "impact": "Lets every affiliate demo use WARREN chat, drafting, creative, and analysis without storing a separate token on the prospect brand.",
            })
        if "shared google maps token" not in plan_names:
            plan.insert(1, {
                "name": "Shared Google Maps Token",
                "status": "ready" if token_status.get("maps_ready") else "needs_setup",
                "impact": "Powers demo heatmaps, local-rank context, and commercial discovery from one app-level credential.",
            })
        snapshot["connection_plan"] = plan

        db.update_brand_number_field(brand_id, "business_lat", location["lat"])
        db.update_brand_number_field(brand_id, "business_lng", location["lng"])
        db.update_brand_number_field(brand_id, "kpi_target_leads", int((snapshot.get("metrics") or {}).get("monthly_leads") or data.get("monthly_leads") or 35))
        db.update_brand_number_field(brand_id, "crm_avg_service_price", float(data.get("avg_job_value") or 450))
        for field, value in {
            "ai_provider": "openai",
            "openai_model_chat": "gpt-4o-mini",
            "openai_model_analysis": "gpt-4o-mini",
            "openai_model_images": "gpt-image-2",
            "agent_context": json.dumps({
                "demo_mode": True,
                "shared_tokens": token_status,
                "business_intake": {
                    "good_lead_definition": (data.get("owner_intake") or {}).get("good_lead_definition", ""),
                    "profitable_services": (data.get("owner_intake") or {}).get("profitable_services", ""),
                    "handoff_rules": (data.get("owner_intake") or {}).get("handoff_rules", ""),
                },
            }, sort_keys=True),
        }.items():
            db.update_brand_text_field(brand_id, field, value)

        snapshot["shared_tokens"] = token_status
        snapshot["demo_location"] = location
        dashboard = _demo_dashboard_payload(data, db.get_brand(brand_id) or brand, snapshot)
        db.upsert_dashboard_snapshot(brand_id, month, json.dumps(dashboard, default=str, sort_keys=True), source="affiliate_demo")

        try:
            if not db.get_heatmap_scans(brand_id, limit=1):
                results = _demo_heatmap_results(location, business_name)
                ranked = [cell for cell in results if int(cell.get("rank") or 0) > 0]
                avg_rank = round(sum(int(cell["rank"]) for cell in ranked) / max(len(ranked), 1), 1)
                db.save_heatmap_scan(
                    brand_id,
                    service,
                    3,
                    5,
                    location["lat"],
                    location["lng"],
                    json.dumps(results, sort_keys=True),
                    avg_rank,
                    status="complete",
                    debug_json=json.dumps({"demo": True, "shared_maps_ready": token_status.get("maps_ready")}, sort_keys=True),
                )
        except Exception:
            logger.exception("Failed to seed demo heatmap for brand %s", brand_id)

        try:
            if not db.get_scheduled_posts(brand_id, limit=1):
                db.save_scheduled_post(
                    brand_id,
                    "facebook",
                    f"Considering {service} in {area or 'your area'}? {business_name} can help you understand the next step before you book.",
                    (now + timedelta(days=2)).strftime("%Y-%m-%d 09:00:00"),
                    post_type="value",
                )
        except Exception:
            logger.exception("Failed to seed demo social post for brand %s", brand_id)

        try:
            if not db.get_blog_posts(brand_id, limit=1):
                db.save_blog_post(
                    brand_id,
                    f"What to Know Before Booking {service.title()}",
                    f"This demo draft shows how WARREN turns {business_name}'s services, area, and customer questions into useful local content.",
                    excerpt=f"A simple buyer guide for {service} in {area or 'the local market'}.",
                    slug=f"demo-{re.sub(r'[^a-z0-9]+', '-', service.lower()).strip('-') or 'service'}-guide",
                    status="draft",
                    categories="Demo, Local SEO",
                    tags=services,
                )
        except Exception:
            logger.exception("Failed to seed demo blog post for brand %s", brand_id)

        try:
            if not db.get_brand_tasks(brand_id, status="open", limit=1):
                db.create_brand_task(
                    brand_id,
                    "Activate this WARREN instance",
                    "Replace demo data with live lead sources, SMS, CRM, billing, and OAuth connections after the owner says yes.",
                    steps_json=json.dumps([
                        "Confirm the owner wants to activate the demo workspace.",
                        "Connect lead source, SMS, CRM, billing, AI, and Maps credentials.",
                        "Turn on live sends after compliance and handoff rules are approved.",
                    ]),
                    priority="high",
                    source="affiliate_demo",
                    source_ref=str(snapshot.get("demo_session_id") or ""),
                )
        except Exception:
            logger.exception("Failed to seed demo task for brand %s", brand_id)

        try:
            existing = db.get_agent_findings(brand_id, month=month, limit=1)
            if not existing:
                db.save_agent_finding(brand_id, "lead_command", month, "critical", "Lead speed is the demo bottleneck", f"WARREN modeled {len(snapshot.get('sample_leads') or [])} live-style lead threads that need fast qualification.", "Open the WARREN inbox and show the draft/review flow.")
                db.save_agent_finding(brand_id, "activation", month, "warning", "Live connections are intentionally blocked", "Demo records are safe until activation connects the real lead sources, CRM, billing, and messaging.", "Use activation setup after the owner approves.")
                db.save_agent_finding(brand_id, "warren_brain", month, "positive", "Owner rules are captured", "Good-lead definition, profitable services, and handoff rules were saved from intake.", "Keep these rules when converting the demo to a live client.")
        except Exception:
            logger.exception("Failed to seed demo findings for brand %s", brand_id)

        return snapshot

    def _demo_slug_for_business(name):
        base = re.sub(r"[^a-z0-9]+", "-", (name or "demo-business").strip().lower()).strip("-")[:36]
        return f"demo-{base or 'business'}-{secrets.token_hex(3)}"

    def _create_demo_warren_brand(partner_id, data, referral_code):
        expires = (datetime.now() + timedelta(days=45)).strftime("%Y-%m-%d %H:%M:%S")
        brand_id = db.create_brand({
            "slug": _demo_slug_for_business(data.get("business_name")),
            "display_name": f"{data.get('business_name')} (WARREN Demo)",
            "industry": data.get("industry") or "home_services",
            "service_area": data.get("service_area") or "",
            "website": data.get("website") or "",
            "primary_services": data.get("primary_services") or "",
            "partner_id": partner_id,
            "referral_code": referral_code,
            "attribution": {"source": "affiliate_demo_brand", "referral_code": referral_code},
            "partner_attributed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
        db.update_brand_demo_fields(
            brand_id,
            is_demo=1,
            demo_status="demo_until_activated",
            demo_partner_id=partner_id,
            demo_expires_at=expires,
            sales_bot_enabled=0,
            sales_bot_nurture_enabled=0,
            sales_bot_auto_push_crm=0,
            sales_bot_call_logging=1,
        )
        demo_settings = {
            "sales_bot_channels": json.dumps(["sms", "lead_forms", "calls", "messenger"]),
            "sales_bot_quote_mode": "hybrid",
            "sales_bot_business_hours": "Demo: business hours will be configured during activation. For now WARREN shows after-hours handling without sending real messages.",
            "sales_bot_reply_tone": data.get("tone_preferences") or "Fast, direct, helpful, and specific to the job.",
            "sales_bot_service_menu": data.get("primary_services") or "",
            "sales_bot_pricing_notes": f"Demo average job value: ${float(data.get('avg_job_value') or 0):.0f}. Replace with real pricing during activation.",
            "sales_bot_guardrails": "DEMO MODE: no real SMS, email, CRM pushes, or customer outreach until this WARREN instance is activated.",
            "sales_bot_handoff_rules": (data.get("owner_intake") or {}).get("handoff_rules") or "Escalate urgent jobs, angry customers, pricing objections, and anything outside service area.",
            "sales_bot_collect_fields": "name,phone,email,service,address,timeline,budget",
            "sales_bot_closing_procedure": "Qualify the lead, confirm service area, capture urgency, offer the next booking step, and alert the owner when human review is needed.",
            "sales_bot_booking_success_message": "You are on the schedule request list. The team will confirm the exact window shortly.",
            "sales_bot_lead_form_config": json.dumps({
                "demo_mode": True,
                "sources": data.get("lead_sources") or "Facebook forms, website forms, missed calls",
                "good_lead_definition": (data.get("owner_intake") or {}).get("good_lead_definition", ""),
            }),
        }
        for field, value in demo_settings.items():
            db.update_brand_text_field(brand_id, field, value)
        try:
            db.update_brand_feature_access(
                brand_id,
                {
                    flag["feature_key"]: "on"
                    for flag in db.get_feature_flags()
                    if flag.get("enabled") and flag.get("access_level") != "admin"
                },
            )
        except Exception:
            logger.exception("Failed to enable demo feature access for brand %s", brand_id)
        return brand_id

    def _ensure_demo_client_user(demo, brand):
        if not demo or not brand:
            return None
        demo_email = f"partner-demo-{demo['id']}@warren-demo.local"
        user = db.get_client_user_by_email(demo_email)
        if user and int(user.get("brand_id") or 0) == int(brand["id"]):
            return user
        display_name = demo.get("contact_name") or "Demo Owner"
        temp_password = secrets.token_urlsafe(18)
        user_id = db.create_client_user(brand["id"], demo_email, temp_password, display_name, role="owner")
        return db.get_client_user(user_id) if user_id else None

    def _seed_demo_warren_leads(brand_id, data, snapshot):
        seeded = []
        for idx, lead in enumerate(snapshot.get("sample_leads") or [], start=1):
            channel = "lead_forms" if "Form" in lead.get("source", "") else "calls" if "Call" in lead.get("source", "") else "sms"
            status = "qualified" if lead.get("stage") == "Hot" else "needs_review" if "review" in lead.get("stage", "").lower() else "quoted" if lead.get("stage") == "Proposal" else "nurture"
            thread_id = db.upsert_lead_thread(
                brand_id,
                channel,
                f"demo-{idx}-{lead.get('name','lead').lower().replace(' ', '-')}",
                {
                    "lead_name": lead.get("name", ""),
                    "lead_email": f"demo-lead-{idx}@example.test",
                    "lead_phone": f"+155501230{idx}",
                    "source": lead.get("source", "WARREN Demo"),
                    "status": status,
                    "quote_status": "ready" if status == "qualified" else "not_started",
                    "summary": lead.get("warren_action", ""),
                    "commercial_data_json": json.dumps({
                        "demo": True,
                        "estimated_value": lead.get("value", 0),
                        "need": lead.get("need", ""),
                        "next_step": lead.get("next_step", ""),
                    }),
                },
            )
            messages = lead.get("messages") or [
                {"direction": "inbound", "content": f"Hi, I need help with {lead.get('need', 'a service request')}. Are you available this week?"},
                {"direction": "outbound", "content": lead.get("warren_action", "WARREN qualified the lead and prepared the next step.")},
            ]
            if not db.get_lead_messages(thread_id, limit=1):
                for message in messages:
                    db.add_lead_message(
                        thread_id,
                        message.get("direction") or "inbound",
                        "assistant" if message.get("direction") == "outbound" else "lead",
                        message.get("content") or "",
                        channel=channel,
                        metadata={"demo": True, "source": lead.get("source", ""), "not_sent": message.get("direction") == "outbound"},
                    )
            db.add_lead_event(
                brand_id,
                thread_id,
                "demo_warren_action",
                lead.get("stage", ""),
                {"demo": True, "estimated_value": lead.get("value", 0)},
            )
            seeded.append({**lead, "thread_id": thread_id})
        snapshot["sample_leads"] = seeded
        snapshot["demo_brand_id"] = brand_id
        snapshot["activation_state"] = "Demo until activated: no real outbound communication is sent."
        return snapshot

    def _demo_data_from_session(demo):
        data = dict(demo or {})
        data["owner_intake"] = demo.get("owner_intake") or {}
        data["demo_snapshot"] = demo.get("demo_snapshot") or {}
        return data

    def _ensure_demo_warren_workspace(demo):
        data = _demo_data_from_session(demo)
        partner_id = demo["partner_id"]
        brand = db.get_brand(demo.get("demo_brand_id")) if demo.get("demo_brand_id") else None
        if not brand:
            codes = db.get_partner_referral_codes(partner_id)
            referral_code = codes[0]["code"] if codes else f"partner-{partner_id}"
            if not codes:
                db.create_partner_referral_code(partner_id, referral_code)
            brand_id = _create_demo_warren_brand(partner_id, data, referral_code)
            db.update_partner_demo_session(demo["id"], partner_id, demo_brand_id=brand_id)
            db.update_brand_demo_fields(brand_id, demo_session_id=demo["id"])
            brand = db.get_brand(brand_id)
            demo["demo_brand_id"] = brand_id

        snapshot = data.get("demo_snapshot") or {}
        if not snapshot.get("workspace_modules") or not snapshot.get("automation_timeline"):
            legacy_leads = snapshot.get("sample_leads") or []
            upgraded_snapshot = _build_partner_demo_snapshot(data)
            if legacy_leads:
                legacy_names = {str(lead.get("name") or "").strip().lower() for lead in legacy_leads}
                top_up = [
                    lead for lead in upgraded_snapshot.get("sample_leads") or []
                    if str(lead.get("name") or "").strip().lower() not in legacy_names
                ]
                upgraded_snapshot["sample_leads"] = legacy_leads + top_up[:max(0, 4 - len(legacy_leads))]
            snapshot = upgraded_snapshot
            db.update_partner_demo_session(demo["id"], partner_id, demo_snapshot_json=snapshot)
            demo["demo_snapshot"] = snapshot
            data["demo_snapshot"] = snapshot

        threads = db.get_lead_threads(brand["id"], limit=10) if brand else []
        if brand and not threads:
            snapshot = data.get("demo_snapshot") or _build_partner_demo_snapshot(data)
            if not snapshot.get("sample_leads"):
                snapshot = _build_partner_demo_snapshot(data)
            snapshot = _seed_demo_warren_leads(brand["id"], data, snapshot)
            db.update_partner_demo_session(demo["id"], partner_id, demo_snapshot_json=snapshot)
            demo["demo_snapshot"] = snapshot
            threads = db.get_lead_threads(brand["id"], limit=10)
        elif brand and threads:
            sample_count = len((data.get("demo_snapshot") or {}).get("sample_leads") or [])
            if sample_count and len(threads) < sample_count:
                snapshot = _seed_demo_warren_leads(brand["id"], data, data.get("demo_snapshot") or _build_partner_demo_snapshot(data))
                db.update_partner_demo_session(demo["id"], partner_id, demo_snapshot_json=snapshot)
                demo["demo_snapshot"] = snapshot
                threads = db.get_lead_threads(brand["id"], limit=10)
        if brand:
            snapshot = demo.get("demo_snapshot") or data.get("demo_snapshot") or _build_partner_demo_snapshot(data)
            snapshot["demo_session_id"] = demo["id"]
            snapshot = _seed_demo_operating_data(brand["id"], data, snapshot)
            db.update_partner_demo_session(demo["id"], partner_id, demo_snapshot_json=snapshot)
            demo["demo_snapshot"] = snapshot
            brand = db.get_brand(brand["id"]) or brand
        return brand, threads

    def _decorate_demo_threads(threads):
        decorated = []
        for index, thread in enumerate(threads or []):
            item = dict(thread)
            item["messages"] = db.get_lead_messages(thread["id"], limit=12)
            item["commercial_data"] = _safe_from_json(item.get("commercial_data_json") or "{}")
            item["is_active"] = index == 0
            decorated.append(item)
        return decorated

    def _partner_demo_form_data():
        raw_avg = request.form.get("avg_job_value", "").strip()
        raw_leads = request.form.get("monthly_leads", "").strip()
        try:
            avg_job_value = float(raw_avg or 0)
        except ValueError:
            avg_job_value = 0
        try:
            monthly_leads = int(raw_leads or 0)
        except ValueError:
            monthly_leads = 0
        return {
            "business_name": request.form.get("business_name", "").strip(),
            "contact_name": request.form.get("contact_name", "").strip(),
            "contact_email": request.form.get("contact_email", "").strip().lower(),
            "contact_phone": request.form.get("contact_phone", "").strip(),
            "website": request.form.get("website", "").strip(),
            "industry": request.form.get("industry", "").strip(),
            "service_area": request.form.get("service_area", "").strip(),
            "primary_services": request.form.get("primary_services", "").strip(),
            "avg_job_value": avg_job_value,
            "monthly_leads": monthly_leads,
            "crm_used": request.form.get("crm_used", "").strip(),
            "lead_sources": request.form.get("lead_sources", "").strip(),
            "pain_points": request.form.get("pain_points", "").strip(),
            "owner_goals": request.form.get("owner_goals", "").strip(),
            "tone_preferences": request.form.get("tone_preferences", "").strip(),
            "next_follow_up": request.form.get("next_follow_up", "").strip(),
            "owner_intake": {
                "good_lead_definition": request.form.get("good_lead_definition", "").strip(),
                "profitable_services": request.form.get("profitable_services", "").strip(),
                "bad_fit_leads": request.form.get("bad_fit_leads", "").strip(),
                "handoff_rules": request.form.get("handoff_rules", "").strip(),
            },
        }

    @app.route("/partners/login", methods=["GET", "POST"])
    def partner_login():
        if request.method == "POST":
            email = request.form.get("email", "").strip().lower()
            password = request.form.get("password", "")
            partner_user = db.authenticate_partner_user(email, password)
            if partner_user:
                session.clear()
                session["partner_user_id"] = partner_user["id"]
                session["partner_id"] = partner_user["partner_id"]
                session["partner_name"] = partner_user.get("partner_name", "")
                return redirect(url_for("partner_dashboard"))
            flash("Invalid affiliate login.", "error")
        return render_template("partner/login.html")

    @app.route("/partners/logout")
    def partner_logout():
        session.pop("partner_user_id", None)
        session.pop("partner_id", None)
        session.pop("partner_name", None)
        return redirect(url_for("partner_login"))

    @app.route("/partners")
    @partner_login_required
    def partner_dashboard():
        partner_user = _current_partner_user()
        partner_id = partner_user["partner_id"]
        demos = db.get_partner_demo_sessions(partner_id, limit=100)
        codes = db.get_partner_referral_codes(partner_id)
        commissions = db.get_partner_commissions(partner_id=partner_id, limit=25)
        assignments = db.get_partner_brand_assignments(partner_id=partner_id)
        return render_template(
            "partner/dashboard.html",
            partner_user=partner_user,
            demos=demos,
            codes=codes,
            commissions=commissions,
            assignments=assignments,
            summary=db.get_partner_demo_summary(partner_id),
        )

    @app.route("/partners/demo/new", methods=["GET", "POST"])
    @partner_login_required
    def partner_demo_new():
        partner_user = _current_partner_user()
        partner_id = partner_user["partner_id"]
        if request.method == "POST":
            data = _partner_demo_form_data()
            if not data["business_name"] or not data["contact_name"] or not data["contact_email"]:
                flash("Business name, owner/contact name, and email are required.", "error")
                return render_template("partner/demo_form.html", partner_user=partner_user, data=data)

            codes = db.get_partner_referral_codes(partner_id)
            referral_code = codes[0]["code"] if codes else f"partner-{partner_id}"
            if not codes:
                db.create_partner_referral_code(partner_id, referral_code)
            attribution = {"source": "affiliate_demo", "referral_code": referral_code}
            note = "\n".join(
                line for line in [
                    "Affiliate demo created.",
                    f"Pain points: {data['pain_points']}" if data.get("pain_points") else "",
                    f"Owner goals: {data['owner_goals']}" if data.get("owner_goals") else "",
                    f"Lead sources: {data['lead_sources']}" if data.get("lead_sources") else "",
                ] if line
            )
            prospect_id = db.create_agency_prospect(
                name=data["contact_name"],
                email=data["contact_email"],
                phone=data["contact_phone"],
                business_name=data["business_name"],
                website=data["website"],
                industry=data["industry"],
                service_area=data["service_area"],
                source="affiliate_demo",
                stage="new",
                monthly_budget=str(data.get("avg_job_value") or ""),
                notes=note,
                partner_id=partner_id,
                referral_code=referral_code,
                attribution_json=json.dumps(attribution, sort_keys=True),
                next_follow_up=data.get("next_follow_up", ""),
            )
            data["status"] = "demo_ready"
            data["nurture_status"] = "new"
            demo_brand_id = _create_demo_warren_brand(partner_id, data, referral_code)
            data["demo_snapshot"] = _seed_demo_warren_leads(
                demo_brand_id,
                data,
                _build_partner_demo_snapshot(data),
            )
            demo_id = db.create_partner_demo_session(
                partner_id,
                partner_user["id"],
                data,
                prospect_id=prospect_id,
                demo_brand_id=demo_brand_id,
            )
            db.update_brand_demo_fields(demo_brand_id, demo_session_id=demo_id)
            data["demo_snapshot"]["demo_session_id"] = demo_id
            data["demo_snapshot"] = _seed_demo_operating_data(demo_brand_id, data, data["demo_snapshot"])
            db.update_partner_demo_session(demo_id, partner_id, demo_snapshot_json=data["demo_snapshot"])
            db.record_partner_attribution_event(
                partner_id=partner_id,
                prospect_id=prospect_id,
                brand_id=demo_brand_id,
                referral_code=referral_code,
                source="affiliate_demo",
                metadata={"demo_session_id": demo_id, "business_name": data["business_name"]},
            )
            flash("Demo workspace created.", "success")
            return redirect(url_for("partner_demo_detail", demo_id=demo_id))
        return render_template("partner/demo_form.html", partner_user=partner_user, data={})

    @app.route("/partners/demo/<int:demo_id>")
    @partner_login_required
    def partner_demo_detail(demo_id):
        partner_user = _current_partner_user()
        demo = db.get_partner_demo_session(demo_id, partner_id=partner_user["partner_id"])
        if not demo:
            abort(404)
        events = db.get_partner_demo_events(demo_id, partner_user["partner_id"])
        return render_template("partner/demo_detail.html", partner_user=partner_user, demo=demo, events=events)

    @app.route("/partners/demo/live/<demo_token>")
    def partner_demo_live(demo_token):
        demo = db.get_partner_demo_session_by_token(demo_token)
        if not demo:
            abort(404)
        brand, raw_threads = _ensure_demo_warren_workspace(demo)
        demo_user = _ensure_demo_client_user(demo, brand)
        if not brand or not demo_user:
            abort(404)
        db.add_partner_demo_event(
            demo["id"],
            demo["partner_id"],
            None,
            "owner_demo_viewed",
            "Owner-facing WARREN demo opened in the real client portal.",
            {"demo_brand_id": demo.get("demo_brand_id")},
        )
        session.clear()
        session["client_user_id"] = demo_user["id"]
        session["client_brand_id"] = brand["id"]
        session["client_name"] = demo_user.get("display_name") or demo.get("contact_name") or "Demo Owner"
        session["client_brand_name"] = brand.get("display_name") or demo.get("business_name") or "WARREN Demo"
        session["client_role"] = demo_user.get("role", "owner")
        session["client_demo_mode"] = True
        session["client_demo_session_id"] = demo["id"]
        session["client_demo_partner_id"] = demo["partner_id"]
        db.update_client_user_login(demo_user["id"])
        flash("Demo mode is active. You are inside the real WARREN portal with live sends and external pushes disabled.", "warning")
        return redirect(url_for("client.client_auto_warren", demo="1"))

    @app.route("/partners/demo/<int:demo_id>/nurture", methods=["POST"])
    @partner_login_required
    def partner_demo_nurture(demo_id):
        partner_user = _current_partner_user()
        demo = db.get_partner_demo_session(demo_id, partner_id=partner_user["partner_id"])
        if not demo:
            abort(404)
        status = request.form.get("nurture_status", demo.get("nurture_status") or "follow_up").strip()
        note = request.form.get("note", "").strip()
        next_follow_up = request.form.get("next_follow_up", "").strip()
        db.update_partner_demo_session(
            demo_id,
            partner_user["partner_id"],
            nurture_status=status,
            next_follow_up=next_follow_up,
            last_contact_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        if demo.get("prospect_id"):
            db.update_agency_prospect(demo["prospect_id"], next_follow_up=next_follow_up)
            if note:
                db.add_agency_prospect_note(demo["prospect_id"], note, note_type="partner_note", created_by=partner_user.get("email") or "partner")
        db.add_partner_demo_event(
            demo_id,
            partner_user["partner_id"],
            partner_user["id"],
            "nurture",
            note or f"Status changed to {status}.",
            {"nurture_status": status, "next_follow_up": next_follow_up},
        )
        flash("Demo follow-up updated.", "success")
        return redirect(url_for("partner_demo_detail", demo_id=demo_id))

    @app.route("/health")
    def health_check():
        return jsonify({"ok": True, "service": "gromore-admin"}), 200

    @app.route("/privacy")
    def privacy_policy():
        return render_template("privacy_policy.html")

    @app.route("/terms")
    def terms_of_service():
        return render_template("terms_of_service.html")

    @app.route("/meta/data-deletion")
    def meta_data_deletion():
        return render_template("meta_data_deletion.html")

    @app.route("/meta/data-deletion/status/<confirmation_code>")
    def meta_data_deletion_status(confirmation_code):
        deletion_request = db.get_meta_deletion_request(confirmation_code)
        if not deletion_request:
            abort(404)
        return jsonify(
            {
                "confirmation_code": deletion_request["confirmation_code"],
                "status": deletion_request["status"],
                "requested_at": deletion_request["requested_at"],
                "completed_at": deletion_request["completed_at"],
                "deleted_thread_count": deletion_request["deleted_thread_count"],
                "notes": deletion_request["notes"],
            }
        )

    @app.route("/meta/data-deletion/callback", methods=["POST"])
    @csrf.exempt
    def meta_data_deletion_callback():
        signed_request = (
            request.form.get("signed_request")
            or (request.get_json(silent=True) or {}).get("signed_request")
            or ""
        ).strip()
        if not signed_request:
            return jsonify({"error": "Missing signed_request"}), 400

        app_secret = (db.get_setting("meta_app_secret", "") or app.config.get("META_APP_SECRET", "")).strip()
        if not app_secret:
            return jsonify({"error": "Meta app secret is not configured"}), 500

        try:
            payload = _decode_meta_signed_request(signed_request, app_secret)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

        meta_user_id = str(payload.get("user_id") or "").strip()
        if not meta_user_id:
            return jsonify({"error": "signed_request missing user_id"}), 400

        deletion_request = db.create_meta_deletion_request(
            meta_user_id,
            json.dumps(payload, sort_keys=True),
        )
        deletion_request = db.process_meta_deletion_request(deletion_request["confirmation_code"])

        configured = (app.config.get("APP_URL", "") or "").rstrip("/")
        request_base = request.host_url.rstrip("/")
        public_base = request_base if (not configured or "localhost" in configured) else configured
        status_url = f"{public_base}{url_for('meta_data_deletion_status', confirmation_code=deletion_request['confirmation_code'])}"

        return jsonify(
            {
                "url": status_url,
                "confirmation_code": deletion_request["confirmation_code"],
            }
        )

    # ── Dashboard ──
    @app.route("/")
    @login_required
    def dashboard():
        brands = db.get_all_brands()
        current_month = datetime.now().strftime("%Y-%m")
        stats = {
            "total_brands": len(brands),
            "connected_ga4": sum(1 for b in brands if b.get("ga4_property_id")),
            "connected_meta": sum(1 for b in brands if b.get("meta_ad_account_id")),
            "reports_this_month": db.count_reports_for_month(current_month),
        }

        # Latest report per brand (for quick-action column)
        brand_reports = {}
        for brand in brands:
            rpt = db.get_report_for_brand_month(brand["id"], current_month)
            if rpt:
                brand_reports[brand["id"]] = rpt

        recent_reports = db.get_recent_reports(limit=10)
        recent_ai_briefs = []

        # Health / setup checklist
        openai_ok = bool(db.get_setting("openai_api_key", "").strip() or os.environ.get("OPENAI_API_KEY", "").strip())
        google_ok = bool(db.get_setting("google_client_id", "").strip() or app.config.get("GOOGLE_CLIENT_ID", "").strip())
        meta_ok = bool(db.get_setting("meta_app_id", "").strip() or app.config.get("META_APP_ID", "").strip())
        smtp_ok = bool(db.get_setting("smtp_user", "").strip() or app.config.get("SMTP_USER", "").strip())
        app_url = app.config.get("APP_URL", "")
        app_url_ok = bool(app_url and "localhost" not in app_url)

        expiring_tokens = []
        try:
            expiring_tokens = db.get_expiring_connections(days=14)
        except Exception:
            pass

        health = {
            "openai": openai_ok,
            "google_oauth": google_ok,
            "meta_oauth": meta_ok,
            "smtp": smtp_ok,
            "app_url": app_url_ok,
            "all_configured": all([openai_ok, google_ok, meta_ok, smtp_ok, app_url_ok]),
            "expiring_tokens": expiring_tokens,
        }
        try:
            import json as _json

            for row in db.get_recent_ai_briefs(limit=6):
                client = {}
                try:
                    client = _json.loads(row.get("client_json") or "{}")
                except Exception:
                    client = {}

                recent_ai_briefs.append({
                    "brand_id": row.get("brand_id"),
                    "brand_name": row.get("brand_name"),
                    "month": row.get("month"),
                    "updated_at": row.get("updated_at") or row.get("generated_at") or "",
                    "executive_summary": client.get("executive_summary") or "",
                    "mission_critical": client.get("mission_critical") or [],
                })
        except Exception:
            recent_ai_briefs = []

        return render_template(
            "dashboard.html",
            brands=brands,
            stats=stats,
            recent_reports=recent_reports,
            recent_ai_briefs=recent_ai_briefs,
            health=health,
            brand_reports=brand_reports,
        )

    # ── Brand Management ──
    @app.route("/brands")
    @login_required
    def brands_list():
        brands = db.get_all_brands()
        pulse = db.get_brand_usage_pulse()
        return render_template("brands/list.html", brands=brands, pulse=pulse)

    @app.route("/brands/new", methods=["GET", "POST"])
    @login_required
    def brand_new():
        if request.method == "POST":
            brand_data = _extract_brand_form(request.form)
            brand_id = db.create_brand(brand_data)
            flash(f"Brand '{brand_data['display_name']}' created", "success")
            return redirect(url_for("brand_detail", brand_id=brand_id))
        return render_template("brands/form.html", brand=None, industries=_get_industries())

    @app.route("/brands/<int:brand_id>")
    @login_required
    def brand_detail(brand_id):
        brand = db.get_brand(brand_id)
        if not brand:
            abort(404)
        active_month = request.args.get("month") or datetime.now().strftime("%Y-%m")
        month_finance = db.get_brand_month_finance(brand_id, active_month) or {
            "closed_revenue": 0,
            "closed_deals": 0,
            "notes": "",
        }
        imports_root = Path(app.config.get("IMPORTS_DIR") or (BASE_DIR / "data" / "imports"))
        import_dir = imports_root / brand["slug"] / active_month
        csv_status = {
            "month": active_month,
            "import_dir": str(import_dir),
            "ga": (import_dir / "google_analytics.csv").exists(),
            "meta": (import_dir / "meta_business.csv").exists(),
            "gsc": (import_dir / "search_console.csv").exists(),
        }
        connections = db.get_brand_connections(brand_id)
        reports = db.get_brand_reports(brand_id, limit=12)
        contacts = db.get_brand_contacts(brand_id)

        ai_brief_row = db.get_ai_brief(brand_id, active_month)
        ai_brief = None
        if ai_brief_row:
            try:
                import json as _json

                ai_brief = {
                    "internal": _json.loads(ai_brief_row.get("internal_json") or "{}"),
                    "client": _json.loads(ai_brief_row.get("client_json") or "{}"),
                    "model": ai_brief_row.get("model") or "",
                    "generated_at": ai_brief_row.get("generated_at") or "",
                    "updated_at": ai_brief_row.get("updated_at") or "",
                }
            except Exception:
                ai_brief = None

        openai_key = db.get_setting("openai_api_key", "").strip() or os.environ.get("OPENAI_API_KEY", "").strip()

        ai_chat_messages = db.get_ai_chat_messages(brand_id, active_month, limit=30)

        client_users = db.get_client_users_for_brand(brand_id)
        portal_url = (app.config.get("APP_URL", "") or "").rstrip("/")

        return render_template(
            "brands/detail.html",
            brand=brand,
            connections=connections,
            reports=reports,
            contacts=contacts,
            active_month=active_month,
            csv_status=csv_status,
            ai_brief=ai_brief,
            ai_chat_messages=ai_chat_messages,
            openai_enabled=bool(openai_key),
            month_finance=month_finance,
            client_users=client_users,
            app_url=portal_url,
        )

    @app.route("/brands/<int:brand_id>/insights")
    @login_required
    def brand_insights(brand_id):
        brand = db.get_brand(brand_id)
        if not brand:
            abort(404)

        month = request.args.get("month") or datetime.now().strftime("%Y-%m")

        analysis = {}
        suggestions = []
        error = ""
        ai_plan = None

        try:
            from webapp.report_runner import build_analysis_and_suggestions_for_brand
            analysis, suggestions = build_analysis_and_suggestions_for_brand(db, brand, month)
        except Exception as e:
            error = str(e)

        if analysis:
            seo = analysis.get("search_console") or {}
            google_ads = analysis.get("google_ads") or {}
            competitor_watch = analysis.get("competitor_watch") or {}

            seo_actions = []
            for item in (seo.get("keyword_recommendations") or [])[:10]:
                seo_actions.append({
                    "keyword": item.get("keyword", ""),
                    "position": item.get("position"),
                    "impressions": item.get("impressions"),
                    "reason": item.get("reason", ""),
                    "action": item.get("recommended_action", ""),
                })

            google_ads_actions = []
            for campaign in (google_ads.get("campaign_analysis") or [])[:12]:
                issue = campaign.get("issue") or ("Under target" if campaign.get("status") == "underperforming" else "Monitor")
                action = (
                    "Tighten targeting, rebuild search terms/keywords, and reallocate spend to highest-converting segments."
                    if campaign.get("status") == "underperforming"
                    else "Keep budget stable and test one new ad/message variant this cycle."
                )
                google_ads_actions.append({
                    "campaign": campaign.get("name", "Campaign"),
                    "status": campaign.get("status", "ok"),
                    "issue": issue,
                    "action": action,
                    "spend": (campaign.get("metrics") or {}).get("spend", 0),
                    "results": (campaign.get("metrics") or {}).get("results", 0),
                    "cpa": (campaign.get("metrics") or {}).get("cost_per_result"),
                })

            competitor_actions = []
            for move in (competitor_watch.get("counter_moves") or [])[:8]:
                competitor_actions.append({
                    "title": move.get("title", "Counter move"),
                    "priority": move.get("priority", "medium"),
                    "detail": move.get("detail", ""),
                })

            # Optional GPT-powered deep operator plan
            openai_key = db.get_setting("openai_api_key", "").strip() or os.environ.get("OPENAI_API_KEY", "").strip()
            if openai_key:
                try:
                    from webapp.ai_assistant import generate_account_operator_plan

                    model = (
                        db.get_setting("openai_model", "").strip()
                        or os.environ.get("OPENAI_MODEL", "").strip()
                        or app.config.get("OPENAI_MODEL")
                        or "gpt-4o-mini"
                    )
                    ai_plan = generate_account_operator_plan(
                        api_key=openai_key,
                        analysis=analysis,
                        suggestions=suggestions,
                        model=model,
                    )
                except Exception:
                    ai_plan = None
        else:
            seo_actions = []
            google_ads_actions = []
            competitor_actions = []
            competitor_watch = {}
            seo = {}
            google_ads = {}

        fb_organic = analysis.get("facebook_organic") or {} if analysis else {}

        return render_template(
            "brands/insights.html",
            brand=brand,
            month=month,
            error=error,
            analysis=analysis,
            suggestions=suggestions,
            seo=seo,
            google_ads=google_ads,
            fb_organic=fb_organic,
            competitor_watch=competitor_watch,
            seo_actions=seo_actions,
            google_ads_actions=google_ads_actions,
            competitor_actions=competitor_actions,
            ai_plan=ai_plan,
        )

    @app.route("/brands/<int:brand_id>/test-organic")
    @login_required
    def brand_test_organic(brand_id):
        """Diagnostic endpoint: test Facebook organic API calls and show raw results."""
        brand = db.get_brand(brand_id)
        if not brand:
            abort(404)

        import requests as _req
        results = {"brand": brand["display_name"], "checks": []}

        page_id = (brand.get("facebook_page_id") or "").strip()
        results["facebook_page_id"] = page_id or "(not set)"

        connections = db.get_brand_connections(brand_id)
        meta_conn = connections.get("meta")
        if not meta_conn or meta_conn.get("status") != "connected":
            results["checks"].append({"step": "Meta connection", "status": "FAIL", "detail": "Meta is not connected"})
            return jsonify(results)

        results["checks"].append({"step": "Meta connection", "status": "OK", "detail": f"Connected as {meta_conn.get('account_name', '?')}"})

        # Get token
        from webapp.api_bridge import _get_meta_token, _get_page_access_token, _resolve_facebook_page_context
        try:
            user_token = _get_meta_token(db, brand_id, meta_conn)
            results["checks"].append({"step": "User token", "status": "OK", "detail": f"Token length: {len(user_token)}"})
        except Exception as e:
            results["checks"].append({"step": "User token", "status": "FAIL", "detail": str(e)})
            return jsonify(results)

        # Check me/accounts (what pages does this token have access to?)
        pages_resp = _req.get(
            "https://graph.facebook.com/v21.0/me/accounts",
            params={"access_token": user_token, "fields": "id,name,access_token"},
            timeout=15,
        )
        if pages_resp.status_code == 200:
            pages_data = pages_resp.json().get("data", [])
            page_names = [{"id": p["id"], "name": p.get("name", "?")} for p in pages_data]
            results["checks"].append({
                "step": "me/accounts (pages with access)",
                "status": "OK" if pages_data else "WARN",
                "detail": page_names if pages_data else "No pages returned. Token may lack pages_show_list permission.",
            })
        else:
            results["checks"].append({
                "step": "me/accounts",
                "status": "FAIL",
                "detail": pages_resp.text[:300],
            })
            pages_data = []

        if not page_id:
            results["checks"].append({"step": "Page ID", "status": "FAIL", "detail": "No facebook_page_id set on brand"})
            return jsonify(results)

        # Resolve page context and get page token
        resolved_page_id, resolved_page_token = _resolve_facebook_page_context(db, brand_id, page_id, user_token)
        if resolved_page_id:
            page_id = resolved_page_id
        page_token = resolved_page_token or _get_page_access_token(page_id, user_token)
        is_page_token = page_token != user_token
        results["checks"].append({
            "step": "Page access token",
            "status": "OK" if is_page_token else "WARN",
            "detail": "Got page-specific token" if is_page_token else "Using user token as fallback (page not in me/accounts)",
        })

        # Test page info
        info_resp = _req.get(
            f"https://graph.facebook.com/v21.0/{page_id}",
            params={"access_token": page_token, "fields": "name,fan_count,followers_count"},
            timeout=15,
        )
        results["checks"].append({
            "step": "Page info",
            "status": "OK" if info_resp.status_code == 200 and "error" not in info_resp.json() else "FAIL",
            "detail": info_resp.json(),
        })

        # Test page insights (one metric at a time to isolate failures)
        from datetime import datetime as _dt, date as _date
        today = _date.today()
        first_of_month = today.replace(day=1)
        since_ts = int(_dt(first_of_month.year, first_of_month.month, first_of_month.day).timestamp())
        until_ts = int((_dt(today.year, today.month, today.day) + timedelta(days=1)).timestamp())

        test_metrics = [
            "page_media_view",
            "page_total_media_view_unique",
            "page_impressions_organic",
            "page_post_engagements",
            "page_actions_post_reactions_total",
            "page_daily_follows_unique",
            "page_daily_unfollows_unique",
            "page_follows",
            "page_views_total",
        ]
        for metric in test_metrics:
            try:
                m_resp = _req.get(
                    f"https://graph.facebook.com/v21.0/{page_id}/insights",
                    params={
                        "access_token": page_token,
                        "metric": metric,
                        "period": "day",
                        "since": since_ts,
                        "until": until_ts,
                    },
                    timeout=15,
                )
                if m_resp.status_code == 200:
                    m_data = m_resp.json()
                    if "error" in m_data:
                        results["checks"].append({
                            "step": f"Insight: {metric}",
                            "status": "FAIL",
                            "detail": m_data["error"].get("message", str(m_data["error"])),
                        })
                    else:
                        entries = m_data.get("data", [])
                        total = 0
                        for entry in entries:
                            for val in entry.get("values", []):
                                v = val.get("value", 0)
                                if isinstance(v, (int, float)):
                                    total += v
                                elif isinstance(v, dict):
                                    total += sum(v.values())
                        results["checks"].append({
                            "step": f"Insight: {metric}",
                            "status": "OK",
                            "detail": f"Total: {total} ({len(entries)} entries, {sum(len(e.get('values',[])) for e in entries)} days)",
                        })
                else:
                    results["checks"].append({
                        "step": f"Insight: {metric}",
                        "status": "FAIL",
                        "detail": f"HTTP {m_resp.status_code}: {m_resp.text[:200]}",
                    })
            except Exception as e:
                results["checks"].append({
                    "step": f"Insight: {metric}",
                    "status": "FAIL",
                    "detail": str(e),
                })

        # Test posts
        try:
            posts_resp = _req.get(
                f"https://graph.facebook.com/v21.0/{page_id}/posts",
                params={
                    "access_token": page_token,
                    "fields": "id,message,created_time,type",
                    "since": since_ts,
                    "until": until_ts,
                    "limit": 5,
                },
                timeout=15,
            )
            if posts_resp.status_code == 200:
                posts_data = posts_resp.json()
                if "error" in posts_data:
                    results["checks"].append({
                        "step": "Posts",
                        "status": "FAIL",
                        "detail": posts_data["error"].get("message", str(posts_data["error"])),
                    })
                else:
                    post_list = posts_data.get("data", [])
                    results["checks"].append({
                        "step": "Posts",
                        "status": "OK" if post_list else "WARN",
                        "detail": f"Found {len(post_list)} posts" + (
                            f". First: {post_list[0].get('created_time','')} - {(post_list[0].get('message','') or '')[:60]}"
                            if post_list else ". No posts returned for this date range."
                        ),
                    })
            else:
                results["checks"].append({
                    "step": "Posts",
                    "status": "FAIL",
                    "detail": f"HTTP {posts_resp.status_code}: {posts_resp.text[:200]}",
                })
        except Exception as e:
            results["checks"].append({"step": "Posts", "status": "FAIL", "detail": str(e)})

        return jsonify(results)

    @app.route("/brands/<int:brand_id>/finance", methods=["POST"])
    @login_required
    def brand_save_month_finance(brand_id):
        brand = db.get_brand(brand_id)
        if not brand:
            abort(404)

        month = request.form.get("month") or datetime.now().strftime("%Y-%m")
        revenue = request.form.get("closed_revenue", "0")
        closed_deals = request.form.get("closed_deals", "0")
        notes = request.form.get("finance_notes", "")

        db.upsert_brand_month_finance(brand_id, month, revenue, closed_deals, notes)
        flash(f"Revenue data saved for {month}", "success")
        return redirect(url_for("brand_detail", brand_id=brand_id, month=month))

    @app.route("/brands/<int:brand_id>/ai-chat", methods=["POST"])
    @login_required
    def brand_ai_chat(brand_id):
        brand = db.get_brand(brand_id)
        if not brand:
            if request.is_json or request.headers.get("X-Requested-With") == "XMLHttpRequest":
                return jsonify({"error": "Brand not found"}), 404
            abort(404)

        is_ajax = request.is_json or request.headers.get("X-Requested-With") == "XMLHttpRequest"

        if request.is_json:
            payload = request.get_json(silent=True) or {}
            month = payload.get("month") or datetime.now().strftime("%Y-%m")
            user_message = (payload.get("message") or "").strip()
        else:
            month = request.form.get("month") or datetime.now().strftime("%Y-%m")
            user_message = (request.form.get("message") or "").strip()

        if not user_message:
            if is_ajax:
                return jsonify({"error": "Empty message"}), 400
            return redirect(url_for("brand_detail", brand_id=brand_id, month=month) + "#ai-chat")

        api_key = db.get_setting("openai_api_key", "").strip() or os.environ.get("OPENAI_API_KEY", "").strip()
        if not api_key:
            msg = "OpenAI is not configured. Add your OpenAI API key in Settings."
            if is_ajax:
                return jsonify({"error": msg}), 400
            flash(msg, "error")
            return redirect(url_for("brand_detail", brand_id=brand_id, month=month) + "#ai-chat")

        model = (
            db.get_setting("openai_model", "").strip()
            or os.environ.get("OPENAI_MODEL", "").strip()
            or app.config.get("OPENAI_MODEL")
            or "gpt-4o-mini"
        )

        db.add_ai_chat_message(brand_id, month, "user", user_message)

        try:
            from webapp.report_runner import build_analysis_and_suggestions_for_brand
            from webapp.ai_assistant import chat_with_warren, summarize_analysis_for_ai

            analysis = None
            suggestions = None
            analysis_error = ""
            try:
                analysis, suggestions = build_analysis_and_suggestions_for_brand(db, brand, month)
            except Exception as e:
                analysis_error = str(e)

            history = db.get_ai_chat_messages(brand_id, month, limit=30)
            trimmed = history[-12:] if len(history) > 12 else history
            messages = [{"role": m["role"], "content": m["content"]} for m in trimmed if m.get("content")]

            context = {
                "brand": {
                    "name": brand.get("display_name"),
                    "slug": brand.get("slug"),
                    "industry": brand.get("industry"),
                    "service_area": brand.get("service_area"),
                    "primary_services": brand.get("primary_services"),
                    "monthly_budget": brand.get("monthly_budget"),
                    "website": brand.get("website"),
                    "goals": brand.get("goals"),
                    "brand_voice": brand.get("brand_voice"),
                    "active_offers": brand.get("active_offers"),
                    "target_audience": brand.get("target_audience"),
                    "competitors": brand.get("competitors"),
                    "reporting_notes": brand.get("reporting_notes"),
                    "kpi_target_cpa": brand.get("kpi_target_cpa"),
                    "kpi_target_leads": brand.get("kpi_target_leads"),
                    "kpi_target_roas": brand.get("kpi_target_roas"),
                },
                "month": month,
                "analysis": summarize_analysis_for_ai(analysis) if isinstance(analysis, dict) else None,
                "suggestions": suggestions,
                "analysis_error": analysis_error,
            }

            assistant_reply = chat_with_warren(
                api_key=api_key,
                model=model,
                context=context,
                messages=messages,
                admin_system_prompt=(
                    db.get_setting("ai_chat_system_prompt", "").strip()
                    or app.config.get("AI_CHAT_SYSTEM_PROMPT", "")
                ),
            )

            if assistant_reply:
                db.add_ai_chat_message(brand_id, month, "assistant", assistant_reply)

            if is_ajax:
                return jsonify({"reply": assistant_reply or ""})
        except Exception as e:
            if is_ajax:
                return jsonify({"error": str(e)}), 500
            flash(f"AI chat failed: {str(e)}", "error")

        return redirect(url_for("brand_detail", brand_id=brand_id, month=month) + "#ai-chat")

    @app.route("/brands/<int:brand_id>/ai-brief/generate", methods=["POST"])
    @login_required
    def brand_generate_ai_brief(brand_id):
        brand = db.get_brand(brand_id)
        if not brand:
            abort(404)

        month = request.form.get("month") or datetime.now().strftime("%Y-%m")
        api_key = db.get_setting("openai_api_key", "").strip() or os.environ.get("OPENAI_API_KEY", "").strip()
        if not api_key:
            flash("OpenAI is not configured. Add your OpenAI API key in Settings.", "error")
            return redirect(url_for("brand_detail", brand_id=brand_id, month=month))

        try:
            from webapp.report_runner import build_analysis_and_suggestions_for_brand
            from webapp.ai_assistant import generate_warren_brief

            analysis, suggestions = build_analysis_and_suggestions_for_brand(db, brand, month)

            model = (
                db.get_setting("openai_model", "").strip()
                or os.environ.get("OPENAI_MODEL", "").strip()
                or app.config.get("OPENAI_MODEL")
                or "gpt-4o-mini"
            )
            internal = generate_warren_brief(
                api_key=api_key,
                analysis=analysis,
                suggestions=suggestions,
                variant="internal",
                model=model or None,
            )
            client = generate_warren_brief(
                api_key=api_key,
                analysis=analysis,
                suggestions=suggestions,
                variant="client",
                model=model or None,
            )

            import json as _json

            db.upsert_ai_brief(
                brand_id,
                month,
                internal_json=_json.dumps(internal),
                client_json=_json.dumps(client),
                model=(model or "gpt-4o-mini"),
            )
            flash(f"AI brief generated for {month}", "success")
        except Exception as e:
            flash(f"AI brief failed: {str(e)}", "error")

        return redirect(url_for("brand_detail", brand_id=brand_id, month=month))

    @app.route("/brands/<int:brand_id>/edit", methods=["GET", "POST"])
    @login_required
    def brand_edit(brand_id):
        brand = db.get_brand(brand_id)
        if not brand:
            abort(404)
        if request.method == "POST":
            brand_data = _extract_brand_form(request.form)
            db.update_brand(brand_id, brand_data)
            flash("Brand updated", "success")
            return redirect(url_for("brand_detail", brand_id=brand_id))
        return render_template("brands/form.html", brand=brand, industries=_get_industries())

    @app.route("/brands/<int:brand_id>/settings", methods=["GET", "POST"])
    @login_required
    def brand_settings(brand_id):
        brand = db.get_brand(brand_id)
        if not brand:
            abort(404)
        feature_flags = db.get_feature_flags()
        if request.method == "POST":
            section = request.form.get("section", "")
            if section == "voice":
                db.update_brand_text_field(brand_id, "brand_voice", request.form.get("brand_voice", ""))
                db.update_brand_text_field(brand_id, "active_offers", request.form.get("active_offers", ""))
                db.update_brand_text_field(brand_id, "target_audience", request.form.get("target_audience", ""))
                competitors_raw = request.form.get("competitors", "")
                db.update_brand_text_field(brand_id, "competitors", competitors_raw)

                # Also sync into the structured competitors table used by the client portal.
                import re

                comps = []
                for part in re.split(r"[\n,]+", competitors_raw or ""):
                    part = (part or "").strip()
                    if not part:
                        continue
                    if "|" in part:
                        name, website = part.split("|", 1)
                        comps.append({"name": name.strip(), "website": website.strip()})
                    else:
                        comps.append({"name": part.strip()})
                db.replace_competitors_for_brand(brand_id, comps)

                db.update_brand_text_field(brand_id, "reporting_notes", request.form.get("reporting_notes", ""))
                flash("Brand voice and context saved", "success")
            elif section == "kpis":
                db.update_brand_number_field(brand_id, "kpi_target_cpa", request.form.get("kpi_target_cpa", "0"))
                db.update_brand_number_field(brand_id, "kpi_target_leads", request.form.get("kpi_target_leads", "0"))
                db.update_brand_number_field(brand_id, "kpi_target_roas", request.form.get("kpi_target_roas", "0"))
                db.update_brand_text_field(brand_id, "call_tracking_number", request.form.get("call_tracking_number", ""))
                flash("KPI targets saved", "success")
            elif section == "crm":
                db.update_brand_text_field(brand_id, "crm_type", request.form.get("crm_type", ""))
                crm_api_key = request.form.get("crm_api_key", "").strip()
                if crm_api_key:
                    db.update_brand_text_field(brand_id, "crm_api_key", crm_api_key)
                db.update_brand_text_field(brand_id, "crm_webhook_url", request.form.get("crm_webhook_url", ""))
                db.update_brand_text_field(brand_id, "crm_pipeline_id", request.form.get("crm_pipeline_id", ""))
                db.update_brand_text_field(brand_id, "crm_server_url", request.form.get("crm_server_url", ""))
                db.update_brand_text_field(brand_id, "payment_provider", request.form.get("payment_provider", ""))
                payment_api_key = request.form.get("payment_api_key", "").strip()
                if payment_api_key:
                    db.update_brand_text_field(brand_id, "payment_api_key", payment_api_key)
                payment_webhook_secret = request.form.get("payment_webhook_secret", "").strip()
                if payment_webhook_secret:
                    db.update_brand_text_field(brand_id, "payment_webhook_secret", payment_webhook_secret)
                db.update_brand_text_field(brand_id, "payment_location_id", request.form.get("payment_location_id", ""))
                db.update_brand_text_field(brand_id, "payment_account_id", request.form.get("payment_account_id", ""))
                flash("CRM settings saved", "success")
            elif section == "features":
                brand_feature_access = {}
                for flag in feature_flags:
                    feature_key = flag.get("feature_key")
                    state = request.form.get(f"feature_state_{feature_key}", "on")
                    brand_feature_access[feature_key] = state
                db.update_brand_feature_access(brand_id, brand_feature_access)
                db.update_brand_text_field(brand_id, "upgrade_dev_email", request.form.get("upgrade_dev_email", ""))
                db.update_brand_text_field(brand_id, "upgrade_contact_emails", request.form.get("upgrade_contact_emails", ""))
                flash("Feature access and upgrade contacts saved", "success")
            return redirect(url_for("brand_settings", brand_id=brand_id))
        # Reload brand to get latest data
        brand = db.get_brand(brand_id)
        categories = {}
        for flag in feature_flags:
            category = flag.get("category") or "general"
            categories.setdefault(category, []).append(flag)
        return render_template(
            "brands/settings.html",
            brand=brand,
            app_url=(app.config.get("APP_URL", "") or "").rstrip("/"),
            feature_categories=categories,
            brand_feature_access=db.get_brand_feature_access(brand_id),
        )

    @app.route("/webhooks/crm/revenue/<int:brand_id>", methods=["POST"])
    @app.route("/webhooks/crm/revenue/slug/<slug>", methods=["POST"])
    @csrf.exempt
    def crm_revenue_webhook(brand_id=None, slug=None):
        brand = db.get_brand(brand_id) if brand_id is not None else db.get_brand_by_slug(slug or "")
        if not brand:
            return jsonify({"ok": False, "error": "Brand not found"}), 404

        expected_key = (brand.get("crm_api_key") or "").strip()
        if not expected_key:
            return jsonify({"ok": False, "error": "CRM API key not configured for this brand"}), 400

        payload = request.get_json(silent=True) or {}
        auth_header = (request.headers.get("Authorization") or "").strip()
        bearer_key = auth_header[7:].strip() if auth_header.lower().startswith("bearer ") else ""
        header_key = (request.headers.get("X-Webhook-Key") or "").strip()
        body_key = str(payload.get("api_key") or payload.get("webhook_key") or "").strip()
        provided_key = header_key or bearer_key or body_key

        if not provided_key or not secrets.compare_digest(provided_key, expected_key):
            return jsonify({"ok": False, "error": "Unauthorized"}), 401

        month_raw = str(payload.get("month") or payload.get("period") or datetime.now().strftime("%Y-%m"))
        month = month_raw[:7]
        try:
            datetime.strptime(month, "%Y-%m")
        except ValueError:
            return jsonify({"ok": False, "error": "Invalid month format. Use YYYY-MM"}), 400

        revenue = payload.get("closed_revenue", payload.get("revenue", payload.get("amount", 0)))
        closed_deals = payload.get("closed_deals", payload.get("deals", payload.get("won_deals", 0)))
        notes = str(payload.get("notes") or payload.get("source") or payload.get("description") or "")

        try:
            revenue_num = float(revenue or 0)
        except (TypeError, ValueError):
            revenue_num = 0.0
        try:
            closed_deals_num = int(float(closed_deals or 0))
        except (TypeError, ValueError):
            closed_deals_num = 0

        db.upsert_brand_month_finance(brand["id"], month, revenue_num, closed_deals_num, notes)
        db.mark_brand_webhook_received(brand["id"])

        return jsonify({
            "ok": True,
            "brand_id": brand["id"],
            "brand": brand.get("display_name", ""),
            "month": month,
            "closed_revenue": revenue_num,
            "closed_deals": closed_deals_num,
        })

    @app.route("/brands/<int:brand_id>/crm-webhook/test", methods=["POST"])
    @login_required
    def crm_revenue_webhook_test(brand_id):
        brand = db.get_brand(brand_id)
        if not brand:
            return jsonify({"ok": False, "error": "Brand not found"}), 404

        webhook_key = (brand.get("crm_api_key") or "").strip()
        if not webhook_key:
            return jsonify({"ok": False, "error": "Set CRM API key first in Brand Settings"}), 400

        month = datetime.now().strftime("%Y-%m")
        payload = {
            "month": month,
            "closed_revenue": 1234.56,
            "closed_deals": 2,
            "notes": "Webhook test payload",
        }

        try:
            with app.test_client() as client:
                resp = client.post(
                    url_for("crm_revenue_webhook", brand_id=brand_id),
                    json=payload,
                    headers={"X-Webhook-Key": webhook_key},
                )
                data = resp.get_json(silent=True) or {}

            if resp.status_code == 200 and data.get("ok"):
                return jsonify({
                    "ok": True,
                    "message": f"Webhook OK. Saved test revenue for {month}.",
                    "month": month,
                })
            return jsonify({"ok": False, "error": data.get("error") or f"Status {resp.status_code}"}), 400
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/brands/<int:brand_id>/crm-sync/revenue", methods=["POST"])
    @login_required
    def crm_sync_revenue(brand_id):
        """Pull revenue from a connected CRM or standalone payment provider and save it."""
        brand = db.get_brand(brand_id)
        if not brand:
            return jsonify({"ok": False, "error": "Brand not found"}), 404

        crm_type = (brand.get("crm_type") or "").strip().lower()
        month = request.get_json(silent=True) or {}
        target_month = (month.get("month") or "") or datetime.now().strftime("%Y-%m")

        if crm_type == "sweepandgo":
            from webapp.crm_bridge import pull_sweepandgo_revenue
            revenue, job_count, error = pull_sweepandgo_revenue(brand, target_month)
            if error:
                return jsonify({"ok": False, "error": error}), 400
            db.upsert_brand_month_finance(brand_id, target_month, revenue, job_count,
                                          f"Sweep and Go sync: {job_count} jobs")
            db.mark_brand_webhook_received(brand_id)
            return jsonify({
                "ok": True,
                "month": target_month,
                "closed_revenue": revenue,
                "closed_deals": job_count,
                "source": "sweepandgo",
            })

        elif crm_type == "jobber":
            from webapp.crm_bridge import pull_jobber_revenue
            revenue, inv_count, error = pull_jobber_revenue(brand, target_month)
            if error:
                return jsonify({"ok": False, "error": error}), 400
            db.upsert_brand_month_finance(brand_id, target_month, revenue, inv_count,
                                          f"Jobber sync: {inv_count} invoices")
            db.mark_brand_webhook_received(brand_id)
            return jsonify({
                "ok": True,
                "month": target_month,
                "closed_revenue": revenue,
                "closed_deals": inv_count,
                "source": "jobber",
            })

        elif crm_type == "razorsync":
            from webapp.crm_bridge import pull_razorsync_revenue
            revenue, payment_count, error = pull_razorsync_revenue(brand, target_month)
            if error:
                return jsonify({"ok": False, "error": error}), 400
            db.upsert_brand_month_finance(brand_id, target_month, revenue, payment_count,
                                          f"RazorSync sync: {payment_count} payments")
            db.mark_brand_webhook_received(brand_id)
            return jsonify({
                "ok": True,
                "month": target_month,
                "closed_revenue": revenue,
                "closed_deals": payment_count,
                "source": "razorsync",
            })

        elif crm_type == "gohighlevel":
            from webapp.crm_bridge import pull_gohighlevel_revenue
            revenue, deal_count, error = pull_gohighlevel_revenue(brand, target_month)
            if error:
                return jsonify({"ok": False, "error": error}), 400
            db.upsert_brand_month_finance(brand_id, target_month, revenue, deal_count,
                                          f"GoHighLevel sync: {deal_count} deals")
            db.mark_brand_webhook_received(brand_id)
            return jsonify({
                "ok": True,
                "month": target_month,
                "closed_revenue": revenue,
                "closed_deals": deal_count,
                "source": "gohighlevel",
            })

        elif (brand.get("payment_provider") or "").strip():
            from webapp.crm_bridge import pull_payment_provider_revenue
            provider = (brand.get("payment_provider") or "").strip().lower()
            revenue, payment_count, error = pull_payment_provider_revenue(brand, target_month)
            if error:
                return jsonify({"ok": False, "error": error}), 400
            db.upsert_brand_month_finance(brand_id, target_month, revenue, payment_count,
                                          f"{provider.title()} payment sync: {payment_count} payments")
            db.mark_brand_webhook_received(brand_id)
            return jsonify({
                "ok": True,
                "month": target_month,
                "closed_revenue": revenue,
                "closed_deals": payment_count,
                "source": provider,
            })

        else:
            return jsonify({"ok": False, "error": f"Revenue pull not supported for CRM type: {crm_type or 'none'}"}), 400

    @app.route("/brands/<int:brand_id>/delete", methods=["POST"])
    @login_required
    def brand_delete(brand_id):
        brand = db.get_brand(brand_id)
        if not brand:
            abort(404)
        db.delete_brand(brand_id)
        flash(f"Brand '{brand['display_name']}' deleted", "success")
        return redirect(url_for("brands_list"))

    # ── Contacts ──
    @app.route("/brands/<int:brand_id>/contacts", methods=["POST"])
    @login_required
    def brand_add_contact(brand_id):
        name = request.form.get("contact_name", "").strip()
        email = request.form.get("contact_email", "").strip()
        role = request.form.get("contact_role", "client")
        auto_send = request.form.get("auto_send") == "on"
        if name and email:
            db.add_contact(brand_id, name, email, role, auto_send)
            flash(f"Contact '{name}' added", "success")
        return redirect(url_for("brand_detail", brand_id=brand_id))

    @app.route("/contacts/<int:contact_id>/delete", methods=["POST"])
    @login_required
    def contact_delete(contact_id):
        contact = db.get_contact(contact_id)
        if not contact:
            abort(404)
        db.delete_contact(contact_id)
        flash("Contact removed", "success")
        return redirect(url_for("brand_detail", brand_id=contact["brand_id"]))

    @app.route("/contacts/<int:contact_id>/toggle-autosend", methods=["POST"])
    @login_required
    def contact_toggle_autosend(contact_id):
        contact = db.get_contact(contact_id)
        if not contact:
            abort(404)
        db.toggle_contact_autosend(contact_id)
        return redirect(url_for("brand_detail", brand_id=contact["brand_id"]))

    # ── Client Portal User Management ──
    @app.route("/brands/<int:brand_id>/client-users", methods=["POST"])
    @login_required
    def client_user_create(brand_id):
        brand = db.get_brand(brand_id)
        if not brand:
            abort(404)
        display_name = request.form.get("display_name", "").strip()
        email = request.form.get("email", "").strip()
        password = request.form.get("password", "")
        if display_name and email and password:
            result = db.create_client_user(brand_id, email, password, display_name)
            if result:
                flash(f"Client login created for {display_name}", "success")
            else:
                flash("A client login with that email already exists", "error")
        else:
            flash("Name, email, and password are required", "error")
        return redirect(url_for("brand_detail", brand_id=brand_id))

    @app.route("/client-users/<int:client_user_id>/toggle", methods=["POST"])
    @login_required
    def client_user_toggle(client_user_id):
        cu = db.get_client_user(client_user_id)
        if not cu:
            abort(404)
        db.toggle_client_user_active(client_user_id)
        flash("Client login status updated", "success")
        return redirect(url_for("brand_detail", brand_id=cu["brand_id"]))

    @app.route("/client-users/<int:client_user_id>/delete", methods=["POST"])
    @login_required
    def client_user_delete(client_user_id):
        cu = db.get_client_user(client_user_id)
        if not cu:
            abort(404)
        db.delete_client_user(client_user_id)
        flash("Client login removed", "success")
        return redirect(url_for("brand_detail", brand_id=cu["brand_id"]))

    @app.route("/client-users/<int:client_user_id>/resend-login", methods=["POST"])
    @login_required
    def client_user_resend_login(client_user_id):
        cu = db.get_client_user(client_user_id)
        if not cu:
            abort(404)
        brand = db.get_brand(cu["brand_id"])
        if not brand:
            abort(404)
        import secrets as _secrets
        temp_password = _secrets.token_urlsafe(10)
        db.update_client_user_password(client_user_id, temp_password)
        portal_url = (app.config.get("APP_URL", "") or request.host_url.rstrip("/"))
        login_url = f"{portal_url}/client/login"
        try:
            from webapp.email_sender import send_client_login_email
            send_client_login_email(
                app.config, cu["email"], cu["display_name"],
                temp_password, login_url, brand["display_name"],
            )
            flash(f"Login email sent to {cu['email']}", "success")
        except Exception as e:
            flash(f"Password was reset but email failed to send: {e}", "error")
        return redirect(url_for("brand_detail", brand_id=cu["brand_id"]))

    @app.route("/client-users/<int:client_user_id>/impersonate", methods=["POST"])
    @login_required
    def client_user_impersonate(client_user_id):
        cu = db.get_client_user(client_user_id)
        if not cu:
            abort(404)
        brand = db.get_brand(cu["brand_id"])
        if not brand:
            abort(404)
        session["client_user_id"] = cu["id"]
        session["client_brand_id"] = cu["brand_id"]
        session["client_name"] = cu["display_name"]
        session["client_brand_name"] = brand["display_name"]
        session["client_role"] = cu.get("role", "owner")
        session["client_admin_impersonating"] = True
        session["client_admin_return_brand_id"] = cu["brand_id"]
        return redirect(url_for("client.client_dashboard"))

    @app.route("/client-impersonate/stop")
    @login_required
    def client_impersonate_stop():
        brand_id = session.pop("client_admin_return_brand_id", None)
        session.pop("client_user_id", None)
        session.pop("client_brand_id", None)
        session.pop("client_name", None)
        session.pop("client_brand_name", None)
        session.pop("client_role", None)
        session.pop("client_admin_impersonating", None)
        if brand_id:
            return redirect(url_for("brand_detail", brand_id=brand_id))
        return redirect(url_for("dashboard"))

    # ── Brand API field save (AJAX) ──
    @app.route("/brands/<int:brand_id>/api-field", methods=["POST"])
    @login_required
    def brand_save_api_field(brand_id):
        brand = db.get_brand(brand_id)
        if not brand:
            return jsonify({"ok": False, "error": "not found"}), 404
        data = request.get_json()
        field = data.get("field", "")
        value = data.get("value", "")
        try:
            db.update_brand_api_field(brand_id, field, value)
            return jsonify({"ok": True})
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400

    # ── CSV Upload ──
    @app.route("/brands/<int:brand_id>/upload", methods=["POST"])
    @login_required
    def brand_upload_csv(brand_id):
        brand = db.get_brand(brand_id)
        if not brand:
            abort(404)
        month = request.form.get("month", datetime.now().strftime("%Y-%m"))
        slug = brand["slug"]

        imports_root = Path(app.config.get("IMPORTS_DIR") or (BASE_DIR / "data" / "imports"))
        import_dir = imports_root / slug / month
        import_dir.mkdir(parents=True, exist_ok=True)

        uploaded = 0
        for key in ("ga_file", "meta_file", "gsc_file"):
            f = request.files.get(key)
            if f and f.filename:
                safe_name = {
                    "ga_file": "google_analytics.csv",
                    "meta_file": "meta_business.csv",
                    "gsc_file": "search_console.csv",
                }[key]
                f.save(str(import_dir / safe_name))
                uploaded += 1

        if uploaded:
            flash(f"Uploaded {uploaded} CSV file(s) for {month}", "success")
        else:
            flash("No files selected", "warning")
        return redirect(url_for("brand_detail", brand_id=brand_id, month=month))

    # ── Reports ──
    @app.route("/brands/<int:brand_id>/generate", methods=["POST"])
    @login_required
    def brand_generate_report(brand_id):
        brand = db.get_brand(brand_id)
        if not brand:
            abort(404)
        month = request.form.get("month", datetime.now().strftime("%Y-%m"))
        try:
            from webapp.report_runner import run_report_for_brand
            result = run_report_for_brand(db, brand, month)
            if result["success"]:
                flash(f"Report generated for {month}", "success")
            else:
                flash(f"Report generation issue: {result['error']}", "error")
        except Exception as e:
            flash(f"Error: {str(e)}", "error")
        return redirect(url_for("brand_detail", brand_id=brand_id, month=month))

    @app.route("/reports/<int:report_id>/view/<report_type>")
    @login_required
    def report_view(report_id, report_type):
        report = db.get_report(report_id)
        if not report:
            abort(404)
        if report_type == "internal":
            filepath = report.get("internal_path")
        elif report_type == "client":
            filepath = report.get("client_path")
        else:
            abort(400)
        if not filepath or not Path(filepath).exists():
            abort(404)
        return send_file(filepath)

    @app.route("/reports/<int:report_id>/send", methods=["POST"])
    @login_required
    def report_send(report_id):
        report = db.get_report(report_id)
        if not report:
            abort(404)
        try:
            from webapp.email_sender import send_report_email
            brand = db.get_brand(report["brand_id"])
            contacts = db.get_brand_contacts(report["brand_id"])
            recipients = [c for c in contacts if c.get("auto_send")]
            if not recipients:
                flash("No contacts set to receive reports. Add contacts with auto-send enabled.", "warning")
            else:
                send_report_email(app.config, brand, report, recipients)
                db.mark_report_sent(report_id)
                flash(f"Report sent to {len(recipients)} contact(s)", "success")
        except Exception as e:
            flash(f"Send failed: {str(e)}", "error")
        return redirect(url_for("brand_detail", brand_id=report["brand_id"]))

    @app.route("/reports/<int:report_id>/publish-wp", methods=["POST"])
    @login_required
    def report_publish_wp(report_id):
        report = db.get_report(report_id)
        if not report:
            abort(404)
        try:
            from webapp.wp_publisher import publish_to_wordpress
            brand = db.get_brand(report["brand_id"])
            result = publish_to_wordpress(db, brand, report)
            if result["success"]:
                db.mark_report_published(report_id, result["url"])
                flash(f"Published to WordPress", "success")
            else:
                flash(f"Publish failed: {result['error']}", "error")
        except Exception as e:
            flash(f"Publish error: {str(e)}", "error")
        return redirect(url_for("brand_detail", brand_id=report["brand_id"]))

    # ── Ad Intelligence ──
    @app.route("/ad-intelligence")
    @login_required
    def ad_intelligence():
        from webapp.ad_knowledge import seed_ad_knowledge
        # Auto-seed on first visit if DB is empty
        seed_ad_knowledge(db)

        examples = db.get_ad_examples(limit=100)
        practices = db.get_ad_best_practices()
        digests = db.get_ad_news_digests(limit=20)
        master_prompt = db.get_active_master_prompt("ad_builder", "all", "")
        niche_prompts = db.get_all_niche_prompts()
        strategies = db.get_all_campaign_strategies(active_only=False)

        good_count = sum(1 for e in examples if e.get("quality") == "good")
        bad_count = sum(1 for e in examples if e.get("quality") == "bad")

        return render_template(
            "ad_intelligence.html",
            examples=examples,
            practices=practices,
            digests=digests,
            master_prompt=master_prompt,
            good_count=good_count,
            bad_count=bad_count,
            niche_prompts=niche_prompts,
            strategies=strategies,
            industries=_get_industries(),
        )

    @app.route("/ad-intelligence/add-example", methods=["POST"])
    @login_required
    def ad_intel_add_example():
        import json as _json
        principles = [p.strip() for p in request.form.get("principles", "").split(",") if p.strip()]
        db.add_ad_example(
            platform=request.form.get("platform", "google"),
            fmt=request.form.get("format", "search_rsa"),
            industry=request.form.get("industry", "").strip(),
            headline=request.form.get("headline", "").strip(),
            description=request.form.get("description", "").strip(),
            full_ad_json="{}",
            quality=request.form.get("quality", "good"),
            score=int(request.form.get("score", 7)),
            analysis=request.form.get("analysis", "").strip(),
            principles=_json.dumps(principles),
            source=request.form.get("source", "").strip(),
        )
        flash("Ad example added", "success")
        return redirect(url_for("ad_intelligence") + "#tab-examples")

    @app.route("/ad-intelligence/delete-example/<int:example_id>", methods=["POST"])
    @login_required
    def ad_intel_delete_example(example_id):
        db.delete_ad_example(example_id)
        flash("Example deleted", "success")
        return redirect(url_for("ad_intelligence") + "#tab-examples")

    @app.route("/ad-intelligence/add-practice", methods=["POST"])
    @login_required
    def ad_intel_add_practice():
        db.add_ad_best_practice(
            platform=request.form.get("platform", "all"),
            fmt=request.form.get("format", ""),
            category=request.form.get("category", "general"),
            title=request.form.get("title", "").strip(),
            content=request.form.get("content", "").strip(),
            priority=int(request.form.get("priority", 5)),
            source=request.form.get("source", "").strip(),
        )
        flash("Best practice added", "success")
        return redirect(url_for("ad_intelligence") + "#tab-practices")

    @app.route("/ad-intelligence/delete-practice/<int:bp_id>", methods=["POST"])
    @login_required
    def ad_intel_delete_practice(bp_id):
        db.delete_ad_best_practice(bp_id)
        flash("Practice deleted", "success")
        return redirect(url_for("ad_intelligence") + "#tab-practices")

    @app.route("/ad-intelligence/seed", methods=["POST"])
    @login_required
    def ad_intel_seed():
        from webapp.ad_knowledge import seed_ad_knowledge
        # Force re-seed by passing db (seed_ad_knowledge checks for empty tables)
        seed_ad_knowledge(db)
        flash("Database seeded with starter examples and best practices", "success")
        return redirect(url_for("ad_intelligence"))

    @app.route("/ad-intelligence/run-digest", methods=["POST"])
    @login_required
    def ad_intel_run_digest():
        from webapp.ad_knowledge import run_news_digest
        result = run_news_digest(db)
        if "error" in result:
            flash(f"News digest failed: {result['error']}", "error")
        else:
            flash("News digest complete. Review findings below.", "success")
        return redirect(url_for("ad_intelligence") + "#tab-news")

    @app.route("/ad-intelligence/rebuild-prompt", methods=["POST"])
    @login_required
    def ad_intel_rebuild_prompt():
        from webapp.ad_knowledge import rebuild_master_prompt
        result = rebuild_master_prompt(db)
        if "error" in result:
            flash(f"Master prompt build failed: {result['error']}", "error")
        else:
            flash("Master prompt rebuilt and saved. All future ad generation will use the new version.", "success")
        return redirect(url_for("ad_intelligence"))

    # ── Ad Intelligence: Niche Prompts ──
    @app.route("/ad-intelligence/save-niche", methods=["POST"])
    @login_required
    def ad_intel_save_niche():
        industry = request.form.get("industry", "").strip()
        title = request.form.get("title", "").strip()
        content = request.form.get("content", "").strip()
        if not industry or not content:
            flash("Industry and content are required.", "error")
            return redirect(url_for("ad_intelligence") + "#tab-niches")
        db.save_niche_prompt(industry, title, content)
        flash(f"Niche prompt for '{industry}' saved.", "success")
        return redirect(url_for("ad_intelligence") + "#tab-niches")

    @app.route("/ad-intelligence/delete-niche/<int:niche_id>", methods=["POST"])
    @login_required
    def ad_intel_delete_niche(niche_id):
        db.delete_niche_prompt(niche_id)
        flash("Niche prompt deleted.", "success")
        return redirect(url_for("ad_intelligence") + "#tab-niches")

    @app.route("/ad-intelligence/seed-niches", methods=["POST"])
    @login_required
    def ad_intel_seed_niches():
        from webapp.ad_knowledge import seed_niche_prompts
        seed_niche_prompts(db)
        flash("Starter niche prompts seeded for all industries.", "success")
        return redirect(url_for("ad_intelligence") + "#tab-niches")

    # ── Ad Intelligence: Campaign Strategies ──
    @app.route("/ad-intelligence/save-strategy", methods=["POST"])
    @login_required
    def ad_intel_save_strategy():
        strategy_key = request.form.get("strategy_key", "").strip()
        name = request.form.get("name", "").strip()
        if not strategy_key or not name:
            flash("Strategy key and name are required.", "error")
            return redirect(url_for("ad_intelligence") + "#tab-strategies")
        db.save_campaign_strategy(
            strategy_key=strategy_key,
            platform=request.form.get("platform", "meta"),
            name=name,
            icon=request.form.get("icon", "bi-megaphone-fill").strip(),
            color=request.form.get("color", "#6366f1").strip(),
            tagline=request.form.get("tagline", "").strip(),
            description=request.form.get("description", "").strip(),
            best_for=request.form.get("best_for", "").strip(),
            recommended_min=int(request.form.get("recommended_min", 200)),
            objective=request.form.get("objective", "").strip(),
            sort_order=int(request.form.get("sort_order", 0)),
            blueprint=request.form.get("blueprint", "").strip(),
        )
        flash(f"Strategy '{name}' saved.", "success")
        return redirect(url_for("ad_intelligence") + "#tab-strategies")

    @app.route("/ad-intelligence/delete-strategy/<int:strategy_id>", methods=["POST"])
    @login_required
    def ad_intel_delete_strategy(strategy_id):
        db.delete_campaign_strategy(strategy_id)
        flash("Strategy deleted.", "success")
        return redirect(url_for("ad_intelligence") + "#tab-strategies")

    @app.route("/ad-intelligence/toggle-strategy/<int:strategy_id>", methods=["POST"])
    @login_required
    def ad_intel_toggle_strategy(strategy_id):
        db.toggle_campaign_strategy_active(strategy_id)
        flash("Strategy status toggled.", "success")
        return redirect(url_for("ad_intelligence") + "#tab-strategies")

    @app.route("/ad-intelligence/seed-strategies", methods=["POST"])
    @login_required
    def ad_intel_seed_strategies():
        from webapp.ad_knowledge import seed_campaign_strategies
        seed_campaign_strategies(db)
        flash("Default campaign strategies seeded.", "success")
        return redirect(url_for("ad_intelligence") + "#tab-strategies")

    # ── Settings ──
    @app.route("/settings", methods=["GET", "POST"])
    @login_required
    def settings():
        if request.method == "POST":
            section = request.form.get("section")
            if section == "password":
                current = request.form.get("current_password")
                new_pw = request.form.get("new_password")
                user = db.authenticate_by_id(session["user_id"], current)
                if user:
                    db.update_password(session["user_id"], new_pw)
                    flash("Password updated", "success")
                else:
                    flash("Current password incorrect", "error")
            elif section == "google_oauth":
                db.save_setting("google_client_id", request.form.get("google_client_id", "").strip())
                secret = request.form.get("google_client_secret", "").strip()
                if secret:
                    db.save_setting("google_client_secret", secret)
                app.config["GOOGLE_CLIENT_ID"] = db.get_setting("google_client_id", "")
                app.config["GOOGLE_CLIENT_SECRET"] = db.get_setting("google_client_secret", app.config["GOOGLE_CLIENT_SECRET"])
                flash("Google OAuth credentials saved", "success")
            elif section == "google_ads_api":
                db.save_setting("google_ads_developer_token", request.form.get("google_ads_developer_token", "").strip())
                db.save_setting("google_ads_login_customer_id", request.form.get("google_ads_login_customer_id", "").strip())
                app.config["GOOGLE_ADS_DEVELOPER_TOKEN"] = db.get_setting("google_ads_developer_token", "")
                app.config["GOOGLE_ADS_LOGIN_CUSTOMER_ID"] = db.get_setting("google_ads_login_customer_id", "")
                flash("Google Ads API settings saved", "success")
            elif section == "meta_oauth":
                db.save_setting("meta_app_id", request.form.get("meta_app_id", "").strip())
                secret = request.form.get("meta_app_secret", "").strip()
                if secret:
                    db.save_setting("meta_app_secret", secret)
                verify_token = request.form.get("meta_webhook_verify_token", "").strip()
                if verify_token:
                    db.save_setting("meta_webhook_verify_token", verify_token)
                app.config["META_APP_ID"] = db.get_setting("meta_app_id", "")
                app.config["META_APP_SECRET"] = db.get_setting("meta_app_secret", app.config["META_APP_SECRET"])
                flash("Meta OAuth and webhook settings saved", "success")
            elif section == "smtp":
                db.save_setting("smtp_host", request.form.get("smtp_host", "").strip())
                db.save_setting("smtp_port", request.form.get("smtp_port", "587").strip())
                db.save_setting("smtp_user", request.form.get("smtp_user", "").strip())
                pw = request.form.get("smtp_password", "").strip()
                if pw:
                    db.save_setting("smtp_password", pw)
                db.save_setting("smtp_from_name", request.form.get("smtp_from_name", "").strip())
                db.save_setting("smtp_from_email", request.form.get("smtp_from_email", "").strip())
                # Reload into app config
                app.config["SMTP_HOST"] = db.get_setting("smtp_host", "smtp.gmail.com")
                app.config["SMTP_PORT"] = int(db.get_setting("smtp_port", "587"))
                app.config["SMTP_USER"] = db.get_setting("smtp_user", "")
                app.config["SMTP_PASSWORD"] = db.get_setting("smtp_password", app.config["SMTP_PASSWORD"])
                app.config["SMTP_FROM_NAME"] = db.get_setting("smtp_from_name", "Agency Reports")
                app.config["SMTP_FROM_EMAIL"] = db.get_setting("smtp_from_email", "")
                flash("SMTP settings saved", "success")
            elif section == "app_url":
                db.save_setting("app_url", request.form.get("app_url", "").strip())
                app.config["APP_URL"] = db.get_setting("app_url", "http://localhost:5000")
                flash("App URL saved", "success")
            elif section == "wordpress":
                wp_url = request.form.get("wp_url", "").strip()
                wp_user = request.form.get("wp_user", "").strip()
                wp_app_password = request.form.get("wp_app_password", "").strip()
                db.save_setting("wp_url", wp_url)
                db.save_setting("wp_user", wp_user)
                if wp_app_password:
                    db.save_setting("wp_app_password", wp_app_password)
                flash("WordPress settings saved", "success")
            elif section == "openai":
                try:
                    key = request.form.get("openai_api_key", "").strip()
                    if key:
                        db.save_setting("openai_api_key", key)
                        app.config["OPENAI_API_KEY"] = db.get_setting("openai_api_key", "")
                    openrouter_key = request.form.get("openrouter_api_key", "").strip()
                    if openrouter_key:
                        db.save_setting("openrouter_api_key", openrouter_key)
                        app.config["OPENROUTER_API_KEY"] = openrouter_key
                    perplexity_key = request.form.get("perplexity_api_key", "").strip()
                    if perplexity_key:
                        db.save_setting("perplexity_api_key", perplexity_key)
                        app.config["PERPLEXITY_API_KEY"] = perplexity_key
                    # Chat model
                    model_sel = request.form.get("openai_model", "").strip()
                    model_custom = request.form.get("openai_model_custom", "").strip()
                    model_val = model_custom if model_sel == "custom" and model_custom else (model_sel or "gpt-4o-mini")
                    db.save_setting("openai_model", model_val)
                    app.config["OPENAI_MODEL"] = model_val
                    # Competitor analysis model
                    comp_sel = request.form.get("openai_model_competitor", "").strip()
                    comp_custom = request.form.get("openai_model_competitor_custom", "").strip()
                    comp_val = comp_custom if comp_sel == "custom" and comp_custom else comp_sel
                    db.save_setting("openai_model_competitor", comp_val)
                    # System prompt
                    prompt = request.form.get("ai_chat_system_prompt", "").strip()
                    db.save_setting("ai_chat_system_prompt", prompt)
                    app.config["AI_CHAT_SYSTEM_PROMPT"] = prompt
                    seo_provider = (request.form.get("seo_research_provider") or "openrouter").strip().lower()
                    if seo_provider not in {"openrouter", "perplexity", "off"}:
                        seo_provider = "openrouter"
                    db.save_setting("seo_research_provider", seo_provider)
                    db.save_setting("seo_research_model", (request.form.get("seo_research_model") or "perplexity/sonar").strip()[:120])
                    for field, default, minimum, maximum in (
                        ("seo_research_daily_limit", 5, 0, 100),
                        ("seo_research_cache_days", 14, 1, 90),
                        ("seo_research_max_results", 8, 3, 20),
                    ):
                        try:
                            value = int(float((request.form.get(field) or str(default)).strip()))
                        except Exception:
                            value = default
                        db.save_setting(field, str(max(minimum, min(maximum, value))))
                    flash("OpenAI settings saved", "success")
                except Exception as e:
                    import logging
                    logging.getLogger(__name__).error("OpenAI settings save failed: %s", e)
                    flash(f"Failed to save OpenAI settings: {e}", "error")
            elif section == "stripe":
                key = request.form.get("stripe_secret_key", "").strip()
                wh = request.form.get("stripe_webhook_secret", "").strip()
                if key:
                    db.save_setting("stripe_secret_key", key)
                    app.config["STRIPE_SECRET_KEY"] = key
                if wh:
                    db.save_setting("stripe_webhook_secret", wh)
                    app.config["STRIPE_WEBHOOK_SECRET"] = wh
                flash("Stripe settings saved", "success")
            elif section == "square_oauth":
                app_id = request.form.get("square_app_id", "").strip()
                app_secret = request.form.get("square_app_secret", "").strip()
                api_version = request.form.get("square_api_version", "").strip() or "2026-01-22"
                db.save_setting("square_app_id", app_id)
                if app_secret:
                    db.save_setting("square_app_secret", app_secret)
                db.save_setting("square_api_version", api_version)
                app.config["SQUARE_APP_ID"] = app_id
                app.config["SQUARE_APP_SECRET"] = db.get_setting("square_app_secret", app.config.get("SQUARE_APP_SECRET", ""))
                app.config["SQUARE_API_VERSION"] = api_version
                flash("Square OAuth settings saved", "success")
            elif section == "branding":
                db.save_setting("agency_name", request.form.get("agency_name", "").strip())
                db.save_setting("agency_logo_url", request.form.get("agency_logo_url", "").strip())
                db.save_setting("agency_website", request.form.get("agency_website", "").strip())
                db.save_setting("agency_color", request.form.get("agency_color", "#2c3e50").strip())
                flash("Agency branding saved", "success")
        wp_settings = {
            "wp_url": db.get_setting("wp_url", ""),
            "wp_user": db.get_setting("wp_user", ""),
        }
        openai_configured = bool(
            db.get_setting("openai_api_key", "").strip() or os.environ.get("OPENAI_API_KEY", "").strip()
        )
        openrouter_configured = bool(
            db.get_setting("openrouter_api_key", "").strip() or os.environ.get("OPENROUTER_API_KEY", "").strip()
        )
        perplexity_configured = bool(
            db.get_setting("perplexity_api_key", "").strip() or os.environ.get("PERPLEXITY_API_KEY", "").strip()
        )
        from webapp.ai_assistant import DEFAULT_CHAT_SYSTEM_PROMPT
        ai_chat_system_prompt = (
            db.get_setting("ai_chat_system_prompt", "").strip()
            or app.config.get("AI_CHAT_SYSTEM_PROMPT", "")
            or DEFAULT_CHAT_SYSTEM_PROMPT
        )
        branding = {
            "agency_name": db.get_setting("agency_name", ""),
            "agency_logo_url": db.get_setting("agency_logo_url", ""),
            "agency_website": db.get_setting("agency_website", ""),
            "agency_color": db.get_setting("agency_color", "#2c3e50"),
        }
        openai_model = db.get_setting("openai_model", "").strip() or app.config.get("OPENAI_MODEL", "gpt-4o-mini")
        openai_model_competitor = db.get_setting("openai_model_competitor", "").strip()
        return render_template(
            "settings.html",
            wp_settings=wp_settings,
            openai_configured=openai_configured,
            openrouter_configured=openrouter_configured,
            perplexity_configured=perplexity_configured,
            ai_chat_system_prompt=ai_chat_system_prompt,
            branding=branding,
            openai_model=openai_model,
            openai_model_competitor=openai_model_competitor,
            seo_research_settings={
                "provider": db.get_setting("seo_research_provider", "openrouter"),
                "model": db.get_setting("seo_research_model", "perplexity/sonar"),
                "daily_limit": db.get_setting("seo_research_daily_limit", "5"),
                "cache_days": db.get_setting("seo_research_cache_days", "14"),
                "max_results": db.get_setting("seo_research_max_results", "8"),
            },
            meta_webhook_verify_token=db.get_setting("meta_webhook_verify_token", ""),
            square_app_secret_configured=bool((db.get_setting("square_app_secret", "") or app.config.get("SQUARE_APP_SECRET", "") or "").strip()),
        )

    # ── Settings Test Endpoints ──
    @app.route("/settings/test-smtp", methods=["POST"])
    @login_required
    def test_smtp():
        try:
            import smtplib
            host = db.get_setting("smtp_host", "smtp.gmail.com")
            port = int(db.get_setting("smtp_port", "587"))
            user = db.get_setting("smtp_user", "")
            pw = db.get_setting("smtp_password", "")
            if not user or not pw:
                return jsonify({"ok": False, "error": "SMTP username or password not configured"})
            with smtplib.SMTP(host, port, timeout=10) as server:
                server.starttls()
                server.login(user, pw)
            return jsonify({"ok": True, "message": f"Connected to {host}:{port} successfully"})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)})

    @app.route("/settings/test-openai", methods=["POST"])
    @login_required
    def test_openai():
        try:
            import requests as _req
            key = db.get_setting("openai_api_key", "").strip() or os.environ.get("OPENAI_API_KEY", "").strip()
            if not key:
                return jsonify({"ok": False, "error": "OpenAI API key not configured"})
            resp = _req.get("https://api.openai.com/v1/models", headers={
                "Authorization": f"Bearer {key}",
            }, timeout=10)
            if resp.status_code == 200:
                return jsonify({"ok": True, "message": "OpenAI API key is valid"})
            else:
                return jsonify({"ok": False, "error": f"API returned status {resp.status_code}"})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)})

    @app.route("/settings/test-wordpress", methods=["POST"])
    @login_required
    def test_wordpress():
        try:
            import requests as _req
            wp_url = db.get_setting("wp_url", "").strip().rstrip("/")
            wp_user = db.get_setting("wp_user", "").strip()
            wp_pw = db.get_setting("wp_app_password", "").strip()
            if not wp_url or not wp_user or not wp_pw:
                return jsonify({"ok": False, "error": "WordPress URL, username, or app password not configured"})
            resp = _req.get(
                f"{wp_url}/wp-json/wp/v2/posts?per_page=1",
                auth=(wp_user, wp_pw),
                timeout=10,
            )
            if resp.status_code == 200:
                return jsonify({"ok": True, "message": f"Connected to {wp_url} successfully"})
            else:
                return jsonify({"ok": False, "error": f"WordPress returned status {resp.status_code}: {resp.text[:200]}"})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)})

    # ── Setup Guide ──
    # ── Feature Flags ──

    @app.route("/features")
    @login_required
    def feature_flags_page():
        flags = db.get_feature_flags()
        categories = {}
        for f in flags:
            cat = f["category"] or "general"
            categories.setdefault(cat, []).append(f)
        return render_template("feature_flags.html", categories=categories)

    @app.route("/features/update", methods=["POST"])
    @login_required
    def feature_flags_update():
        flags = db.get_feature_flags()
        for f in flags:
            key = f["feature_key"]
            level = request.form.get(f"level_{key}", f["access_level"])
            enabled = "1" in request.form.getlist(f"enabled_{key}")
            if level not in ("all", "beta", "admin", "brand"):
                level = "all"
            db.update_feature_flag(key, level, enabled)
        flash("Feature flags updated.", "success")
        return redirect(url_for("feature_flags_page"))

    # ── Agency CRM ──

    @app.route("/crm")
    @login_required
    def agency_crm():
        # Auto-import new assessment/signup leads
        db.import_assessment_leads_to_crm()
        db.import_signup_leads_to_crm()

        stage_filter = request.args.get("stage", "")
        prospects = db.get_agency_prospects(stage=stage_filter or None)
        counts = db.get_agency_pipeline_counts()
        revenue = db.get_stripe_revenue_summary()

        stages = ["new", "contacted", "qualified", "proposal", "negotiation", "won", "lost"]
        return render_template("crm/pipeline.html",
                               prospects=prospects, counts=counts,
                               stages=stages, current_stage=stage_filter,
                               revenue=revenue)

    def _is_commercial_agency_prospect(prospect):
        prospect = prospect or {}
        return bool(
            (prospect.get("source") == "commercial_scrape")
            or (prospect.get("account_type") or "").strip()
            or (prospect.get("source_details_json") or "").strip()
        )

    def _build_commercial_brief_preview(prospect, overrides=None):
        from webapp.commercial_strategy import build_commercial_outreach_brief

        preview = dict(prospect or {})
        for key, value in (overrides or {}).items():
            if value is not None:
                preview[key] = value
        return build_commercial_outreach_brief(preview)

    @app.route("/crm/commercial")
    @login_required
    def agency_crm_commercial():
        from webapp.commercial_prospector import COMMERCIAL_PROSPECT_TYPES

        sequences = db.get_drip_sequences()
        maps_api_key = (os.environ.get("GOOGLE_MAPS_API_KEY") or db.get_setting("google_maps_api_key") or "").strip()
        return render_template(
            "crm/commercial_prospector.html",
            results=[],
            location="",
            search_criteria="",
            selected_types=[item["key"] for item in COMMERCIAL_PROSPECT_TYPES[:3]],
            prospect_types=COMMERCIAL_PROSPECT_TYPES,
            sequences=sequences,
            has_maps_api_key=bool(maps_api_key),
            search_performed=False,
        )

    @app.route("/crm/commercial/search", methods=["POST"])
    @login_required
    def agency_crm_commercial_search():
        from webapp.commercial_prospector import COMMERCIAL_PROSPECT_TYPES, search_commercial_prospects

        location = request.form.get("location", "").strip()
        search_criteria = (request.form.get("search_criteria", "") or "").strip()[:220]
        selected_types = [value.strip() for value in request.form.getlist("prospect_types") if value.strip()]
        max_results = request.form.get("max_results", "8").strip()
        try:
            max_results = max(3, min(int(max_results or 8), 15))
        except ValueError:
            max_results = 8

        maps_api_key = (os.environ.get("GOOGLE_MAPS_API_KEY") or db.get_setting("google_maps_api_key") or "").strip()
        results = []
        if location:
            try:
                results = search_commercial_prospects(
                    location,
                    selected_types,
                    api_key=maps_api_key,
                    max_results_per_type=max_results,
                    search_criteria=search_criteria,
                )
            except Exception as exc:
                flash(f"Commercial search failed: {str(exc)[:160]}", "error")
        else:
            flash("Enter a location before searching.", "error")

        sequences = db.get_drip_sequences()
        return render_template(
            "crm/commercial_prospector.html",
            results=results,
            location=location,
            search_criteria=search_criteria,
            selected_types=selected_types,
            prospect_types=COMMERCIAL_PROSPECT_TYPES,
            sequences=sequences,
            has_maps_api_key=bool(maps_api_key),
            search_performed=True,
        )

    @app.route("/crm/commercial/import", methods=["POST"])
    @login_required
    def agency_crm_commercial_import():
        raw_results = request.form.getlist("selected_results")
        selected = []
        for raw in raw_results:
            try:
                selected.append(json.loads(raw))
            except Exception:
                continue

        if not selected:
            flash("Select at least one scraped prospect to import.", "error")
            return redirect(url_for("agency_crm_commercial"))

        sequence_id = request.form.get("sequence_id", "").strip()
        try:
            sequence_id = int(sequence_id) if sequence_id else 0
        except ValueError:
            sequence_id = 0

        imported = 0
        updated = 0
        enrolled = 0
        skipped_no_email = 0

        for item in selected:
            business_name = (item.get("business_name") or item.get("contact_name") or "Commercial Prospect").strip()
            contact_name = (item.get("contact_name") or business_name).strip()
            emails = [value.strip().lower() for value in (item.get("emails") or []) if isinstance(value, str) and value.strip()]
            primary_email = emails[0] if emails else ""
            phone = (item.get("phone") or "").strip()
            website = (item.get("website") or "").strip()
            service_area = (item.get("service_area") or "").strip()
            source_query = (item.get("source_query") or "").strip()
            type_label = (item.get("prospect_type_label") or item.get("prospect_type") or "Commercial Property").strip()
            address = (item.get("address") or "").strip()
            score = int(item.get("score") or 0)
            audit_snapshot = item.get("audit_snapshot") if isinstance(item.get("audit_snapshot"), dict) else {}
            source_details = {
                "emails": emails,
                "address": address,
                "phone": phone,
                "website": website,
                "service_area": service_area,
                "prospect_type": item.get("prospect_type") or "",
                "prospect_type_label": type_label,
                "source_query": source_query,
                "rating": item.get("rating"),
                "review_count": item.get("review_count") or 0,
                "maps_url": item.get("maps_url") or "",
            }
            note_lines = [
                f"Commercial scraper import - {type_label}",
                f"Search: {source_query}" if source_query else "",
                f"Address: {address}" if address else "",
                f"Public emails: {', '.join(emails)}" if emails else "No public email found during scrape.",
            ]
            note_text = "\n".join(line for line in note_lines if line)
            from webapp.commercial_strategy import build_commercial_outreach_brief
            preview = {
                "name": contact_name,
                "email": primary_email,
                "phone": phone,
                "business_name": business_name,
                "website": website,
                "industry": type_label,
                "account_type": item.get("prospect_type") or "",
                "service_area": service_area,
                "stage": "new",
                "source": "commercial_scrape",
                "source_details_json": json.dumps(source_details),
                "audit_snapshot_json": json.dumps(audit_snapshot),
            }
            brief = build_commercial_outreach_brief(preview)

            existing = db.find_agency_prospect(email=primary_email, website=website, business_name=business_name)
            if existing:
                db.update_agency_prospect(
                    existing["id"],
                    name=contact_name or existing.get("name") or business_name,
                    email=primary_email or existing.get("email") or "",
                    phone=phone or existing.get("phone") or "",
                    business_name=business_name or existing.get("business_name") or "",
                    website=website or existing.get("website") or "",
                    industry=type_label,
                    service_area=service_area or existing.get("service_area") or "",
                    source="commercial_scrape",
                    score=max(score, int(existing.get("score") or 0)),
                    account_type=item.get("prospect_type") or existing.get("account_type") or "",
                    source_details_json=json.dumps(source_details),
                    outreach_angle=brief["outreach_angle"],
                    proposal_status=brief["proposal_readiness"]["status"],
                    pain_points_json=json.dumps(brief["pain_points"]),
                    next_action=(brief["next_actions"] or [""])[0],
                    audit_snapshot_json=json.dumps(audit_snapshot),
                )
                prospect_id = existing["id"]
                updated += 1
            else:
                prospect_id = db.create_agency_prospect(
                    name=contact_name,
                    email=primary_email,
                    phone=phone,
                    business_name=business_name,
                    website=website,
                    industry=type_label,
                    service_area=service_area,
                    source="commercial_scrape",
                    stage="new",
                    score=score,
                    notes=note_text,
                    account_type=item.get("prospect_type") or "",
                    source_details_json=json.dumps(source_details),
                    outreach_angle=brief["outreach_angle"],
                    proposal_status=brief["proposal_readiness"]["status"],
                    pain_points_json=json.dumps(brief["pain_points"]),
                    next_action=(brief["next_actions"] or [""])[0],
                    audit_snapshot_json=json.dumps(audit_snapshot),
                )
                imported += 1

            db.add_agency_prospect_note(prospect_id, note_text, note_type="system", created_by="admin")

            if sequence_id and primary_email:
                enrollment = db.enroll_in_drip(sequence_id, primary_email, contact_name, lead_source="commercial_scrape", lead_id=prospect_id)
                if enrollment:
                    enrolled += 1
                    db.add_agency_prospect_message(
                        prospect_id,
                        content=f"Enrolled in drip sequence #{sequence_id} from commercial prospecting workspace.",
                        direction="outbound",
                        channel="email",
                        subject="Drip enrollment",
                        status="queued",
                    )
            elif sequence_id and not primary_email:
                skipped_no_email += 1

        summary = f"Commercial import finished. {imported} new, {updated} updated"
        if sequence_id:
            summary += f", {enrolled} enrolled"
            if skipped_no_email:
                summary += f", {skipped_no_email} skipped for nurture because no public email was found"
        flash(summary + ".", "success")
        return redirect(url_for("agency_crm"))

    @app.route("/crm/prospect/new", methods=["POST"])
    @login_required
    def agency_crm_new_prospect():
        referral_code = request.form.get("referral_code", "").strip()
        partner_ref = db.resolve_partner_referral_code(referral_code) if referral_code else None
        attribution = {
            "source": "admin_prospect",
            "referral_code": (partner_ref or {}).get("code") or referral_code,
            "utm_source": request.form.get("utm_source", "").strip(),
            "utm_medium": request.form.get("utm_medium", "").strip(),
            "utm_campaign": request.form.get("utm_campaign", "").strip(),
        }
        pid = db.create_agency_prospect(
            name=request.form.get("name", "").strip(),
            email=request.form.get("email", "").strip(),
            phone=request.form.get("phone", "").strip(),
            business_name=request.form.get("business_name", "").strip(),
            website=request.form.get("website", "").strip(),
            industry=request.form.get("industry", "").strip(),
            service_area=request.form.get("service_area", "").strip(),
            source=request.form.get("source", "manual").strip(),
            monthly_budget=request.form.get("monthly_budget", "").strip(),
            notes=request.form.get("notes", "").strip(),
            partner_id=(partner_ref or {}).get("partner_id"),
            referral_code=attribution["referral_code"],
            utm_source=attribution["utm_source"],
            utm_medium=attribution["utm_medium"],
            utm_campaign=attribution["utm_campaign"],
            attribution_json=json.dumps(attribution, sort_keys=True),
        )
        if partner_ref:
            db.record_partner_attribution_event(
                partner_id=partner_ref["partner_id"],
                prospect_id=pid,
                referral_code=partner_ref["code"],
                source="admin_prospect",
                utm_source=attribution["utm_source"],
                utm_medium=attribution["utm_medium"],
                utm_campaign=attribution["utm_campaign"],
                metadata={"created_by": "admin"},
            )
        flash(f"Prospect created.", "success")
        return redirect(url_for("agency_crm_detail", prospect_id=pid))

    @app.route("/crm/prospect/<int:prospect_id>")
    @login_required
    def agency_crm_detail(prospect_id):
        prospect = db.get_agency_prospect(prospect_id)
        if not prospect:
            abort(404)
        notes = db.get_agency_prospect_notes(prospect_id)
        messages = db.get_agency_prospect_messages(prospect_id)
        # Billing data should be optional for CRM review flows.
        try:
            from webapp.stripe_billing import get_plans
            plans = get_plans(db)
        except Exception:
            plans = []

        # If converted to brand, get brand info
        brand = None
        if prospect.get("converted_brand_id"):
            brand = db.get_brand(prospect["converted_brand_id"])

        is_commercial = _is_commercial_agency_prospect(prospect)
        commercial_brief = _build_commercial_brief_preview(prospect) if is_commercial else None

        return render_template("crm/prospect_detail.html",
                               prospect=prospect, notes=notes,
                               messages=messages, plans=plans, brand=brand,
                               is_commercial=is_commercial,
                               commercial_brief=commercial_brief)

    @app.route("/crm/prospect/<int:prospect_id>/update", methods=["POST"])
    @login_required
    def agency_crm_update_prospect(prospect_id):
        prospect = db.get_agency_prospect(prospect_id)
        if not prospect:
            abort(404)
        fields = {}
        for key in ["name", "email", "phone", "business_name", "website",
                     "industry", "service_area", "stage", "score",
                     "monthly_budget", "notes", "assigned_to", "next_follow_up",
                     "account_type", "decision_maker_role", "property_count",
                     "current_vendor_status", "next_action"]:
            val = request.form.get(key)
            if val is not None:
                fields[key] = val.strip()
        if "score" in fields:
            try:
                fields["score"] = int(fields["score"])
            except ValueError:
                fields["score"] = 0
        blocked_stage = ""
        brief = None
        if _is_commercial_agency_prospect({**prospect, **fields}):
            brief = _build_commercial_brief_preview(prospect, fields)
            requested_stage = (fields.get("stage") or "").strip().lower()
            if requested_stage in {"proposal", "negotiation"} and brief["proposal_readiness"]["status"] != "ready":
                blocked_stage = requested_stage
                fields["stage"] = prospect.get("stage") or "qualified"
            fields["outreach_angle"] = brief["outreach_angle"]
            fields["proposal_status"] = brief["proposal_readiness"]["status"]
            fields["pain_points_json"] = json.dumps(brief["pain_points"])
            if not (fields.get("next_action") or "").strip():
                fields["next_action"] = (brief["next_actions"] or [""])[0]
        db.update_agency_prospect(prospect_id, **fields)
        if blocked_stage and brief:
            missing = ", ".join(brief["proposal_readiness"]["missing"][:4])
            flash(f"Saved prospect changes, but blocked move to {blocked_stage}. Missing: {missing}.", "error")
        else:
            flash("Prospect updated.", "success")
        return redirect(url_for("agency_crm_detail", prospect_id=prospect_id))

    @app.route("/crm/prospect/<int:prospect_id>/qualification", methods=["POST"])
    @login_required
    def agency_crm_save_commercial_qualification(prospect_id):
        prospect = db.get_agency_prospect(prospect_id)
        if not prospect:
            abort(404)

        from webapp.commercial_strategy import COMMERCIAL_QUALIFICATION_CORE_FIELDS, COMMERCIAL_QUALIFICATION_FIELDS

        try:
            answers = json.loads(prospect.get("qualification_answers_json") or "{}")
        except Exception:
            answers = {}

        fields = {}
        for field in COMMERCIAL_QUALIFICATION_CORE_FIELDS:
            fields[field["key"]] = request.form.get(field["key"], "").strip()
        for field in COMMERCIAL_QUALIFICATION_FIELDS:
            answers[field["key"]] = request.form.get(field["key"], "").strip()

        fields["qualification_answers_json"] = json.dumps(answers)
        brief = _build_commercial_brief_preview(prospect, fields)
        fields["outreach_angle"] = brief["outreach_angle"]
        fields["proposal_status"] = brief["proposal_readiness"]["status"]
        fields["pain_points_json"] = json.dumps(brief["pain_points"])
        fields["next_action"] = (brief["next_actions"] or [""])[0]
        if brief["proposal_readiness"]["status"] == "ready" and (prospect.get("stage") or "new") in {"new", "contacted"}:
            fields["stage"] = "qualified"

        db.update_agency_prospect(prospect_id, **fields)
        db.add_agency_prospect_note(
            prospect_id,
            f"Commercial qualification saved. {brief['qualification_summary']['complete_count']}/{brief['qualification_summary']['required_count']} required points confirmed.",
            note_type="system",
            created_by="admin",
        )
        flash("Commercial qualification saved.", "success")
        return redirect(url_for("agency_crm_detail", prospect_id=prospect_id))

    @app.route("/crm/prospect/<int:prospect_id>/commercial-refresh", methods=["POST"])
    @login_required
    def agency_crm_refresh_commercial_brief(prospect_id):
        prospect = db.get_agency_prospect(prospect_id)
        if not prospect:
            abort(404)

        website = (prospect.get("website") or "").strip()
        if not website:
            flash("Add a website before refreshing the commercial audit.", "error")
            return redirect(url_for("agency_crm_detail", prospect_id=prospect_id))

        from webapp.commercial_prospector import _extract_public_emails
        from webapp.competitor_intel import _scrape_website
        from webapp.commercial_strategy import build_commercial_outreach_brief

        try:
            source_details = json.loads(prospect.get("source_details_json") or "{}")
        except Exception:
            source_details = {}

        site_data = _scrape_website({"website": website}) or {}
        source_details.update({
            "emails": _extract_public_emails(website),
            "website": website,
            "service_area": prospect.get("service_area") or source_details.get("service_area") or "",
            "address": source_details.get("address") or "",
            "review_count": source_details.get("review_count") or 0,
            "rating": source_details.get("rating"),
        })

        preview = dict(prospect)
        preview["source_details_json"] = json.dumps(source_details)
        preview["audit_snapshot_json"] = json.dumps(site_data)
        brief = build_commercial_outreach_brief(preview)

        db.update_agency_prospect(
            prospect_id,
            source_details_json=json.dumps(source_details),
            audit_snapshot_json=json.dumps(site_data),
            outreach_angle=brief["outreach_angle"],
            proposal_status=brief["proposal_readiness"]["status"],
            pain_points_json=json.dumps(brief["pain_points"]),
            next_action=(brief["next_actions"] or [""])[0],
        )
        db.add_agency_prospect_note(
            prospect_id,
            f"Commercial audit refreshed. Angle: {brief['outreach_angle']}.",
            note_type="system",
            created_by="admin",
        )
        flash("Commercial brief refreshed.", "success")
        return redirect(url_for("agency_crm_detail", prospect_id=prospect_id))

    @app.route("/crm/prospect/<int:prospect_id>/note", methods=["POST"])
    @login_required
    def agency_crm_add_note(prospect_id):
        content = request.form.get("content", "").strip()
        if content:
            db.add_agency_prospect_note(prospect_id, content, note_type="note", created_by="admin")
        return redirect(url_for("agency_crm_detail", prospect_id=prospect_id))

    @app.route("/crm/prospect/<int:prospect_id>/stage", methods=["POST"])
    @login_required
    def agency_crm_update_stage(prospect_id):
        prospect = db.get_agency_prospect(prospect_id)
        if not prospect:
            abort(404)
        stage = request.form.get("stage", "").strip()
        if stage:
            updates = {"stage": stage}
            if _is_commercial_agency_prospect(prospect):
                brief = _build_commercial_brief_preview(prospect, {"stage": stage})
                if stage in {"proposal", "negotiation"} and brief["proposal_readiness"]["status"] != "ready":
                    db.update_agency_prospect(
                        prospect_id,
                        proposal_status=brief["proposal_readiness"]["status"],
                        pain_points_json=json.dumps(brief["pain_points"]),
                        next_action=(brief["next_actions"] or [""])[0],
                    )
                    missing = ", ".join(brief["proposal_readiness"]["missing"][:4])
                    flash(f"Cannot move to {stage} until qualification is complete. Missing: {missing}.", "error")
                    return redirect(request.referrer or url_for("agency_crm"))
                updates.update({
                    "outreach_angle": brief["outreach_angle"],
                    "proposal_status": brief["proposal_readiness"]["status"],
                    "pain_points_json": json.dumps(brief["pain_points"]),
                    "next_action": (brief["next_actions"] or [""])[0],
                })
            db.update_agency_prospect(prospect_id, **updates)
            db.add_agency_prospect_note(prospect_id, f"Stage changed to {stage}",
                                         note_type="system", created_by="admin")
        return redirect(request.referrer or url_for("agency_crm"))

    @app.route("/crm/prospect/<int:prospect_id>/delete", methods=["POST"])
    @login_required
    def agency_crm_delete_prospect(prospect_id):
        db.delete_agency_prospect(prospect_id)
        flash("Prospect deleted.", "success")
        return redirect(url_for("agency_crm"))

    @app.route("/crm/prospect/<int:prospect_id>/convert", methods=["POST"])
    @login_required
    def agency_crm_convert(prospect_id):
        """Convert a prospect into a brand and optionally create a Stripe subscription."""
        prospect = db.get_agency_prospect(prospect_id)
        if not prospect:
            abort(404)

        # Create the brand
        attribution = {}
        try:
            attribution = json.loads(prospect.get("attribution_json") or "{}")
        except Exception:
            attribution = {}
        if prospect.get("referral_code") and not attribution.get("referral_code"):
            attribution["referral_code"] = prospect.get("referral_code")
        attribution["source"] = attribution.get("source") or prospect.get("source") or "agency_crm"
        brand_id = db.create_brand({
            "slug": (prospect.get("business_name") or prospect["name"]).lower().replace(" ", "_")[:30],
            "display_name": prospect.get("business_name") or prospect["name"],
            "industry": prospect.get("industry", ""),
            "service_area": prospect.get("service_area", ""),
            "website": prospect.get("website", ""),
            "partner_id": prospect.get("partner_id"),
            "referral_code": prospect.get("referral_code") or "",
            "attribution": attribution,
            "partner_attributed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S") if prospect.get("partner_id") else "",
        })

        db.update_agency_prospect(prospect_id, stage="won", converted_brand_id=brand_id)
        db.add_agency_prospect_note(prospect_id, f"Converted to brand #{brand_id}",
                                     note_type="system", created_by="admin")
        if prospect.get("partner_id"):
            db.assign_partner_to_brand(
                prospect["partner_id"],
                brand_id,
                relationship="referred_by",
                access_level="reporting",
                billing_owner="brand",
                first_touch=True,
                attribution=attribution,
            )
            db.record_partner_attribution_event(
                partner_id=prospect["partner_id"],
                brand_id=brand_id,
                prospect_id=prospect_id,
                referral_code=prospect.get("referral_code") or "",
                source="prospect_conversion",
                metadata={"converted_brand_id": brand_id},
            )

        # Stripe: create customer + subscription if price selected
        price_id = request.form.get("price_id", "").strip()
        trial_days = request.form.get("trial_days", "").strip()
        if price_id:
            brand = db.get_brand(brand_id)
            from webapp.stripe_billing import create_subscription
            result = create_subscription(
                db, brand, price_id,
                trial_days=int(trial_days) if trial_days else None,
            )
            if result:
                db.update_brand_stripe(brand_id, onboarded_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                flash(f"Brand created and Stripe subscription started.", "success")
            else:
                flash(f"Brand created but Stripe subscription failed. Set it up manually.", "warning")
        else:
            flash(f"Prospect converted to brand.", "success")

        return redirect(url_for("brand_detail", brand_id=brand_id))

    # ── Stripe Billing Routes ──

    @app.route("/crm/billing")
    @login_required
    def agency_billing():
        revenue = db.get_stripe_revenue_summary()
        brands = db.get_all_brands()
        billing_brands = [b for b in brands if b.get("stripe_customer_id")]
        from webapp.stripe_billing import get_plans
        plans = get_plans(db)
        return render_template("crm/billing.html",
                               revenue=revenue, brands=billing_brands, plans=plans)

    @app.route("/crm/partners", methods=["GET", "POST"])
    @login_required
    def agency_partners():
        if request.method == "POST":
            partner_id = db.create_partner(
                partner_type=request.form.get("partner_type", "affiliate").strip(),
                status=request.form.get("status", "active").strip(),
                name=request.form.get("name", "").strip(),
                company_name=request.form.get("company_name", "").strip(),
                email=request.form.get("email", "").strip(),
                phone=request.form.get("phone", "").strip(),
                website=request.form.get("website", "").strip(),
                payout_email=request.form.get("payout_email", "").strip(),
                notes=request.form.get("notes", "").strip(),
                referral_code=request.form.get("referral_code", "").strip(),
            )
            portal_password = request.form.get("portal_password", "").strip()
            if request.form.get("email", "").strip() and portal_password:
                db.create_partner_user(
                    partner_id,
                    request.form.get("email", "").strip(),
                    portal_password,
                    request.form.get("name", "").strip() or request.form.get("company_name", "").strip(),
                )
            if not request.form.get("referral_code", "").strip():
                db.create_partner_referral_code(partner_id, f"partner-{partner_id}")
            flash("Partner created.", "success")
            return redirect(url_for("agency_partners"))

        partners = db.get_partners()
        codes_by_partner = {}
        for code in db.get_partner_referral_codes():
            codes_by_partner.setdefault(code["partner_id"], []).append(code)
        return render_template(
            "crm/partners.html",
            partners=partners,
            codes_by_partner=codes_by_partner,
            summary=db.get_partner_program_summary(),
            assignments=db.get_partner_brand_assignments(active_only=True),
            commissions=db.get_partner_commissions(limit=100),
            payout_batches=db.get_partner_payout_batches(limit=20),
            plans=db.get_commission_plans(active_only=False),
            brands=db.get_all_brands(),
            partner_users_by_partner={p["id"]: db.get_partner_users(p["id"]) for p in partners},
        )

    @app.route("/crm/partners/<int:partner_id>/update", methods=["POST"])
    @login_required
    def agency_partner_update(partner_id):
        db.update_partner(
            partner_id,
            partner_type=request.form.get("partner_type", "affiliate").strip(),
            status=request.form.get("status", "active").strip(),
            name=request.form.get("name", "").strip(),
            company_name=request.form.get("company_name", "").strip(),
            email=request.form.get("email", "").strip(),
            phone=request.form.get("phone", "").strip(),
            website=request.form.get("website", "").strip(),
            payout_email=request.form.get("payout_email", "").strip(),
            tax_status=request.form.get("tax_status", "not_collected").strip(),
            kyc_status=request.form.get("kyc_status", "not_started").strip(),
            notes=request.form.get("notes", "").strip(),
        )
        code = request.form.get("new_referral_code", "").strip()
        if code:
            db.create_partner_referral_code(partner_id, code)
        portal_password = request.form.get("portal_password", "").strip()
        portal_email = request.form.get("portal_email", "").strip() or request.form.get("email", "").strip()
        if portal_password and portal_email:
            db.create_partner_user(
                partner_id,
                portal_email,
                portal_password,
                request.form.get("name", "").strip(),
            )
        flash("Partner updated.", "success")
        return redirect(url_for("agency_partners"))

    @app.route("/crm/partners/assign-brand", methods=["POST"])
    @login_required
    def agency_partner_assign_brand():
        try:
            partner_id = int(request.form.get("partner_id") or 0)
            brand_id = int(request.form.get("brand_id") or 0)
        except ValueError:
            partner_id = brand_id = 0
        if not partner_id or not brand_id:
            flash("Select a partner and brand.", "error")
            return redirect(url_for("agency_partners"))
        db.assign_partner_to_brand(
            partner_id,
            brand_id,
            relationship=request.form.get("relationship", "referred_by").strip(),
            access_level=request.form.get("access_level", "reporting").strip(),
            billing_owner=request.form.get("billing_owner", "brand").strip(),
            attribution={"source": "admin_assignment", "referral_code": request.form.get("referral_code", "").strip()},
        )
        db.record_partner_attribution_event(
            partner_id=partner_id,
            brand_id=brand_id,
            referral_code=request.form.get("referral_code", "").strip(),
            source="admin_assignment",
        )
        flash("Partner assigned to brand.", "success")
        return redirect(url_for("agency_partners"))

    @app.route("/crm/partners/payout-batch", methods=["POST"])
    @login_required
    def agency_partner_create_payout_batch():
        batch_id = db.create_partner_payout_batch(notes=request.form.get("notes", "").strip())
        if batch_id:
            flash(f"Payout batch #{batch_id} created.", "success")
        else:
            flash("No eligible commissions are ready for payout.", "info")
        return redirect(url_for("agency_partners"))

    @app.route("/crm/brand/<int:brand_id>/stripe/create-customer", methods=["POST"])
    @login_required
    def stripe_create_customer(brand_id):
        brand = db.get_brand(brand_id)
        if not brand:
            abort(404)
        from webapp.stripe_billing import create_customer
        cid = create_customer(db, brand)
        if cid:
            flash(f"Stripe customer created: {cid}", "success")
        else:
            flash("Failed to create Stripe customer. Check your API key in Settings.", "error")
        return redirect(request.referrer or url_for("brand_detail", brand_id=brand_id))

    @app.route("/crm/brand/<int:brand_id>/stripe/subscribe", methods=["POST"])
    @login_required
    def stripe_subscribe(brand_id):
        brand = db.get_brand(brand_id)
        if not brand:
            abort(404)
        price_id = request.form.get("price_id", "").strip()
        trial_days = request.form.get("trial_days", "").strip()
        if not price_id:
            flash("Select a plan.", "error")
            return redirect(request.referrer or url_for("brand_detail", brand_id=brand_id))

        from webapp.stripe_billing import create_subscription
        result = create_subscription(db, brand, price_id,
                                      trial_days=int(trial_days) if trial_days else None)
        if result:
            flash(f"Subscription created: {result['status']}", "success")
        else:
            flash("Subscription creation failed.", "error")
        return redirect(request.referrer or url_for("brand_detail", brand_id=brand_id))

    @app.route("/crm/brand/<int:brand_id>/stripe/change-plan", methods=["POST"])
    @login_required
    def stripe_change_plan(brand_id):
        brand = db.get_brand(brand_id)
        if not brand:
            abort(404)
        new_price_id = request.form.get("price_id", "").strip()
        if not new_price_id:
            flash("Select a plan.", "error")
            return redirect(request.referrer)
        from webapp.stripe_billing import update_subscription
        result = update_subscription(db, brand, new_price_id)
        if result:
            flash(f"Plan updated. New MRR: ${result['mrr']:.0f}", "success")
        else:
            flash("Plan change failed.", "error")
        return redirect(request.referrer or url_for("brand_detail", brand_id=brand_id))

    @app.route("/crm/brand/<int:brand_id>/stripe/cancel", methods=["POST"])
    @login_required
    def stripe_cancel(brand_id):
        brand = db.get_brand(brand_id)
        if not brand:
            abort(404)
        immediate = request.form.get("immediate") == "1"
        from webapp.stripe_billing import cancel_subscription
        result = cancel_subscription(db, brand, at_period_end=not immediate)
        if result:
            flash(f"Subscription {'canceled' if immediate else 'set to cancel at period end'}.", "success")
        else:
            flash("Cancel failed.", "error")
        return redirect(request.referrer or url_for("brand_detail", brand_id=brand_id))

    @app.route("/webhooks/stripe", methods=["POST"])
    def stripe_webhook():
        """Handle incoming Stripe webhook events. No login required."""
        from webapp.stripe_billing import handle_webhook
        payload = request.get_data(as_text=True)
        sig = request.headers.get("Stripe-Signature", "")
        success, msg = handle_webhook(db, payload, sig)
        if success:
            return msg, 200
        return msg, 400

    # ── Drip Campaigns Admin ──

    @app.route("/drip")
    @login_required
    def drip_dashboard():
        stats = db.get_drip_stats()
        sequences = db.get_drip_sequences()
        leads = db.get_assessment_leads(limit=50)
        signup_leads = db.get_signup_leads(limit=50)
        enrollments = db.get_drip_enrollments(limit=100)
        recent_sends = db.get_drip_sends(limit=50)
        return render_template(
            "drip_campaigns.html",
            stats=stats, sequences=sequences, leads=leads,
            signup_leads=signup_leads, enrollments=enrollments,
            recent_sends=recent_sends,
        )

    @app.route("/drip/sequence/new", methods=["POST"])
    @login_required
    def drip_sequence_create():
        name = request.form.get("name", "").strip()
        description = request.form.get("description", "").strip()
        trigger = request.form.get("trigger", "assessment")
        if not name:
            flash("Sequence name is required.", "error")
            return redirect(url_for("drip_dashboard"))
        seq_id = db.create_drip_sequence(name, description, trigger)
        flash(f"Sequence '{name}' created.", "success")
        return redirect(url_for("drip_sequence_detail", seq_id=seq_id))

    @app.route("/drip/sequence/<int:seq_id>")
    @login_required
    def drip_sequence_detail(seq_id):
        seq = db.get_drip_sequence(seq_id)
        if not seq:
            flash("Sequence not found.", "error")
            return redirect(url_for("drip_dashboard"))
        steps = db.get_drip_steps(seq_id)
        enrollments = db.get_drip_enrollments(sequence_id=seq_id)
        return render_template("drip_sequence_detail.html", seq=seq, steps=steps, enrollments=enrollments)

    @app.route("/drip/sequence/<int:seq_id>/update", methods=["POST"])
    @login_required
    def drip_sequence_update(seq_id):
        name = request.form.get("name", "").strip()
        description = request.form.get("description", "").strip()
        is_active = request.form.get("is_active") == "1"
        db.update_drip_sequence(seq_id, name, description, is_active)
        flash("Sequence updated.", "success")
        return redirect(url_for("drip_sequence_detail", seq_id=seq_id))

    @app.route("/drip/sequence/<int:seq_id>/delete", methods=["POST"])
    @login_required
    def drip_sequence_delete(seq_id):
        db.delete_drip_sequence(seq_id)
        flash("Sequence deleted.", "success")
        return redirect(url_for("drip_dashboard"))

    @app.route("/drip/sequence/<int:seq_id>/step/new", methods=["POST"])
    @login_required
    def drip_step_create(seq_id):
        step_order = int(request.form.get("step_order", 1))
        delay_days = int(request.form.get("delay_days", 1))
        subject = request.form.get("subject", "").strip()
        body_html = request.form.get("body_html", "").strip()
        body_text = request.form.get("body_text", "").strip()
        if not subject or not body_html:
            flash("Subject and HTML body are required.", "error")
            return redirect(url_for("drip_sequence_detail", seq_id=seq_id))
        db.create_drip_step(seq_id, step_order, delay_days, subject, body_html, body_text)
        flash(f"Step {step_order} added.", "success")
        return redirect(url_for("drip_sequence_detail", seq_id=seq_id))

    @app.route("/drip/step/<int:step_id>/update", methods=["POST"])
    @login_required
    def drip_step_update(step_id):
        step = db.get_drip_step(step_id)
        if not step:
            flash("Step not found.", "error")
            return redirect(url_for("drip_dashboard"))
        step_order = int(request.form.get("step_order", step["step_order"]))
        delay_days = int(request.form.get("delay_days", step["delay_days"]))
        subject = request.form.get("subject", "").strip()
        body_html = request.form.get("body_html", "").strip()
        body_text = request.form.get("body_text", "").strip()
        db.update_drip_step(step_id, step_order, delay_days, subject, body_html, body_text)
        flash("Step updated.", "success")
        return redirect(url_for("drip_sequence_detail", seq_id=step["sequence_id"]))

    @app.route("/drip/step/<int:step_id>/delete", methods=["POST"])
    @login_required
    def drip_step_delete(step_id):
        step = db.get_drip_step(step_id)
        if not step:
            flash("Step not found.", "error")
            return redirect(url_for("drip_dashboard"))
        seq_id = step["sequence_id"]
        db.delete_drip_step(step_id)
        flash("Step deleted.", "success")
        return redirect(url_for("drip_sequence_detail", seq_id=seq_id))

    @app.route("/drip/enrollment/<int:enrollment_id>/cancel", methods=["POST"])
    @login_required
    def drip_enrollment_cancel(enrollment_id):
        reason = request.form.get("reason", "unsubscribed")
        db.complete_drip_enrollment(enrollment_id, reason)
        flash("Enrollment cancelled.", "success")
        return redirect(url_for("drip_dashboard"))

    @app.route("/drip/process", methods=["POST"])
    @login_required
    def drip_process():
        """Manually trigger drip processing (also called by cron/scheduler)."""
        from webapp.drip_engine import process_pending_drips
        sent, failed = process_pending_drips(app.config, db)
        flash(f"Drip processed: {sent} sent, {failed} failed.", "success" if failed == 0 else "warning")
        return redirect(url_for("drip_dashboard"))

    @app.route("/drip/enroll", methods=["POST"])
    @login_required
    def drip_enroll_manual():
        """Manually enroll a lead into a sequence."""
        seq_id = int(request.form.get("sequence_id", 0))
        email = request.form.get("email", "").strip().lower()
        name = request.form.get("name", "").strip()
        if not seq_id or not email:
            flash("Sequence and email are required.", "error")
            return redirect(url_for("drip_dashboard"))
        result = db.enroll_in_drip(seq_id, email, name, lead_source="manual")
        if result:
            flash(f"Enrolled {email} in drip.", "success")
        else:
            flash(f"{email} is already active in this sequence.", "info")
        return redirect(url_for("drip_dashboard"))

    # ── Beta Testers Admin ──

    def _feedback_ai_model():
        return (
            db.get_setting("openai_model", "").strip()
            or os.environ.get("OPENAI_MODEL", "").strip()
            or app.config.get("OPENAI_MODEL")
            or "gpt-4o-mini"
        )

    def _feedback_ai_key():
        return db.get_setting("openai_api_key", "").strip() or os.environ.get("OPENAI_API_KEY", "").strip()

    def _feedback_ai_scope():
        scope = (request.form.get("scope") or "new").strip().lower()
        category = (request.form.get("category") or "").strip().lower() or None
        ids_raw = (request.form.get("feedback_ids") or "").strip()
        selected_ids = [part.strip() for part in ids_raw.split(",") if part.strip()]

        filters = {}
        scope_label = "New feedback"
        items = []

        if selected_ids:
            items = db.get_beta_feedback_by_ids(selected_ids)
            scope = "selected"
            scope_label = f"Selected feedback ({len(items)})"
            filters["feedback_ids"] = [int(fid) for fid in selected_ids]
        elif scope == "all":
            items = db.get_beta_feedback_filtered(category=category, limit=100)
            scope_label = "Recent feedback"
        else:
            items = db.get_beta_feedback_filtered(status="new", category=category, limit=100)
            scope = "new"
            scope_label = "New feedback"

        if category:
            filters["category"] = category
            scope_label += f" - {category.replace('_', ' ').title()}"

        return scope, scope_label, filters, items

    def _run_feedback_ai_review(feedback_items, api_key, model):
        payload = []
        for item in feedback_items:
            payload.append({
                "id": item.get("id"),
                "brand_name": item.get("brand_name") or "",
                "tester_name": item.get("tester_name") or "",
                "tester_email": item.get("tester_email") or "",
                "category": item.get("category") or "general",
                "rating": int(item.get("rating") or 0),
                "status": item.get("status") or "new",
                "page": item.get("page") or "",
                "message": item.get("message") or "",
                "created_at": item.get("created_at") or "",
            })

        prompt = (
            "You are a product operations assistant helping a SaaS team triage customer feedback. "
            "You will receive a JSON array of feedback items. Return ONLY valid JSON with these top-level keys: "
            "summary, dev_plan, reply_drafts.\n\n"
            "Rules:\n"
            "- Do not use em dashes.\n"
            "- Be concrete, concise, and evidence-based.\n"
            "- Never promise a fix date in customer replies.\n"
            "- Keep customer replies warm, brief, and non-technical.\n"
            "- recommended_status must be one of: new, reviewed, resolved.\n"
            "- reply_drafts must include one entry for every feedback item id.\n\n"
            "summary must include: executive_summary, counts, top_themes, priority_recommendations.\n"
            "counts must include: total_feedback, bugs, feature_requests, ui_ux, likes, dislikes, general.\n"
            "top_themes must be an array of objects with: title, category, frequency, why_it_matters, evidence_ids.\n"
            "priority_recommendations must be an array of objects with: title, reason, urgency.\n\n"
            "dev_plan must include: title, objective, likely_areas, implementation_steps, qa_checks, rollout_order, customer_comms.\n"
            "likely_areas and rollout_order must be arrays of strings. implementation_steps and qa_checks must be arrays of strings.\n"
            "customer_comms must be an array of short bullets.\n\n"
            "reply_drafts must be an array of objects with: feedback_id, reply_subject, reply_draft, internal_note, recommended_status, confidence, needs_manual_review.\n"
            "confidence should be a number between 0 and 1.\n\n"
            f"Feedback items JSON:\n{json.dumps(payload)}"
        )

        import openai

        client = openai.OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You convert product feedback into structured summaries, implementation plans, and customer reply drafts."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
            response_format={"type": "json_object"},
        )
        raw = (response.choices[0].message.content or "{}").strip()
        result = json.loads(raw)
        return {
            "summary": result.get("summary") or {},
            "dev_plan": result.get("dev_plan") or {},
            "reply_drafts": result.get("reply_drafts") or [],
        }

    @app.route("/beta")
    @login_required
    def beta_dashboard():
        stats = db.get_beta_stats()
        testers = db.get_beta_testers()
        feedback = db.get_beta_feedback(limit=50)
        drafts = db.get_feedback_ai_drafts([item["id"] for item in feedback])
        drafts_by_feedback = {item["feedback_id"]: item for item in drafts}
        fb_summary = db.get_beta_feedback_summary()
        themes = db.get_feedback_themes()
        latest_feedback_ai_run = db.get_latest_feedback_ai_run()
        considerations = db.get_upgrade_considerations()
        upgrade_stats = db.get_upgrade_stats()
        return render_template(
            "beta_admin.html",
            stats=stats,
            testers=testers,
            feedback=feedback,
            drafts_by_feedback=drafts_by_feedback,
            fb_summary=fb_summary,
            themes=themes,
            latest_feedback_ai_run=latest_feedback_ai_run,
            considerations=considerations,
            upgrade_stats=upgrade_stats,
        )

    @app.route("/beta/feedback/ai/generate", methods=["POST"])
    @login_required
    def beta_feedback_ai_generate():
        is_ajax = request.is_json or request.headers.get("X-Requested-With") == "XMLHttpRequest"
        api_key = _feedback_ai_key()
        if not api_key:
            message = "OpenAI is not configured. Add your OpenAI API key in Settings."
            if is_ajax:
                return jsonify({"ok": False, "message": message}), 400
            flash(message, "error")
            return redirect(url_for("beta_dashboard"))

        scope_type, scope_label, filters, items = _feedback_ai_scope()
        if not items:
            message = "No feedback matched that AI review scope."
            if is_ajax:
                return jsonify({"ok": False, "message": message}), 400
            flash(message, "warning")
            return redirect(url_for("beta_dashboard"))

        try:
            model = _feedback_ai_model()
            result = _run_feedback_ai_review(items, api_key, model)
            run_id = db.create_feedback_ai_run({
                "scope_type": scope_type,
                "scope_label": scope_label,
                "feedback_ids": [item["id"] for item in items],
                "filters": filters,
                "model": model,
                "prompt_version": "feedback-review-v1",
                "summary": result["summary"],
                "dev_plan": result["dev_plan"],
                "created_by": session.get("user_id"),
            })

            feedback_ids = {item["id"] for item in items}
            draft_count = 0
            for draft in result["reply_drafts"]:
                feedback_id = int(draft.get("feedback_id") or 0)
                if feedback_id not in feedback_ids:
                    continue
                db.save_feedback_ai_draft({
                    "feedback_id": feedback_id,
                    "run_id": run_id,
                    "reply_subject": draft.get("reply_subject") or "Reply to your GroMore feedback",
                    "reply_draft": draft.get("reply_draft") or "",
                    "internal_note": draft.get("internal_note") or "",
                    "recommended_status": draft.get("recommended_status") or "reviewed",
                    "confidence": draft.get("confidence") or 0,
                    "needs_manual_review": bool(draft.get("needs_manual_review")),
                })
                draft_count += 1

            message = f"AI review generated for {len(items)} feedback item(s), with {draft_count} reply drafts."
            if is_ajax:
                return jsonify({
                    "ok": True,
                    "message": message,
                    "run_id": run_id,
                    "item_count": len(items),
                    "draft_count": draft_count,
                    "redirect_url": url_for("beta_dashboard") + "#tab-feedback",
                })
            flash(message, "success")
        except Exception as exc:
            message = f"AI feedback review failed: {exc}"
            if is_ajax:
                return jsonify({"ok": False, "message": message}), 500
            flash(message, "error")

        return redirect(url_for("beta_dashboard") + "#tab-feedback")

    @app.route("/beta/feedback/<int:feedback_id>/draft/use", methods=["POST"])
    @login_required
    def beta_feedback_use_draft(feedback_id):
        draft = db.get_feedback_ai_draft(feedback_id)
        if not draft:
            flash("AI draft not found for that feedback item.", "warning")
            return redirect(url_for("beta_dashboard"))

        status = draft.get("recommended_status") or "reviewed"
        admin_response = (draft.get("reply_draft") or "").strip()
        db.update_beta_feedback_status(feedback_id, status, admin_response)
        db.approve_feedback_ai_draft(feedback_id, session.get("user_id"))
        flash("AI draft copied into the feedback reply.", "success")
        return redirect(url_for("beta_dashboard") + "#tab-feedback")

    @app.route("/beta/feedback/<int:feedback_id>/draft/send", methods=["POST"])
    @login_required
    def beta_feedback_send_draft(feedback_id):
        draft = db.get_feedback_ai_draft(feedback_id)
        if not draft:
            flash("AI draft not found for that feedback item.", "warning")
            return redirect(url_for("beta_dashboard"))

        if draft.get("sent_at"):
            flash("That feedback reply was already sent.", "info")
            return redirect(url_for("beta_dashboard") + "#tab-feedback")

        feedback_items = db.get_beta_feedback_by_ids([feedback_id])
        if not feedback_items:
            abort(404)
        item = feedback_items[0]
        recipient = (item.get("tester_email") or "").strip()
        if not recipient:
            flash("That feedback item has no reply email available.", "warning")
            return redirect(url_for("beta_dashboard") + "#tab-feedback")

        status = draft.get("recommended_status") or "reviewed"
        reply_body = (draft.get("reply_draft") or "").strip()
        subject = (draft.get("reply_subject") or "Reply to your GroMore feedback").strip()

        try:
            from webapp.email_sender import send_simple_email

            send_simple_email(app.config, recipient, subject, reply_body)
            db.update_beta_feedback_status(feedback_id, status, reply_body)
            db.approve_feedback_ai_draft(feedback_id, session.get("user_id"))
            db.mark_feedback_ai_draft_sent(feedback_id)
            flash(f"Feedback reply sent to {recipient}.", "success")
        except Exception as exc:
            db.mark_feedback_ai_draft_sent(feedback_id, str(exc)[:300])
            flash(f"Feedback reply failed: {exc}", "warning")

        return redirect(url_for("beta_dashboard") + "#tab-feedback")

    @app.route("/beta/broadcast", methods=["POST"])
    @login_required
    def beta_broadcast_email():
        audience = request.form.get("audience", "")
        single_email = request.form.get("single_email", "")
        subject = request.form.get("subject", "").strip()
        message = request.form.get("message", "").strip()

        if not subject or not message:
            flash("Subject and message are required.", "error")
            return redirect(url_for("beta_dashboard"))

        recipients = _collect_broadcast_recipients(audience, single_email=single_email)
        if not recipients:
            flash("No recipients matched that audience.", "warning")
            return redirect(url_for("beta_dashboard"))

        try:
            from webapp.email_sender import send_bulk_email

            # Log broadcast and get per-recipient tracking tokens
            broadcast_id, tokens = db.create_email_broadcast(
                subject, message, audience, session.get("user_name", "admin"), recipients
            )
            token_map = {t["email"]: t["token"] for t in tokens}
            base_url = app.config.get("APP_URL", request.host_url.rstrip("/"))

            sent_count = send_bulk_email(app.config, recipients, subject, message,
                                         tracking_base_url=base_url, token_map=token_map)
            flash(f"Broadcast sent to {sent_count} recipient(s).", "success")
        except Exception as exc:
            flash(f"Broadcast failed: {exc}", "warning")

        return redirect(url_for("beta_dashboard"))

    @app.route("/t/<token>.gif")
    def email_tracking_pixel(token):
        """1x1 transparent GIF that records email opens."""
        db.record_email_open(token)
        # 1x1 transparent GIF
        import base64
        gif = base64.b64decode("R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7")
        return app.response_class(gif, mimetype="image/gif",
                                  headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})

    @app.route("/beta/broadcasts")
    @login_required
    def beta_broadcast_history():
        broadcasts = db.get_email_broadcasts(limit=50)
        return jsonify(broadcasts=broadcasts)

    @app.route("/beta/broadcasts/<int:broadcast_id>/recipients")
    @login_required
    def beta_broadcast_recipients(broadcast_id):
        recipients = db.get_email_broadcast_recipients(broadcast_id)
        return jsonify(recipients=recipients)

    @app.route("/beta/approve/<int:tester_id>", methods=["POST"])
    @login_required
    def beta_approve(tester_id):
        tester = db.get_beta_tester(tester_id)
        if not tester:
            abort(404)

        if tester["status"] == "approved":
            flash(f"{tester['name']} is already approved.", "warning")
            return redirect(url_for("beta_dashboard"))

        import secrets as _secrets
        import re as _re
        temp_password = _secrets.token_urlsafe(10)

        # Create brand for the tester
        slug = _re.sub(r'[^a-z0-9]+', '_', (tester["business_name"] or tester["name"]).lower()).strip('_')

        # Ensure unique slug
        existing = db.get_brand_by_slug(slug)
        if existing:
            slug = slug + "_" + str(tester_id)

        try:
            brand_id = db.create_brand({
                "slug": slug,
                "display_name": tester["business_name"] or tester["name"],
                "industry": tester.get("industry") or "general",
                "website": tester.get("website") or "",
            })
        except Exception as e:
            flash(f"Failed to create brand: {e}", "error")
            return redirect(url_for("beta_dashboard"))

        # Create client user
        client_user_id = db.create_client_user(brand_id, tester["email"], temp_password, tester["name"])
        if not client_user_id:
            flash(f"Failed to create client user - email '{tester['email']}' may already exist.", "error")
            return redirect(url_for("beta_dashboard"))

        # Update tester status - store temp password for activation later
        db.update_beta_tester_status(
            tester_id, "approved",
            brand_id=brand_id, client_user_id=client_user_id,
            temp_password=temp_password,
        )

        flash(f"Approved {tester['name']}. Add them as testers on FB/Google, then click Send Active.", "success")
        return redirect(url_for("beta_dashboard"))

    @app.route("/beta/activate/<int:tester_id>", methods=["POST"])
    @login_required
    def beta_activate(tester_id):
        tester = db.get_beta_tester(tester_id)
        if not tester:
            abort(404)
        if tester["status"] != "approved":
            flash("Tester must be approved first.", "error")
            return redirect(url_for("beta_dashboard"))

        temp_password = tester.get("temp_password", "")
        if not temp_password:
            # Generate a new password if the stored one is missing
            import secrets as _secrets
            temp_password = _secrets.token_urlsafe(10)
            from werkzeug.security import generate_password_hash
            conn = db._conn()
            conn.execute(
                "UPDATE client_users SET password_hash = ? WHERE id = ?",
                (generate_password_hash(temp_password), tester["client_user_id"]),
            )
            conn.commit()
            conn.close()

        db.update_beta_tester_status(
            tester_id, "approved",
            activated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )

        try:
            from webapp.email_sender import send_beta_activation_email
            login_url = app.config.get("APP_URL", request.host_url.rstrip("/")) + "/client/login"
            send_beta_activation_email(app.config, tester, temp_password, login_url)
            flash(f"Activation email sent to {tester['name']}.", "success")
        except Exception as e:
            flash(f"Activation failed: {e}", "warning")

        return redirect(url_for("beta_dashboard"))

    @app.route("/beta/remove/<int:tester_id>", methods=["POST"])
    @login_required
    def beta_remove(tester_id):
        tester = db.get_beta_tester(tester_id)
        if not tester:
            abort(404)
        db.deactivate_beta_tester(tester_id)
        flash(f"Removed {tester['name']} from the beta program.", "info")
        return redirect(url_for("beta_dashboard"))

    @app.route("/beta/reject/<int:tester_id>", methods=["POST"])
    @login_required
    def beta_reject(tester_id):
        tester = db.get_beta_tester(tester_id)
        if not tester:
            abort(404)
        db.update_beta_tester_status(tester_id, "rejected")
        flash(f"Rejected {tester['name']}.", "info")
        return redirect(url_for("beta_dashboard"))

    @app.route("/beta/feedback/<int:feedback_id>/respond", methods=["POST"])
    @login_required
    def beta_feedback_respond(feedback_id):
        status = request.form.get("status", "reviewed")
        admin_response = request.form.get("admin_response", "").strip()
        db.update_beta_feedback_status(feedback_id, status, admin_response)
        flash("Feedback updated.", "success")
        return redirect(url_for("beta_dashboard"))

    @app.route("/beta/feedback/<int:feedback_id>/promote", methods=["POST"])
    @login_required
    def beta_feedback_promote(feedback_id):
        """Promote a piece of feedback into the upgrade considerations list."""
        fb = db.get_beta_feedback(limit=200)
        item = next((f for f in fb if f["id"] == feedback_id), None)
        if not item:
            abort(404)
        title = request.form.get("title", "").strip() or item["message"][:80]
        db.create_upgrade_consideration({
            "title": title,
            "description": item["message"],
            "category": item.get("category", "feature"),
            "source_feedback_ids": str(feedback_id),
            "request_count": 1,
        })
        db.update_beta_feedback_status(feedback_id, "promoted", "Promoted to upgrade consideration.")
        flash(f"Added to upgrade considerations: {title[:50]}", "success")
        return redirect(url_for("beta_dashboard"))

    @app.route("/beta/promote-theme", methods=["POST"])
    @login_required
    def beta_promote_theme():
        """Promote a whole theme (group of similar feedback) into considerations."""
        title = request.form.get("title", "").strip()
        description = request.form.get("description", "").strip()
        feedback_ids = request.form.get("feedback_ids", "").strip()
        count = len(feedback_ids.split(",")) if feedback_ids else 1
        db.create_upgrade_consideration({
            "title": title,
            "description": description,
            "category": request.form.get("category", "feature"),
            "source_feedback_ids": feedback_ids,
            "request_count": count,
        })
        flash(f"Theme promoted: {title[:50]} ({count} requests)", "success")
        return redirect(url_for("beta_dashboard"))

    @app.route("/beta/consideration/<int:cid>/update", methods=["POST"])
    @login_required
    def beta_consideration_update(cid):
        data = {
            "title": request.form.get("title", "").strip(),
            "description": request.form.get("description", "").strip(),
            "category": request.form.get("category", "feature"),
            "feasibility": request.form.get("feasibility", "unknown"),
            "safety_risk": request.form.get("safety_risk", "low"),
            "priority": request.form.get("priority", "medium"),
            "status": request.form.get("status", "proposed"),
            "decision_notes": request.form.get("decision_notes", "").strip(),
            "request_count": int(request.form.get("request_count", 1) or 1),
        }
        db.update_upgrade_consideration(cid, data)
        flash("Consideration updated.", "success")
        return redirect(url_for("beta_dashboard"))

    @app.route("/beta/consideration/<int:cid>/delete", methods=["POST"])
    @login_required
    def beta_consideration_delete(cid):
        db.delete_upgrade_consideration(cid)
        flash("Consideration removed.", "info")
        return redirect(url_for("beta_dashboard"))

    # ── Site Builder Admin ──

    @app.route("/site-builder-admin")
    @login_required
    def site_builder_admin():
        from webapp.font_catalog import GOOGLE_FONT_CHOICES

        db.ensure_default_site_builder_kits()
        tab = request.args.get("tab", "templates")
        templates = db.get_sb_templates(active_only=False)
        themes = db.get_sb_themes(active_only=False)
        site_templates = db.get_sb_site_templates(active_only=False)
        overrides = db.get_sb_prompt_overrides()
        categories = db.get_sb_image_categories()
        images = db.get_sb_images(limit=60)
        image_count = db.count_sb_images()
        # Group overrides by page_type for easier template rendering
        overrides_by_page = {}
        for o in overrides:
            overrides_by_page.setdefault(o["page_type"], []).append(o)
        # Available page types from site_builder
        page_types = [
            "home", "about", "services", "service_detail",
            "service_area", "contact", "faq", "testimonials",
            "global_rules", "system_message",
        ]
        return render_template(
            "site_builder_admin.html",
            tab=tab,
            templates=templates,
            themes=themes,
            site_templates=site_templates,
            overrides=overrides,
            overrides_by_page=overrides_by_page,
            categories=categories,
            images=images,
            image_count=image_count,
            page_types=page_types,
            google_font_choices=GOOGLE_FONT_CHOICES,
        )

    @app.route("/site-builder-admin/generate")
    @login_required
    def site_builder_admin_generate():
        db.ensure_default_site_builder_kits()
        brand_id = int(request.args.get("brand_id") or 0)
        brand = db.get_brand(brand_id) if brand_id else {}
        if brand_id and not brand:
            flash("Selected brand was not found.", "warning")
            return redirect(url_for("site_builder_admin_generate"))
        return _render_admin_site_builder(mode="landing", brand=brand)

    @app.route("/site-builder-admin/generate", methods=["POST"])
    @login_required
    def site_builder_admin_generate_post():
        is_ajax = request.is_json or request.headers.get("X-Requested-With") == "XMLHttpRequest"

        try:
            brand, brand_id = _site_builder_admin_upsert_brand(request.form)
        except ValueError as exc:
            if is_ajax:
                return jsonify(ok=False, error=str(exc)), 400
            flash(str(exc), "error")
            return redirect(url_for("site_builder_admin_generate"))

        from webapp.site_builder import (
            build_brand_context,
            build_site_blueprint,
            generate_page_content,
            assemble_page,
        )

        api_key = client_portal_module._get_openai_api_key(brand)
        if not api_key:
            msg = "OpenAI API key not configured. Add it in Settings before generating a site."
            if is_ajax:
                return jsonify(ok=False, error=msg), 400
            flash(msg, "error")
            return redirect(url_for("site_builder_admin_generate", brand_id=brand_id))

        model = client_portal_module._pick_ai_model(brand, "analysis")
        services = (request.form.get("services") or "").strip() or None
        areas = (request.form.get("areas") or "").strip() or None

        intake = {
            "business_name": (request.form.get("business_name") or "").strip(),
            "industry": (request.form.get("industry") or "").strip(),
            "website": (request.form.get("website") or "").strip(),
            "phone": (request.form.get("phone") or "").strip(),
            "email": (request.form.get("email") or "").strip(),
            "address": (request.form.get("address") or "").strip(),
            "brand_voice": (request.form.get("brand_voice") or "").strip(),
            "target_audience": (request.form.get("target_audience") or "").strip(),
            "tagline": (request.form.get("tagline") or "").strip(),
            "active_offers": (request.form.get("active_offers") or "").strip(),
            "unique_selling_points": (request.form.get("unique_selling_points") or "").strip(),
            "services_to_highlight": (request.form.get("services_to_highlight") or "").strip(),
            "service_plan_options": (request.form.get("service_plan_options") or "").strip(),
            "service_add_ons": (request.form.get("service_add_ons") or "").strip(),
            "priority_seo_locations": (request.form.get("priority_seo_locations") or "").strip(),
            "company_story": (request.form.get("company_story") or "").strip(),
            "site_vision": (request.form.get("site_vision") or "").strip(),
            "design_notes": (request.form.get("design_notes") or "").strip(),
            "competitors": (request.form.get("competitors") or "").strip(),
            "content_goals": (request.form.get("content_goals") or "").strip(),
            "lead_form_type": (request.form.get("lead_form_type") or "").strip(),
            "lead_form_shortcode": (request.form.get("lead_form_shortcode") or "").strip(),
            "quote_tool_source": (request.form.get("quote_tool_source") or "").strip(),
            "quote_tool_embed": (request.form.get("quote_tool_embed") or "").strip(),
            "quote_tool_zip_mode": (request.form.get("quote_tool_zip_mode") or "").strip(),
            "quote_tool_collect_dogs": bool(request.form.get("quote_tool_collect_dogs")),
            "quote_tool_collect_frequency": bool(request.form.get("quote_tool_collect_frequency")),
            "quote_tool_collect_last_cleaned": bool(request.form.get("quote_tool_collect_last_cleaned")),
            "quote_tool_phone_mode": (request.form.get("quote_tool_phone_mode") or "").strip(),
            "quote_tool_notes": (request.form.get("quote_tool_notes") or "").strip(),
            "plugins": (request.form.get("plugins") or "").strip(),
            "cta_text": (request.form.get("cta_text") or "").strip(),
            "cta_phone": (request.form.get("cta_phone") or "").strip(),
            "color_palette": (request.form.get("color_palette") or "").strip(),
            "font_pair": (request.form.get("font_pair") or "").strip(),
            "layout_style": (request.form.get("layout_style") or "").strip(),
            "wireframe_style": (request.form.get("wireframe_style") or "").strip(),
            "hero_layout": (request.form.get("hero_layout") or "").strip(),
            "services_widget_layout": (request.form.get("services_widget_layout") or "").strip(),
            "proof_widget_layout": (request.form.get("proof_widget_layout") or "").strip(),
            "cta_widget_layout": (request.form.get("cta_widget_layout") or "").strip(),
            "button_style": (request.form.get("button_style") or "").strip(),
            "color_primary": (request.form.get("color_primary") or "").strip(),
            "color_accent": (request.form.get("color_accent") or "").strip(),
            "color_dark": (request.form.get("color_dark") or "").strip(),
            "color_light": (request.form.get("color_light") or "").strip(),
            "font_heading": client_portal_module.normalize_google_font_family(request.form.get("font_heading") or ""),
            "font_body": client_portal_module.normalize_google_font_family(request.form.get("font_body") or ""),
            "style_preset": (request.form.get("style_preset") or "").strip(),
            "reference_url": (request.form.get("reference_url") or "").strip(),
            "reference_mode": client_portal_module._site_builder_reference_mode(request.form.get("reference_mode")),
        }
        intake["image_slots"] = client_portal_module._site_builder_collect_image_slots(
            brand_id,
            (intake.get("industry") or brand.get("industry") or "").strip(),
        )

        page_selection_raw = (request.form.get("page_selection") or "").strip()
        page_selection = [page.strip() for page in page_selection_raw.split(",") if page.strip()] or None

        landing_pages = []
        lp_names = request.form.getlist("lp_name[]")
        lp_keywords = request.form.getlist("lp_keyword[]")
        lp_offers = request.form.getlist("lp_offer[]")
        lp_audiences = request.form.getlist("lp_audience[]")
        for index, name in enumerate(lp_names):
            cleaned_name = (name or "").strip()
            if not cleaned_name:
                continue
            landing_pages.append({
                "name": cleaned_name,
                "keyword": (lp_keywords[index] if index < len(lp_keywords) else "").strip(),
                "offer": (lp_offers[index] if index < len(lp_offers) else "").strip(),
                "audience": (lp_audiences[index] if index < len(lp_audiences) else "").strip(),
            })

        custom_pages = []
        cp_names = request.form.getlist("cp_name[]")
        cp_slugs = request.form.getlist("cp_slug[]")
        cp_purposes = request.form.getlist("cp_purpose[]")
        for index, name in enumerate(cp_names):
            cleaned_name = (name or "").strip()
            if not cleaned_name:
                continue
            custom_pages.append({
                "name": cleaned_name,
                "slug": (cp_slugs[index] if index < len(cp_slugs) else "").strip(),
                "purpose": (cp_purposes[index] if index < len(cp_purposes) else "").strip(),
            })

        if intake.get("reference_url"):
            try:
                intake["reference_site_brief"] = client_portal_module._site_builder_reference_style_brief(
                    intake.get("reference_url"),
                    intake.get("reference_mode"),
                    intake.get("industry") or brand.get("industry"),
                    intake.get("business_name") or brand.get("display_name"),
                    brand=brand,
                )
            except Exception as exc:
                current_app.logger.warning("Reference site brief failed: %s", exc)

        selected_site_template = None
        selected_site_template_id = int(request.form.get("site_template_id") or 0)
        if selected_site_template_id:
            selected_site_template = db.get_sb_site_template(selected_site_template_id)
        if not selected_site_template:
            selected_site_template = db.get_sb_default_site_template()

        if selected_site_template:
            site_theme = db.get_sb_theme(selected_site_template.get("theme_id")) if int(selected_site_template.get("theme_id") or 0) else {}
            active_templates = []
            template_lookup = {
                int(item.get("id") or 0): item
                for item in db.get_sb_templates(active_only=True)
                if int(item.get("id") or 0)
            }
            for template_id in selected_site_template.get("template_ids") or []:
                template = template_lookup.get(int(template_id or 0))
                if template:
                    active_templates.append(template)
            intake["builder_site_template"] = client_portal_module._site_builder_site_template_snapshot(
                selected_site_template,
                theme=site_theme,
                templates=active_templates,
            )
            intake["builder_theme"] = client_portal_module._site_builder_theme_snapshot(site_theme or {})
            intake["builder_templates"] = client_portal_module._site_builder_template_snapshots(active_templates)
        else:
            intake["builder_theme"] = client_portal_module._site_builder_theme_snapshot(db.get_sb_default_theme() or {})
            intake["builder_templates"] = client_portal_module._site_builder_template_snapshots(
                db.get_sb_templates(active_only=True)
            )
        intake["builder_prompt_overrides"] = client_portal_module._site_builder_prompt_override_snapshots(
            db.get_sb_prompt_overrides()
        )

        brand_ctx = build_brand_context(brand, intake=intake)
        blueprint = build_site_blueprint(
            brand_ctx,
            services=services,
            areas=areas,
            landing_pages=landing_pages,
            page_selection=page_selection,
            custom_pages=custom_pages,
        )

        if not blueprint:
            msg = "Could not create a site blueprint. Add at least one service and one service area before generating."
            if is_ajax:
                return jsonify(ok=False, error=msg), 400
            flash(msg, "warning")
            return redirect(url_for("site_builder_admin_generate", brand_id=brand_id))

        build_id = db.create_site_build(
            brand_id,
            blueprint,
            model=model,
            created_by=session.get("user_id", 0),
            intake=intake,
        )
        db.update_site_build_status(build_id, "running")

        pages_done = 0
        errors = []
        for page_spec in blueprint:
            try:
                content = generate_page_content(page_spec, brand_ctx, api_key, model)
                assembled = assemble_page(page_spec, brand_ctx, content)
                db.save_site_page({
                    "build_id": build_id,
                    "brand_id": brand_id,
                    "page_type": page_spec["page_type"],
                    "label": page_spec["label"],
                    "slug": page_spec.get("slug", ""),
                    "title": content.get("title", ""),
                    "content": assembled.get("body_html") or content.get("content", ""),
                    "excerpt": content.get("excerpt", ""),
                    "seo_title": content.get("seo_title", ""),
                    "seo_description": content.get("seo_description", ""),
                    "primary_keyword": content.get("primary_keyword", ""),
                    "secondary_keywords": content.get("secondary_keywords", ""),
                    "faq_items": content.get("faq_items") or [],
                    "schemas": assembled.get("schemas") or [],
                    "schema_html": assembled.get("schema_html", ""),
                    "full_html": assembled.get("full_html", ""),
                })
                pages_done += 1
                db.update_site_build_status(build_id, "running", pages_completed=pages_done)
            except Exception as exc:
                errors.append(f"{page_spec['label']}: {exc}")

        if errors:
            db.update_site_build_status(
                build_id,
                "completed",
                pages_completed=pages_done,
                error_message="; ".join(errors)[:500],
            )
        else:
            db.update_site_build_status(build_id, "completed", pages_completed=pages_done)

        if is_ajax:
            return jsonify(ok=True, build_id=build_id, pages_generated=pages_done, errors=errors)
        flash(f"Site build complete: {pages_done} pages generated.", "success")
        return redirect(url_for("site_builder_admin_review", build_id=build_id))

    @app.route("/site-builder-admin/builds/<int:build_id>")
    @login_required
    def site_builder_admin_review(build_id):
        build = db.get_site_build(build_id)
        if not build:
            flash("Site build not found.", "warning")
            return redirect(url_for("site_builder_admin_generate"))

        brand = db.get_brand(build.get("brand_id")) or {}
        pages = db.get_site_pages(build_id)
        is_ajax = request.headers.get("X-Requested-With") in {"XMLHttpRequest", "PJAX"} or request.is_json
        if is_ajax:
            return jsonify(ok=True, build=build, pages=pages)
        return _render_admin_site_builder(mode="review", brand=brand, build=build, pages=pages)

    @app.route("/site-builder-admin/builds/<int:build_id>/delete", methods=["POST"])
    @login_required
    def site_builder_admin_delete(build_id):
        wants_json = request.is_json or request.headers.get("X-Requested-With") in {"XMLHttpRequest", "PJAX"}
        build = db.get_site_build(build_id)
        if not build:
            if wants_json:
                return jsonify(ok=False, error="Build not found."), 404
            flash("Site build not found.", "warning")
            return redirect(url_for("site_builder_admin_generate"))

        brand = db.get_brand(build.get("brand_id")) or {}
        pages = db.get_site_pages(build_id)
        published_pages = [page for page in pages if int(page.get("wp_page_id") or 0)]

        if published_pages and not client_portal_module._wp_connected(brand):
            msg = "Reconnect WordPress before deleting this build so the published pages can be removed too."
            if wants_json:
                return jsonify(ok=False, error=msg), 400
            flash(msg, "error")
            return redirect(url_for("site_builder_admin_review", build_id=build_id))

        errors = []
        deleted_wp_pages = 0
        for page in published_pages:
            result = client_portal_module._delete_wp_page(brand, page.get("wp_page_id"))
            if result.get("ok"):
                if result.get("deleted"):
                    deleted_wp_pages += 1
                continue
            errors.append(f"{page.get('label') or page.get('title') or 'Page'}: {result.get('error', 'WordPress delete failed')}")

        if errors:
            if wants_json:
                return jsonify(ok=False, error="Failed to delete one or more WordPress pages.", errors=errors), 502
            flash(errors[0], "error")
            return redirect(url_for("site_builder_admin_review", build_id=build_id))

        db.delete_site_build(build_id, brand_id=build.get("brand_id"))

        if wants_json:
            return jsonify(ok=True, deleted_build_id=build_id, deleted_wordpress_pages=deleted_wp_pages)
        flash(f"Deleted build #{build_id}.", "success")
        return redirect(url_for("site_builder_admin_generate", brand_id=build.get("brand_id") or 0))

    @app.route("/site-builder-admin/builds/<int:build_id>/publish", methods=["POST"])
    @login_required
    def site_builder_admin_publish(build_id):
        build = db.get_site_build(build_id)
        if not build:
            return jsonify(ok=False, error="Build not found."), 404

        brand = db.get_brand(build.get("brand_id")) or {}
        if not client_portal_module._wp_connected(brand):
            return jsonify(ok=False, error="WordPress is not connected. Add credentials in the intake and regenerate or reconnect the brand."), 400

        pages = db.get_site_pages(build_id)
        published = 0
        errors = []
        for page in pages:
            if page.get("wp_page_id"):
                published += 1
                continue
            result = client_portal_module._publish_wp_page(
                brand,
                title=page["title"],
                content=page["full_html"] or page["content"],
                excerpt=page.get("excerpt", ""),
                slug=page.get("slug", ""),
                seo_title=page.get("seo_title", ""),
                seo_description=page.get("seo_description", ""),
            )
            if result.get("ok"):
                db.update_site_page_wp(page["id"], result["wp_page_id"], result["wp_page_url"])
                published += 1
            else:
                errors.append(f"{page['label']}: {result.get('error', 'Unknown error')}")

        return jsonify(ok=True, published=published, total=len(pages), errors=errors)

    @app.route("/site-builder-admin/pages/<int:page_id>")
    @login_required
    def site_builder_admin_page_get(page_id):
        page = db.get_site_page(page_id)
        if not page:
            return jsonify(ok=False, error="Page not found."), 404
        build = db.get_site_build(page.get("build_id"))
        if not build:
            return jsonify(ok=False, error="Build not found."), 404
        return jsonify(ok=True, page=page)

    @app.route("/site-builder-admin/pages/<int:page_id>/save", methods=["POST"])
    @login_required
    def site_builder_admin_page_save(page_id):
        page = db.get_site_page(page_id)
        if not page:
            return jsonify(ok=False, error="Page not found."), 404
        build = db.get_site_build(page.get("build_id"))
        if not build:
            return jsonify(ok=False, error="Build not found."), 404

        data = request.get_json(silent=True)
        if data is None and request.get_data(cache=False):
            return jsonify(ok=False, error="Invalid JSON payload."), 400

        try:
            update = client_portal_module._normalize_site_builder_page_save_payload(data)
        except ValueError as exc:
            return jsonify(ok=False, error=str(exc)), 400

        if update:
            db.update_site_page_content(page_id, update)

        return jsonify(ok=True)

    @app.route("/site-builder-admin/pages/<int:page_id>/rewrite", methods=["POST"])
    @login_required
    def site_builder_admin_page_rewrite(page_id):
        page = db.get_site_page(page_id)
        if not page:
            return jsonify(ok=False, error="Page not found."), 404
        build = db.get_site_build(page.get("build_id"))
        if not build:
            return jsonify(ok=False, error="Build not found."), 404

        brand = db.get_brand(build.get("brand_id")) or {}
        api_key = client_portal_module._get_openai_api_key(brand)
        if not api_key:
            return jsonify(ok=False, error="OpenAI API key not configured."), 400

        model = client_portal_module._pick_ai_model(brand, "analysis")
        data = request.get_json(silent=True)
        if data is None and request.get_data(cache=False):
            return jsonify(ok=False, error="Invalid JSON payload."), 400
        if data is not None and not isinstance(data, dict):
            return jsonify(ok=False, error="Invalid rewrite payload."), 400

        instructions = client_portal_module._normalize_site_builder_text(
            (data or {}).get("instructions"),
            "Rewrite instructions",
            max_length=client_portal_module._SITE_BUILDER_MAX_REWRITE_INSTRUCTIONS_LENGTH,
        )

        from webapp.site_builder import (
            build_brand_context,
            _brand_block,
            _seo_intel_block,
            _GLOBAL_RULES,
            _OUTPUT_FORMAT,
            _system_msg,
            _extract_json,
        )

        intake = {}
        try:
            intake = json.loads(build.get("intake_json") or "{}")
        except Exception:
            intake = {}
        brand_ctx = build_brand_context(brand, intake=intake)

        existing_content = page.get("content") or ""
        existing_title = page.get("title") or ""
        existing_seo_title = page.get("seo_title") or ""
        existing_seo_desc = page.get("seo_description") or ""

        user_msg = (
            f"REWRITE the following website page content.\n\n"
            f"BUSINESS CONTEXT:\n{_brand_block(brand_ctx)}\n\n"
            f"{_seo_intel_block(brand_ctx)}"
            f"PAGE TYPE: {page.get('page_type', 'generic')}\n"
            f"PAGE LABEL: {page.get('label', '')}\n"
            f"CURRENT TITLE: {existing_title}\n"
            f"CURRENT SEO TITLE: {existing_seo_title}\n"
            f"CURRENT SEO DESCRIPTION: {existing_seo_desc}\n\n"
            f"CURRENT CONTENT:\n{existing_content}\n\n"
        )
        if instructions:
            user_msg += f"USER REWRITE INSTRUCTIONS (follow these closely):\n{instructions}\n\n"
        else:
            user_msg += (
                "REWRITE INSTRUCTIONS:\n"
                "Improve the content quality, conversion copy, and SEO without changing the core structure or message. "
                "Tighten the writing, strengthen CTAs, add specificity.\n\n"
            )
        user_msg += f"{_GLOBAL_RULES}\n{_OUTPUT_FORMAT}"

        try:
            import openai

            client = openai.OpenAI(api_key=api_key)
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": _system_msg()},
                    {"role": "user", "content": user_msg},
                ],
                temperature=0.5,
                response_format={"type": "json_object"},
            )
            raw = (response.choices[0].message.content or "{}").strip()
            result = _extract_json(raw)
            if not isinstance(result, dict):
                raise ValueError("AI rewrite returned an unexpected response.")
        except Exception as exc:
            return jsonify(ok=False, error=f"AI rewrite failed: {str(exc)[:200]}"), 500

        rewritten_content = client_portal_module._normalize_site_builder_text(
            result.get("content") or existing_content,
            "Page content",
            trim=False,
        )
        try:
            client_portal_module._validate_site_builder_blob_size(
                rewritten_content,
                "Page content",
                client_portal_module._SITE_BUILDER_MAX_CONTENT_BYTES,
            )
            update = {
                "title": client_portal_module._normalize_site_builder_text(
                    result.get("title") or existing_title,
                    "Page title",
                    max_length=client_portal_module._SITE_BUILDER_MAX_TITLE_LENGTH,
                ),
                "content": rewritten_content,
                "excerpt": result.get("excerpt") or page.get("excerpt", ""),
                "seo_title": client_portal_module._normalize_site_builder_text(
                    result.get("seo_title") or existing_seo_title,
                    "SEO title",
                    max_length=client_portal_module._SITE_BUILDER_MAX_SEO_TITLE_LENGTH,
                ),
                "seo_description": client_portal_module._normalize_site_builder_text(
                    result.get("seo_description") or existing_seo_desc,
                    "SEO description",
                    max_length=client_portal_module._SITE_BUILDER_MAX_SEO_DESCRIPTION_LENGTH,
                ),
                "primary_keyword": result.get("primary_keyword") or page.get("primary_keyword", ""),
                "secondary_keywords": result.get("secondary_keywords") or page.get("secondary_keywords", ""),
            }
        except ValueError as exc:
            return jsonify(ok=False, error=str(exc)), 400

        if result.get("faq_items"):
            update["faq_items_json"] = json.dumps(result["faq_items"])
        update["full_html"] = update["content"]
        db.update_site_page_content(page_id, update)
        return jsonify(ok=True, page=update)

    @app.route("/site-builder-admin/upload-image", methods=["POST"])
    @login_required
    def site_builder_admin_upload_image():
        import uuid as uuid_lib
        from werkzeug.utils import secure_filename

        if "image" not in request.files:
            file = request.files.get("files[]")
            if not file:
                return jsonify(ok=False, error="No image file provided."), 400
        else:
            file = request.files["image"]

        if not file.filename:
            return jsonify(ok=False, error="Empty filename."), 400

        allowed_ext = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg"}
        ext = os.path.splitext(file.filename)[1].lower()
        if ext not in allowed_ext:
            return jsonify(ok=False, error=f"File type {ext} not allowed.", allowed_types=sorted(allowed_ext)), 400

        file.seek(0, 2)
        size = file.tell()
        file.seek(0)
        if size > 10 * 1024 * 1024:
            return jsonify(ok=False, error="Image too large. Max 10MB.", max_size_bytes=10 * 1024 * 1024), 400

        safe_name = secure_filename(f"{uuid_lib.uuid4().hex}{ext}")
        if not safe_name:
            return jsonify(ok=False, error="Could not generate a safe filename."), 400

        upload_dir = os.path.join(current_app.static_folder or "static", "uploads", "site_builder")
        os.makedirs(upload_dir, exist_ok=True)
        save_path = os.path.join(upload_dir, safe_name)
        try:
            file.save(save_path)
        except Exception as exc:
            current_app.logger.exception("site builder image upload failed")
            return jsonify(ok=False, error=f"Image upload failed: {str(exc)[:160]}"), 500

        img_url = url_for("static", filename=f"uploads/site_builder/{safe_name}")
        return jsonify(ok=True, data=[img_url], url=img_url, filename=safe_name)

    # -- Templates CRUD --

    @app.route("/site-builder-admin/templates", methods=["POST"])
    @login_required
    def sb_admin_template_save():
        template_id = request.form.get("template_id")
        data = {
            "name": request.form.get("name", "").strip(),
            "category": request.form.get("category", "section"),
            "page_types": request.form.get("page_types", ""),
            "html_content": request.form.get("html_content", ""),
            "css_content": request.form.get("css_content", ""),
            "description": request.form.get("description", ""),
            "sort_order": int(request.form.get("sort_order", 0)),
            "is_active": 1 if request.form.get("is_active") else 0,
        }
        if not data["name"]:
            flash("Template name is required.", "danger")
            return redirect(url_for("site_builder_admin", tab="templates"))
        if template_id:
            db.update_sb_template(int(template_id), data)
            flash("Template updated.", "success")
        else:
            db.create_sb_template(data)
            flash("Template created.", "success")
        return redirect(url_for("site_builder_admin", tab="templates"))

    @app.route("/site-builder-admin/templates/<int:tid>/delete", methods=["POST"])
    @login_required
    def sb_admin_template_delete(tid):
        db.delete_sb_template(tid)
        flash("Template deleted.", "info")
        return redirect(url_for("site_builder_admin", tab="templates"))

    @app.route("/api/site-builder-admin/templates/<int:tid>")
    @login_required
    def sb_admin_template_get(tid):
        t = db.get_sb_template(tid)
        if not t:
            return jsonify({"error": "not found"}), 404
        return jsonify(t)

    # -- Themes CRUD --

    @app.route("/site-builder-admin/themes", methods=["POST"])
    @login_required
    def sb_admin_theme_save():
        from webapp.font_catalog import normalize_google_font_family

        theme_id = request.form.get("theme_id")
        data = {
            "name": request.form.get("name", "").strip(),
            "description": request.form.get("description", ""),
            "primary_color": request.form.get("primary_color", "#2563eb"),
            "secondary_color": request.form.get("secondary_color", "#1e40af"),
            "accent_color": request.form.get("accent_color", "#f59e0b"),
            "text_color": request.form.get("text_color", "#1f2937"),
            "bg_color": request.form.get("bg_color", "#ffffff"),
            "font_heading": normalize_google_font_family(request.form.get("font_heading", "Inter")),
            "font_body": normalize_google_font_family(request.form.get("font_body", "Inter")),
            "button_style": request.form.get("button_style", "rounded"),
            "layout_style": request.form.get("layout_style", "modern"),
            "custom_css": request.form.get("custom_css", ""),
            "is_default": 1 if request.form.get("is_default") else 0,
            "is_active": 1 if request.form.get("is_active") else 0,
        }
        if not data["name"]:
            flash("Theme name is required.", "danger")
            return redirect(url_for("site_builder_admin", tab="themes"))
        if theme_id:
            db.update_sb_theme(int(theme_id), data)
            flash("Theme updated.", "success")
        else:
            db.create_sb_theme(data)
            flash("Theme created.", "success")
        return redirect(url_for("site_builder_admin", tab="themes"))

    @app.route("/site-builder-admin/themes/<int:tid>/delete", methods=["POST"])
    @login_required
    def sb_admin_theme_delete(tid):
        db.delete_sb_theme(tid)
        flash("Theme deleted.", "info")
        return redirect(url_for("site_builder_admin", tab="themes"))

    @app.route("/api/site-builder-admin/themes/<int:tid>")
    @login_required
    def sb_admin_theme_get(tid):
        t = db.get_sb_theme(tid)
        if not t:
            return jsonify({"error": "not found"}), 404
        return jsonify(t)

    # -- Full Site Templates CRUD --

    @app.route("/site-builder-admin/site-templates", methods=["POST"])
    @login_required
    def sb_admin_site_template_save():
        from webapp.site_builder import _slugify

        site_template_id = request.form.get("site_template_id")
        template_ids = []
        for raw_id in request.form.getlist("template_ids"):
            try:
                template_ids.append(int(raw_id))
            except Exception:
                continue
        data = {
            "name": request.form.get("name", "").strip(),
            "slug": (request.form.get("slug", "").strip() or _slugify(request.form.get("name", ""))),
            "description": request.form.get("description", ""),
            "theme_id": int(request.form.get("theme_id") or 0),
            "template_ids": template_ids,
            "prompt_notes": request.form.get("prompt_notes", ""),
            "sort_order": int(request.form.get("sort_order", 0)),
            "is_default": 1 if request.form.get("is_default") else 0,
            "is_active": 1 if request.form.get("is_active") else 0,
        }
        if not data["name"]:
            flash("Site template name is required.", "danger")
            return redirect(url_for("site_builder_admin", tab="site-templates"))
        if not data["slug"]:
            flash("Site template slug is required.", "danger")
            return redirect(url_for("site_builder_admin", tab="site-templates"))
        if site_template_id:
            db.update_sb_site_template(int(site_template_id), data)
            flash("Site template updated.", "success")
        else:
            db.create_sb_site_template(data)
            flash("Site template created.", "success")
        return redirect(url_for("site_builder_admin", tab="site-templates"))

    @app.route("/site-builder-admin/site-templates/install-defaults", methods=["POST"])
    @login_required
    def sb_admin_site_template_install_defaults():
        result = db.seed_default_site_builder_kits()
        flash(
            "Installed production site kits: "
            f"{result['site_templates_created']} created, {result['site_templates_updated']} refreshed.",
            "success",
        )
        return redirect(url_for("site_builder_admin", tab="site-templates"))

    @app.route("/site-builder-admin/site-templates/<int:tid>/delete", methods=["POST"])
    @login_required
    def sb_admin_site_template_delete(tid):
        db.delete_sb_site_template(tid)
        flash("Site template deleted.", "info")
        return redirect(url_for("site_builder_admin", tab="site-templates"))

    @app.route("/api/site-builder-admin/site-templates/<int:tid>")
    @login_required
    def sb_admin_site_template_get(tid):
        site_template = db.get_sb_site_template(tid)
        if not site_template:
            return jsonify({"error": "not found"}), 404
        return jsonify(site_template)

    # -- Prompt Overrides --

    @app.route("/site-builder-admin/prompts", methods=["POST"])
    @login_required
    def sb_admin_prompt_save():
        page_type = request.form.get("page_type", "").strip()
        section = request.form.get("section", "user_prompt").strip()
        content = request.form.get("content", "")
        notes = request.form.get("notes", "")
        if not page_type:
            flash("Page type is required.", "danger")
            return redirect(url_for("site_builder_admin", tab="prompts"))
        db.save_sb_prompt_override(page_type, section, content, notes=notes, updated_by="admin")
        flash(f"Prompt override saved for {page_type} / {section}.", "success")
        return redirect(url_for("site_builder_admin", tab="prompts"))

    @app.route("/site-builder-admin/prompts/<int:pid>/toggle", methods=["POST"])
    @login_required
    def sb_admin_prompt_toggle(pid):
        is_active = int(request.form.get("is_active", 0))
        db.toggle_sb_prompt_override(pid, is_active)
        flash("Prompt override toggled.", "info")
        return redirect(url_for("site_builder_admin", tab="prompts"))

    @app.route("/site-builder-admin/prompts/<int:pid>/delete", methods=["POST"])
    @login_required
    def sb_admin_prompt_delete(pid):
        db.delete_sb_prompt_override(pid)
        flash("Prompt override deleted.", "info")
        return redirect(url_for("site_builder_admin", tab="prompts"))

    # -- Image Categories --

    @app.route("/site-builder-admin/image-categories", methods=["POST"])
    @login_required
    def sb_admin_image_category_save():
        cat_id = request.form.get("category_id")
        name = request.form.get("name", "").strip()
        slug = request.form.get("slug", "").strip().lower().replace(" ", "-")
        description = request.form.get("description", "")
        if not name or not slug:
            flash("Category name and slug are required.", "danger")
            return redirect(url_for("site_builder_admin", tab="images"))
        if cat_id:
            db.update_sb_image_category(int(cat_id), name, slug, description)
            flash("Category updated.", "success")
        else:
            db.create_sb_image_category(name, slug, description)
            flash("Category created.", "success")
        return redirect(url_for("site_builder_admin", tab="images"))

    @app.route("/site-builder-admin/image-categories/<int:cid>/delete", methods=["POST"])
    @login_required
    def sb_admin_image_category_delete(cid):
        db.delete_sb_image_category(cid)
        flash("Category deleted. Images were unlinked, not deleted.", "info")
        return redirect(url_for("site_builder_admin", tab="images"))

    # -- Image Upload & Management --

    @app.route("/site-builder-admin/images/upload", methods=["POST"])
    @login_required
    def sb_admin_image_upload():
        import os
        import uuid
        from werkzeug.utils import secure_filename

        files = request.files.getlist("images")
        if not files:
            flash("No files selected.", "danger")
            return redirect(url_for("site_builder_admin", tab="images"))

        allowed_ext = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg"}
        upload_dir = os.path.join(
            current_app.static_folder or "static", "uploads", "sb_images"
        )
        os.makedirs(upload_dir, exist_ok=True)

        category_id = request.form.get("category_id") or None
        if category_id:
            category_id = int(category_id)
        tags = request.form.get("tags", "")
        industry = request.form.get("industry", "")

        uploaded = 0
        for f in files:
            if not f.filename:
                continue
            ext = os.path.splitext(f.filename)[1].lower()
            if ext not in allowed_ext:
                continue
            # Size check (20MB)
            f.seek(0, 2)
            size = f.tell()
            f.seek(0)
            if size > 20 * 1024 * 1024:
                continue
            safe_name = secure_filename(f"{uuid.uuid4().hex}{ext}")
            save_path = os.path.join(upload_dir, safe_name)
            f.save(save_path)

            # Try to get image dimensions
            width, height = 0, 0
            try:
                from PIL import Image
                img = Image.open(save_path)
                width, height = img.size
                img.close()
            except Exception:
                pass

            mime_map = {
                ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
                ".png": "image/png", ".gif": "image/gif",
                ".webp": "image/webp", ".svg": "image/svg+xml",
            }
            db.create_sb_image({
                "filename": safe_name,
                "original_name": f.filename,
                "file_path": f"uploads/sb_images/{safe_name}",
                "file_size": size,
                "mime_type": mime_map.get(ext, "image/jpeg"),
                "width": width,
                "height": height,
                "alt_text": os.path.splitext(f.filename)[0].replace("-", " ").replace("_", " "),
                "title": os.path.splitext(f.filename)[0].replace("-", " ").replace("_", " "),
                "category_id": category_id,
                "tags": tags,
                "industry": industry,
                "source": "upload",
            })
            uploaded += 1

        flash(f"{uploaded} image(s) uploaded.", "success")
        return redirect(url_for("site_builder_admin", tab="images"))

    @app.route("/site-builder-admin/images/<int:iid>/edit", methods=["POST"])
    @login_required
    def sb_admin_image_edit(iid):
        data = {
            "alt_text": request.form.get("alt_text", ""),
            "title": request.form.get("title", ""),
            "tags": request.form.get("tags", ""),
            "industry": request.form.get("industry", ""),
            "page_types": request.form.get("page_types", ""),
        }
        cat_id = request.form.get("category_id")
        if cat_id:
            data["category_id"] = int(cat_id)
        db.update_sb_image(iid, data)
        flash("Image updated.", "success")
        return redirect(url_for("site_builder_admin", tab="images"))

    @app.route("/site-builder-admin/images/<int:iid>/delete", methods=["POST"])
    @login_required
    def sb_admin_image_delete(iid):
        db.delete_sb_image(iid)
        flash("Image removed.", "info")
        return redirect(url_for("site_builder_admin", tab="images"))

    @app.route("/site-builder-admin/images/bulk", methods=["POST"])
    @login_required
    def sb_admin_image_bulk():
        action = request.form.get("action", "")
        image_ids = request.form.getlist("image_ids")
        image_ids = [int(i) for i in image_ids if i.isdigit()]
        if not image_ids:
            flash("No images selected.", "danger")
            return redirect(url_for("site_builder_admin", tab="images"))
        if action == "delete":
            for iid in image_ids:
                db.delete_sb_image(iid)
            flash(f"{len(image_ids)} image(s) removed.", "info")
        elif action == "categorize":
            cat_id = request.form.get("bulk_category_id")
            if cat_id:
                db.bulk_update_sb_images(image_ids, {"category_id": int(cat_id)})
                flash(f"{len(image_ids)} image(s) categorized.", "success")
        elif action == "tag":
            tags = request.form.get("bulk_tags", "")
            if tags:
                db.bulk_update_sb_images(image_ids, {"tags": tags})
                flash(f"{len(image_ids)} image(s) tagged.", "success")
        return redirect(url_for("site_builder_admin", tab="images"))

    @app.route("/site-builder-admin/images/wp-publish", methods=["POST"])
    @login_required
    def sb_admin_image_wp_publish():
        """Publish selected images to WordPress media library."""
        import os
        image_ids = request.form.getlist("image_ids")
        brand_id = request.form.get("brand_id")
        if not image_ids or not brand_id:
            flash("Select images and a brand to publish to.", "danger")
            return redirect(url_for("site_builder_admin", tab="images"))
        brand_id = int(brand_id)
        brand = db.get_brand(brand_id)
        if not brand:
            flash("Brand not found.", "danger")
            return redirect(url_for("site_builder_admin", tab="images"))

        wp_url = (brand.get("website") or "").rstrip("/")
        wp_user = brand.get("wp_username") or ""
        wp_pass = brand.get("wp_app_password") or ""
        if not all([wp_url, wp_user, wp_pass]):
            flash("WordPress credentials not configured for this brand.", "danger")
            return redirect(url_for("site_builder_admin", tab="images"))

        import requests as req
        published = 0
        for iid in image_ids:
            iid = int(iid)
            img = db.get_sb_image(iid)
            if not img or img.get("wp_media_id"):
                continue
            file_path = os.path.join(
                current_app.static_folder or "static", img["file_path"]
            )
            if not os.path.exists(file_path):
                continue
            with open(file_path, "rb") as fh:
                file_data = fh.read()
            headers = {
                "Content-Disposition": f'attachment; filename="{img["filename"]}"',
                "Content-Type": img.get("mime_type", "image/jpeg"),
            }
            try:
                resp = req.post(
                    f"{wp_url}/wp-json/wp/v2/media",
                    headers=headers,
                    data=file_data,
                    auth=(wp_user, wp_pass),
                    timeout=60,
                )
                if resp.status_code in (200, 201):
                    wp_data = resp.json()
                    wp_media_id = wp_data.get("id", 0)
                    wp_media_url = wp_data.get("source_url", "")
                    # Set alt text via update
                    if img.get("alt_text"):
                        req.post(
                            f"{wp_url}/wp-json/wp/v2/media/{wp_media_id}",
                            json={"alt_text": img["alt_text"], "title": img.get("title", "")},
                            auth=(wp_user, wp_pass),
                            timeout=15,
                        )
                    db.update_sb_image(iid, {
                        "wp_media_id": wp_media_id,
                        "wp_media_url": wp_media_url,
                    })
                    published += 1
            except Exception as e:
                logger.warning("WP media upload failed for image %s: %s", iid, e)
                continue

        flash(f"{published} image(s) published to WordPress.", "success")
        return redirect(url_for("site_builder_admin", tab="images"))

    @app.route("/api/site-builder-admin/images")
    @login_required
    def sb_admin_images_api():
        """JSON endpoint for image library browsing/filtering."""
        category_id = request.args.get("category_id")
        industry = request.args.get("industry")
        tags = request.args.get("tags")
        page = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 40))
        offset = (page - 1) * per_page
        images = db.get_sb_images(
            category_id=int(category_id) if category_id else None,
            industry=industry,
            tags=tags,
            limit=per_page,
            offset=offset,
        )
        total = db.count_sb_images(
            category_id=int(category_id) if category_id else None,
            industry=industry,
        )
        for img in images:
            img["url"] = url_for("static", filename=img["file_path"])
        return jsonify({"images": images, "total": total, "page": page, "per_page": per_page})

    @app.route("/setup-guide")
    @login_required
    def setup_guide():
        app_url = db.get_setting("app_url", request.host_url.rstrip("/"))
        return render_template("setup_guide.html", app_url=app_url)

    # ── API status endpoint ──
    @app.route("/api/brand/<int:brand_id>/status")
    @login_required
    def api_brand_status(brand_id):
        brand = db.get_brand(brand_id)
        if not brand:
            return jsonify({"error": "not found"}), 404
        connections = db.get_brand_connections(brand_id)
        return jsonify({
            "brand": brand["display_name"],
            "connections": connections,
        })

    @app.route("/api/brand/<int:brand_id>/diagnose")
    @login_required
    def api_brand_diagnose(brand_id):
        """Debug endpoint: show exactly why a refresh would fail."""
        brand = db.get_brand(brand_id)
        if not brand:
            return jsonify({"error": "Brand not found"}), 404

        connections = db.get_brand_connections(brand_id) or {}
        google_conn = connections.get("google", {})
        meta_conn = connections.get("meta", {})

        diag = {
            "brand": brand.get("display_name"),
            "slug": brand.get("slug"),
            "has_slug": bool(brand.get("slug")),
            "google": {
                "connected": google_conn.get("status") == "connected",
                "has_access_token": bool(google_conn.get("access_token")),
                "has_refresh_token": bool(google_conn.get("refresh_token")),
                "token_expiry": google_conn.get("token_expiry", ""),
                "ga4_property_id": brand.get("ga4_property_id", ""),
                "gsc_site_url": brand.get("gsc_site_url", ""),
                "google_ads_customer_id": brand.get("google_ads_customer_id", ""),
            },
            "meta": {
                "connected": meta_conn.get("status") == "connected",
                "has_access_token": bool(meta_conn.get("access_token")),
                "token_expiry": meta_conn.get("token_expiry", ""),
                "meta_ad_account_id": brand.get("meta_ad_account_id", ""),
                "facebook_page_id": brand.get("facebook_page_id", ""),
            },
            "snapshot": None,
            "live_pull_test": None,
        }

        # Check latest snapshot
        month = datetime.now().strftime("%Y-%m")
        try:
            snap = db.get_dashboard_snapshot(brand_id, month)
            if snap:
                diag["snapshot"] = {
                    "month": month,
                    "created_at": snap.get("created_at"),
                    "source": snap.get("source", ""),
                }
            else:
                diag["snapshot"] = {"month": month, "exists": False}
        except Exception as e:
            diag["snapshot"] = {"error": str(e)}

        # Try a live pull and capture the exact error
        try:
            from webapp.report_runner import get_analysis_and_suggestions_for_brand
            analysis, suggestions = get_analysis_and_suggestions_for_brand(
                db, brand, month, force_refresh=True
            )
            sources = [k for k in ("google_analytics", "meta_business", "search_console", "google_ads", "facebook_organic") if analysis.get(k)]
            diag["live_pull_test"] = {
                "success": True,
                "sources_returned": sources,
                "suggestion_count": len(suggestions),
            }
        except Exception as e:
            diag["live_pull_test"] = {
                "success": False,
                "error": str(e),
            }

        return jsonify(diag)

    def _extract_brand_form(form):
        return {
            "display_name": form.get("display_name", "").strip(),
            "slug": form.get("slug", "").strip().lower().replace(" ", "_"),
            "industry": form.get("industry", "plumbing"),
            "monthly_budget": float(form.get("monthly_budget", 0) or 0),
            "website": form.get("website", "").strip(),
            "service_area": form.get("service_area", "").strip(),
            "primary_services": form.get("primary_services", "").strip(),
            "goals": form.getlist("goals"),
        }

    def _get_industries():
        return [
            ("plumbing", "Plumbing"),
            ("hvac", "HVAC"),
            ("electrical", "Electrical"),
            ("roofing", "Roofing"),
            ("landscaping", "Landscaping"),
            ("lawn_care", "Lawn Care"),
            ("tree_service", "Tree Service"),
            ("pest_control", "Pest Control"),
            ("cleaning", "Cleaning Services"),
            ("carpet_cleaning", "Carpet Cleaning"),
            ("pressure_washing", "Pressure Washing"),
            ("pool_service", "Pool Service"),
            ("general_contracting", "General Contracting"),
            ("painting", "Painting"),
            ("garage_door", "Garage Door"),
            ("fencing", "Fencing"),
            ("concrete", "Concrete / Masonry"),
            ("foundation_repair", "Foundation Repair"),
            ("water_damage", "Water Damage Restoration"),
            ("mold_remediation", "Mold Remediation"),
            ("pet_waste_removal", "Pet Waste Removal"),
            ("dog_grooming", "Dog Grooming"),
            ("pet_sitting", "Pet Sitting / Dog Walking"),
            ("moving", "Moving / Hauling"),
            ("junk_removal", "Junk Removal"),
            ("flooring", "Flooring"),
            ("windows_doors", "Windows & Doors"),
            ("solar", "Solar"),
            ("appliance_repair", "Appliance Repair"),
            ("locksmith", "Locksmith"),
            ("towing", "Towing"),
            ("auto_detailing", "Auto Detailing"),
            ("dental", "Dental"),
            ("chiropractic", "Chiropractic"),
            ("med_spa", "Med Spa / Aesthetics"),
            ("fitness", "Fitness / Gym"),
            ("legal", "Legal Services"),
            ("real_estate", "Real Estate"),
            ("restaurant", "Restaurant"),
            ("ecommerce", "E-Commerce"),
            ("saas", "SaaS"),
            ("other", "Other"),
        ]

    def _site_builder_admin_unique_slug(name):
        from webapp.site_builder import _slugify

        base = _slugify(name or "site-builder-brand") or "site-builder-brand"
        slug = base
        suffix = 2
        while db.get_brand_by_slug(slug):
            slug = f"{base}-{suffix}"
            suffix += 1
        return slug

    def _site_builder_admin_upsert_brand(form):
        selected_brand_id = int(form.get("brand_id") or 0)
        business_name = (form.get("business_name") or "").strip()
        industry = (form.get("industry") or "").strip() or "other"
        website = (form.get("website") or "").strip()
        service_area = (form.get("areas") or form.get("service_area") or "").strip()
        primary_services = (form.get("services") or form.get("primary_services") or "").strip()

        if selected_brand_id:
            brand = db.get_brand(selected_brand_id)
            if not brand:
                raise ValueError("Selected brand was not found.")
            db.update_brand(selected_brand_id, {
                "display_name": business_name or brand.get("display_name") or "Untitled Brand",
                "slug": (brand.get("slug") or _site_builder_admin_unique_slug(business_name or "brand")).strip(),
                "industry": industry or brand.get("industry") or "other",
                "monthly_budget": brand.get("monthly_budget") or 0,
                "website": website or brand.get("website") or "",
                "service_area": service_area or brand.get("service_area") or "",
                "primary_services": primary_services or brand.get("primary_services") or "",
                "goals": brand.get("goals") or [],
            })
            brand_id = selected_brand_id
        else:
            if not business_name:
                raise ValueError("Business name is required.")
            brand_id = db.create_brand({
                "display_name": business_name,
                "slug": _site_builder_admin_unique_slug(business_name),
                "industry": industry,
                "website": website,
                "service_area": service_area,
                "primary_services": primary_services,
                "goals": [],
            })

        wp_site_url = (form.get("wp_site_url") or "").strip().rstrip("/")
        wp_username = (form.get("wp_username") or "").strip()
        wp_app_password = (form.get("wp_app_password") or "").strip()
        if wp_site_url or not selected_brand_id:
            db.update_brand_text_field(brand_id, "wp_site_url", wp_site_url)
        if wp_username or not selected_brand_id:
            db.update_brand_text_field(brand_id, "wp_username", wp_username)
        if wp_app_password:
            db.update_brand_text_field(brand_id, "wp_app_password", wp_app_password)

        for field in ("brand_voice", "target_audience", "active_offers"):
            value = (form.get(field) or "").strip()
            if value:
                db.update_brand_text_field(brand_id, field, value)

        return db.get_brand(brand_id) or {}, brand_id

    def _render_admin_site_builder(mode="landing", brand=None, build=None, pages=None):
        brand = brand or {}
        brand_id = int(brand.get("id") or 0)
        brand_palette = client_portal_module._site_builder_brand_palette(brand)
        brand_primary_color = brand_palette[0] if brand_palette else ""
        brand_accent_color = brand_palette[1] if len(brand_palette) > 1 else brand_primary_color
        wp_ok = client_portal_module._wp_connected(brand)
        builds = db.get_site_builds(brand_id, limit=20) if brand_id and mode == "landing" else []
        site_templates = db.get_sb_site_templates(active_only=True)
        default_site_template = db.get_sb_default_site_template() or (site_templates[0] if site_templates else None)
        unpublished = 0
        if pages:
            unpublished = sum(1 for page in pages if not page.get("wp_page_id"))

        return render_template(
            "client/client_site_builder.html",
            layout_template="base.html",
            builder_mode="admin",
            mode=mode,
            brand_fields_locked=False,
            show_runtime_wp_fields=True,
            builder_home_url=url_for("site_builder_admin_generate", brand_id=brand_id) if brand_id else url_for("site_builder_admin_generate"),
            builder_generate_url=url_for("site_builder_admin_generate_post"),
            builder_settings_url="",
            builder_review_endpoint="site_builder_admin_review",
            builder_delete_endpoint="site_builder_admin_delete",
            builder_publish_endpoint="site_builder_admin_publish",
            builder_page_get_endpoint="site_builder_admin_page_get",
            builder_page_save_endpoint="site_builder_admin_page_save",
            builder_page_rewrite_endpoint="site_builder_admin_page_rewrite",
            builder_upload_image_url=url_for("site_builder_admin_upload_image"),
            builder_seo_intel_url="",
            builder_brand_picker_url=url_for("site_builder_admin_generate"),
            available_brands=db.get_all_brands(),
            selected_brand_id=brand_id,
            wp_connected=wp_ok,
            wp_site_url=(brand.get("wp_site_url") or "").strip().rstrip("/"),
            builds=builds,
            build=build,
            pages=pages or [],
            unpublished_count=unpublished,
            brand_services=(brand.get("primary_services") or "").strip(),
            brand_areas=(brand.get("service_area") or "").strip(),
            brand_name=(brand.get("display_name") or "").strip(),
            brand_industry=(brand.get("industry") or "").strip(),
            brand_website=(brand.get("website") or "").strip(),
            brand_voice=(brand.get("brand_voice") or "").strip(),
            brand_target_audience=(brand.get("target_audience") or "").strip(),
            brand_tagline=(brand.get("tagline") or "").strip(),
            brand_phone=(brand.get("phone") or brand.get("business_phone") or "").strip(),
            brand_wp_username=(brand.get("wp_username") or "").strip(),
            brand_active_offers=(brand.get("active_offers") or "").strip(),
            brand_logo_url=client_portal_module._site_builder_brand_logo_url(brand),
            brand_logo_path=(brand.get("logo_path") or "").strip(),
            builder_brand_colors=brand_palette,
            builder_primary_color=brand_primary_color,
            builder_accent_color=brand_accent_color,
            brand_font_heading=(brand.get("font_heading") or "").strip(),
            brand_font_body=(brand.get("font_body") or "").strip(),
            google_font_choices=client_portal_module.GOOGLE_FONT_CHOICES,
            font_pair_choices=client_portal_module.SITE_BUILDER_FONT_PAIR_CHOICES,
            font_groups=client_portal_module.SITE_BUILDER_FONT_GROUPS,
            font_preview_stylesheets=client_portal_module.SITE_BUILDER_FONT_PREVIEW_STYLESHEETS,
            editor_font_family_options=client_portal_module.SITE_BUILDER_EDITOR_FONT_OPTIONS,
            image_slots=client_portal_module._SITE_BUILDER_INTAKE_IMAGE_SLOTS,
            site_templates=site_templates,
            default_site_template_id=(default_site_template or {}).get("id") or 0,
            wp_admin_url=client_portal_module._site_builder_wp_admin_url(brand),
            gsc_connected=False,
            gsc_needs_property=False,
        )

    if on_render:
        start_background_appointment_runner(app)

    return app


app = create_app()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "true").lower() == "true"
    app.run(host="0.0.0.0", port=port, debug=debug)
