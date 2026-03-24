"""
Ad Agency Analytics - Web Admin Application

Flask web app for managing client brands, API connections,
report generation, email delivery, and WordPress publishing.
"""
import os
import json
import secrets
from pathlib import Path
from datetime import datetime, timedelta
from functools import wraps

from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, session, jsonify, send_file, abort
)
from flask_wtf.csrf import CSRFProtect
from werkzeug.security import generate_password_hash, check_password_hash

from webapp.database import WebDB
from webapp.oauth_google import google_bp
from webapp.oauth_meta import meta_bp
from webapp.client_oauth_google import client_google_bp
from webapp.client_oauth_meta import client_meta_bp
from webapp.jobs import jobs_bp
from webapp.client_portal import client_bp

BASE_DIR = Path(__file__).parent.parent


def create_app():
    app = Flask(
        __name__,
        template_folder=str(BASE_DIR / "webapp" / "templates"),
        static_folder=str(BASE_DIR / "webapp" / "static"),
    )

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
    app.jinja_env.filters["from_json"] = lambda s: _json.loads(s) if s else []

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

    # Create default admin if none exists
    if not db.get_users():
        default_pw = os.environ.get("ADMIN_PASSWORD", "changeme123")
        db.create_user("admin", default_pw, "Admin")
        print(f"Created default admin user (username: admin)")

    # Optional break-glass: reset admin password via env vars
    reset_admin = os.environ.get("RESET_ADMIN_PASSWORD", "").strip().lower() in ("1", "true", "yes")
    admin_pw = os.environ.get("ADMIN_PASSWORD", "").strip()
    if reset_admin:
        if not admin_pw:
            print("RESET_ADMIN_PASSWORD is set but ADMIN_PASSWORD is empty; skipping reset")
        else:
            db.create_user("admin", admin_pw, "Admin")
            db.update_password_by_username("admin", admin_pw)
            print("Admin password reset via RESET_ADMIN_PASSWORD")

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
    app.register_blueprint(client_bp)

    # Exempt OAuth callback routes from CSRF (external redirects have no token)
    csrf.exempt(google_bp)
    csrf.exempt(meta_bp)
    csrf.exempt(client_google_bp)
    csrf.exempt(client_meta_bp)

    # ── Auth decorator ──
    def login_required(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if "user_id" not in session:
                return redirect(url_for("login"))
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

    # ── Context processor ──
    @app.context_processor
    def inject_globals():
        user = None
        if "user_id" in session:
            user = db.get_user(session["user_id"])
        return {
            "current_user": user,
            "now": datetime.now(),
            "app_name": "Agency Analytics",
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
        return render_template("brands/list.html", brands=brands)

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
        from webapp.api_bridge import _get_meta_token, _get_page_access_token
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

        # Get page token
        page_token = _get_page_access_token(page_id, user_token)
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
        until_ts = int(_dt(today.year, today.month, today.day).timestamp())

        test_metrics = [
            "page_impressions",
            "page_impressions_organic",
            "page_engaged_users",
            "page_post_engagements",
            "page_fan_adds",
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
                flash("CRM settings saved", "success")
            return redirect(url_for("brand_settings", brand_id=brand_id))
        # Reload brand to get latest data
        brand = db.get_brand(brand_id)
        return render_template("brands/settings.html", brand=brand, app_url=(app.config.get("APP_URL", "") or "").rstrip("/"))

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
        """Pull revenue from Sweep and Go or Jobber and save it."""
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

        else:
            return jsonify({"ok": False, "error": f"Revenue pull not supported for CRM type: {crm_type}"}), 400

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
                app.config["META_APP_ID"] = db.get_setting("meta_app_id", "")
                app.config["META_APP_SECRET"] = db.get_setting("meta_app_secret", app.config["META_APP_SECRET"])
                flash("Meta OAuth credentials saved", "success")
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
                key = request.form.get("openai_api_key", "").strip()
                if key:
                    db.save_setting("openai_api_key", key)
                    app.config["OPENAI_API_KEY"] = db.get_setting("openai_api_key", "")
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
                flash("OpenAI settings saved", "success")
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
            ai_chat_system_prompt=ai_chat_system_prompt,
            branding=branding,
            openai_model=openai_model,
            openai_model_competitor=openai_model_competitor,
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
    # ── Beta Testers Admin ──

    @app.route("/beta")
    @login_required
    def beta_dashboard():
        stats = db.get_beta_stats()
        testers = db.get_beta_testers()
        feedback = db.get_beta_feedback(limit=50)
        fb_summary = db.get_beta_feedback_summary()
        themes = db.get_feedback_themes()
        considerations = db.get_upgrade_considerations()
        upgrade_stats = db.get_upgrade_stats()
        return render_template(
            "beta_admin.html",
            stats=stats,
            testers=testers,
            feedback=feedback,
            fb_summary=fb_summary,
            themes=themes,
            considerations=considerations,
            upgrade_stats=upgrade_stats,
        )

    @app.route("/beta/approve/<int:tester_id>", methods=["POST"])
    @login_required
    def beta_approve(tester_id):
        tester = db.get_beta_tester(tester_id)
        if not tester:
            abort(404)

        import secrets as _secrets
        import re as _re
        temp_password = _secrets.token_urlsafe(10)

        # Create brand for the tester
        slug = _re.sub(r'[^a-z0-9]+', '_', (tester["business_name"] or tester["name"]).lower()).strip('_')
        brand_id = db.create_brand({
            "slug": slug,
            "display_name": tester["business_name"] or tester["name"],
            "industry": tester.get("industry") or "general",
            "website": tester.get("website") or "",
        })

        # Create client user (inactive until manually activated)
        client_user_id = db.create_client_user(brand_id, tester["email"], temp_password, tester["name"])

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
            ("pest_control", "Pest Control"),
            ("cleaning", "Cleaning Services"),
            ("general_contracting", "General Contracting"),
            ("painting", "Painting"),
            ("garage_door", "Garage Door"),
            ("foundation_repair", "Foundation Repair"),
            ("water_damage", "Water Damage Restoration"),
            ("pet_waste_removal", "Pet Waste Removal"),
        ]

    return app


app = create_app()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "true").lower() == "true"
    app.run(host="0.0.0.0", port=port, debug=debug)
