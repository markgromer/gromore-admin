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
from webapp.jobs import jobs_bp

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

    # Storage paths for uploads + generated reports
    # Local/dev uses repo folders. Render uses persisted disk under /data.
    if on_render:
        app.config["IMPORTS_DIR"] = os.environ.get("IMPORTS_DIR", "/data/imports")
        app.config["REPORTS_DIR"] = os.environ.get("REPORTS_DIR", "/data/reports")
    else:
        app.config["IMPORTS_DIR"] = os.environ.get("IMPORTS_DIR", str(BASE_DIR / "data" / "imports"))
        app.config["REPORTS_DIR"] = os.environ.get("REPORTS_DIR", str(BASE_DIR / "reports"))

    # Make available to helper modules (report_runner reads these env vars)
    os.environ.setdefault("IMPORTS_DIR", app.config["IMPORTS_DIR"])
    os.environ.setdefault("REPORTS_DIR", app.config["REPORTS_DIR"])

    # Log paths at startup for debugging persistence issues
    print(f"[startup] DATABASE_PATH = {app.config['DATABASE_PATH']}")
    print(f"[startup] IMPORTS_DIR   = {app.config['IMPORTS_DIR']}")
    print(f"[startup] REPORTS_DIR   = {app.config['REPORTS_DIR']}")
    print(f"[startup] on_render     = {on_render}")

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
    app.register_blueprint(jobs_bp, url_prefix="/jobs")

    # Exempt OAuth callback routes from CSRF (external redirects have no token)
    csrf.exempt(google_bp)
    csrf.exempt(meta_bp)

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
        current_url = app.config.get("APP_URL", "")
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
        )

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
            from webapp.ai_assistant import chat_with_jarvis, summarize_analysis_for_ai

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

            assistant_reply = chat_with_jarvis(
                api_key=api_key,
                model=model,
                context=context,
                messages=messages,
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
            from webapp.ai_assistant import generate_jarvis_brief

            analysis, suggestions = build_analysis_and_suggestions_for_brand(db, brand, month)

            model = (
                db.get_setting("openai_model", "").strip()
                or os.environ.get("OPENAI_MODEL", "").strip()
                or app.config.get("OPENAI_MODEL")
                or "gpt-4o-mini"
            )
            internal = generate_jarvis_brief(
                api_key=api_key,
                analysis=analysis,
                suggestions=suggestions,
                variant="internal",
                model=model or None,
            )
            client = generate_jarvis_brief(
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
                db.update_brand_text_field(brand_id, "competitors", request.form.get("competitors", ""))
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
                flash("CRM settings saved", "success")
            return redirect(url_for("brand_settings", brand_id=brand_id))
        # Reload brand to get latest data
        brand = db.get_brand(brand_id)
        return render_template("brands/settings.html", brand=brand)

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
                    flash("OpenAI API key saved", "success")
                else:
                    flash("OpenAI API key unchanged (blank)", "warning")
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
        branding = {
            "agency_name": db.get_setting("agency_name", ""),
            "agency_logo_url": db.get_setting("agency_logo_url", ""),
            "agency_website": db.get_setting("agency_website", ""),
            "agency_color": db.get_setting("agency_color", "#2c3e50"),
        }
        return render_template(
            "settings.html",
            wp_settings=wp_settings,
            openai_configured=openai_configured,
            branding=branding,
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
