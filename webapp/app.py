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
    app.config["DATABASE_PATH"] = os.environ.get(
        "DATABASE_PATH", str(BASE_DIR / "data" / "database" / "webapp.db")
    )

    # Initialize DB first so we can load settings from it
    db = WebDB(app.config["DATABASE_PATH"])
    db.init()
    app.db = db

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

    # Register blueprints
    app.register_blueprint(google_bp, url_prefix="/oauth/google")
    app.register_blueprint(meta_bp, url_prefix="/oauth/meta")
    app.register_blueprint(jobs_bp, url_prefix="/jobs")

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
        stats = {
            "total_brands": len(brands),
            "connected_ga4": sum(1 for b in brands if b.get("ga4_property_id")),
            "connected_meta": sum(1 for b in brands if b.get("meta_ad_account_id")),
            "reports_this_month": db.count_reports_for_month(datetime.now().strftime("%Y-%m")),
        }
        recent_reports = db.get_recent_reports(limit=10)
        return render_template("dashboard.html", brands=brands, stats=stats, recent_reports=recent_reports)

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
        import_dir = Path(BASE_DIR / "data" / "imports" / brand["slug"] / active_month)
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
        return render_template(
            "brands/detail.html",
            brand=brand,
            connections=connections,
            reports=reports,
            contacts=contacts,
            active_month=active_month,
            csv_status=csv_status,
        )

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

        import_dir = Path(BASE_DIR / "data" / "imports" / slug / month)
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
        wp_settings = {
            "wp_url": db.get_setting("wp_url", ""),
            "wp_user": db.get_setting("wp_user", ""),
        }
        return render_template("settings.html", wp_settings=wp_settings)

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
