"""
Client Portal Blueprint

Separate login and dashboard for clients (brand owners) to see their
ad performance, understand what the numbers mean, and get step-by-step
action instructions for improving their ads.
"""
import os
from functools import wraps
from datetime import datetime

from flask import (
    Blueprint, render_template, request, redirect,
    url_for, flash, session, abort,
)

client_bp = Blueprint(
    "client",
    __name__,
    template_folder="templates/client",
    url_prefix="/client",
)


def client_login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "client_user_id" not in session:
            return redirect(url_for("client.client_login"))
        return f(*args, **kwargs)
    return decorated


# ── Auth ──

@client_bp.route("/login", methods=["GET", "POST"])
def client_login():
    if request.method == "POST":
        email = request.form.get("email", "").strip()
        password = request.form.get("password", "")
        db = _get_db()
        user = db.authenticate_client(email, password)
        if user:
            session["client_user_id"] = user["id"]
            session["client_brand_id"] = user["brand_id"]
            session["client_name"] = user["display_name"]
            session["client_brand_name"] = user["brand_name"]
            db.update_client_user_login(user["id"])
            return redirect(url_for("client.client_dashboard"))
        flash("Invalid email or password", "error")
    return render_template("client_login.html")


@client_bp.route("/logout")
def client_logout():
    session.pop("client_user_id", None)
    session.pop("client_brand_id", None)
    session.pop("client_name", None)
    session.pop("client_brand_name", None)
    return redirect(url_for("client.client_login"))


# ── Dashboard ──

@client_bp.route("/")
@client_bp.route("/dashboard")
@client_login_required
def client_dashboard():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        flash("Your account is not linked to an active brand.", "error")
        return redirect(url_for("client.client_logout"))

    month = request.args.get("month") or datetime.now().strftime("%Y-%m")

    analysis = {}
    suggestions = []
    dashboard_data = None
    error = ""

    try:
        from webapp.report_runner import build_analysis_and_suggestions_for_brand
        from webapp.client_advisor import build_client_dashboard

        analysis, suggestions = build_analysis_and_suggestions_for_brand(db, brand, month)
        if analysis:
            dashboard_data = build_client_dashboard(analysis, suggestions, brand)
    except Exception as e:
        error = str(e)

    return render_template(
        "client_dashboard.html",
        brand=brand,
        month=month,
        dashboard=dashboard_data,
        error=error,
        client_name=session.get("client_name", ""),
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


# ── Actions Detail ──

@client_bp.route("/actions")
@client_login_required
def client_actions():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    month = request.args.get("month") or datetime.now().strftime("%Y-%m")

    actions = []
    error = ""

    try:
        from webapp.report_runner import build_analysis_and_suggestions_for_brand
        from webapp.client_advisor import build_client_dashboard

        analysis, suggestions = build_analysis_and_suggestions_for_brand(db, brand, month)
        if analysis:
            data = build_client_dashboard(analysis, suggestions, brand)
            actions = data.get("actions", [])
    except Exception as e:
        error = str(e)

    return render_template(
        "client_actions.html",
        brand=brand,
        month=month,
        actions=actions,
        error=error,
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


# ── Context processor ──

@client_bp.context_processor
def inject_client_globals():
    return {
        "client_user": session.get("client_name"),
        "client_brand": session.get("client_brand_name"),
        "now": datetime.now(),
    }


# ── Helper ──

def _get_db():
    from flask import current_app
    return current_app.db
