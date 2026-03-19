"""
Client Portal Blueprint

Separate login and dashboard for clients (brand owners) to see their
ad performance, understand what the numbers mean, get step-by-step
action instructions, and manage their ad campaigns directly.
"""
import os
import json
from functools import wraps
from datetime import datetime

from flask import (
    Blueprint, render_template, request, redirect,
    url_for, flash, session, abort, jsonify,
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

    # Dashboard renders instantly; data is fetched async via /dashboard/data
    return render_template(
        "client_dashboard.html",
        brand=brand,
        month=month,
        dashboard=None,
        error="",
        async_load=True,
        client_name=session.get("client_name", ""),
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


@client_bp.route("/dashboard/data")
@client_login_required
def client_dashboard_data():
    """JSON endpoint for async dashboard loading."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify({"error": "Brand not found"}), 404

    month = request.args.get("month") or datetime.now().strftime("%Y-%m")

    try:
        from webapp.report_runner import build_analysis_and_suggestions_for_brand
        from webapp.client_advisor import build_client_dashboard

        analysis, suggestions = build_analysis_and_suggestions_for_brand(db, brand, month)
        if analysis:
            dashboard_data = build_client_dashboard(analysis, suggestions, brand)
            return jsonify({"dashboard": dashboard_data, "error": ""})
        else:
            return jsonify({"dashboard": None, "error": "No data available for this month."})
    except Exception as e:
        return jsonify({"dashboard": None, "error": str(e)})


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


# ── Ad Builder ──

@client_bp.route("/ad-builder")
@client_login_required
def client_ad_builder():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    month = request.args.get("month") or datetime.now().strftime("%Y-%m")

    has_data = False
    error = ""
    try:
        from webapp.report_runner import build_analysis_and_suggestions_for_brand
        analysis, _ = build_analysis_and_suggestions_for_brand(db, brand, month)
        has_data = bool(analysis)
    except Exception as e:
        error = str(e)

    return render_template(
        "client_ad_builder.html",
        brand=brand,
        month=month,
        has_data=has_data,
        google_ads=None,
        facebook_ads=None,
        error=error,
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


@client_bp.route("/ad-builder/generate", methods=["POST"])
@client_login_required
def client_ad_builder_generate():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    month = request.form.get("month") or datetime.now().strftime("%Y-%m")

    platform = request.form.get("platform", "")
    strategy = request.form.get("strategy", "")

    if platform not in ("google", "facebook"):
        flash("Select a platform.", "error")
        return redirect(url_for("client.client_ad_builder", month=month))

    analysis = None
    error = ""
    try:
        from webapp.report_runner import build_analysis_and_suggestions_for_brand
        analysis, _ = build_analysis_and_suggestions_for_brand(db, brand, month)
    except Exception as e:
        error = str(e)

    if not analysis:
        flash(error or "No data available for this month.", "error")
        return redirect(url_for("client.client_ad_builder", month=month))

    google_ads = None
    facebook_ads = None


    from webapp.ad_builder import generate_google_ads, generate_facebook_ads

    if platform == "google":
        google_ads = generate_google_ads(analysis, brand, strategy)
        if not google_ads:
            flash("AI generation failed. Check that your OpenAI key is configured in Settings.", "error")
            return redirect(url_for("client.client_ad_builder", month=month))
    else:
        facebook_ads = generate_facebook_ads(analysis, brand, strategy)
        if not facebook_ads:
            flash("AI generation failed. Check that your OpenAI key is configured in Settings.", "error")
            return redirect(url_for("client.client_ad_builder", month=month))

    return render_template(
        "client_ad_builder.html",
        brand=brand,
        month=month,
        has_data=True,
        google_ads=google_ads,
        facebook_ads=facebook_ads,
        error="",
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


# ── Campaigns ──

@client_bp.route("/campaigns")
@client_login_required
def client_campaigns():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    month = request.args.get("month") or datetime.now().strftime("%Y-%m")

    from webapp.campaign_manager import list_all_campaigns, get_campaign_recommendations

    campaigns = list_all_campaigns(db, brand, month)
    recommendations = []

    if any(campaigns.values()):
        try:
            recommendations = get_campaign_recommendations(brand, campaigns)
        except Exception:
            pass

    changes = db.get_campaign_changes(brand_id, limit=20)

    return render_template(
        "client_campaigns.html",
        brand=brand,
        month=month,
        campaigns=campaigns,
        recommendations=recommendations,
        changes=changes,
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


@client_bp.route("/campaigns/<platform>/<campaign_id>")
@client_login_required
def client_campaign_detail(platform, campaign_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    month = request.args.get("month") or datetime.now().strftime("%Y-%m")

    if platform not in ("google", "meta"):
        abort(404)

    from webapp.campaign_manager import get_google_campaign_detail, get_meta_campaign_detail

    if platform == "google":
        campaign = get_google_campaign_detail(db, brand, campaign_id, month)
    else:
        campaign = get_meta_campaign_detail(db, brand, campaign_id, month)

    if not campaign:
        flash("Campaign not found or API error.", "error")
        return redirect(url_for("client.client_campaigns"))

    changes = db.get_campaign_changes(brand_id, limit=20)

    return render_template(
        "client_campaign_detail.html",
        brand=brand,
        campaign=campaign,
        platform=platform,
        month=month,
        changes=[c for c in changes if c.get("campaign_id") == campaign_id],
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


@client_bp.route("/campaigns/<platform>/<campaign_id>/status", methods=["POST"])
@client_login_required
def client_campaign_status(platform, campaign_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    new_status = request.form.get("status", "").upper()
    if platform == "google" and new_status not in ("PAUSED", "ENABLED"):
        flash("Invalid status.", "error")
        return redirect(url_for("client.client_campaigns"))
    if platform == "meta" and new_status not in ("PAUSED", "ACTIVE"):
        flash("Invalid status.", "error")
        return redirect(url_for("client.client_campaigns"))

    from webapp.campaign_manager import update_google_campaign_status, update_meta_campaign_status

    changed_by = session.get("client_name", "client")

    if platform == "google":
        result = update_google_campaign_status(db, brand, campaign_id, new_status, changed_by)
    else:
        result = update_meta_campaign_status(db, brand, campaign_id, new_status, changed_by)

    if result.get("success"):
        label = "paused" if new_status in ("PAUSED",) else "enabled"
        flash(f"Campaign {label} successfully.", "success")
    else:
        flash(f"Failed: {result.get('error', 'Unknown error')}", "error")

    return redirect(url_for("client.client_campaign_detail", platform=platform, campaign_id=campaign_id))


@client_bp.route("/campaigns/<platform>/<campaign_id>/budget", methods=["POST"])
@client_login_required
def client_campaign_budget(platform, campaign_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    try:
        new_budget = float(request.form.get("daily_budget", 0))
    except (ValueError, TypeError):
        flash("Invalid budget amount.", "error")
        return redirect(url_for("client.client_campaign_detail", platform=platform, campaign_id=campaign_id))

    if new_budget < 1 or new_budget > 10000:
        flash("Budget must be between $1 and $10,000 per day.", "error")
        return redirect(url_for("client.client_campaign_detail", platform=platform, campaign_id=campaign_id))

    from webapp.campaign_manager import update_google_budget, update_meta_budget

    changed_by = session.get("client_name", "client")

    if platform == "google":
        budget_resource = request.form.get("budget_resource", "")
        result = update_google_budget(db, brand, campaign_id, budget_resource, new_budget, changed_by)
    else:
        result = update_meta_budget(db, brand, campaign_id, new_budget, changed_by)

    if result.get("success"):
        flash(f"Daily budget updated to ${new_budget:.2f}.", "success")
    else:
        flash(f"Failed: {result.get('error', 'Unknown error')}", "error")

    return redirect(url_for("client.client_campaign_detail", platform=platform, campaign_id=campaign_id))


@client_bp.route("/campaigns/google/<campaign_id>/negative-keyword", methods=["POST"])
@client_login_required
def client_add_negative_keyword(campaign_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    keyword = request.form.get("keyword", "").strip()
    match_type = request.form.get("match_type", "BROAD").upper()

    if not keyword:
        flash("Keyword cannot be empty.", "error")
        return redirect(url_for("client.client_campaign_detail", platform="google", campaign_id=campaign_id))

    if match_type not in ("BROAD", "PHRASE", "EXACT"):
        match_type = "BROAD"

    from webapp.campaign_manager import add_google_negative_keyword

    changed_by = session.get("client_name", "client")
    result = add_google_negative_keyword(db, brand, campaign_id, keyword, match_type, changed_by)

    if result.get("success"):
        flash(f'Negative keyword "{keyword}" added.', "success")
    else:
        flash(f"Failed: {result.get('error', 'Unknown error')}", "error")

    return redirect(url_for("client.client_campaign_detail", platform="google", campaign_id=campaign_id))


# ── Campaign Creator ──

@client_bp.route("/campaigns/new")
@client_login_required
def client_campaign_create():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    connections = db.get_brand_connections(brand_id)
    has_google = (connections.get("google", {}).get("status") == "connected"
                  and brand.get("google_ads_customer_id"))
    has_meta = (connections.get("meta", {}).get("status") == "connected"
                and brand.get("meta_ad_account_id"))

    return render_template(
        "client_campaign_create.html",
        brand=brand,
        has_google=has_google,
        has_meta=has_meta,
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


@client_bp.route("/campaigns/new/generate", methods=["POST"])
@client_login_required
def client_campaign_generate():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify({"success": False, "error": "Brand not found"})

    service = request.form.get("service", "").strip()
    location = request.form.get("location", "").strip()
    monthly_budget = request.form.get("monthly_budget", "0").strip()
    platform = request.form.get("platform", "").strip()
    notes = request.form.get("notes", "").strip()

    if not service or not location or not monthly_budget or not platform:
        return jsonify({"success": False, "error": "All fields are required"})

    try:
        monthly_budget = float(monthly_budget)
        if monthly_budget < 100:
            return jsonify({"success": False, "error": "Minimum monthly budget is $100"})
    except (ValueError, TypeError):
        return jsonify({"success": False, "error": "Invalid budget"})

    from webapp.campaign_manager import generate_campaign_plan

    result = generate_campaign_plan(brand, service, location, monthly_budget, platform, notes)
    return jsonify(result)


@client_bp.route("/campaigns/new/launch", methods=["POST"])
@client_login_required
def client_campaign_launch():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify({"success": False, "error": "Brand not found"})

    plan_json = request.form.get("plan", "")
    if not plan_json:
        return jsonify({"success": False, "error": "No campaign plan provided"})

    try:
        plan = json.loads(plan_json)
    except json.JSONDecodeError:
        return jsonify({"success": False, "error": "Invalid plan data"})

    platform = plan.get("platform", "")
    changed_by = session.get("client_name", "client")

    from webapp.campaign_manager import launch_google_campaign, launch_meta_campaign

    if platform == "google":
        result = launch_google_campaign(db, brand, plan, changed_by)
    elif platform == "meta":
        result = launch_meta_campaign(db, brand, plan, changed_by)
    else:
        return jsonify({"success": False, "error": "Invalid platform"})

    return jsonify(result)


# ── Settings / Connections ──

@client_bp.route("/my-business", methods=["GET", "POST"])
@client_login_required
def client_my_business():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    if request.method == "POST":
        section = request.form.get("section", "")

        if section == "voice":
            # Guardrails: cap text fields to reasonable lengths
            brand_voice = request.form.get("brand_voice", "")[:2000].strip()
            active_offers = request.form.get("active_offers", "")[:1000].strip()
            target_audience = request.form.get("target_audience", "")[:2000].strip()
            competitors = request.form.get("competitors", "")[:1000].strip()
            reporting_notes = request.form.get("reporting_notes", "")[:1000].strip()

            db.update_brand_text_field(brand_id, "brand_voice", brand_voice)
            db.update_brand_text_field(brand_id, "active_offers", active_offers)
            db.update_brand_text_field(brand_id, "target_audience", target_audience)
            db.update_brand_text_field(brand_id, "competitors", competitors)
            db.update_brand_text_field(brand_id, "reporting_notes", reporting_notes)
            flash("Brand profile updated.", "success")

        elif section == "targets":
            # Guardrails: clamp KPI targets to sane ranges
            cpa_raw = request.form.get("kpi_target_cpa", "0")
            leads_raw = request.form.get("kpi_target_leads", "0")
            roas_raw = request.form.get("kpi_target_roas", "0")
            call_num = request.form.get("call_tracking_number", "")[:30].strip()

            db.update_brand_number_field(brand_id, "kpi_target_cpa", cpa_raw)
            db.update_brand_number_field(brand_id, "kpi_target_leads", leads_raw)
            db.update_brand_number_field(brand_id, "kpi_target_roas", roas_raw)
            db.update_brand_text_field(brand_id, "call_tracking_number", call_num)
            flash("Performance targets saved.", "success")

        return redirect(url_for("client.client_my_business"))

    # Reload latest
    brand = db.get_brand(brand_id)

    # Calculate completion score for the profile
    profile_fields = [
        brand.get("brand_voice"),
        brand.get("active_offers"),
        brand.get("target_audience"),
        brand.get("competitors"),
    ]
    target_fields = [
        brand.get("kpi_target_cpa") and float(brand.get("kpi_target_cpa", 0)) > 0,
        brand.get("kpi_target_leads") and int(float(brand.get("kpi_target_leads", 0))) > 0,
    ]
    filled = sum(1 for f in profile_fields if f and str(f).strip()) + sum(1 for f in target_fields if f)
    profile_score = round(filled / (len(profile_fields) + len(target_fields)) * 100)

    return render_template(
        "client_my_business.html",
        brand=brand,
        profile_score=profile_score,
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


@client_bp.route("/settings")
@client_login_required
def client_settings():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    connections = db.get_brand_connections(brand_id)
    google_conn = connections.get("google", {})
    meta_conn = connections.get("meta", {})

    return render_template(
        "client_settings.html",
        brand=brand,
        google_connected=(google_conn.get("status") == "connected"),
        meta_connected=(meta_conn.get("status") == "connected"),
        google_conn=google_conn,
        meta_conn=meta_conn,
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


@client_bp.route("/settings/ads-id", methods=["POST"])
@client_login_required
def client_save_ads_id():
    db = _get_db()
    brand_id = session["client_brand_id"]

    raw = request.form.get("google_ads_customer_id", "").strip()
    # Keep only digits and dashes
    cleaned = "".join(c for c in raw if c.isdigit() or c == "-")
    db.update_brand_api_field(brand_id, "google_ads_customer_id", cleaned)
    flash("Google Ads Customer ID saved.", "success")
    return redirect(url_for("client.client_settings"))


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
