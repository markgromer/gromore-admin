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

        elif section == "branding":
            brand_colors = request.form.get("brand_colors", "")[:200].strip()
            db.update_brand_text_field(brand_id, "brand_colors", brand_colors)
            flash("Brand colors saved.", "success")

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


# ── Logo Upload ──

@client_bp.route("/upload-logo", methods=["POST"])
@client_login_required
def client_upload_logo():
    from pathlib import Path
    from flask import current_app
    from werkzeug.utils import secure_filename

    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    f = request.files.get("logo")
    if not f or not f.filename:
        flash("No file selected.", "error")
        return redirect(url_for("client.client_my_business"))

    ALLOWED_EXT = {"png", "jpg", "jpeg", "svg", "webp"}
    ext = f.filename.rsplit(".", 1)[-1].lower() if "." in f.filename else ""
    if ext not in ALLOWED_EXT:
        flash("Invalid file type. Use PNG, JPG, SVG, or WebP.", "error")
        return redirect(url_for("client.client_my_business"))

    # 5MB limit
    f.seek(0, 2)
    size = f.tell()
    f.seek(0)
    if size > 5 * 1024 * 1024:
        flash("File too large. Maximum 5MB.", "error")
        return redirect(url_for("client.client_my_business"))

    uploads_dir = Path(current_app.config.get("UPLOADS_DIR", "data/uploads"))
    logo_dir = uploads_dir / "logos" / str(brand_id)
    logo_dir.mkdir(parents=True, exist_ok=True)

    filename = secure_filename(f"logo_{brand_id}.{ext}")
    filepath = logo_dir / filename
    f.save(str(filepath))

    # Store relative path: logos/<brand_id>/logo_<id>.<ext>
    rel_path = f"logos/{brand_id}/{filename}"
    db.update_brand_text_field(brand_id, "logo_path", rel_path)
    flash("Logo uploaded.", "success")
    return redirect(url_for("client.client_my_business"))


@client_bp.route("/uploads/<path:filename>")
@client_login_required
def client_serve_upload(filename):
    from pathlib import Path
    from flask import current_app, send_from_directory

    uploads_dir = Path(current_app.config.get("UPLOADS_DIR", "data/uploads"))
    return send_from_directory(str(uploads_dir), filename)


# ── Creative Center ──

@client_bp.route("/creative")
@client_login_required
def client_creative():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    return render_template(
        "client_creative.html",
        brand=brand,
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


@client_bp.route("/creative/generate", methods=["POST"])
@client_login_required
def client_creative_generate():
    from pathlib import Path
    from flask import current_app

    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify({"error": "Brand not found"}), 404

    # Get inputs
    image_file = request.files.get("image")
    ad_copy_headline = request.form.get("headline", "").strip()[:90]
    ad_copy_body = request.form.get("body_text", "").strip()[:150]
    cta_text = request.form.get("cta_text", "").strip()[:30]
    ad_format = request.form.get("ad_format", "facebook_feed")
    description = request.form.get("image_description", "").strip()[:500]
    color_scheme = request.form.get("color_scheme", "auto")

    if not image_file or not image_file.filename:
        return jsonify({"error": "Please upload a background image."}), 400

    if not ad_copy_headline:
        return jsonify({"error": "Headline is required."}), 400

    # Validate image
    ext = image_file.filename.rsplit(".", 1)[-1].lower() if "." in image_file.filename else ""
    if ext not in {"png", "jpg", "jpeg", "webp"}:
        return jsonify({"error": "Image must be PNG, JPG, or WebP."}), 400

    image_file.seek(0, 2)
    if image_file.tell() > 10 * 1024 * 1024:
        return jsonify({"error": "Image too large. Max 10MB."}), 400
    image_file.seek(0)

    # Format dimensions
    FORMAT_SIZES = {
        "facebook_feed": (1200, 628),
        "facebook_story": (1080, 1920),
        "instagram_feed": (1080, 1080),
        "instagram_story": (1080, 1920),
        "google_display_landscape": (1200, 628),
        "google_display_square": (1200, 1200),
    }
    target_size = FORMAT_SIZES.get(ad_format, (1200, 628))

    try:
        from PIL import Image, ImageDraw, ImageFont, ImageFilter
        import io
        import uuid

        # Open and resize the background image
        bg = Image.open(image_file).convert("RGBA")
        bg = _fit_cover(bg, target_size)

        # Darken bottom portion for text readability
        overlay = Image.new("RGBA", target_size, (0, 0, 0, 0))
        draw_overlay = ImageDraw.Draw(overlay)
        h = target_size[1]
        for y in range(h // 2, h):
            alpha = int(180 * (y - h // 2) / (h // 2))
            draw_overlay.rectangle([0, y, target_size[0], y + 1], fill=(0, 0, 0, alpha))
        bg = Image.alpha_composite(bg, overlay)

        # Load logo if available
        logo_img = None
        if brand.get("logo_path"):
            uploads_dir = Path(current_app.config.get("UPLOADS_DIR", "data/uploads"))
            logo_file = uploads_dir / brand["logo_path"]
            if logo_file.exists():
                try:
                    logo_img = Image.open(str(logo_file)).convert("RGBA")
                    # Scale logo to ~12% of image width
                    logo_w = int(target_size[0] * 0.12)
                    ratio = logo_w / logo_img.width
                    logo_h = int(logo_img.height * ratio)
                    logo_img = logo_img.resize((logo_w, logo_h), Image.LANCZOS)
                except Exception:
                    logo_img = None

        # Draw text
        draw = ImageDraw.Draw(bg)
        w, h = target_size
        margin = int(w * 0.06)

        # Try to load a font, fall back to default
        font_headline = _get_font(int(h * 0.065), bold=True)
        font_body = _get_font(int(h * 0.038))
        font_cta = _get_font(int(h * 0.04), bold=True)

        # Headline
        y_cursor = int(h * 0.62)
        _draw_text_wrapped(draw, ad_copy_headline, margin, y_cursor, w - margin * 2, font_headline, fill="white")
        headline_lines = _count_lines(ad_copy_headline, w - margin * 2, font_headline)
        y_cursor += int(headline_lines * _font_size(font_headline) * 1.3) + 8

        # Body text
        if ad_copy_body:
            _draw_text_wrapped(draw, ad_copy_body, margin, y_cursor, w - margin * 2, font_body, fill=(220, 220, 220))
            body_lines = _count_lines(ad_copy_body, w - margin * 2, font_body)
            y_cursor += int(body_lines * _font_size(font_body) * 1.3) + 12

        # CTA button
        if cta_text:
            cta_bbox = draw.textbbox((0, 0), cta_text, font=font_cta)
            cta_w = cta_bbox[2] - cta_bbox[0] + 36
            cta_h = cta_bbox[3] - cta_bbox[1] + 20
            cta_x = margin
            cta_y = y_cursor
            # Draw rounded CTA button
            draw.rounded_rectangle([cta_x, cta_y, cta_x + cta_w, cta_y + cta_h], radius=8, fill=(99, 102, 241))
            draw.text((cta_x + 18, cta_y + 10), cta_text, fill="white", font=font_cta)

        # Place logo (top-left or top-right)
        if logo_img:
            logo_margin = int(w * 0.04)
            bg.paste(logo_img, (logo_margin, logo_margin), logo_img)

        # Save output
        output_dir = Path(current_app.config.get("UPLOADS_DIR", "data/uploads")) / "creatives" / str(brand_id)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_name = f"creative_{uuid.uuid4().hex[:8]}.png"
        output_path = output_dir / output_name

        final = bg.convert("RGB")
        final.save(str(output_path), "PNG", quality=95)

        rel_path = f"creatives/{brand_id}/{output_name}"
        return jsonify({
            "image_url": url_for("client.client_serve_upload", filename=rel_path),
            "filename": output_name,
        })

    except Exception as e:
        return jsonify({"error": f"Failed to generate creative: {str(e)}"}), 500


@client_bp.route("/creative/ai-copy", methods=["POST"])
@client_login_required
def client_creative_ai_copy():
    """Use AI to generate ad copy from an image description."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify({"error": "Brand not found"}), 404

    description = request.form.get("description", "").strip()
    ad_format = request.form.get("ad_format", "facebook_feed")
    if not description:
        return jsonify({"error": "Please describe the image."}), 400

    # Get API key - brand's own key first, then system key
    api_key = (brand.get("openai_api_key") or "").strip()
    if not api_key:
        from flask import current_app
        api_key = current_app.config.get("OPENAI_API_KEY", "")
    if not api_key:
        return jsonify({"error": "No OpenAI API key configured. Add one in Connections."}), 400

    model = (brand.get("openai_model") or "").strip() or "gpt-4o-mini"

    prompt = f"""Generate ad copy for a {ad_format.replace('_', ' ')} ad creative.

Brand: {brand.get('display_name', '')}
Industry: {brand.get('industry', '')}
Brand Voice: {brand.get('brand_voice', 'professional and friendly')}
Active Offers: {brand.get('active_offers', 'none specified')}
Image Description: {description}

Return JSON only with these fields:
- headline: max 40 characters, punchy and attention-grabbing
- body_text: max 125 characters, supports the headline, includes value proposition
- cta_text: max 20 characters, action-oriented button text (e.g. "Get Your Quote", "Book Now", "Learn More")

JSON only, no markdown."""

    import requests as req
    try:
        resp = req.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"model": model, "messages": [{"role": "user", "content": prompt}], "temperature": 0.7},
            timeout=30,
        )
        if resp.status_code != 200:
            return jsonify({"error": "AI request failed. Check your API key."}), 500
        content = resp.json()["choices"][0]["message"]["content"]
        # Strip markdown fences if present
        content = content.strip()
        if content.startswith("```"):
            content = content.split("\n", 1)[1] if "\n" in content else content[3:]
            if content.endswith("```"):
                content = content[:-3]
        import json as _json
        data = _json.loads(content)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": f"AI generation failed: {str(e)}"}), 500


# ── Creative helpers ──

def _fit_cover(img, target_size):
    """Resize and crop image to cover target_size (center crop)."""
    from PIL import Image
    tw, th = target_size
    iw, ih = img.size
    scale = max(tw / iw, th / ih)
    new_w, new_h = int(iw * scale), int(ih * scale)
    img = img.resize((new_w, new_h), Image.LANCZOS)
    left = (new_w - tw) // 2
    top = (new_h - th) // 2
    return img.crop((left, top, left + tw, top + th))


def _get_font(size, bold=False):
    """Try to load a system font, fall back to Pillow default."""
    from PIL import ImageFont
    import os
    # Common font paths by name and full Linux paths
    candidates = []
    if bold:
        candidates = [
            "arialbd.ttf", "Arial Bold.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/TTF/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
            "DejaVuSans-Bold.ttf", "LiberationSans-Bold.ttf",
        ]
    else:
        candidates = [
            "arial.ttf", "Arial.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/TTF/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
            "DejaVuSans.ttf", "LiberationSans-Regular.ttf",
        ]
    for name in candidates:
        try:
            f = ImageFont.truetype(name, size)
            f._fallback_size = size  # stash size for our helpers
            return f
        except (OSError, IOError):
            continue
    # Last resort: default bitmap font
    try:
        f = ImageFont.load_default(size=size)
    except TypeError:
        f = ImageFont.load_default()
    f._fallback_size = size
    return f


def _font_size(font):
    """Get the effective font size, works with both truetype and default fonts."""
    if hasattr(font, 'size') and font.size:
        return font.size
    return getattr(font, '_fallback_size', 16)


def _draw_text_wrapped(draw, text, x, y, max_width, font, fill="white"):
    """Draw text wrapping at max_width."""
    words = text.split()
    lines = []
    current = ""
    for word in words:
        test = f"{current} {word}".strip()
        bbox = draw.textbbox((0, 0), test, font=font)
        if bbox[2] - bbox[0] <= max_width:
            current = test
        else:
            if current:
                lines.append(current)
            current = word
    if current:
        lines.append(current)

    for line in lines:
        draw.text((x, y), line, fill=fill, font=font)
        y += int(_font_size(font) * 1.3)


def _count_lines(text, max_width, font):
    """Estimate number of wrapped lines."""
    from PIL import ImageDraw, Image
    tmp = Image.new("RGB", (1, 1))
    draw = ImageDraw.Draw(tmp)
    words = text.split()
    lines = 1
    current = ""
    for word in words:
        test = f"{current} {word}".strip()
        bbox = draw.textbbox((0, 0), test, font=font)
        if bbox[2] - bbox[0] <= max_width:
            current = test
        else:
            lines += 1
            current = word
    return lines


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


@client_bp.route("/settings/openai", methods=["POST"])
@client_login_required
def client_save_openai():
    db = _get_db()
    brand_id = session["client_brand_id"]

    api_key = request.form.get("openai_api_key", "").strip()
    model = request.form.get("openai_model", "").strip()

    ALLOWED_MODELS = {
        "", "gpt-4o-mini", "gpt-4o", "gpt-4-turbo", "gpt-4.1-mini", "gpt-4.1",
        "o3-mini", "o4-mini",
    }
    if model not in ALLOWED_MODELS:
        model = "gpt-4o-mini"

    # Only update key if user actually entered something (don't blank it on empty submit)
    if api_key:
        if not api_key.startswith("sk-"):
            flash("Invalid API key format. OpenAI keys start with sk-", "error")
            return redirect(url_for("client.client_settings"))
        db.update_brand_text_field(brand_id, "openai_api_key", api_key)

    db.update_brand_text_field(brand_id, "openai_model", model)
    flash("AI settings saved.", "success")
    return redirect(url_for("client.client_settings"))


# ── Context processor ──

@client_bp.context_processor
def inject_client_globals():
    return {
        "client_user": session.get("client_name"),
        "client_brand": session.get("client_brand_name"),
        "now": datetime.now(),
    }


# ── Help Center ──

@client_bp.route("/help")
@client_login_required
def client_help():
    topic = request.args.get("topic", "")
    return render_template(
        "client_help.html",
        active_topic=topic,
        brand_name=session.get("client_brand_name", ""),
    )


# ── Helper ──

def _get_db():
    from flask import current_app
    return current_app.db
