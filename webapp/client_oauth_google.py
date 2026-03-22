"""
Client-facing Google OAuth2 flow.

The connect route initiates the OAuth flow using the admin callback URL
(already whitelisted). The admin callback in oauth_google.py detects the
client flow via a session flag and branches accordingly.
"""
from urllib.parse import urlencode

from flask import Blueprint, request, redirect, session, flash, url_for, current_app

client_google_bp = Blueprint("client_google_oauth", __name__)

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"

SCOPES = [
    "https://www.googleapis.com/auth/analytics.readonly",
    "https://www.googleapis.com/auth/webmasters.readonly",
    "https://www.googleapis.com/auth/adwords",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/business.manage",
]


def _client_required():
    """Return redirect if not logged in as client, else None."""
    if "client_user_id" not in session:
        return redirect(url_for("client.client_login"))
    return None


@client_google_bp.route("/connect")
def connect():
    redir = _client_required()
    if redir:
        return redir

    brand_id = session.get("client_brand_id")
    db = current_app.db
    brand = db.get_brand(brand_id)
    if not brand:
        flash("Brand not found.", "error")
        return redirect(url_for("client.client_settings"))

    client_id = (db.get_setting("google_client_id", "") or current_app.config.get("GOOGLE_CLIENT_ID", "")).strip()
    client_secret = (db.get_setting("google_client_secret", "") or current_app.config.get("GOOGLE_CLIENT_SECRET", "")).strip()
    if not client_id or not client_secret:
        flash("Google OAuth is not configured yet. Please contact your account manager.", "error")
        return redirect(url_for("client.client_settings"))

    # Use the ADMIN callback URL (already whitelisted in Google Cloud Console)
    callback_url = current_app.config["APP_URL"].rstrip("/") + url_for("google_oauth.callback")
    session["google_oauth_brand_id"] = brand_id
    session["google_oauth_source"] = "client"

    params = {
        "client_id": client_id,
        "redirect_uri": callback_url,
        "response_type": "code",
        "scope": " ".join(SCOPES),
        "access_type": "offline",
        "prompt": "consent",
        "state": str(brand_id),
    }
    return redirect(f"{GOOGLE_AUTH_URL}?{urlencode(params)}")


# NOTE: No /callback route here. The admin callback at /oauth/google/callback
# handles both flows, checking session["google_oauth_source"] to branch.


@client_google_bp.route("/select-properties", methods=["POST"])
def select_properties():
    redir = _client_required()
    if redir:
        return redir

    brand_id = session.pop("client_google_pick_brand_id", None)
    session.pop("client_google_pick_ga4", None)
    session.pop("client_google_pick_gsc", None)

    if not brand_id:
        flash("Session expired, try connecting again.", "error")
        return redirect(url_for("client.client_settings"))

    db = current_app.db
    ga4_property_id = request.form.get("ga4_property_id", "").strip()
    gsc_site_url = request.form.get("gsc_site_url", "").strip()

    if ga4_property_id:
        db.update_brand_api_field(brand_id, "ga4_property_id", ga4_property_id)
    if gsc_site_url:
        db.update_brand_api_field(brand_id, "gsc_site_url", gsc_site_url)

    # Auto-setup Drive folders if folder ID was already saved
    brand = db.get_brand(brand_id)
    folder_id = (brand.get("google_drive_folder_id") or "").strip()
    if folder_id:
        from webapp.google_drive import setup_brand_drive
        result = setup_brand_drive(db, brand_id)
        if result.get("ok"):
            flash("Google account connected and Drive folders created.", "success")
        else:
            flash("Google account connected. Drive folder setup pending - try saving Drive settings again.", "warning")
    else:
        flash("Google account connected and properties selected.", "success")
    return redirect(url_for("client.client_settings"))


@client_google_bp.route("/disconnect", methods=["POST"])
def disconnect():
    redir = _client_required()
    if redir:
        return redir

    brand_id = session.get("client_brand_id")
    if not brand_id:
        flash("No brand found.", "error")
        return redirect(url_for("client.client_settings"))

    db = current_app.db
    db.disconnect_platform(brand_id, "google")
    flash("Google account disconnected.", "success")
    return redirect(url_for("client.client_settings"))
