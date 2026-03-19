"""
Client-facing Meta (Facebook) OAuth2 flow.

The connect route initiates the OAuth flow using the admin callback URL
(already whitelisted). The admin callback in oauth_meta.py detects the
client flow via a session flag and branches accordingly.
"""
from urllib.parse import urlencode

from flask import Blueprint, request, redirect, session, flash, url_for, current_app

client_meta_bp = Blueprint("client_meta_oauth", __name__)

META_AUTH_URL = "https://www.facebook.com/v21.0/dialog/oauth"

SCOPES = [
    "ads_read",
    "ads_management",
    "read_insights",
    "business_management",
]


def _client_required():
    """Return redirect if not logged in as client, else None."""
    if "client_user_id" not in session:
        return redirect(url_for("client.client_login"))
    return None


@client_meta_bp.route("/connect")
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

    app_id = (db.get_setting("meta_app_id", "") or current_app.config.get("META_APP_ID", "")).strip()
    app_secret = (db.get_setting("meta_app_secret", "") or current_app.config.get("META_APP_SECRET", "")).strip()
    if not app_id or not app_secret:
        flash("Meta OAuth is not configured yet. Please contact your account manager.", "error")
        return redirect(url_for("client.client_settings"))

    # Use the ADMIN callback URL (already whitelisted in Facebook App Settings)
    callback_url = current_app.config["APP_URL"].rstrip("/") + url_for("meta_oauth.callback")
    session["meta_oauth_brand_id"] = brand_id
    session["meta_oauth_source"] = "client"

    params = {
        "client_id": app_id,
        "redirect_uri": callback_url,
        "scope": ",".join(SCOPES),
        "response_type": "code",
        "state": str(brand_id),
    }
    return redirect(f"{META_AUTH_URL}?{urlencode(params)}")


# NOTE: No /callback route here. The admin callback at /oauth/meta/callback
# handles both flows, checking session["meta_oauth_source"] to branch.


@client_meta_bp.route("/select-account", methods=["POST"])
def select_account():
    redir = _client_required()
    if redir:
        return redir

    brand_id = session.pop("client_meta_temp_brand_id", None)
    access_token = session.pop("client_meta_temp_token", None)
    expiry = session.pop("client_meta_temp_expiry", None)
    account_id = request.form.get("account_id", "")
    account_name = request.form.get("account_name", "")

    if not brand_id or not access_token or not account_id:
        flash("Session expired, try connecting again.", "error")
        return redirect(url_for("client.client_settings"))

    db = current_app.db
    acct = {"account_id": account_id, "name": account_name}
    _finalize_meta_connection(db, brand_id, access_token, expiry, acct)

    flash(f"Meta ad account connected: {account_name or account_id}", "success")
    return redirect(url_for("client.client_settings"))


@client_meta_bp.route("/disconnect", methods=["POST"])
def disconnect():
    redir = _client_required()
    if redir:
        return redir

    brand_id = session.get("client_brand_id")
    if not brand_id:
        flash("No brand found.", "error")
        return redirect(url_for("client.client_settings"))

    db = current_app.db
    db.disconnect_platform(brand_id, "meta")
    db.update_brand_api_field(brand_id, "meta_ad_account_id", "")
    flash("Meta account disconnected.", "success")
    return redirect(url_for("client.client_settings"))


def _finalize_meta_connection(db, brand_id, access_token, expiry, acct):
    account_id = acct.get("account_id", acct.get("id", "")).replace("act_", "")
    db.upsert_connection(brand_id, "meta", {
        "access_token": access_token,
        "refresh_token": "",
        "token_expiry": expiry,
        "account_id": account_id,
        "account_name": acct.get("name", ""),
        "scopes": ",".join(SCOPES),
    })
    db.update_brand_api_field(brand_id, "meta_ad_account_id", account_id)
