"""
Client-facing Meta (Facebook) OAuth2 flow.

Mirrors oauth_meta.py but checks client_user_id in session and redirects
back to the client portal settings page instead of admin brand detail.
"""
from datetime import datetime, timedelta
from urllib.parse import urlencode

from flask import Blueprint, request, redirect, session, flash, url_for, current_app, render_template
import requests

client_meta_bp = Blueprint("client_meta_oauth", __name__)

META_AUTH_URL = "https://www.facebook.com/v21.0/dialog/oauth"
META_TOKEN_URL = "https://graph.facebook.com/v21.0/oauth/access_token"
META_LONG_LIVED_URL = "https://graph.facebook.com/v21.0/oauth/access_token"

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

    callback_url = current_app.config["APP_URL"].rstrip("/") + url_for("client_meta_oauth.callback")
    session["client_meta_oauth_brand_id"] = brand_id

    params = {
        "client_id": app_id,
        "redirect_uri": callback_url,
        "scope": ",".join(SCOPES),
        "response_type": "code",
        "state": str(brand_id),
    }
    return redirect(f"{META_AUTH_URL}?{urlencode(params)}")


@client_meta_bp.route("/callback")
def callback():
    redir = _client_required()
    if redir:
        return redir

    error = request.args.get("error")
    if error:
        desc = request.args.get("error_description", error)
        flash(f"Meta authorization failed: {desc}", "error")
        return redirect(url_for("client.client_settings"))

    code = request.args.get("code")
    brand_id = session.pop("client_meta_oauth_brand_id", None)
    if not code or not brand_id:
        flash("Invalid OAuth callback.", "error")
        return redirect(url_for("client.client_settings"))

    db = current_app.db
    brand = db.get_brand(brand_id)
    if not brand:
        flash("Brand not found.", "error")
        return redirect(url_for("client.client_settings"))

    app_id = (db.get_setting("meta_app_id", "") or current_app.config.get("META_APP_ID", "")).strip()
    app_secret = (db.get_setting("meta_app_secret", "") or current_app.config.get("META_APP_SECRET", "")).strip()

    callback_url = current_app.config["APP_URL"].rstrip("/") + url_for("client_meta_oauth.callback")

    # Exchange code for short-lived token
    token_resp = requests.get(META_TOKEN_URL, params={
        "client_id": app_id,
        "client_secret": app_secret,
        "redirect_uri": callback_url,
        "code": code,
    }, timeout=30)

    if token_resp.status_code != 200:
        flash(f"Token exchange failed: {token_resp.text}", "error")
        return redirect(url_for("client.client_settings"))

    short_token = token_resp.json().get("access_token")

    # Exchange for long-lived token (60 days)
    ll_resp = requests.get(META_LONG_LIVED_URL, params={
        "grant_type": "fb_exchange_token",
        "client_id": app_id,
        "client_secret": app_secret,
        "fb_exchange_token": short_token,
    }, timeout=30)

    if ll_resp.status_code == 200:
        ll_data = ll_resp.json()
        access_token = ll_data.get("access_token", short_token)
        expires_in = ll_data.get("expires_in", 5184000)
    else:
        access_token = short_token
        expires_in = 3600

    expiry = (datetime.now() + timedelta(seconds=expires_in)).isoformat()

    # Fetch ad accounts
    accounts_resp = requests.get(
        "https://graph.facebook.com/v21.0/me/adaccounts",
        params={
            "access_token": access_token,
            "fields": "id,name,account_id,account_status",
            "limit": 50,
        },
        timeout=30,
    )

    ad_accounts = []
    if accounts_resp.status_code == 200:
        ad_accounts = accounts_resp.json().get("data", [])

    session["client_meta_temp_token"] = access_token
    session["client_meta_temp_expiry"] = expiry
    session["client_meta_temp_brand_id"] = brand_id

    if len(ad_accounts) == 1:
        acct = ad_accounts[0]
        _finalize_meta_connection(db, brand_id, access_token, expiry, acct)
        flash(f"Meta ad account connected: {acct.get('name', acct['account_id'])}", "success")
        return redirect(url_for("client.client_settings"))

    return render_template(
        "client/client_meta_pick_account.html",
        brand=brand,
        ad_accounts=ad_accounts,
    )


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
