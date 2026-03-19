"""
Client-facing Google OAuth2 flow.

Mirrors oauth_google.py but checks client_user_id in session and redirects
back to the client portal settings page instead of admin brand detail.
"""
from datetime import datetime, timedelta
from urllib.parse import urlencode

from flask import Blueprint, request, redirect, session, flash, url_for, current_app, render_template
import requests

client_google_bp = Blueprint("client_google_oauth", __name__)

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"

SCOPES = [
    "https://www.googleapis.com/auth/analytics.readonly",
    "https://www.googleapis.com/auth/webmasters.readonly",
    "https://www.googleapis.com/auth/adwords",
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

    callback_url = current_app.config["APP_URL"].rstrip("/") + url_for("client_google_oauth.callback")
    session["client_google_oauth_brand_id"] = brand_id

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


@client_google_bp.route("/callback")
def callback():
    redir = _client_required()
    if redir:
        return redir

    error = request.args.get("error")
    if error:
        flash(f"Google authorization failed: {error}", "error")
        return redirect(url_for("client.client_settings"))

    code = request.args.get("code")
    brand_id = session.pop("client_google_oauth_brand_id", None)
    if not code or not brand_id:
        flash("Invalid OAuth callback.", "error")
        return redirect(url_for("client.client_settings"))

    db = current_app.db
    brand = db.get_brand(brand_id)
    if not brand:
        flash("Brand not found.", "error")
        return redirect(url_for("client.client_settings"))

    callback_url = current_app.config["APP_URL"].rstrip("/") + url_for("client_google_oauth.callback")
    client_id = (db.get_setting("google_client_id", "") or current_app.config.get("GOOGLE_CLIENT_ID", "")).strip()
    client_secret = (db.get_setting("google_client_secret", "") or current_app.config.get("GOOGLE_CLIENT_SECRET", "")).strip()

    token_resp = requests.post(GOOGLE_TOKEN_URL, data={
        "code": code,
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": callback_url,
        "grant_type": "authorization_code",
    }, timeout=30)

    if token_resp.status_code != 200:
        flash(f"Token exchange failed: {token_resp.text}", "error")
        return redirect(url_for("client.client_settings"))

    tokens = token_resp.json()
    expiry = ""
    if "expires_in" in tokens:
        expiry = (datetime.now() + timedelta(seconds=tokens["expires_in"])).isoformat()

    db.upsert_connection(brand_id, "google", {
        "access_token": tokens.get("access_token", ""),
        "refresh_token": tokens.get("refresh_token", ""),
        "token_expiry": expiry,
        "scopes": " ".join(SCOPES),
    })

    access_token = tokens.get("access_token", "")
    ga4_properties = _fetch_ga4_properties(access_token)
    gsc_sites = _fetch_gsc_sites(access_token)

    session["client_google_pick_brand_id"] = brand_id
    session["client_google_pick_ga4"] = ga4_properties
    session["client_google_pick_gsc"] = gsc_sites

    return render_template(
        "client/client_google_pick_properties.html",
        brand=brand,
        ga4_properties=ga4_properties,
        gsc_sites=gsc_sites,
    )


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


# ── Helpers ──

def _fetch_ga4_properties(access_token):
    try:
        resp = requests.get(
            "https://analyticsadmin.googleapis.com/v1beta/accountSummaries",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=15,
        )
        if resp.status_code != 200:
            return []
        data = resp.json()
        properties = []
        for account in data.get("accountSummaries", []):
            account_name = account.get("displayName", "")
            for prop in account.get("propertySummaries", []):
                prop_id = prop.get("property", "").replace("properties/", "")
                properties.append({
                    "property_id": prop_id,
                    "display_name": prop.get("displayName", prop_id),
                    "account_name": account_name,
                })
        return properties
    except Exception:
        return []


def _fetch_gsc_sites(access_token):
    try:
        resp = requests.get(
            "https://searchconsole.googleapis.com/webmasters/v3/sites",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=15,
        )
        if resp.status_code != 200:
            return []
        data = resp.json()
        sites = []
        for entry in data.get("siteEntry", []):
            sites.append({
                "site_url": entry.get("siteUrl", ""),
                "permission_level": entry.get("permissionLevel", ""),
            })
        return sites
    except Exception:
        return []
