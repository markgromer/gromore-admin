"""
Google OAuth2 flow for connecting GA4 and Search Console.

Uses authorization code flow. User clicks "Connect Google" on a brand page,
gets redirected to Google consent screen, comes back with a code, we exchange
it for tokens and store them.
"""
import os
import json
from datetime import datetime, timedelta
from urllib.parse import urlencode

from flask import Blueprint, request, redirect, session, flash, url_for, current_app, render_template
import requests

google_bp = Blueprint("google_oauth", __name__)

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"

SCOPES = [
    "https://www.googleapis.com/auth/analytics.readonly",
    "https://www.googleapis.com/auth/webmasters.readonly",
]


@google_bp.route("/connect/<int:brand_id>")
def connect(brand_id):
    if "user_id" not in session:
        return redirect(url_for("login"))

    db = current_app.db
    brand = db.get_brand(brand_id)
    if not brand:
        flash("Brand not found", "error")
        return redirect(url_for("brands_list"))

    client_id = (db.get_setting("google_client_id", "") or current_app.config.get("GOOGLE_CLIENT_ID", "")).strip()
    client_secret = (db.get_setting("google_client_secret", "") or current_app.config.get("GOOGLE_CLIENT_SECRET", "")).strip()
    if not client_id or not client_secret:
        flash(
            "Google OAuth not configured. Go to Settings to add your Google OAuth Client ID and Client Secret (one-time agency setup).",
            "error",
        )
        return redirect(url_for("brand_detail", brand_id=brand_id))

    callback_url = current_app.config["APP_URL"] + url_for("google_oauth.callback")

    # Store brand_id in session for the callback
    session["google_oauth_brand_id"] = brand_id

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


@google_bp.route("/callback")
def callback():
    if "user_id" not in session:
        return redirect(url_for("login"))

    error = request.args.get("error")
    if error:
        flash(f"Google authorization failed: {error}", "error")
        return redirect(url_for("brands_list"))

    code = request.args.get("code")
    brand_id = session.pop("google_oauth_brand_id", None)
    if not code or not brand_id:
        flash("Invalid OAuth callback", "error")
        return redirect(url_for("brands_list"))

    db = current_app.db
    brand = db.get_brand(brand_id)
    if not brand:
        flash("Brand not found", "error")
        return redirect(url_for("brands_list"))

    # Exchange code for tokens
    callback_url = current_app.config["APP_URL"] + url_for("google_oauth.callback")
    token_resp = requests.post(GOOGLE_TOKEN_URL, data={
        "code": code,
        "client_id": current_app.config["GOOGLE_CLIENT_ID"],
        "client_secret": current_app.config["GOOGLE_CLIENT_SECRET"],
        "redirect_uri": callback_url,
        "grant_type": "authorization_code",
    }, timeout=30)

    if token_resp.status_code != 200:
        flash(f"Token exchange failed: {token_resp.text}", "error")
        return redirect(url_for("brand_detail", brand_id=brand_id))

    tokens = token_resp.json()
    expiry = ""
    if "expires_in" in tokens:
        expiry = (datetime.now() + timedelta(seconds=tokens["expires_in"])).isoformat()

    # Save connection
    db.upsert_connection(brand_id, "google", {
        "access_token": tokens.get("access_token", ""),
        "refresh_token": tokens.get("refresh_token", ""),
        "token_expiry": expiry,
        "scopes": " ".join(SCOPES),
    })

    # Fetch GA4 properties and GSC sites so user can pick
    access_token = tokens.get("access_token", "")
    ga4_properties = _fetch_ga4_properties(access_token)
    gsc_sites = _fetch_gsc_sites(access_token)

    # Store in session for the picker page
    session["google_pick_brand_id"] = brand_id
    session["google_pick_ga4"] = ga4_properties
    session["google_pick_gsc"] = gsc_sites

    return render_template(
        "oauth/google_pick_properties.html",
        brand=brand,
        ga4_properties=ga4_properties,
        gsc_sites=gsc_sites,
    )


@google_bp.route("/select-properties", methods=["POST"])
def select_properties():
    if "user_id" not in session:
        return redirect(url_for("login"))

    brand_id = session.pop("google_pick_brand_id", None)
    session.pop("google_pick_ga4", None)
    session.pop("google_pick_gsc", None)

    if not brand_id:
        flash("Session expired, try connecting again", "error")
        return redirect(url_for("brands_list"))

    db = current_app.db

    ga4_property_id = request.form.get("ga4_property_id", "").strip()
    gsc_site_url = request.form.get("gsc_site_url", "").strip()

    if ga4_property_id:
        db.update_brand_api_field(brand_id, "ga4_property_id", ga4_property_id)
    if gsc_site_url:
        db.update_brand_api_field(brand_id, "gsc_site_url", gsc_site_url)

    flash("Google account connected and properties selected", "success")
    return redirect(url_for("brand_detail", brand_id=brand_id))


@google_bp.route("/disconnect/<int:brand_id>", methods=["POST"])
def disconnect(brand_id):
    if "user_id" not in session:
        return redirect(url_for("login"))

    db = current_app.db
    db.disconnect_platform(brand_id, "google")
    flash("Google account disconnected", "success")
    return redirect(url_for("brand_detail", brand_id=brand_id))


def refresh_google_token(app_config, db, brand_id):
    """Refresh an expired Google access token. Returns new access token or None."""
    connections = db.get_brand_connections(brand_id)
    google_conn = connections.get("google")
    if not google_conn or not google_conn.get("refresh_token"):
        return None

    resp = requests.post(GOOGLE_TOKEN_URL, data={
        "client_id": app_config["GOOGLE_CLIENT_ID"],
        "client_secret": app_config["GOOGLE_CLIENT_SECRET"],
        "refresh_token": google_conn["refresh_token"],
        "grant_type": "refresh_token",
    }, timeout=30)

    if resp.status_code != 200:
        return None

    tokens = resp.json()
    expiry = ""
    if "expires_in" in tokens:
        expiry = (datetime.now() + timedelta(seconds=tokens["expires_in"])).isoformat()

    db.upsert_connection(brand_id, "google", {
        "access_token": tokens["access_token"],
        "refresh_token": google_conn["refresh_token"],
        "token_expiry": expiry,
        "scopes": google_conn.get("scopes", ""),
    })

    return tokens["access_token"]


def _fetch_ga4_properties(access_token):
    """Fetch all GA4 properties the authenticated user has access to."""
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
    """Fetch all Search Console sites the authenticated user has access to."""
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
