"""
Meta (Facebook) OAuth2 flow for connecting ad accounts.

Facebook uses a similar auth code flow. User authorizes, we get a short-lived
token, exchange it for a long-lived one, then list their ad accounts so they
can pick which one to connect.
"""
import os
from datetime import datetime, timedelta
from urllib.parse import urlencode

from flask import Blueprint, request, redirect, session, flash, url_for, current_app, render_template
import requests

meta_bp = Blueprint("meta_oauth", __name__)

META_AUTH_URL = "https://www.facebook.com/v21.0/dialog/oauth"
META_TOKEN_URL = "https://graph.facebook.com/v21.0/oauth/access_token"
META_LONG_LIVED_URL = "https://graph.facebook.com/v21.0/oauth/access_token"

SCOPES = [
    "ads_read",
    "ads_management",
    "read_insights",
    "business_management",
    "pages_show_list",
    "pages_read_engagement",
    "pages_manage_posts",
]


@meta_bp.route("/connect/<int:brand_id>")
def connect(brand_id):
    if "user_id" not in session:
        return redirect(url_for("login"))

    db = current_app.db
    brand = db.get_brand(brand_id)
    if not brand:
        flash("Brand not found", "error")
        return redirect(url_for("brands_list"))

    app_id = (db.get_setting("meta_app_id", "") or current_app.config.get("META_APP_ID", "")).strip()
    app_secret = (db.get_setting("meta_app_secret", "") or current_app.config.get("META_APP_SECRET", "")).strip()
    if not app_id or not app_secret:
        flash(
            "Meta OAuth not configured. Go to Settings to add your Meta App ID and App Secret (one-time agency setup).",
            "error",
        )
        return redirect(url_for("brand_detail", brand_id=brand_id))

    callback_url = current_app.config["APP_URL"].rstrip("/") + url_for("meta_oauth.callback")
    session["meta_oauth_brand_id"] = brand_id

    params = {
        "client_id": app_id,
        "redirect_uri": callback_url,
        "scope": ",".join(SCOPES),
        "response_type": "code",
        "state": str(brand_id),
    }
    return redirect(f"{META_AUTH_URL}?{urlencode(params)}")


@meta_bp.route("/callback")
def callback():
    # Accept either admin or client portal sessions
    is_client = session.pop("meta_oauth_source", None) == "client"
    if not is_client and "user_id" not in session:
        return redirect(url_for("login"))
    if is_client and "client_user_id" not in session:
        return redirect(url_for("client.client_login"))

    error_redirect = url_for("client.client_settings") if is_client else url_for("brands_list")

    error = request.args.get("error")
    if error:
        desc = request.args.get("error_description", error)
        flash(f"Meta authorization failed: {desc}", "error")
        return redirect(error_redirect)

    code = request.args.get("code")
    brand_id = session.pop("meta_oauth_brand_id", None)
    if not code or not brand_id:
        flash("Invalid OAuth callback", "error")
        return redirect(error_redirect)

    db = current_app.db
    brand = db.get_brand(brand_id)
    if not brand:
        flash("Brand not found", "error")
        return redirect(error_redirect)

    app_id = (db.get_setting("meta_app_id", "") or current_app.config.get("META_APP_ID", "")).strip()
    app_secret = (db.get_setting("meta_app_secret", "") or current_app.config.get("META_APP_SECRET", "")).strip()
    if not app_id or not app_secret:
        flash("Meta OAuth not configured.", "error")
        return redirect(error_redirect)

    callback_url = current_app.config["APP_URL"].rstrip("/") + url_for("meta_oauth.callback")

    # Exchange code for short-lived token
    token_resp = requests.get(META_TOKEN_URL, params={
        "client_id": app_id,
        "client_secret": app_secret,
        "redirect_uri": callback_url,
        "code": code,
    }, timeout=30)

    if token_resp.status_code != 200:
        try:
            err_data = token_resp.json()
            err_msg = err_data.get("error", {}).get("message", "") if isinstance(err_data.get("error"), dict) else str(err_data)
        except Exception:
            err_msg = token_resp.text[:200]

        if "client secret" in err_msg.lower() or "validating client" in err_msg.lower():
            msg = (
                "Meta rejected your App Secret. Go to developers.facebook.com > "
                "your app > App Settings > Basic, click Show next to App Secret, "
                "copy it, and paste it into the Meta App Secret field in your admin Settings."
            )
        else:
            msg = f"Meta OAuth error: {err_msg}"

        flash(msg, "error")
        return redirect(error_redirect)

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
        expires_in = ll_data.get("expires_in", 5184000)  # default 60 days
    else:
        access_token = short_token
        expires_in = 3600

    expiry = (datetime.now() + timedelta(seconds=expires_in)).isoformat()

    # Fetch ad accounts so user can pick one
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

    if is_client:
        # Client portal flow
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
            ad_accounts=_enrich_ad_accounts(ad_accounts),
            # client_base.html expects these from the client_bp context processor,
            # but this route lives in meta_bp so we supply them manually.
            client_brand=session.get("client_brand_name", brand.get("display_name", "")),
            client_user=session.get("client_name", ""),
            assistant_enabled=False,
            assistant_messages=[],
            assistant_month="",
            assistant_model_chat="gpt-4o-mini",
            assistant_models=[],
        )

    # Admin flow
    session["meta_temp_token"] = access_token
    session["meta_temp_expiry"] = expiry
    session["meta_temp_brand_id"] = brand_id

    if len(ad_accounts) == 1:
        # Auto-select if there's only one
        acct = ad_accounts[0]
        _finalize_meta_connection(db, brand_id, access_token, expiry, acct)
        flash(f"Meta ad account connected: {acct.get('name', acct['account_id'])}", "success")
        return redirect(url_for("brand_detail", brand_id=brand_id))

    return render_template(
        "oauth/meta_pick_account.html",
        brand=brand,
        ad_accounts=_enrich_ad_accounts(ad_accounts),
    )


def _enrich_ad_accounts(ad_accounts):
    """Add human-readable _status_label and _status_class to each account dict."""
    status_map = {
        1: ("Active", "success"),
        2: ("Disabled", "warning"),
        3: ("Unsettled", "danger"),
        7: ("Pending Review", "info"),
        8: ("Pending Closure", "warning"),
        9: ("In Grace Period", "info"),
        100: ("Pending Settlement", "info"),
        101: ("Active (Limited)", "warning"),
        201: ("Any Closed", "secondary"),
    }
    for acct in ad_accounts:
        code = acct.get("account_status")
        label, cls = status_map.get(code, (f"Unknown ({code})", "secondary"))
        acct["_status_label"] = label
        acct["_status_class"] = cls
    return ad_accounts


@meta_bp.route("/select-account", methods=["POST"])
def select_account():
    if "user_id" not in session:
        return redirect(url_for("login"))

    brand_id = session.pop("meta_temp_brand_id", None)
    access_token = session.pop("meta_temp_token", None)
    expiry = session.pop("meta_temp_expiry", None)
    account_id = request.form.get("account_id", "")
    account_name = request.form.get("account_name", "")

    if not brand_id or not access_token or not account_id:
        flash("Session expired, try connecting again", "error")
        return redirect(url_for("brands_list"))

    db = current_app.db
    acct = {"account_id": account_id, "name": account_name}
    _finalize_meta_connection(db, brand_id, access_token, expiry, acct)

    flash(f"Meta ad account connected: {account_name or account_id}", "success")
    return redirect(url_for("brand_detail", brand_id=brand_id))


@meta_bp.route("/disconnect/<int:brand_id>", methods=["POST"])
def disconnect(brand_id):
    if "user_id" not in session:
        return redirect(url_for("login"))

    db = current_app.db
    db.disconnect_platform(brand_id, "meta")
    db.update_brand_api_field(brand_id, "meta_ad_account_id", "")
    db.update_brand_api_field(brand_id, "facebook_page_id", "")
    flash("Meta account disconnected", "success")
    return redirect(url_for("brand_detail", brand_id=brand_id))


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

    # Auto-detect Facebook Page for organic tracking
    brand = db.get_brand(brand_id)
    pages = _fetch_facebook_pages(access_token)
    if pages:
        # If page ID already set and still in list, keep it
        current_page_id = (brand.get("facebook_page_id") or "").strip() if brand else ""
        matched = any(p["id"] == current_page_id for p in pages) if current_page_id else False
        if not matched:
            db.update_brand_api_field(brand_id, "facebook_page_id", pages[0]["id"])
        page_names = ", ".join(f"{p.get('name', 'Unknown')} ({p['id']})" for p in pages[:5])
        flash(f"Facebook Pages detected: {page_names}. Using first page for organic tracking.", "info")
    else:
        flash(
            "No Facebook Pages detected. Organic tracking won't work until a page is linked. "
            "When reconnecting Meta, make sure to check your business page in the 'Pages' permissions screen.",
            "warning",
        )


def _fetch_facebook_pages(access_token):
    """Fetch Facebook Pages the user manages."""
    try:
        resp = requests.get(
            "https://graph.facebook.com/v21.0/me/accounts",
            params={
                "access_token": access_token,
                "fields": "id,name,category,fan_count,followers_count",
                "limit": 50,
            },
            timeout=15,
        )
        if resp.status_code != 200:
            return []
        return resp.json().get("data", [])
    except Exception:
        return []
