"""
Bridge between web app OAuth tokens and the existing API pull modules.

Uses stored OAuth tokens from the web DB instead of credential files.
Handles automatic token refresh when tokens expire.
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR))


def _refresh_google_token(db, brand_id, connection):
    """Refresh an expired Google access token using the refresh token."""
    import requests as _req

    refresh_token = connection.get("refresh_token", "")
    if not refresh_token:
        return None

    from flask import current_app
    client_id = (db.get_setting("google_client_id", "") or current_app.config.get("GOOGLE_CLIENT_ID", "")).strip()
    client_secret = (db.get_setting("google_client_secret", "") or current_app.config.get("GOOGLE_CLIENT_SECRET", "")).strip()

    if not client_id or not client_secret:
        return None

    resp = _req.post("https://oauth2.googleapis.com/token", data={
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }, timeout=30)

    if resp.status_code != 200:
        return None

    tokens = resp.json()
    new_access = tokens.get("access_token", "")
    if not new_access:
        return None

    expiry = ""
    if "expires_in" in tokens:
        expiry = (datetime.now() + timedelta(seconds=tokens["expires_in"])).isoformat()

    # Update the stored token
    db.upsert_connection(brand_id, "google", {
        "access_token": new_access,
        "refresh_token": refresh_token,
        "token_expiry": expiry,
        "scopes": connection.get("scopes", ""),
        "account_id": connection.get("account_id", ""),
        "account_name": connection.get("account_name", ""),
    })
    return new_access


def _get_google_token(db, brand_id, connection):
    """Get a valid Google access token, refreshing if needed."""
    token = connection.get("access_token", "")
    expiry_str = connection.get("token_expiry", "")

    # Check if token is expired or about to expire (5 min buffer)
    if expiry_str:
        try:
            expiry = datetime.fromisoformat(expiry_str)
            if datetime.now() >= expiry - timedelta(minutes=5):
                refreshed = _refresh_google_token(db, brand_id, connection)
                if refreshed:
                    return refreshed
        except (ValueError, TypeError):
            pass

    return token


def _refresh_meta_token(db, brand_id, connection):
    """Refresh an expiring Meta long-lived token by exchanging it for a new one."""
    import requests as _req

    current_token = connection.get("access_token", "")
    if not current_token:
        return None

    from flask import current_app
    app_id = (db.get_setting("meta_app_id", "") or current_app.config.get("META_APP_ID", "")).strip()
    app_secret = (db.get_setting("meta_app_secret", "") or current_app.config.get("META_APP_SECRET", "")).strip()

    if not app_id or not app_secret:
        return None

    resp = _req.get("https://graph.facebook.com/v21.0/oauth/access_token", params={
        "grant_type": "fb_exchange_token",
        "client_id": app_id,
        "client_secret": app_secret,
        "fb_exchange_token": current_token,
    }, timeout=30)

    if resp.status_code != 200:
        return None

    data = resp.json()
    new_token = data.get("access_token", "")
    if not new_token:
        return None

    expires_in = data.get("expires_in", 5184000)  # default 60 days
    expiry = (datetime.now() + timedelta(seconds=expires_in)).isoformat()

    db.upsert_connection(brand_id, "meta", {
        "access_token": new_token,
        "refresh_token": "",
        "token_expiry": expiry,
        "scopes": connection.get("scopes", ""),
        "account_id": connection.get("account_id", ""),
        "account_name": connection.get("account_name", ""),
    })
    return new_token


def _get_meta_token(db, brand_id, connection):
    """Get a valid Meta access token, refreshing if within 7 days of expiry."""
    token = connection.get("access_token", "")
    expiry_str = connection.get("token_expiry", "")

    if expiry_str:
        try:
            expiry = datetime.fromisoformat(expiry_str)
            # Refresh if within 7 days of expiry (Meta tokens last ~60 days)
            if datetime.now() >= expiry - timedelta(days=7):
                refreshed = _refresh_meta_token(db, brand_id, connection)
                if refreshed:
                    return refreshed
        except (ValueError, TypeError):
            pass

    return token


def pull_api_data_for_brand(brand, connections, month):
    """
    Pull data from connected APIs for a brand.
    Uses OAuth tokens stored in the web database.
    Returns (data_dict, errors_list).
    """
    data = {}
    errors = []

    # Parse month into date range
    year, mon = month.split("-")
    year, mon = int(year), int(mon)
    import calendar
    last_day = calendar.monthrange(year, mon)[1]
    start_date = f"{year}-{mon:02d}-01"
    end_date = f"{year}-{mon:02d}-{last_day}"

    google_conn = connections.get("google")
    meta_conn = connections.get("meta")

    # Get DB for token refresh
    from flask import current_app
    db = current_app.db

    # Google Analytics
    if google_conn and google_conn.get("status") == "connected" and brand.get("ga4_property_id"):
        try:
            token = _get_google_token(db, brand["id"], google_conn)
            data["google_analytics"] = _pull_ga4(
                brand["ga4_property_id"],
                token,
                start_date,
                end_date,
            )
        except Exception as e:
            errors.append(f"GA4 pull failed: {str(e)}")

    # Google Search Console
    if google_conn and google_conn.get("status") == "connected" and brand.get("gsc_site_url"):
        try:
            token = _get_google_token(db, brand["id"], google_conn)
            data["search_console"] = _pull_gsc(
                brand["gsc_site_url"],
                token,
                start_date,
                end_date,
            )
        except Exception as e:
            errors.append(f"GSC pull failed: {str(e)}")

    # Meta Ads
    if meta_conn and meta_conn.get("status") == "connected" and brand.get("meta_ad_account_id"):
        try:
            token = _get_meta_token(db, brand["id"], meta_conn)
            data["meta_business"] = _pull_meta(
                brand["meta_ad_account_id"],
                token,
                start_date,
                end_date,
            )
        except Exception as e:
            errors.append(f"Meta pull failed: {str(e)}")

    return data, errors


def _pull_ga4(property_id, access_token, start_date, end_date):
    """Pull GA4 data using REST API with OAuth token."""
    import requests

    url = f"https://analyticsdata.googleapis.com/v1beta/properties/{property_id}:runReport"
    headers = {"Authorization": f"Bearer {access_token}"}

    body = {
        "dateRanges": [{"startDate": start_date, "endDate": end_date}],
        "metrics": [
            {"name": "sessions"},
            {"name": "totalUsers"},
            {"name": "newUsers"},
            {"name": "bounceRate"},
            {"name": "averageSessionDuration"},
            {"name": "screenPageViews"},
            {"name": "conversions"},
        ],
    }

    resp = requests.post(url, json=body, headers=headers, timeout=30)
    resp.raise_for_status()
    result = resp.json()

    row = result.get("rows", [{}])[0] if result.get("rows") else {}
    values = [v.get("value", "0") for v in row.get("metricValues", [])]

    totals = {
        "sessions": int(float(values[0])) if len(values) > 0 else 0,
        "users": int(float(values[1])) if len(values) > 1 else 0,
        "new_users": int(float(values[2])) if len(values) > 2 else 0,
        "bounce_rate": float(values[3]) if len(values) > 3 else 0,
        "avg_session_duration": float(values[4]) if len(values) > 4 else 0,
        "pageviews": int(float(values[5])) if len(values) > 5 else 0,
        "conversions": int(float(values[6])) if len(values) > 6 else 0,
    }

    # Calculate derived metrics
    if totals["sessions"] > 0 and totals["conversions"] > 0:
        totals["conversion_rate"] = round(totals["conversions"] / totals["sessions"] * 100, 2)

    return {"totals": totals, "by_source": {}, "row_count": 1, "columns_found": list(totals.keys())}


def _pull_gsc(site_url, access_token, start_date, end_date):
    """Pull Search Console data using REST API with OAuth token."""
    import requests

    url = f"https://searchconsole.googleapis.com/webmasters/v3/sites/{requests.utils.quote(site_url, safe='')}/searchAnalytics/query"
    headers = {"Authorization": f"Bearer {access_token}"}

    body = {
        "startDate": start_date,
        "endDate": end_date,
        "dimensions": ["query"],
        "rowLimit": 100,
    }

    resp = requests.post(url, json=body, headers=headers, timeout=30)
    resp.raise_for_status()
    result = resp.json()

    rows = result.get("rows", [])
    total_clicks = sum(r.get("clicks", 0) for r in rows)
    total_impressions = sum(r.get("impressions", 0) for r in rows)
    avg_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
    avg_position = sum(r.get("position", 0) for r in rows) / len(rows) if rows else 0

    top_queries = []
    for r in rows[:20]:
        top_queries.append({
            "query": r["keys"][0] if r.get("keys") else "",
            "clicks": r.get("clicks", 0),
            "impressions": r.get("impressions", 0),
            "ctr": r.get("ctr", 0) * 100,
            "position": r.get("position", 0),
        })

    return {
        "totals": {
            "clicks": total_clicks,
            "impressions": total_impressions,
            "ctr": avg_ctr,
            "avg_position": avg_position,
        },
        "top_queries": top_queries,
        "top_pages": [],
        "opportunity_queries": [],
        "row_count": len(rows),
    }


def _pull_meta(ad_account_id, access_token, start_date, end_date):
    """Pull Meta Ads data using Graph API with OAuth token."""
    import requests

    url = f"https://graph.facebook.com/v21.0/act_{ad_account_id}/insights"
    params = {
        "access_token": access_token,
        "time_range": f'{{"since":"{start_date}","until":"{end_date}"}}',
        "fields": "spend,impressions,clicks,ctr,cpc,cpm,reach,frequency,actions,cost_per_action_type",
        "level": "account",
    }

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    result = resp.json()

    data_row = result.get("data", [{}])[0] if result.get("data") else {}

    leads = 0
    cost_per_lead = 0
    for action in data_row.get("actions", []):
        if action.get("action_type") in ("lead", "onsite_conversion.lead_grouped", "offsite_conversion.fb_pixel_lead"):
            leads += int(action.get("value", 0))
    for cpa in data_row.get("cost_per_action_type", []):
        if cpa.get("action_type") in ("lead", "onsite_conversion.lead_grouped", "offsite_conversion.fb_pixel_lead"):
            cost_per_lead = float(cpa.get("value", 0))

    spend = float(data_row.get("spend", 0))
    clicks = int(data_row.get("clicks", 0))
    impressions = int(data_row.get("impressions", 0))

    totals = {
        "spend": spend,
        "impressions": impressions,
        "clicks": clicks,
        "ctr": float(data_row.get("ctr", 0)),
        "cpc": float(data_row.get("cpc", 0)),
        "cpm": float(data_row.get("cpm", 0)),
        "reach": int(data_row.get("reach", 0)),
        "frequency": float(data_row.get("frequency", 0)),
        "results": leads,
        "cost_per_result": cost_per_lead,
    }

    return {"totals": totals, "by_campaign": {}, "by_ad_set": {}, "row_count": 1}
