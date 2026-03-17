"""
Bridge between web app OAuth tokens and the existing API pull modules.

Uses stored OAuth tokens from the web DB instead of credential files.
"""
import sys
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR))


def pull_api_data_for_brand(brand, connections, month):
    """
    Pull data from connected APIs for a brand.
    Uses OAuth tokens stored in the web database.
    """
    data = {}

    # Parse month into date range
    year, mon = month.split("-")
    year, mon = int(year), int(mon)
    import calendar
    last_day = calendar.monthrange(year, mon)[1]
    start_date = f"{year}-{mon:02d}-01"
    end_date = f"{year}-{mon:02d}-{last_day}"

    google_conn = connections.get("google")
    meta_conn = connections.get("meta")

    # Google Analytics
    if google_conn and google_conn.get("status") == "connected" and brand.get("ga4_property_id"):
        try:
            data["google_analytics"] = _pull_ga4(
                brand["ga4_property_id"],
                google_conn["access_token"],
                start_date,
                end_date,
            )
        except Exception:
            pass

    # Google Search Console
    if google_conn and google_conn.get("status") == "connected" and brand.get("gsc_site_url"):
        try:
            data["search_console"] = _pull_gsc(
                brand["gsc_site_url"],
                google_conn["access_token"],
                start_date,
                end_date,
            )
        except Exception:
            pass

    # Meta Ads
    if meta_conn and meta_conn.get("status") == "connected" and brand.get("meta_ad_account_id"):
        try:
            data["meta_business"] = _pull_meta(
                brand["meta_ad_account_id"],
                meta_conn["access_token"],
                start_date,
                end_date,
            )
        except Exception:
            pass

    return data


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

    return {
        "sessions": int(float(values[0])) if len(values) > 0 else 0,
        "users": int(float(values[1])) if len(values) > 1 else 0,
        "new_users": int(float(values[2])) if len(values) > 2 else 0,
        "bounce_rate": float(values[3]) if len(values) > 3 else 0,
        "avg_session_duration": float(values[4]) if len(values) > 4 else 0,
        "pageviews": int(float(values[5])) if len(values) > 5 else 0,
        "conversions": int(float(values[6])) if len(values) > 6 else 0,
    }


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
        "total_clicks": total_clicks,
        "total_impressions": total_impressions,
        "avg_ctr": avg_ctr,
        "avg_position": avg_position,
        "top_queries": top_queries,
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

    return {
        "spend": spend,
        "impressions": int(data_row.get("impressions", 0)),
        "clicks": int(data_row.get("clicks", 0)),
        "ctr": float(data_row.get("ctr", 0)),
        "cpc": float(data_row.get("cpc", 0)),
        "cpm": float(data_row.get("cpm", 0)),
        "reach": int(data_row.get("reach", 0)),
        "frequency": float(data_row.get("frequency", 0)),
        "leads": leads,
        "cost_per_lead": cost_per_lead,
        "roas": 0,
    }
