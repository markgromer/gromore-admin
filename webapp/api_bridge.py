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

    # Google Ads
    if google_conn and google_conn.get("status") == "connected" and brand.get("google_ads_customer_id"):
        try:
            token = _get_google_token(db, brand["id"], google_conn)
            data["google_ads"] = _pull_google_ads(
                brand.get("google_ads_customer_id", ""),
                token,
                start_date,
                end_date,
            )
        except Exception as e:
            errors.append(f"Google Ads pull failed: {str(e)}")

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

    def _run_report(metric_names, dimension_names=None, limit=100):
        body = {
            "dateRanges": [{"startDate": start_date, "endDate": end_date}],
            "metrics": [{"name": metric_name} for metric_name in metric_names],
            "limit": limit,
        }
        if dimension_names:
            body["dimensions"] = [{"name": dimension_name} for dimension_name in dimension_names]

        response = requests.post(url, json=body, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json().get("rows", [])

    totals_rows = _run_report(
        [
            "sessions",
            "totalUsers",
            "newUsers",
            "bounceRate",
            "averageSessionDuration",
            "screenPageViews",
            "conversions",
        ],
        limit=1,
    )
    totals_row = totals_rows[0] if totals_rows else {}
    values = [v.get("value", "0") for v in totals_row.get("metricValues", [])]

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

    by_source = {}
    source_rows = _run_report(
        ["sessions", "conversions", "totalUsers"],
        dimension_names=["sessionSourceMedium"],
        limit=100,
    )
    for source_row in source_rows:
        dimensions = source_row.get("dimensionValues", [])
        metrics = source_row.get("metricValues", [])
        source_name = dimensions[0].get("value", "(not set)") if dimensions else "(not set)"
        source_sessions = int(float(metrics[0].get("value", "0"))) if len(metrics) > 0 else 0
        source_conversions = int(float(metrics[1].get("value", "0"))) if len(metrics) > 1 else 0
        source_users = int(float(metrics[2].get("value", "0"))) if len(metrics) > 2 else 0
        by_source[source_name] = {
            "sessions": source_sessions,
            "conversions": source_conversions,
            "users": source_users,
        }

    by_page = []
    landing_rows = _run_report(
        ["sessions", "conversions", "totalUsers", "bounceRate", "averageSessionDuration"],
        dimension_names=["landingPagePlusQueryString"],
        limit=100,
    )
    for landing_row in landing_rows:
        dimensions = landing_row.get("dimensionValues", [])
        metrics = landing_row.get("metricValues", [])
        page_path = dimensions[0].get("value", "(not set)") if dimensions else "(not set)"
        page_sessions = int(float(metrics[0].get("value", "0"))) if len(metrics) > 0 else 0
        page_conversions = int(float(metrics[1].get("value", "0"))) if len(metrics) > 1 else 0
        page_users = int(float(metrics[2].get("value", "0"))) if len(metrics) > 2 else 0
        page_bounce_rate = float(metrics[3].get("value", "0")) if len(metrics) > 3 else 0.0
        page_avg_session_duration = float(metrics[4].get("value", "0")) if len(metrics) > 4 else 0.0
        page_conversion_rate = round((page_conversions / page_sessions) * 100, 2) if page_sessions > 0 else 0.0
        by_page.append(
            {
                "page": page_path,
                "sessions": page_sessions,
                "conversions": page_conversions,
                "users": page_users,
                "bounce_rate": page_bounce_rate,
                "avg_session_duration": page_avg_session_duration,
                "conversion_rate": page_conversion_rate,
            }
        )

    by_page.sort(key=lambda item: item.get("sessions", 0), reverse=True)

    return {
        "totals": totals,
        "by_source": by_source,
        "by_page": by_page,
        "row_count": max(1, len(source_rows), len(landing_rows)),
        "columns_found": list(totals.keys()),
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


def _pull_google_ads(customer_id, access_token, start_date, end_date):
    """Pull Google Ads metrics using Google Ads API searchStream."""
    import requests
    import re
    from flask import current_app

    clean_customer_id = re.sub(r"\D", "", str(customer_id or ""))
    if not clean_customer_id:
        raise ValueError("Google Ads customer ID is missing or invalid")

    developer_token = (current_app.config.get("GOOGLE_ADS_DEVELOPER_TOKEN", "") or "").strip()
    if not developer_token:
        raise ValueError("Google Ads developer token not configured in Settings")

    login_customer_id = re.sub(r"\D", "", (current_app.config.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "") or "").strip())
    url = f"https://googleads.googleapis.com/v19/customers/{clean_customer_id}/googleAds:searchStream"

    def _search_stream(gaql_query: str):
        resp = requests.post(url, json={"query": gaql_query}, headers=headers, timeout=30)
        if resp.status_code != 200:
            detail = resp.text[:300]
            if "ACCESS_TOKEN_SCOPE_INSUFFICIENT" in detail or "insufficient authentication scopes" in detail.lower():
                raise ValueError("Google token missing Ads scope. Reconnect Google to grant Google Ads access.")
            raise ValueError(f"Google Ads API error {resp.status_code}: {detail}")

        payload = resp.json()
        return payload if isinstance(payload, list) else [payload]

    query = (
        "SELECT "
        "campaign.id, "
        "campaign.name, "
        "campaign.advertising_channel_type, "
        "metrics.impressions, "
        "metrics.clicks, "
        "metrics.cost_micros, "
        "metrics.conversions, "
        "metrics.ctr, "
        "metrics.average_cpc "
        "FROM campaign "
        f"WHERE segments.date BETWEEN '{start_date}' AND '{end_date}' "
        "AND campaign.status != 'REMOVED'"
    )

    headers = {
        "Authorization": f"Bearer {access_token}",
        "developer-token": developer_token,
        "Content-Type": "application/json",
    }
    if login_customer_id:
        headers["login-customer-id"] = login_customer_id

    chunks = _search_stream(query)

    by_campaign = {}
    totals = {
        "impressions": 0,
        "clicks": 0,
        "spend": 0.0,
        "results": 0.0,
        "ctr": 0.0,
        "cpc": 0.0,
        "cost_per_result": 0.0,
    }

    for chunk in chunks:
        for row in chunk.get("results", []) or []:
            campaign = row.get("campaign", {})
            metrics = row.get("metrics", {})
            campaign_name = campaign.get("name") or campaign.get("id") or "Unknown Campaign"
            impressions = int(metrics.get("impressions", 0) or 0)
            clicks = int(metrics.get("clicks", 0) or 0)
            spend = float(metrics.get("costMicros", 0) or 0) / 1_000_000.0
            conversions = float(metrics.get("conversions", 0) or 0)
            ctr = float(metrics.get("ctr", 0) or 0) * 100.0
            average_cpc = float(metrics.get("averageCpc", 0) or 0) / 1_000_000.0
            cpr = (spend / conversions) if conversions > 0 else 0.0

            by_campaign[campaign_name] = {
                "campaign_id": campaign.get("id", ""),
                "channel_type": campaign.get("advertisingChannelType", ""),
                "impressions": impressions,
                "clicks": clicks,
                "spend": round(spend, 2),
                "results": round(conversions, 2),
                "ctr": round(ctr, 2),
                "cpc": round(average_cpc, 2),
                "cost_per_result": round(cpr, 2),
            }

            totals["impressions"] += impressions
            totals["clicks"] += clicks
            totals["spend"] += spend
            totals["results"] += conversions

    # Search terms (in-account query data) for keyword grounding
    search_terms = []
    try:
        st_query = (
            "SELECT "
            "search_term_view.search_term, "
            "campaign.id, "
            "campaign.name, "
            "ad_group.id, "
            "ad_group.name, "
            "metrics.impressions, "
            "metrics.clicks, "
            "metrics.cost_micros, "
            "metrics.conversions, "
            "metrics.ctr, "
            "metrics.average_cpc "
            "FROM search_term_view "
            f"WHERE segments.date BETWEEN '{start_date}' AND '{end_date}' "
            "AND campaign.status != 'REMOVED' "
            "AND metrics.clicks > 0 "
            "ORDER BY metrics.clicks DESC "
            "LIMIT 50"
        )
        st_chunks = _search_stream(st_query)
        for chunk in st_chunks:
            for row in chunk.get("results", []) or []:
                st = (row.get("searchTermView") or {})
                campaign = row.get("campaign", {})
                ad_group = row.get("adGroup", {})
                metrics = row.get("metrics", {})

                term = (st.get("searchTerm") or "").strip()
                if not term:
                    continue

                impressions = int(metrics.get("impressions", 0) or 0)
                clicks = int(metrics.get("clicks", 0) or 0)
                spend = float(metrics.get("costMicros", 0) or 0) / 1_000_000.0
                conversions = float(metrics.get("conversions", 0) or 0)
                ctr = float(metrics.get("ctr", 0) or 0) * 100.0
                average_cpc = float(metrics.get("averageCpc", 0) or 0) / 1_000_000.0
                cpr = (spend / conversions) if conversions > 0 else 0.0

                search_terms.append(
                    {
                        "term": term,
                        "campaign_id": campaign.get("id", ""),
                        "campaign_name": campaign.get("name", ""),
                        "ad_group_id": ad_group.get("id", ""),
                        "ad_group_name": ad_group.get("name", ""),
                        "impressions": impressions,
                        "clicks": clicks,
                        "spend": round(spend, 2),
                        "results": round(conversions, 2),
                        "ctr": round(ctr, 2),
                        "cpc": round(average_cpc, 2),
                        "cost_per_result": round(cpr, 2),
                    }
                )
    except Exception:
        # Best-effort: do not fail the whole pull if search terms are unavailable.
        search_terms = []

    if totals["impressions"] > 0:
        totals["ctr"] = round((totals["clicks"] / totals["impressions"]) * 100.0, 2)
    if totals["clicks"] > 0:
        totals["cpc"] = round(totals["spend"] / totals["clicks"], 2)
    if totals["results"] > 0:
        totals["cost_per_result"] = round(totals["spend"] / totals["results"], 2)

    totals["spend"] = round(totals["spend"], 2)
    totals["results"] = round(totals["results"], 2)

    sorted_campaigns = sorted(
        by_campaign.items(),
        key=lambda item: (item[1].get("results", 0), item[1].get("spend", 0)),
        reverse=True,
    )

    return {
        "totals": totals,
        "by_campaign": dict(sorted_campaigns[:30]),
        "search_terms": search_terms[:50],
        "row_count": len(by_campaign),
    }


def _pull_meta(ad_account_id, access_token, start_date, end_date):
    """Pull Meta Ads data using Graph API with OAuth token."""
    import requests

    def _extract_results_and_cpr(row):
        results = 0
        cpr = 0
        for action in row.get("actions", []) or []:
            if action.get("action_type") in (
                "lead",
                "onsite_conversion.lead_grouped",
                "offsite_conversion.fb_pixel_lead",
            ):
                results += int(float(action.get("value", 0) or 0))
        for cpa in row.get("cost_per_action_type", []) or []:
            if cpa.get("action_type") in (
                "lead",
                "onsite_conversion.lead_grouped",
                "offsite_conversion.fb_pixel_lead",
            ):
                cpr = float(cpa.get("value", 0) or 0)
        return results, cpr

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
    leads, cost_per_lead = _extract_results_and_cpr(data_row)

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

    by_campaign = {}
    campaign_resp = requests.get(
        url,
        params={
            "access_token": access_token,
            "time_range": f'{{"since":"{start_date}","until":"{end_date}"}}',
            "fields": "campaign_id,campaign_name,spend,impressions,clicks,ctr,cpc,cpm,reach,frequency,actions,cost_per_action_type",
            "level": "campaign",
            "limit": 200,
        },
        timeout=30,
    )
    if campaign_resp.status_code == 200:
        for row in campaign_resp.json().get("data", []):
            campaign_results, campaign_cpr = _extract_results_and_cpr(row)
            campaign_name = row.get("campaign_name") or row.get("campaign_id") or "Unknown Campaign"
            by_campaign[campaign_name] = {
                "campaign_id": row.get("campaign_id", ""),
                "spend": float(row.get("spend", 0) or 0),
                "impressions": int(float(row.get("impressions", 0) or 0)),
                "clicks": int(float(row.get("clicks", 0) or 0)),
                "ctr": float(row.get("ctr", 0) or 0),
                "cpc": float(row.get("cpc", 0) or 0),
                "cpm": float(row.get("cpm", 0) or 0),
                "reach": int(float(row.get("reach", 0) or 0)),
                "frequency": float(row.get("frequency", 0) or 0),
                "results": campaign_results,
                "cost_per_result": campaign_cpr,
            }

    top_ads = []
    ad_resp = requests.get(
        url,
        params={
            "access_token": access_token,
            "time_range": f'{{"since":"{start_date}","until":"{end_date}"}}',
            "fields": "ad_id,ad_name,campaign_name,spend,impressions,clicks,ctr,cpc,cpm,actions,cost_per_action_type",
            "level": "ad",
            "limit": 200,
        },
        timeout=30,
    )
    if ad_resp.status_code == 200:
        for row in ad_resp.json().get("data", []):
            ad_results, ad_cpr = _extract_results_and_cpr(row)
            top_ads.append(
                {
                    "ad_id": row.get("ad_id", ""),
                    "ad_name": row.get("ad_name") or row.get("ad_id") or "Unknown Ad",
                    "campaign_name": row.get("campaign_name", ""),
                    "spend": float(row.get("spend", 0) or 0),
                    "impressions": int(float(row.get("impressions", 0) or 0)),
                    "clicks": int(float(row.get("clicks", 0) or 0)),
                    "ctr": float(row.get("ctr", 0) or 0),
                    "cpc": float(row.get("cpc", 0) or 0),
                    "cpm": float(row.get("cpm", 0) or 0),
                    "results": ad_results,
                    "cost_per_result": ad_cpr,
                }
            )

    top_ads.sort(key=lambda x: (x.get("results", 0), x.get("spend", 0)), reverse=True)

    return {
        "totals": totals,
        "by_campaign": by_campaign,
        "by_ad_set": {},
        "top_ads": top_ads[:20],
        "row_count": len(top_ads) if top_ads else 1,
    }
