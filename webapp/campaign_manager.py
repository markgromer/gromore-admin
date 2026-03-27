"""
Campaign Manager

Read and write operations for Google Ads and Meta Ads campaigns.
Supports: listing campaigns, pausing/enabling, adjusting budgets,
adding negative keywords (Google), and creating new campaigns.

All mutations are logged to the campaign_changes audit table.
"""
import re
import json
import logging
from datetime import datetime, timedelta
import calendar

import requests

logger = logging.getLogger(__name__)

GOOGLE_ADS_API_VERSION = "v19"


def _parse_google_error(resp):
    """Extract a human-readable error from a Google Ads API error response."""
    try:
        data = resp.json()
        # Standard Google Ads API error structure
        err = data.get("error", {})
        if err.get("message"):
            return f"{err['message']} (HTTP {resp.status_code})"
        # Sometimes nested in details
        details = err.get("details", [])
        for d in details:
            for e in d.get("errors", []):
                msg = e.get("message", "")
                if msg:
                    return f"{msg} (HTTP {resp.status_code})"
    except (ValueError, AttributeError):
        pass
    # Fallback: if response is HTML (e.g. 404 error page), don't dump it
    text = resp.text[:200]
    if "<html" in text.lower() or "<!doctype" in text.lower():
        return f"Google Ads API returned HTTP {resp.status_code}. The API version or endpoint may be invalid."
    return f"HTTP {resp.status_code}: {text}"


# ── Helpers ──

def _google_headers(access_token, developer_token, login_customer_id=None):
    h = {
        "Authorization": f"Bearer {access_token}",
        "developer-token": developer_token,
        "Content-Type": "application/json",
    }
    if login_customer_id:
        h["login-customer-id"] = login_customer_id
    return h


def _clean_customer_id(raw):
    return re.sub(r"\D", "", str(raw or ""))


def _clean_meta_account_id(raw):
    value = str(raw or "").strip()
    if value.lower().startswith("act_"):
        value = value[4:]
    return re.sub(r"\D", "", value)


def _parse_meta_error(resp):
    """Extract a short human-readable error from a Meta Graph response."""
    try:
        data = resp.json()
        err = data.get("error", {})
        message = err.get("error_user_msg") or err.get("message") or err.get("error_user_title")
        if message:
            code = err.get("code")
            subcode = err.get("error_subcode")
            suffix = []
            if code:
                suffix.append(f"code {code}")
            if subcode:
                suffix.append(f"subcode {subcode}")
            # Include blame fields for debugging
            blame = err.get("error_data", "") or ""
            if not blame:
                blame = str(err.get("blame_field_specs", "") or "")
            base = f"{message} ({', '.join(suffix)})" if suffix else message
            if blame and blame != "":
                base += f" [blame: {blame}]"
            return base
    except (ValueError, AttributeError):
        pass
    return resp.text[:500]


def _month_range(month: str):
    """Return (start, end) as YYYY-MM-DD for a YYYY-MM month string."""
    try:
        if not month or not isinstance(month, str):
            raise ValueError("missing")
        year_s, mon_s = month.split("-", 1)
        year = int(year_s)
        mon = int(mon_s)
        if mon < 1 or mon > 12:
            raise ValueError("bad month")
        last_day = calendar.monthrange(year, mon)[1]
        start = f"{year:04d}-{mon:02d}-01"
        end = f"{year:04d}-{mon:02d}-{last_day:02d}"
        return start, end
    except Exception:
        end = datetime.now().strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        return start, end


def _google_config():
    from flask import current_app
    dev_token = (current_app.config.get("GOOGLE_ADS_DEVELOPER_TOKEN", "") or "").strip()
    login_cid = _clean_customer_id(
        current_app.config.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "") or ""
    )
    return dev_token, login_cid


def _get_tokens(db, brand_id, platform):
    """Get a valid token for the given platform, refreshing if needed."""
    from webapp.api_bridge import _get_google_token, _get_meta_token
    connections = db.get_brand_connections(brand_id)
    conn = connections.get(platform)
    if not conn or conn.get("status") != "connected":
        return None, None
    if platform == "google":
        token = _get_google_token(db, brand_id, conn)
    else:
        token = _get_meta_token(db, brand_id, conn)
    return token, conn


def _log_change(db, brand_id, platform, campaign_id, campaign_name,
                action, details, changed_by):
    """Record a campaign change in the audit log."""
    try:
        db.log_campaign_change(
            brand_id=brand_id,
            platform=platform,
            campaign_id=str(campaign_id),
            campaign_name=campaign_name,
            action=action,
            details=json.dumps(details) if isinstance(details, dict) else str(details),
            changed_by=changed_by,
        )
    except Exception as e:
        logger.error("Failed to log campaign change: %s", e)


# ═══════════════════════════════════════════════════════════════════
#  GOOGLE ADS
# ═══════════════════════════════════════════════════════════════════

def list_google_campaigns(db, brand, month: str = ""):
    """List all non-removed Google Ads campaigns and merge in metrics for the selected month.

    Key behavior: campaigns are returned even if they had zero activity in the time window.
    """
    customer_id = _clean_customer_id(brand.get("google_ads_customer_id"))
    if not customer_id:
        return []

    token, conn = _get_tokens(db, brand["id"], "google")
    if not token:
        return []

    dev_token, login_cid = _google_config()
    if not dev_token:
        return []

    start, end = _month_range(month)

    # Always list campaigns, regardless of whether they had activity in the selected window.
    list_query = (
        "SELECT "
        "campaign.id, campaign.name, campaign.status, "
        "campaign.advertising_channel_type, "
        "campaign_budget.amount_micros, campaign_budget.resource_name "
        "FROM campaign "
        "WHERE campaign.status != 'REMOVED'"
    )

    # Pull metrics for the selected window. If a campaign had no activity, it may not appear.
    metrics_query = (
        "SELECT "
        "campaign.id, "
        "metrics.impressions, metrics.clicks, metrics.cost_micros, "
        "metrics.conversions "
        "FROM campaign "
        f"WHERE segments.date BETWEEN '{start}' AND '{end}' "
        "AND campaign.status != 'REMOVED'"
    )

    url = f"https://googleads.googleapis.com/{GOOGLE_ADS_API_VERSION}/customers/{customer_id}/googleAds:searchStream"
    headers = _google_headers(token, dev_token, login_cid)

    # Query 1: campaign list
    resp = requests.post(url, json={"query": list_query}, headers=headers, timeout=30)
    if resp.status_code != 200:
        logger.error("Google Ads list error: %s", resp.text[:300])
        return []

    payload = resp.json()
    chunks = payload if isinstance(payload, list) else [payload]

    campaigns = {}
    for chunk in chunks:
        for row in chunk.get("results", []) or []:
            camp = row.get("campaign", {})
            budget = row.get("campaignBudget", {})
            cid = str(camp.get("id", ""))
            if not cid:
                continue
            daily_budget = float(budget.get("amountMicros", 0) or 0) / 1_000_000
            campaigns[cid] = {
                "id": cid,
                "name": camp.get("name", "Unknown"),
                "status": camp.get("status", "UNKNOWN"),
                "channel_type": camp.get("advertisingChannelType", ""),
                "daily_budget": round(daily_budget, 2),
                "budget_resource": budget.get("resourceName", ""),
                "impressions": 0,
                "clicks": 0,
                "spend": 0.0,
                "conversions": 0.0,
                "platform": "google",
            }

    # Query 2: metrics for the selected window
    resp_m = requests.post(url, json={"query": metrics_query}, headers=headers, timeout=30)
    if resp_m.status_code == 200:
        payload_m = resp_m.json()
        chunks_m = payload_m if isinstance(payload_m, list) else [payload_m]
        for chunk in chunks_m:
            for row in chunk.get("results", []) or []:
                camp = row.get("campaign", {})
                metrics = row.get("metrics", {})
                cid = str(camp.get("id", ""))
                if not cid or cid not in campaigns:
                    continue
                existing = campaigns[cid]
                existing["impressions"] += int(metrics.get("impressions", 0) or 0)
                existing["clicks"] += int(metrics.get("clicks", 0) or 0)
                existing["spend"] += float(metrics.get("costMicros", 0) or 0) / 1_000_000
                existing["conversions"] += float(metrics.get("conversions", 0) or 0)

    result = []
    for c in campaigns.values():
        c["spend"] = round(c["spend"], 2)
        c["conversions"] = round(c["conversions"], 1)
        c["ctr"] = round((c["clicks"] / c["impressions"] * 100), 2) if c["impressions"] > 0 else 0.0
        c["cpc"] = round(c["spend"] / c["clicks"], 2) if c["clicks"] > 0 else 0.0
        c["cpa"] = round(c["spend"] / c["conversions"], 2) if c["conversions"] > 0 else 0.0
        result.append(c)

    result.sort(key=lambda x: x["spend"], reverse=True)
    return result


def update_google_campaign_status(db, brand, campaign_id, new_status, changed_by):
    """Pause or enable a Google Ads campaign. new_status: 'PAUSED' or 'ENABLED'."""
    customer_id = _clean_customer_id(brand.get("google_ads_customer_id"))
    token, conn = _get_tokens(db, brand["id"], "google")
    dev_token, login_cid = _google_config()

    if not all([customer_id, token, dev_token]):
        return {"success": False, "error": "Missing Google Ads configuration"}

    resource = f"customers/{customer_id}/campaigns/{campaign_id}"
    url = f"https://googleads.googleapis.com/{GOOGLE_ADS_API_VERSION}/customers/{customer_id}/campaigns:mutate"
    headers = _google_headers(token, dev_token, login_cid)

    body = {
        "operations": [{
            "updateMask": "status",
            "update": {
                "resourceName": resource,
                "status": new_status,
            }
        }]
    }

    resp = requests.post(url, json=body, headers=headers, timeout=30)
    if resp.status_code != 200:
        return {"success": False, "error": resp.text[:300]}

    _log_change(db, brand["id"], "google", campaign_id, "",
                f"status_{new_status.lower()}", {"new_status": new_status}, changed_by)

    return {"success": True}


def update_google_budget(db, brand, campaign_id, budget_resource, new_daily_budget, changed_by):
    """Update the daily budget for a Google Ads campaign."""
    customer_id = _clean_customer_id(brand.get("google_ads_customer_id"))
    token, conn = _get_tokens(db, brand["id"], "google")
    dev_token, login_cid = _google_config()

    if not all([customer_id, token, dev_token, budget_resource]):
        return {"success": False, "error": "Missing Google Ads configuration or budget resource"}

    url = f"https://googleads.googleapis.com/{GOOGLE_ADS_API_VERSION}/customers/{customer_id}/campaignBudgets:mutate"
    headers = _google_headers(token, dev_token, login_cid)

    amount_micros = str(int(float(new_daily_budget) * 1_000_000))

    body = {
        "operations": [{
            "updateMask": "amountMicros",
            "update": {
                "resourceName": budget_resource,
                "amountMicros": amount_micros,
            }
        }]
    }

    resp = requests.post(url, json=body, headers=headers, timeout=30)
    if resp.status_code != 200:
        return {"success": False, "error": resp.text[:300]}

    _log_change(db, brand["id"], "google", campaign_id, "",
                "budget_update", {"new_daily_budget": new_daily_budget}, changed_by)

    return {"success": True}


def add_google_negative_keyword(db, brand, campaign_id, keyword_text, match_type, changed_by):
    """Add a negative keyword to a Google Ads campaign."""
    customer_id = _clean_customer_id(brand.get("google_ads_customer_id"))
    token, conn = _get_tokens(db, brand["id"], "google")
    dev_token, login_cid = _google_config()

    if not all([customer_id, token, dev_token]):
        return {"success": False, "error": "Missing Google Ads configuration"}

    url = f"https://googleads.googleapis.com/{GOOGLE_ADS_API_VERSION}/customers/{customer_id}/campaignCriteria:mutate"
    headers = _google_headers(token, dev_token, login_cid)

    body = {
        "operations": [{
            "create": {
                "campaign": f"customers/{customer_id}/campaigns/{campaign_id}",
                "negative": True,
                "keyword": {
                    "text": keyword_text,
                    "matchType": match_type.upper(),  # BROAD, PHRASE, EXACT
                }
            }
        }]
    }

    resp = requests.post(url, json=body, headers=headers, timeout=30)
    if resp.status_code != 200:
        return {"success": False, "error": resp.text[:300]}

    _log_change(db, brand["id"], "google", campaign_id, "",
                "negative_keyword", {"keyword": keyword_text, "match_type": match_type}, changed_by)

    return {"success": True}


def get_google_campaign_detail(db, brand, campaign_id, month: str = ""):
    """Get detailed info for a single Google Ads campaign including ad groups and keywords."""
    customer_id = _clean_customer_id(brand.get("google_ads_customer_id"))
    token, conn = _get_tokens(db, brand["id"], "google")
    dev_token, login_cid = _google_config()

    if not all([customer_id, token, dev_token]):
        return None

    headers = _google_headers(token, dev_token, login_cid)
    start, end = _month_range(month)

    # Campaign + budget info
    camp_query = (
        "SELECT campaign.id, campaign.name, campaign.status, "
        "campaign.advertising_channel_type, "
        "campaign_budget.amount_micros, campaign_budget.resource_name "
        "FROM campaign "
        f"WHERE campaign.id = {campaign_id}"
    )

    url = f"https://googleads.googleapis.com/{GOOGLE_ADS_API_VERSION}/customers/{customer_id}/googleAds:searchStream"
    resp = requests.post(url, json={"query": camp_query}, headers=headers, timeout=30)
    if resp.status_code != 200:
        return None

    payload = resp.json()
    chunks = payload if isinstance(payload, list) else [payload]
    camp_data = None
    for chunk in chunks:
        for row in chunk.get("results", []) or []:
            c = row.get("campaign", {})
            b = row.get("campaignBudget", {})
            camp_data = {
                "id": str(c.get("id", "")),
                "name": c.get("name", "Unknown"),
                "status": c.get("status", "UNKNOWN"),
                "channel_type": c.get("advertisingChannelType", ""),
                "daily_budget": round(float(b.get("amountMicros", 0) or 0) / 1_000_000, 2),
                "budget_resource": b.get("resourceName", ""),
                "platform": "google",
            }
            break

    if not camp_data:
        return None

    # Ad groups with metrics
    ag_query = (
        "SELECT ad_group.id, ad_group.name, ad_group.status, "
        "metrics.impressions, metrics.clicks, metrics.cost_micros, "
        "metrics.conversions, metrics.ctr "
        "FROM ad_group "
        f"WHERE campaign.id = {campaign_id} "
        f"AND segments.date BETWEEN '{start}' AND '{end}'"
    )

    resp2 = requests.post(url, json={"query": ag_query}, headers=headers, timeout=30)
    ad_groups_raw = {}
    if resp2.status_code == 200:
        p2 = resp2.json()
        ch2 = p2 if isinstance(p2, list) else [p2]
        for chunk in ch2:
            for row in chunk.get("results", []) or []:
                ag = row.get("adGroup", {})
                m = row.get("metrics", {})
                agid = str(ag.get("id", ""))
                if agid in ad_groups_raw:
                    ad_groups_raw[agid]["impressions"] += int(m.get("impressions", 0) or 0)
                    ad_groups_raw[agid]["clicks"] += int(m.get("clicks", 0) or 0)
                    ad_groups_raw[agid]["spend"] += float(m.get("costMicros", 0) or 0) / 1_000_000
                    ad_groups_raw[agid]["conversions"] += float(m.get("conversions", 0) or 0)
                else:
                    ad_groups_raw[agid] = {
                        "id": agid,
                        "name": ag.get("name", "Unknown"),
                        "status": ag.get("status", "UNKNOWN"),
                        "impressions": int(m.get("impressions", 0) or 0),
                        "clicks": int(m.get("clicks", 0) or 0),
                        "spend": float(m.get("costMicros", 0) or 0) / 1_000_000,
                        "conversions": float(m.get("conversions", 0) or 0),
                    }

    ad_groups = []
    for ag in ad_groups_raw.values():
        ag["spend"] = round(ag["spend"], 2)
        ag["ctr"] = round((ag["clicks"] / ag["impressions"] * 100), 2) if ag["impressions"] > 0 else 0
        ad_groups.append(ag)
    ad_groups.sort(key=lambda x: x["spend"], reverse=True)

    # Keywords for search campaigns
    keywords = []
    if camp_data.get("channel_type") == "SEARCH":
        kw_query = (
            "SELECT ad_group_criterion.keyword.text, "
            "ad_group_criterion.keyword.match_type, "
            "ad_group_criterion.negative, "
            "ad_group.name, "
            "metrics.impressions, metrics.clicks, metrics.cost_micros, "
            "metrics.conversions "
            "FROM keyword_view "
            f"WHERE campaign.id = {campaign_id} "
            f"AND segments.date BETWEEN '{start}' AND '{end}'"
        )

        resp3 = requests.post(url, json={"query": kw_query}, headers=headers, timeout=30)
        if resp3.status_code == 200:
            p3 = resp3.json()
            ch3 = p3 if isinstance(p3, list) else [p3]
            kw_agg = {}
            for chunk in ch3:
                for row in chunk.get("results", []) or []:
                    criterion = row.get("adGroupCriterion", {})
                    kw = criterion.get("keyword", {})
                    m = row.get("metrics", {})
                    text = kw.get("text", "")
                    if text in kw_agg:
                        kw_agg[text]["impressions"] += int(m.get("impressions", 0) or 0)
                        kw_agg[text]["clicks"] += int(m.get("clicks", 0) or 0)
                        kw_agg[text]["spend"] += float(m.get("costMicros", 0) or 0) / 1_000_000
                        kw_agg[text]["conversions"] += float(m.get("conversions", 0) or 0)
                    else:
                        kw_agg[text] = {
                            "keyword": text,
                            "match_type": kw.get("matchType", ""),
                            "negative": criterion.get("negative", False),
                            "ad_group": row.get("adGroup", {}).get("name", ""),
                            "impressions": int(m.get("impressions", 0) or 0),
                            "clicks": int(m.get("clicks", 0) or 0),
                            "spend": float(m.get("costMicros", 0) or 0) / 1_000_000,
                            "conversions": float(m.get("conversions", 0) or 0),
                        }

            for kw in kw_agg.values():
                kw["spend"] = round(kw["spend"], 2)
                kw["ctr"] = round((kw["clicks"] / kw["impressions"] * 100), 2) if kw["impressions"] > 0 else 0
                keywords.append(kw)
            keywords.sort(key=lambda x: x["spend"], reverse=True)

    camp_data["ad_groups"] = ad_groups
    camp_data["keywords"] = keywords
    return camp_data


# ═══════════════════════════════════════════════════════════════════
#  META ADS
# ═══════════════════════════════════════════════════════════════════

def list_meta_campaigns(db, brand, month: str = ""):
    """List all Meta ad campaigns with metrics for the selected month."""
    account_id = brand.get("meta_ad_account_id", "")
    if not account_id:
        return []

    token, conn = _get_tokens(db, brand["id"], "meta")
    if not token:
        return []

    start, end = _month_range(month)

    # Get campaign list with status
    campaigns_url = f"https://graph.facebook.com/v21.0/act_{account_id}/campaigns"
    campaigns_resp = requests.get(campaigns_url, params={
        "access_token": token,
        "fields": "id,name,status,objective,daily_budget,lifetime_budget,budget_remaining",
        "limit": 200,
    }, timeout=30)

    if campaigns_resp.status_code != 200:
        logger.error("Meta campaigns list error: %s", campaigns_resp.text[:300])
        return []

    campaigns_data = campaigns_resp.json().get("data", [])

    # Get metrics for each campaign
    insights_url = f"https://graph.facebook.com/v21.0/act_{account_id}/insights"
    insights_resp = requests.get(insights_url, params={
        "access_token": token,
        "time_range": json.dumps({"since": start, "until": end}),
        "fields": "campaign_id,campaign_name,spend,impressions,clicks,ctr,cpc,cpm,reach,frequency,actions,cost_per_action_type",
        "level": "campaign",
        "limit": 200,
    }, timeout=30)

    metrics_by_id = {}
    if insights_resp.status_code == 200:
        for row in insights_resp.json().get("data", []):
            cid = row.get("campaign_id", "")
            leads = 0
            cpl = 0
            for action in row.get("actions", []) or []:
                if action.get("action_type") in ("lead", "onsite_conversion.lead_grouped",
                                                   "offsite_conversion.fb_pixel_lead"):
                    leads += int(float(action.get("value", 0) or 0))
            for cpa in row.get("cost_per_action_type", []) or []:
                if cpa.get("action_type") in ("lead", "onsite_conversion.lead_grouped",
                                                "offsite_conversion.fb_pixel_lead"):
                    cpl = float(cpa.get("value", 0) or 0)

            metrics_by_id[cid] = {
                "spend": round(float(row.get("spend", 0) or 0), 2),
                "impressions": int(row.get("impressions", 0) or 0),
                "clicks": int(row.get("clicks", 0) or 0),
                "ctr": round(float(row.get("ctr", 0) or 0), 2),
                "cpc": round(float(row.get("cpc", 0) or 0), 2),
                "reach": int(row.get("reach", 0) or 0),
                "frequency": round(float(row.get("frequency", 0) or 0), 1),
                "conversions": leads,
                "cpa": round(cpl, 2),
            }

    result = []
    for camp in campaigns_data:
        cid = camp.get("id", "")
        m = metrics_by_id.get(cid, {})

        daily = float(camp.get("daily_budget", 0) or 0) / 100  # Meta stores in cents
        lifetime = float(camp.get("lifetime_budget", 0) or 0) / 100

        result.append({
            "id": cid,
            "name": camp.get("name", "Unknown"),
            "status": camp.get("status", "UNKNOWN"),
            "objective": camp.get("objective", ""),
            "daily_budget": round(daily, 2),
            "lifetime_budget": round(lifetime, 2),
            "impressions": m.get("impressions", 0),
            "clicks": m.get("clicks", 0),
            "spend": m.get("spend", 0),
            "ctr": m.get("ctr", 0),
            "cpc": m.get("cpc", 0),
            "reach": m.get("reach", 0),
            "frequency": m.get("frequency", 0),
            "conversions": m.get("conversions", 0),
            "cpa": m.get("cpa", 0),
            "platform": "meta",
        })

    result.sort(key=lambda x: x["spend"], reverse=True)
    return result


def update_meta_campaign_status(db, brand, campaign_id, new_status, changed_by):
    """Pause or enable a Meta campaign. new_status: 'PAUSED' or 'ACTIVE'."""
    token, conn = _get_tokens(db, brand["id"], "meta")
    if not token:
        return {"success": False, "error": "Meta not connected"}

    url = f"https://graph.facebook.com/v21.0/{campaign_id}"
    resp = requests.post(url, data={
        "access_token": token,
        "status": new_status,
    }, timeout=30)

    if resp.status_code != 200:
        return {"success": False, "error": resp.text[:300]}

    _log_change(db, brand["id"], "meta", campaign_id, "",
                f"status_{new_status.lower()}", {"new_status": new_status}, changed_by)

    return {"success": True}


def update_meta_budget(db, brand, campaign_id, new_daily_budget, changed_by):
    """Update daily budget for a Meta campaign."""
    token, conn = _get_tokens(db, brand["id"], "meta")
    if not token:
        return {"success": False, "error": "Meta not connected"}

    # Meta API expects budget in cents
    budget_cents = str(int(float(new_daily_budget) * 100))

    url = f"https://graph.facebook.com/v21.0/{campaign_id}"
    resp = requests.post(url, data={
        "access_token": token,
        "daily_budget": budget_cents,
    }, timeout=30)

    if resp.status_code != 200:
        return {"success": False, "error": resp.text[:300]}

    _log_change(db, brand["id"], "meta", campaign_id, "",
                "budget_update", {"new_daily_budget": new_daily_budget}, changed_by)

    return {"success": True}


def get_meta_campaign_detail(db, brand, campaign_id, month: str = ""):
    """Get detailed info for a single Meta campaign including ad sets and ads."""
    token, conn = _get_tokens(db, brand["id"], "meta")
    if not token:
        return None

    start, end = _month_range(month)
    time_range = json.dumps({"since": start, "until": end})

    # Campaign info
    camp_resp = requests.get(f"https://graph.facebook.com/v21.0/{campaign_id}", params={
        "access_token": token,
        "fields": "id,name,status,objective,daily_budget,lifetime_budget",
    }, timeout=30)

    if camp_resp.status_code != 200:
        return None

    camp = camp_resp.json()
    daily = float(camp.get("daily_budget", 0) or 0) / 100
    lifetime = float(camp.get("lifetime_budget", 0) or 0) / 100

    result = {
        "id": camp.get("id", ""),
        "name": camp.get("name", "Unknown"),
        "status": camp.get("status", ""),
        "objective": camp.get("objective", ""),
        "daily_budget": round(daily, 2),
        "lifetime_budget": round(lifetime, 2),
        "platform": "meta",
    }

    # Ad sets
    adsets_resp = requests.get(f"https://graph.facebook.com/v21.0/{campaign_id}/adsets", params={
        "access_token": token,
        "fields": "id,name,status,daily_budget,targeting",
        "limit": 100,
    }, timeout=30)

    ad_sets = []
    if adsets_resp.status_code == 200:
        for adset in adsets_resp.json().get("data", []):
            targeting = adset.get("targeting", {})
            geo = targeting.get("geo_locations", {})
            cities = [c.get("name", "") for c in geo.get("cities", [])]
            regions = [r.get("name", "") for r in geo.get("regions", [])]
            location_text = ", ".join(cities + regions) or "Not specified"

            age_min = targeting.get("age_min", "")
            age_max = targeting.get("age_max", "")
            age_text = f"{age_min}-{age_max}" if age_min else ""

            ad_sets.append({
                "id": adset.get("id", ""),
                "name": adset.get("name", "Unknown"),
                "status": adset.get("status", ""),
                "daily_budget": round(float(adset.get("daily_budget", 0) or 0) / 100, 2),
                "location": location_text,
                "age_range": age_text,
            })

    # Ad set insights
    adset_insights_resp = requests.get(
        f"https://graph.facebook.com/v21.0/{campaign_id}/insights", params={
            "access_token": token,
            "time_range": time_range,
            "fields": "adset_id,adset_name,spend,impressions,clicks,ctr,cpc,actions,cost_per_action_type",
            "level": "adset",
            "limit": 100,
        }, timeout=30,
    )

    if adset_insights_resp.status_code == 200:
        insights_map = {}
        for row in adset_insights_resp.json().get("data", []):
            insights_map[row.get("adset_id", "")] = row

        for adset in ad_sets:
            insight = insights_map.get(adset["id"], {})
            leads = 0
            for action in insight.get("actions", []) or []:
                if action.get("action_type") in ("lead", "onsite_conversion.lead_grouped",
                                                   "offsite_conversion.fb_pixel_lead"):
                    leads += int(float(action.get("value", 0) or 0))
            adset["spend"] = round(float(insight.get("spend", 0) or 0), 2)
            adset["impressions"] = int(insight.get("impressions", 0) or 0)
            adset["clicks"] = int(insight.get("clicks", 0) or 0)
            adset["ctr"] = round(float(insight.get("ctr", 0) or 0), 2)
            adset["conversions"] = leads

    # Top ads
    ads_resp = requests.get(
        f"https://graph.facebook.com/v21.0/{campaign_id}/insights", params={
            "access_token": token,
            "time_range": time_range,
            "fields": "ad_id,ad_name,spend,impressions,clicks,ctr,cpc,actions",
            "level": "ad",
            "limit": 50,
        }, timeout=30,
    )

    ads = []
    if ads_resp.status_code == 200:
        for row in ads_resp.json().get("data", []):
            ad_leads = 0
            for action in row.get("actions", []) or []:
                if action.get("action_type") in ("lead", "onsite_conversion.lead_grouped",
                                                   "offsite_conversion.fb_pixel_lead"):
                    ad_leads += int(float(action.get("value", 0) or 0))
            ads.append({
                "id": row.get("ad_id", ""),
                "name": row.get("ad_name", "Unknown"),
                "spend": round(float(row.get("spend", 0) or 0), 2),
                "impressions": int(row.get("impressions", 0) or 0),
                "clicks": int(row.get("clicks", 0) or 0),
                "ctr": round(float(row.get("ctr", 0) or 0), 2),
                "conversions": ad_leads,
            })
        ads.sort(key=lambda x: x["spend"], reverse=True)

    result["ad_sets"] = ad_sets
    result["ads"] = ads
    return result


# ═══════════════════════════════════════════════════════════════════
#  UNIFIED INTERFACE
# ═══════════════════════════════════════════════════════════════════

def list_all_campaigns(db, brand, month: str = ""):
    """List campaigns from all connected platforms."""
    campaigns = {"google": [], "meta": []}

    if brand.get("google_ads_customer_id"):
        try:
            campaigns["google"] = list_google_campaigns(db, brand, month)
        except Exception as e:
            logger.error("Error listing Google campaigns: %s", e)

    if brand.get("meta_ad_account_id"):
        try:
            campaigns["meta"] = list_meta_campaigns(db, brand, month)
        except Exception as e:
            logger.error("Error listing Meta campaigns: %s", e)

    return campaigns


# ═══════════════════════════════════════════════════════════════════
#  AI CAMPAIGN CREATION
# ═══════════════════════════════════════════════════════════════════


def _get_brand_api_key(brand):
    """Return the brand's OpenAI key, falling back to the app-level key."""
    key = ((brand or {}).get("openai_api_key") or "").strip()
    if key:
        return key
    try:
        from flask import current_app
        return (current_app.config.get("OPENAI_API_KEY", "") or "").strip()
    except RuntimeError:
        import os
        return os.environ.get("OPENAI_API_KEY", "").strip()


def _get_brand_model(brand, purpose="ads"):
    """Return the brand's preferred model for a purpose, with fallback."""
    field_map = {
        "ads": "openai_model_ads",
        "chat": "openai_model_chat",
        "analysis": "openai_model_analysis",
    }
    field = field_map.get(purpose, "openai_model_ads")
    model = ((brand or {}).get(field) or "").strip()
    if not model:
        model = ((brand or {}).get("openai_model") or "").strip()
    return model or "gpt-4o-mini"


def _brand_context_block(brand):
    """Build a brand identity block to inject into campaign prompts."""
    lines = []
    voice = (brand.get("brand_voice") or "").strip()
    if voice:
        lines.append(f"Brand voice / tone: {voice}")
    audience = (brand.get("target_audience") or "").strip()
    if audience:
        lines.append(f"Target audience: {audience}")
    offers = (brand.get("active_offers") or "").strip()
    if offers:
        lines.append(f"Active offers / promotions: {offers}")
    services = (brand.get("primary_services") or "").strip()
    if services:
        lines.append(f"Primary services: {services}")
    competitors = (brand.get("competitors") or "").strip()
    if competitors:
        lines.append(f"Known competitors: {competitors}")
    return "\n".join(lines)


def _proofread_campaign_plan(plan, brand):
    """Run a second AI pass that reviews ad copy as a skeptical target customer.

    Rewrites anything that sounds generic, forced, or off-putting, then
    returns the cleaned plan. Failures silently return the original plan.
    """
    import openai

    api_key = _get_brand_api_key(brand)
    if not api_key:
        return plan

    model = _get_brand_model(brand, "ads")
    client = openai.OpenAI(api_key=api_key)

    industry = brand.get("industry", "home services")
    audience = brand.get("target_audience", "local homeowners")

    review_prompt = f"""You are a proofreader for ad copy. You are NOT the advertiser.
You are the TARGET CUSTOMER: a real person in the {industry} market ({audience}).

Review every piece of ad copy in this campaign plan and ask yourself:
- Would I actually click this, or does it sound like every other ad?
- Does anything feel fake, exaggerated, or like obvious marketing-speak?
- Is the language natural, like something a friend would say?
- Are there cliches that would make me scroll past? ("Transform your...", "Don't miss out!", "Act now!", etc.)
- Would I trust this business based on this copy, or does it try too hard?

RULES for rewriting:
- Keep headlines under 40 characters (Meta) or 30 characters (Google).
- Keep descriptions under 90 characters.
- Primary text should be 2-4 sentences max, conversational.
- Be specific over generic. Actual numbers beat vague claims.
- Do NOT use: "Transform", "Unlock", "Elevate", "Don't miss out", "Act now",
  "Say goodbye to", "Game-changer", or exclamation marks in every line.
- Sound like a confident local business, not a marketing agency.
- Preserve the strategic intent and call to action of each ad.
- Keep campaign_name, objective, budget, targeting, and structure untouched.

Return the FULL JSON plan with only the ad copy improved. No commentary, no markdown fences, just valid JSON."""

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": review_prompt},
                {"role": "user", "content": json.dumps(plan)},
            ],
            temperature=0.4,
            max_tokens=3000,
        )
        text = response.choices[0].message.content.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()

        reviewed = json.loads(text)
        # Preserve fields the proofreader shouldn't touch
        reviewed["platform"] = plan.get("platform", "")
        reviewed["strategy"] = plan.get("strategy", "")
        return reviewed
    except Exception:
        logger.info("Proofreader pass failed, using original plan")
        return plan


def generate_campaign_plan(brand, service, location, monthly_budget,
                           platform, notes="", strategy_type=""):
    """Use GPT to generate a complete campaign plan ready for launch.

    When *strategy_type* is provided (e.g. "meta_omnipresent"), the
    campaign_templates module builds a strategy-specific prompt that
    includes the ad-knowledge master prompt.  Otherwise falls back to a
    generic prompt.
    """
    import openai

    api_key = _get_brand_api_key(brand)
    if not api_key:
        return {"success": False, "error": "OpenAI API key not configured"}

    model = _get_brand_model(brand, "ads")
    client = openai.OpenAI(api_key=api_key)

    # ── Strategy-based prompt (preferred) ──
    if strategy_type:
        from webapp.campaign_templates import build_strategy_prompt
        result = build_strategy_prompt(
            strategy_type, brand, service, location, monthly_budget, notes,
        )
        if result:
            system_prompt, user_prompt = result
            try:
                response = client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    temperature=0.3,
                    max_tokens=3000,
                )
                text = response.choices[0].message.content.strip()
                if text.startswith("```"):
                    text = text.split("\n", 1)[1] if "\n" in text else text[3:]
                    if text.endswith("```"):
                        text = text[:-3]
                    text = text.strip()

                plan = json.loads(text)
                plan["platform"] = platform
                plan["strategy"] = strategy_type
                plan = _proofread_campaign_plan(plan, brand)
                return {"success": True, "plan": plan}

            except json.JSONDecodeError:
                return {"success": False, "error": "AI returned invalid plan format. Please try again."}
            except Exception as e:
                return {"success": False, "error": str(e)}

    # ── Generic fallback (no strategy selected) ──
    industry = brand.get("industry", "home services")
    brand_name = brand.get("display_name", brand.get("name", ""))
    brand_ctx = _brand_context_block(brand)
    daily = round(float(monthly_budget) / 30, 2)

    if platform == "google":
        prompt = f"""Create a Google Ads Search campaign plan for a {industry} business.

Business: {brand_name}
Service to promote: {service}
Target location: {location}
Monthly budget: ${monthly_budget}
{brand_ctx}
{f'Additional notes: {notes}' if notes else ''}

Return a JSON object with this exact structure:
{{
    "campaign_name": "descriptive campaign name",
    "daily_budget": {daily},
    "ad_groups": [
        {{
            "name": "ad group name",
            "keywords": ["keyword 1", "keyword 2", "keyword 3"],
            "negative_keywords": ["negative 1", "negative 2"],
            "headlines": ["headline 1 (max 30 chars)", "headline 2", "headline 3", "headline 4", "headline 5"],
            "descriptions": ["description 1 (max 90 chars)", "description 2"]
        }}
    ],
    "campaign_negative_keywords": ["free", "diy", "how to"],
    "location_targeting": "{location}",
    "rationale": "brief explanation of the strategy"
}}

Requirements:
- Create 2-3 ad groups focused on different keyword themes
- 10-15 keywords per ad group, mix of exact and phrase match
- Headlines must be under 30 characters each
- Descriptions must be under 90 characters each
- Include relevant negative keywords to prevent wasted spend
- Focus on high-intent commercial keywords
- Daily budget should be ${daily}
- Ad copy MUST reflect the brand voice and tone described above
- If competitors are listed, position against their weaknesses
- If active offers exist, feature them prominently in ad copy"""

    else:  # meta
        prompt = f"""Create a Facebook/Instagram Ads campaign plan for a {industry} business.

Business: {brand_name}
Service to promote: {service}
Target location: {location}
Monthly budget: ${monthly_budget}
{brand_ctx}
{f'Additional notes: {notes}' if notes else ''}

Return a JSON object with this exact structure:
{{
    "campaign_name": "descriptive campaign name",
    "objective": "OUTCOME_LEADS",
    "daily_budget": {daily},
    "ad_sets": [
        {{
            "name": "ad set name",
            "targeting_description": "who this targets and why",
            "age_min": 25,
            "age_max": 65,
            "ad_copy": [
                {{
                    "headline": "attention-grabbing headline",
                    "primary_text": "compelling ad body text that drives action",
                    "description": "short description",
                    "call_to_action": "GET_QUOTE"
                }}
            ]
        }}
    ],
    "location_targeting": "{location}",
    "rationale": "brief explanation of the strategy"
}}

Requirements:
- Create 2-3 ad sets with different targeting approaches
- Each ad set should have 2-3 ad variations
- Focus on lead generation
- Use compelling, benefit-focused copy that matches the brand voice above
- Headlines should be short and punchy
- Primary text should address pain points and include a clear call to action
- If competitors are listed, differentiate against them
- If active offers exist, weave them into ad copy
- Call to action options: GET_QUOTE, LEARN_MORE, CONTACT_US, SIGN_UP, BOOK_NOW
- Daily budget should be ${daily}"""

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a senior digital advertising strategist working inside "
                        "GroMore, an ad management platform. Generate campaign plans that "
                        "are practical, conversion-focused, and ready to launch. "
                        "The client's brand identity and competitive landscape are included "
                        "in the prompt - use them to make ad copy specific, not generic. "
                        "Return ONLY valid JSON, no markdown."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
            max_tokens=2000,
        )

        text = response.choices[0].message.content.strip()
        # Strip markdown fences if present
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()

        plan = json.loads(text)
        plan["platform"] = platform
        plan = _proofread_campaign_plan(plan, brand)
        return {"success": True, "plan": plan}

    except json.JSONDecodeError:
        return {"success": False, "error": "AI returned invalid plan format. Please try again."}
    except Exception as e:
        return {"success": False, "error": str(e)}


def check_google_ads_config(db, brand):
    """Return a dict describing which Google Ads config pieces are present/missing."""
    customer_id = _clean_customer_id(brand.get("google_ads_customer_id"))
    token, conn = _get_tokens(db, brand["id"], "google")
    dev_token, login_cid = _google_config()
    missing = []
    if not customer_id:
        missing.append("Google Ads Customer ID (set in brand settings)")
    if not token:
        missing.append("Google OAuth connection (connect via Settings > Google)")
    if not dev_token:
        missing.append("Google Ads Developer Token (set in admin Settings > Google Ads API)")
    return {
        "ready": len(missing) == 0,
        "missing": missing,
        "customer_id": customer_id,
        "has_token": bool(token),
        "has_dev_token": bool(dev_token),
    }


def check_meta_ads_config(db, brand):
    """Return a dict describing which Meta Ads config pieces are present/missing."""
    account_id = brand.get("meta_ad_account_id", "")
    token, conn = _get_tokens(db, brand["id"], "meta")
    missing = []
    if not account_id:
        missing.append("Meta Ad Account ID (set in brand settings)")
    if not token:
        missing.append("Meta OAuth connection (connect via Settings > Facebook)")
    return {
        "ready": len(missing) == 0,
        "missing": missing,
        "account_id": account_id,
        "has_token": bool(token),
    }


def launch_google_campaign(db, brand, plan, changed_by):
    """Create a Google Ads campaign from an AI-generated plan."""
    customer_id = _clean_customer_id(brand.get("google_ads_customer_id"))
    token, conn = _get_tokens(db, brand["id"], "google")
    dev_token, login_cid = _google_config()

    if not all([customer_id, token, dev_token]):
        missing = []
        if not customer_id:
            missing.append("Google Ads Customer ID")
        if not token:
            missing.append("Google OAuth connection")
        if not dev_token:
            missing.append("Developer Token")
        return {"success": False, "error": f"Missing Google Ads configuration: {', '.join(missing)}. Use Save as Draft to keep this plan."}

    headers = _google_headers(token, dev_token, login_cid)
    base_url = f"https://googleads.googleapis.com/{GOOGLE_ADS_API_VERSION}/customers/{customer_id}"

    # Step 1: Create campaign budget
    daily_micros = str(int(float(plan.get("daily_budget", 50)) * 1_000_000))
    budget_body = {
        "operations": [{
            "create": {
                "name": f"{plan.get('campaign_name', 'New Campaign')} Budget",
                "amountMicros": daily_micros,
                "deliveryMethod": "STANDARD",
            }
        }]
    }

    budget_resp = requests.post(
        f"{base_url}/campaignBudgets:mutate",
        json=budget_body, headers=headers, timeout=30,
    )
    if budget_resp.status_code != 200:
        err_msg = _parse_google_error(budget_resp)
        if budget_resp.status_code == 501 or "not implemented" in err_msg.lower():
            err_msg = ("Your Google Ads developer token only has test-level access, "
                       "which cannot create campaigns in production accounts. "
                       "Apply for Basic Access in your MCC account under "
                       "Tools & Settings > API Center. Save as Draft to keep this plan.")
        return {"success": False, "error": f"Failed to create budget: {err_msg}"}

    budget_resource = budget_resp.json().get("results", [{}])[0].get("resourceName", "")
    if not budget_resource:
        return {"success": False, "error": "Budget created but no resource name returned"}

    # Step 2: Create campaign
    campaign_body = {
        "operations": [{
            "create": {
                "name": plan.get("campaign_name", "New Campaign"),
                "advertisingChannelType": "SEARCH",
                "status": "PAUSED",  # Always start paused for safety
                "campaignBudget": budget_resource,
                "networkSettings": {
                    "targetGoogleSearch": True,
                    "targetSearchNetwork": False,
                    "targetContentNetwork": False,
                },
                "biddingStrategyType": "MAXIMIZE_CONVERSIONS",
            }
        }]
    }

    camp_resp = requests.post(
        f"{base_url}/campaigns:mutate",
        json=campaign_body, headers=headers, timeout=30,
    )
    if camp_resp.status_code != 200:
        return {"success": False, "error": f"Failed to create campaign: {_parse_google_error(camp_resp)}"}

    camp_resource = camp_resp.json().get("results", [{}])[0].get("resourceName", "")
    camp_id = camp_resource.split("/")[-1] if camp_resource else ""

    # Step 3: Create ad groups + keywords + ads
    created_ad_groups = []
    for ag_plan in plan.get("ad_groups", []):
        # Create ad group
        ag_body = {
            "operations": [{
                "create": {
                    "name": ag_plan.get("name", "Ad Group"),
                    "campaign": camp_resource,
                    "status": "ENABLED",
                    "type": "SEARCH_STANDARD",
                }
            }]
        }

        ag_resp = requests.post(
            f"{base_url}/adGroups:mutate",
            json=ag_body, headers=headers, timeout=30,
        )
        if ag_resp.status_code != 200:
            continue

        ag_resource = ag_resp.json().get("results", [{}])[0].get("resourceName", "")

        # Add keywords
        kw_ops = []
        for kw in ag_plan.get("keywords", []):
            # Determine match type from notation
            if kw.startswith("[") and kw.endswith("]"):
                match_type = "EXACT"
                kw_text = kw[1:-1]
            elif kw.startswith('"') and kw.endswith('"'):
                match_type = "PHRASE"
                kw_text = kw[1:-1]
            else:
                match_type = "PHRASE"  # Default to phrase match for safety
                kw_text = kw

            kw_ops.append({
                "create": {
                    "adGroup": ag_resource,
                    "keyword": {"text": kw_text, "matchType": match_type},
                    "status": "ENABLED",
                }
            })

        if kw_ops:
            requests.post(
                f"{base_url}/adGroupCriteria:mutate",
                json={"operations": kw_ops}, headers=headers, timeout=30,
            )

        # Add negative keywords at campaign level
        neg_ops = []
        for nk in ag_plan.get("negative_keywords", []):
            neg_ops.append({
                "create": {
                    "campaign": camp_resource,
                    "negative": True,
                    "keyword": {"text": nk, "matchType": "BROAD"},
                }
            })
        if neg_ops:
            requests.post(
                f"{base_url}/campaignCriteria:mutate",
                json={"operations": neg_ops}, headers=headers, timeout=30,
            )

        # Create responsive search ad
        headlines = ag_plan.get("headlines", [])[:15]
        descriptions = ag_plan.get("descriptions", [])[:4]

        if headlines and descriptions:
            # Build RSA asset structure
            headline_assets = [{"text": h[:30]} for h in headlines]
            desc_assets = [{"text": d[:90]} for d in descriptions]

            ad_body = {
                "operations": [{
                    "create": {
                        "adGroup": ag_resource,
                        "ad": {
                            "responsiveSearchAd": {
                                "headlines": headline_assets,
                                "descriptions": desc_assets,
                            },
                            "finalUrls": [brand.get("website_url") or brand.get("website") or "https://example.com"],
                        },
                        "status": "ENABLED",
                    }
                }]
            }
            requests.post(
                f"{base_url}/adGroupAds:mutate",
                json=ad_body, headers=headers, timeout=30,
            )

        created_ad_groups.append(ag_plan.get("name", "Ad Group"))

    # Also add campaign-level negative keywords
    camp_neg_ops = []
    for nk in plan.get("campaign_negative_keywords", []):
        camp_neg_ops.append({
            "create": {
                "campaign": camp_resource,
                "negative": True,
                "keyword": {"text": nk, "matchType": "BROAD"},
            }
        })
    if camp_neg_ops:
        requests.post(
            f"{base_url}/campaignCriteria:mutate",
            json={"operations": camp_neg_ops}, headers=headers, timeout=30,
        )

    # Location targeting - resolve location name to geo target constant
    location_text = plan.get("location_targeting", "").strip()
    if location_text:
        # Search for geo target constant using Google Ads API
        geo_search_body = {
            "query": f"""
                SELECT geo_target_constant.resource_name,
                       geo_target_constant.name,
                       geo_target_constant.canonical_name,
                       geo_target_constant.target_type
                FROM geo_target_constant
                WHERE geo_target_constant.name LIKE '%{location_text.replace("'", "")}%'
                LIMIT 5
            """
        }
        geo_resp = requests.post(
            f"https://googleads.googleapis.com/{GOOGLE_ADS_API_VERSION}/customers/{customer_id}/googleAds:searchStream",
            json=geo_search_body, headers=headers, timeout=15,
        )
        if geo_resp.status_code == 200:
            geo_results = geo_resp.json()
            # Extract first matching geo target constant
            geo_resource = None
            for batch in geo_results if isinstance(geo_results, list) else [geo_results]:
                for result in batch.get("results", []):
                    geo_resource = result.get("geoTargetConstant", {}).get("resourceName")
                    if geo_resource:
                        break
                if geo_resource:
                    break

            if geo_resource:
                loc_body = {
                    "operations": [{
                        "create": {
                            "campaign": camp_resource,
                            "location": {"geoTargetConstant": geo_resource},
                        }
                    }]
                }
                loc_resp = requests.post(
                    f"{base_url}/campaignCriteria:mutate",
                    json=loc_body, headers=headers, timeout=30,
                )
                if loc_resp.status_code != 200:
                    logger.warning("Failed to set location targeting: %s", loc_resp.text[:300])
        else:
            logger.warning("Failed to search geo targets for '%s': %s", location_text, geo_resp.text[:300])

    _log_change(db, brand["id"], "google", camp_id, plan.get("campaign_name", ""),
                "campaign_created", {
                    "campaign_name": plan.get("campaign_name"),
                    "strategy": plan.get("strategy", "custom"),
                    "daily_budget": plan.get("daily_budget"),
                    "ad_groups": created_ad_groups,
                }, changed_by)

    return {
        "success": True,
        "campaign_id": camp_id,
        "campaign_name": plan.get("campaign_name", ""),
        "status": "PAUSED",
        "message": "Campaign created and paused. Review it in the campaigns list, then enable it when ready.",
    }


def _upload_image_to_meta(account_id, token, image_bytes):
    """Upload an image to Meta Ads and return the image_hash, or None on failure."""
    try:
        resp = requests.post(
            f"https://graph.facebook.com/v21.0/act_{account_id}/adimages",
            files={"filename": ("creative.png", image_bytes, "image/png")},
            data={"access_token": token},
            timeout=60,
        )
        if resp.status_code != 200:
            logger.warning("Meta image upload failed (%s): %s", resp.status_code, resp.text[:300])
            return None
        # Response: {"images": {"creative.png": {"hash": "abc123..."}}}
        images = resp.json().get("images", {})
        for _name, info in images.items():
            return info.get("hash")
        return None
    except Exception as exc:
        logger.warning("Meta image upload error: %s", exc)
        return None


def _resolve_ad_image(db, brand_id, ad_copy):
    """
    If an ad_copy dict includes a drive_file_id, download the image from Drive
    and return the raw bytes. Returns None if no image assigned or download fails.
    """
    drive_file_id = (ad_copy.get("drive_file_id") or "").strip()
    if not drive_file_id:
        return None
    try:
        from webapp.google_drive import download_file
        data, _mime = download_file(db, brand_id, drive_file_id)
        return data
    except Exception as exc:
        logger.warning("Failed to download Drive image %s: %s", drive_file_id, exc)
        return None


def launch_meta_campaign(db, brand, plan, changed_by):
    """Create a Meta campaign from an AI-generated plan."""
    account_id = _clean_meta_account_id(brand.get("meta_ad_account_id", ""))
    token, conn = _get_tokens(db, brand["id"], "meta")

    if not token or not account_id:
        missing = []
        if not account_id:
            missing.append("Meta Ad Account ID")
        if not token:
            missing.append("Meta OAuth connection")
        return {"success": False, "error": f"Missing Meta Ads configuration: {', '.join(missing)}. Use Save as Draft to keep this plan."}

    page_id = (brand.get("facebook_page_id") or "").strip()
    page_access_token = None

    # Always fetch pages from Meta to get the page access token and validate/fix the page_id
    try:
        pages_resp = requests.get(
            "https://graph.facebook.com/v21.0/me/accounts",
            params={"access_token": token, "fields": "id,name,access_token"},
            timeout=15,
        )
        if pages_resp.status_code == 200:
            pages = pages_resp.json().get("data", [])
            if pages:
                # If stored page_id is numeric and matches one of the pages, use it
                matched = None
                for p in pages:
                    if p["id"] == page_id:
                        matched = p
                        break
                if not matched:
                    # Stored value is missing, wrong, or a slug - use first available page
                    matched = pages[0]
                    logger.info("Replacing stored page_id '%s' with numeric ID %s", page_id, matched["id"])
                page_id = matched["id"]
                page_access_token = matched.get("access_token")
                # Save the correct numeric ID
                db.update_brand_api_field(brand["id"], "facebook_page_id", page_id)
    except Exception as e:
        logger.warning("Facebook page lookup failed: %s", e)
    if not page_id:
        return {"success": False, "error": "No Facebook Page linked. Go to Connections and connect your Facebook Page before launching Meta ads."}

    website_url = ((brand.get("website_url") or brand.get("website") or "")).strip()
    if not website_url:
        return {"success": False, "error": "No website URL is set for this brand. Add it in Business Settings before launching Meta ads."}
    if not re.match(r"^https?://", website_url, re.I):
        website_url = f"https://{website_url.lstrip('/')}"

    requested_objective = plan.get("objective", "OUTCOME_LEADS")
    requested_pixel_id = (plan.get("pixel_id") or "").strip()
    objective = requested_objective
    pixel_id = requested_pixel_id
    launch_notes = []

    # Website lead campaigns need a valid conversion object. If none is configured,
    # fall back to website traffic instead of creating an empty campaign shell.
    if objective == "OUTCOME_LEADS" and not pixel_id:
        objective = "OUTCOME_TRAFFIC"
        launch_notes.append("Lead Generation was switched to Website Traffic because no Meta Pixel was selected.")

    # Step 1: Create campaign
    camp_resp = requests.post(
        f"https://graph.facebook.com/v21.0/act_{account_id}/campaigns",
        data={
            "access_token": token,
            "name": plan.get("campaign_name", "New Campaign"),
            "objective": objective,
            "status": "PAUSED",
            "special_ad_categories": "[]",
            "is_adset_budget_sharing_enabled": "false",
        },
        timeout=30,
    )

    if camp_resp.status_code != 200:
        return {"success": False, "error": f"Failed to create campaign: {_parse_meta_error(camp_resp)}"}

    campaign_id = camp_resp.json().get("id", "")

    # Step 2: Create ad sets
    daily_cents = int(float(plan.get("daily_budget", 50)) * 100)
    created_adsets = []

    # Map objective to optimization goal
    opt_goal_map = {
        "OUTCOME_LEADS": "LEAD_GENERATION",
        "OUTCOME_AWARENESS": "REACH",
        "OUTCOME_ENGAGEMENT": "POST_ENGAGEMENT",
        "OUTCOME_TRAFFIC": "LINK_CLICKS",
        "OUTCOME_SALES": "OFFSITE_CONVERSIONS",
    }
    opt_goal = opt_goal_map.get(objective, "LEAD_GENERATION")

    adset_errors = []
    creative_errors = []
    created_ads = 0
    launch_notes = list(launch_notes)

    for adset_plan in plan.get("ad_sets", []):
        location = (plan.get("location_targeting", "") or "").strip()
        radius = adset_plan.get("radius_miles", 25)

        # Resolve location via Meta Targeting Search API for valid geo keys
        geo_locations = {"countries": ["US"]}  # safe default
        if location:
            loc_type = "adgeolocation" if not re.fullmatch(r"\d{5}", location) else "adgeolocation"
            loc_params = {
                "access_token": token,
                "type": loc_type,
                "q": location,
                "limit": 1,
            }
            if re.fullmatch(r"\d{5}", location):
                loc_params["location_types"] = '["zip"]'
            else:
                loc_params["location_types"] = '["city","region"]'
            try:
                loc_resp = requests.get(
                    "https://graph.facebook.com/v21.0/search",
                    params=loc_params, timeout=15,
                )
                if loc_resp.status_code == 200:
                    loc_data = loc_resp.json().get("data", [])
                    if loc_data:
                        loc_hit = loc_data[0]
                        loc_hit_type = loc_hit.get("type", "")
                        if loc_hit_type == "zip":
                            geo_locations = {"zips": [{"key": loc_hit["key"]}]}
                        elif loc_hit_type == "city":
                            geo_locations = {"cities": [{"key": loc_hit["key"], "radius": int(radius), "distance_unit": "mile"}]}
                        elif loc_hit_type == "region":
                            geo_locations = {"regions": [{"key": loc_hit["key"]}]}
                        else:
                            geo_locations = {"cities": [{"key": loc_hit["key"], "radius": int(radius), "distance_unit": "mile"}]}
                        logger.info("Resolved location '%s' -> type=%s key=%s", location, loc_hit_type, loc_hit.get("key"))
                    else:
                        logger.warning("Meta location search returned no results for '%s'", location)
                        launch_notes.append(f"Could not resolve location '{location}' - using US-wide targeting.")
                else:
                    logger.warning("Meta location search failed for '%s': %s", location, loc_resp.text[:200])
            except Exception as e:
                logger.warning("Meta location search error for '%s': %s", location, e)

        # Force numeric types for targeting fields
        age_min = int(adset_plan.get("age_min", 25) or 25)
        age_max = int(adset_plan.get("age_max", 65) or 65)

        targeting = {
            "age_min": age_min,
            "age_max": age_max,
            "geo_locations": geo_locations,
        }

        # Gender targeting: 1=male, 2=female, omit for all
        gender = adset_plan.get("gender", "")
        if gender == "male":
            targeting["genders"] = [1]
        elif gender == "female":
            targeting["genders"] = [2]

        # Interest/detailed targeting
        interests = adset_plan.get("interests", [])
        if interests:
            resolved_interests = []
            for interest_name in interests:
                try:
                    search_resp = requests.get(
                        "https://graph.facebook.com/v21.0/search",
                        params={
                            "access_token": token,
                            "type": "adinterest",
                            "q": interest_name,
                            "limit": 1,
                        },
                        timeout=15,
                    )
                    if search_resp.status_code == 200:
                        data = search_resp.json().get("data", [])
                        if data:
                            resolved_interests.append({"id": str(data[0]["id"]), "name": data[0]["name"]})
                except Exception:
                    pass
            if resolved_interests:
                targeting["flexible_spec"] = [{"interests": resolved_interests}]

        logger.info("Ad set targeting: %s", json.dumps(targeting))

        adset_data = {
            "access_token": token,
            "campaign_id": campaign_id,
            "name": adset_plan.get("name", "Ad Set"),
            "billing_event": "IMPRESSIONS",
            "optimization_goal": opt_goal,
            "bid_strategy": "LOWEST_COST_WITHOUT_CAP",
            "daily_budget": daily_cents,
            "status": "PAUSED",
            "targeting": json.dumps(targeting),
        }
        if objective in ("OUTCOME_TRAFFIC", "OUTCOME_LEADS"):
            adset_data["destination_type"] = "WEBSITE"

        # Pixel / conversion tracking
        if pixel_id and objective in ("OUTCOME_SALES", "OUTCOME_LEADS"):
            adset_data["promoted_object"] = json.dumps({"pixel_id": pixel_id, "custom_event_type": "LEAD" if objective == "OUTCOME_LEADS" else "PURCHASE"})

        adset_resp = requests.post(
            f"https://graph.facebook.com/v21.0/act_{account_id}/adsets",
            data=adset_data, timeout=30,
        )

        # Fallback 1: retry with US-wide geo if location targeting was rejected
        retried_without_geo = False
        if adset_resp.status_code != 200 and geo_locations != {"countries": ["US"]}:
            fallback_targeting = dict(targeting)
            fallback_targeting["geo_locations"] = {"countries": ["US"]}
            fallback_data = dict(adset_data)
            fallback_data["targeting"] = json.dumps(fallback_targeting)
            retry_resp = requests.post(
                f"https://graph.facebook.com/v21.0/act_{account_id}/adsets",
                data=fallback_data, timeout=30,
            )
            if retry_resp.status_code == 200:
                adset_resp = retry_resp
                retried_without_geo = True
                launch_notes.append(
                    f"Location targeting for '{adset_plan.get('name', 'Ad Set')}' was simplified to US-wide."
                )

        # Fallback 2: ultra-minimal targeting (just geo + age) if still failing
        if adset_resp.status_code != 200:
            minimal_targeting = {
                "age_min": 18,
                "age_max": 65,
                "geo_locations": {"countries": ["US"]},
            }
            minimal_data = {
                "access_token": token,
                "campaign_id": campaign_id,
                "name": adset_plan.get("name", "Ad Set"),
                "billing_event": "IMPRESSIONS",
                "optimization_goal": opt_goal,
                "bid_strategy": "LOWEST_COST_WITHOUT_CAP",
                "daily_budget": daily_cents,
                "status": "PAUSED",
                "targeting": json.dumps(minimal_targeting),
            }
            if objective in ("OUTCOME_TRAFFIC", "OUTCOME_LEADS"):
                minimal_data["destination_type"] = "WEBSITE"
            retry_resp2 = requests.post(
                f"https://graph.facebook.com/v21.0/act_{account_id}/adsets",
                data=minimal_data, timeout=30,
            )
            if retry_resp2.status_code == 200:
                adset_resp = retry_resp2
                launch_notes.append(
                    f"Ad set '{adset_plan.get('name', 'Ad Set')}' was created with minimal targeting due to Meta validation errors."
                )
            else:
                logger.warning("Ultra-minimal ad set also failed: %s | Raw: %s", _parse_meta_error(retry_resp2), retry_resp2.text[:500])

        if adset_resp.status_code == 200:
            adset_id = adset_resp.json().get("id", "")
            created_adsets.append({
                "id": adset_id,
                "name": adset_plan.get("name", "Ad Set"),
            })

            # Step 3: Create ad creative + ad for each ad copy variation
            for idx, copy in enumerate(adset_plan.get("ad_copy", [])):
                ad_name = f"{adset_plan.get('name', 'Ad Set')} - Ad {idx + 1}"

                # Resolve images for all three aspect ratios
                ratio_hashes = {}  # ratio -> image_hash
                for ratio in ("square", "landscape", "story"):
                    img_bytes = None
                    # Check ratio-specific Drive file
                    drive_id = (copy.get(f"drive_{ratio}") or "").strip()
                    if drive_id:
                        try:
                            from webapp.google_drive import download_file
                            img_bytes, _mime = download_file(db, brand["id"], drive_id)
                        except Exception as exc:
                            logger.warning("Failed to download Drive image %s for %s: %s", drive_id, ratio, exc)
                    # Check ratio-specific uploaded file
                    if not img_bytes:
                        upload_url = (copy.get(f"upload_{ratio}") or "").strip()
                        if upload_url:
                            import os
                            static_root = os.path.join(os.path.dirname(os.path.dirname(__file__)), "webapp", "static")
                            local_path = os.path.join(static_root, upload_url.lstrip("/static/"))
                            if os.path.exists(local_path):
                                with open(local_path, "rb") as f:
                                    img_bytes = f.read()
                    # Fallback: old single-image fields for square
                    if not img_bytes and ratio == "square":
                        img_bytes = _resolve_ad_image(db, brand["id"], copy)
                        if not img_bytes:
                            uploaded_url = (copy.get("uploaded_image_url") or "").strip()
                            if uploaded_url:
                                import os
                                static_root = os.path.join(os.path.dirname(os.path.dirname(__file__)), "webapp", "static")
                                local_path = os.path.join(static_root, uploaded_url.lstrip("/static/"))
                                if os.path.exists(local_path):
                                    with open(local_path, "rb") as f:
                                        img_bytes = f.read()
                    if img_bytes:
                        h = _upload_image_to_meta(account_id, token, img_bytes)
                        if h:
                            ratio_hashes[ratio] = h

                # Use the first available hash as primary image
                primary_hash = ratio_hashes.get("square") or ratio_hashes.get("landscape") or ratio_hashes.get("story")

                # Build object_story_spec (required for Meta ads)
                link_data = {
                    "message": copy.get("primary_text", ""),
                    "name": copy.get("headline", ""),
                    "description": copy.get("description", ""),
                    "call_to_action": {
                        "type": copy.get("call_to_action", "LEARN_MORE"),
                        "value": {"link": website_url},
                    },
                    "link": website_url,
                }
                if primary_hash:
                    link_data["image_hash"] = primary_hash

                # Use page access token for creative creation if available
                creative_token = page_access_token or token

                creative_data = {
                    "access_token": creative_token,
                    "name": ad_name,
                    "object_story_spec": json.dumps({
                        "page_id": page_id,
                        "link_data": link_data,
                    }),
                }

                # If multiple aspect ratios uploaded, use asset_feed_spec for multi-placement optimization
                if len(ratio_hashes) > 1:
                    asset_images = []
                    for r_key, r_hash in ratio_hashes.items():
                        asset_images.append({"hash": r_hash})
                    asset_bodies = [{"text": copy.get("primary_text", "")}]
                    asset_titles = [{"text": copy.get("headline", "")}]
                    asset_descs = [{"text": copy.get("description", "")}] if copy.get("description") else []
                    asset_cta = [{"type": copy.get("call_to_action", "LEARN_MORE"), "value": {"link": website_url}}]
                    asset_link = [{"website_url": website_url}]

                    asset_feed = {
                        "images": asset_images,
                        "bodies": asset_bodies,
                        "titles": asset_titles,
                        "call_to_actions": asset_cta,
                        "link_urls": asset_link,
                        "ad_formats": ["SINGLE_IMAGE"],
                    }
                    if asset_descs:
                        asset_feed["descriptions"] = asset_descs

                    creative_data = {
                        "access_token": creative_token,
                        "name": ad_name,
                        "object_story_spec": json.dumps({
                            "page_id": page_id,
                            "link_data": {
                                "message": copy.get("primary_text", ""),
                                "link": website_url,
                                "call_to_action": {"type": copy.get("call_to_action", "LEARN_MORE"), "value": {"link": website_url}},
                            },
                        }),
                        "asset_feed_spec": json.dumps(asset_feed),
                    }

                creative_resp = requests.post(
                    f"https://graph.facebook.com/v21.0/act_{account_id}/adcreatives",
                    data=creative_data, timeout=30,
                )
                if creative_resp.status_code != 200:
                    error_text = _parse_meta_error(creative_resp)
                    logger.warning("Failed to create ad creative: %s | page_id=%s | Raw: %s", error_text, page_id, creative_resp.text[:500])
                    creative_errors.append(f"{ad_name}: {error_text} (page_id={page_id})")
                    continue

                creative_id = creative_resp.json().get("id", "")

                # Create the ad linking creative to ad set
                ad_data = {
                    "access_token": token,
                    "name": ad_name,
                    "adset_id": adset_id,
                    "creative": json.dumps({"creative_id": creative_id}),
                    "status": "PAUSED",
                }
                ad_resp = requests.post(
                    f"https://graph.facebook.com/v21.0/act_{account_id}/ads",
                    data=ad_data, timeout=30,
                )
                if ad_resp.status_code != 200:
                    error_text = _parse_meta_error(ad_resp)
                    logger.warning("Failed to create ad: %s", error_text)
                    creative_errors.append(f"{ad_name}: {error_text}")
                else:
                    created_ads += 1
        else:
            error_text = _parse_meta_error(adset_resp)
            # Also log the full raw body for debugging
            logger.warning("Failed to create ad set '%s': %s | Raw: %s", adset_plan.get("name", "Ad Set"), error_text, adset_resp.text[:500])
            if retried_without_geo:
                error_text = f"{error_text} (also failed with US-wide targeting)"
            adset_errors.append(f"{adset_plan.get('name', 'Ad Set')}: {error_text}")

    if not created_adsets:
        # Include raw response for debugging
        raw_hint = ""
        try:
            raw_hint = " | Raw: " + adset_resp.text[:300]
        except Exception:
            pass
        return {
            "success": False,
            "error": "Campaign was created in Meta, but no ad sets could be created. " + (adset_errors[0] if adset_errors else "Check your objective, pixel, and targeting settings.") + raw_hint,
            "campaign_id": campaign_id,
        }

    if created_adsets and created_ads == 0:
        detail = creative_errors[0] if creative_errors else "Meta rejected all ad creative for this launch."
        return {
            "success": False,
            "error": f"Campaign and ad set were created, but no ads could be created. {detail}",
            "campaign_id": campaign_id,
        }

    _log_change(db, brand["id"], "meta", campaign_id, plan.get("campaign_name", ""),
                "campaign_created", {
                    "campaign_name": plan.get("campaign_name"),
                    "strategy": plan.get("strategy", "custom"),
                    "daily_budget": plan.get("daily_budget"),
                    "objective": objective,
                    "ad_sets": [a["name"] for a in created_adsets],
                    "ad_count": created_ads,
                    "notes": launch_notes,
                }, changed_by)

    message = "Campaign created and paused. Review it in the campaigns list, then enable it when ready."
    if launch_notes:
        message = f"{message} {' '.join(launch_notes)}"

    return {
        "success": True,
        "campaign_id": campaign_id,
        "campaign_name": plan.get("campaign_name", ""),
        "status": "PAUSED",
        "message": message,
    }


# ═══════════════════════════════════════════════════════════════════
#  AI RECOMMENDATIONS FOR EXISTING CAMPAIGNS
# ═══════════════════════════════════════════════════════════════════

def get_campaign_recommendations(brand, campaigns):
    """Get AI-powered recommendations for existing campaigns."""
    import openai

    api_key = _get_brand_api_key(brand)
    if not api_key:
        return []

    model = _get_brand_model(brand, "ads")
    client = openai.OpenAI(api_key=api_key)

    # Build a summary of campaign performance
    summary_lines = []
    for platform, camp_list in campaigns.items():
        if not camp_list:
            continue
        summary_lines.append(f"\n{platform.upper()} CAMPAIGNS:")
        for c in camp_list[:10]:
            status = c.get("status", "")
            name = c.get("name", "")
            spend = c.get("spend", 0)
            clicks = c.get("clicks", 0)
            conversions = c.get("conversions", 0)
            ctr = c.get("ctr", 0)
            cpc = c.get("cpc", 0)
            cpa = c.get("cpa", 0)
            budget = c.get("daily_budget", 0)

            summary_lines.append(
                f"- {name} [{status}]: ${spend:.2f} spend, {clicks} clicks, "
                f"{conversions} conversions, {ctr}% CTR, ${cpc:.2f} CPC, "
                f"${cpa:.2f} CPA, ${budget:.2f}/day budget"
            )

    if not summary_lines:
        return []

    industry = brand.get("industry", "home services")
    summary = "\n".join(summary_lines)

    brand_ctx = _brand_context_block(brand)
    brand_note = f"\n\nBrand context:\n{brand_ctx}" if brand_ctx else ""

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a senior paid media strategist analyzing campaign performance "
                        f"for a {industry} business. Give specific, actionable recommendations "
                        "that align with the brand voice and competitive positioning. "
                        "Be direct and practical. No fluff."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Here are the campaigns for the last 30 days:\n{summary}"
                        f"{brand_note}\n\n"
                        "Give me 3-5 specific recommendations. For each, include:\n"
                        "1. Which campaign it applies to\n"
                        "2. What the problem or opportunity is\n"
                        "3. The specific action to take\n"
                        "4. Expected impact\n\n"
                        "Return JSON array:\n"
                        '[{"campaign": "name", "issue": "...", "action": "...", '
                        '"impact": "...", "priority": "high|medium|low"}]'
                    ),
                },
            ],
            temperature=0.3,
            max_tokens=1500,
        )

        text = response.choices[0].message.content.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()

        return json.loads(text)

    except Exception as e:
        logger.error("Campaign recommendations error: %s", e)
        return []
