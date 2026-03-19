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

    url = f"https://googleads.googleapis.com/v18/customers/{customer_id}/googleAds:searchStream"
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
    url = f"https://googleads.googleapis.com/v18/customers/{customer_id}/campaigns:mutate"
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

    url = f"https://googleads.googleapis.com/v18/customers/{customer_id}/campaignBudgets:mutate"
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

    url = f"https://googleads.googleapis.com/v18/customers/{customer_id}/campaignCriteria:mutate"
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

    url = f"https://googleads.googleapis.com/v18/customers/{customer_id}/googleAds:searchStream"
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

def generate_campaign_plan(brand, service, location, monthly_budget, platform, notes=""):
    """Use GPT to generate a complete campaign plan ready for launch."""
    import openai
    from flask import current_app

    api_key = (current_app.config.get("OPENAI_API_KEY", "") or "").strip()
    if not api_key:
        return {"success": False, "error": "OpenAI API key not configured"}

    client = openai.OpenAI(api_key=api_key)

    industry = brand.get("industry", "home services")
    brand_name = brand.get("display_name", brand.get("name", ""))

    if platform == "google":
        prompt = f"""Create a Google Ads Search campaign plan for a {industry} business.

Business: {brand_name}
Service to promote: {service}
Target location: {location}
Monthly budget: ${monthly_budget}
{f'Additional notes: {notes}' if notes else ''}

Return a JSON object with this exact structure:
{{
    "campaign_name": "descriptive campaign name",
    "daily_budget": {round(float(monthly_budget) / 30, 2)},
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
- Daily budget should be ${round(float(monthly_budget) / 30, 2)}"""

    else:  # meta
        prompt = f"""Create a Facebook/Instagram Ads campaign plan for a {industry} business.

Business: {brand_name}
Service to promote: {service}
Target location: {location}
Monthly budget: ${monthly_budget}
{f'Additional notes: {notes}' if notes else ''}

Return a JSON object with this exact structure:
{{
    "campaign_name": "descriptive campaign name",
    "objective": "OUTCOME_LEADS",
    "daily_budget": {round(float(monthly_budget) / 30, 2)},
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
- Use compelling, benefit-focused copy
- Headlines should be short and punchy
- Primary text should address pain points and include a clear call to action
- Call to action options: GET_QUOTE, LEARN_MORE, CONTACT_US, SIGN_UP, BOOK_NOW
- Daily budget should be ${round(float(monthly_budget) / 30, 2)}"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a senior digital advertising strategist. "
                        "Generate campaign plans that are practical, conversion-focused, "
                        "and ready to launch. Return ONLY valid JSON, no markdown."
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
        return {"success": True, "plan": plan}

    except json.JSONDecodeError:
        return {"success": False, "error": "AI returned invalid plan format. Please try again."}
    except Exception as e:
        return {"success": False, "error": str(e)}


def launch_google_campaign(db, brand, plan, changed_by):
    """Create a Google Ads campaign from an AI-generated plan."""
    customer_id = _clean_customer_id(brand.get("google_ads_customer_id"))
    token, conn = _get_tokens(db, brand["id"], "google")
    dev_token, login_cid = _google_config()

    if not all([customer_id, token, dev_token]):
        return {"success": False, "error": "Missing Google Ads configuration"}

    headers = _google_headers(token, dev_token, login_cid)
    base_url = f"https://googleads.googleapis.com/v18/customers/{customer_id}"

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
        return {"success": False, "error": f"Failed to create budget: {budget_resp.text[:300]}"}

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
        return {"success": False, "error": f"Failed to create campaign: {camp_resp.text[:300]}"}

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
                            "finalUrls": [brand.get("website_url", "https://example.com")],
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

    _log_change(db, brand["id"], "google", camp_id, plan.get("campaign_name", ""),
                "campaign_created", {
                    "campaign_name": plan.get("campaign_name"),
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


def launch_meta_campaign(db, brand, plan, changed_by):
    """Create a Meta campaign from an AI-generated plan."""
    account_id = brand.get("meta_ad_account_id", "")
    token, conn = _get_tokens(db, brand["id"], "meta")

    if not token or not account_id:
        return {"success": False, "error": "Meta Ads not connected"}

    # Step 1: Create campaign
    camp_resp = requests.post(
        f"https://graph.facebook.com/v21.0/act_{account_id}/campaigns",
        data={
            "access_token": token,
            "name": plan.get("campaign_name", "New Campaign"),
            "objective": plan.get("objective", "OUTCOME_LEADS"),
            "status": "PAUSED",  # Always start paused
            "special_ad_categories": "[]",
        },
        timeout=30,
    )

    if camp_resp.status_code != 200:
        return {"success": False, "error": f"Failed to create campaign: {camp_resp.text[:300]}"}

    campaign_id = camp_resp.json().get("id", "")

    # Step 2: Create ad sets
    daily_cents = str(int(float(plan.get("daily_budget", 50)) * 100))
    created_adsets = []

    for adset_plan in plan.get("ad_sets", []):
        location = plan.get("location_targeting", "")

        adset_data = {
            "access_token": token,
            "campaign_id": campaign_id,
            "name": adset_plan.get("name", "Ad Set"),
            "billing_event": "IMPRESSIONS",
            "optimization_goal": "LEAD_GENERATION",
            "daily_budget": daily_cents,
            "status": "PAUSED",
            "targeting": json.dumps({
                "age_min": adset_plan.get("age_min", 25),
                "age_max": adset_plan.get("age_max", 65),
                "geo_locations": {
                    "location_types": ["home"],
                    "custom_locations": [{
                        "address_string": location,
                        "radius": 25,
                        "distance_unit": "mile",
                    }] if location else [],
                },
            }),
        }

        adset_resp = requests.post(
            f"https://graph.facebook.com/v21.0/act_{account_id}/adsets",
            data=adset_data, timeout=30,
        )

        if adset_resp.status_code == 200:
            adset_id = adset_resp.json().get("id", "")
            created_adsets.append({
                "id": adset_id,
                "name": adset_plan.get("name", "Ad Set"),
            })

    _log_change(db, brand["id"], "meta", campaign_id, plan.get("campaign_name", ""),
                "campaign_created", {
                    "campaign_name": plan.get("campaign_name"),
                    "daily_budget": plan.get("daily_budget"),
                    "ad_sets": [a["name"] for a in created_adsets],
                }, changed_by)

    return {
        "success": True,
        "campaign_id": campaign_id,
        "campaign_name": plan.get("campaign_name", ""),
        "status": "PAUSED",
        "message": "Campaign created and paused. Review it in the campaigns list, then enable it when ready.",
    }


# ═══════════════════════════════════════════════════════════════════
#  AI RECOMMENDATIONS FOR EXISTING CAMPAIGNS
# ═══════════════════════════════════════════════════════════════════

def get_campaign_recommendations(brand, campaigns):
    """Get AI-powered recommendations for existing campaigns."""
    import openai
    from flask import current_app

    api_key = (current_app.config.get("OPENAI_API_KEY", "") or "").strip()
    if not api_key:
        return []

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

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a senior paid media strategist analyzing campaign performance "
                        f"for a {industry} business. Give specific, actionable recommendations. "
                        "Be direct and practical. No fluff."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Here are the campaigns for the last 30 days:\n{summary}\n\n"
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
