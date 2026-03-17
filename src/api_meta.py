"""
Meta (Facebook/Instagram) Marketing API Integration

Pulls ad performance data directly from the Meta Marketing API.

Setup:
  1. Go to developers.facebook.com and create an app (type: Business)
  2. Add "Marketing API" product to your app
  3. Generate a System User access token with ads_read permission
     (Business Settings > System Users > Generate Token)
  4. Get the Ad Account ID from Ads Manager (format: act_123456789)
  5. Save credentials to config/meta_credentials.json
"""
import json
import calendar
from pathlib import Path
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adsinsights import AdsInsights


CREDENTIALS_PATH = Path(__file__).parent.parent / "config" / "meta_credentials.json"


def _init_api(credentials_path=None):
    """Initialize the Facebook Ads API with stored credentials."""
    creds_path = credentials_path or CREDENTIALS_PATH
    if not Path(creds_path).exists():
        raise FileNotFoundError(
            f"Meta credentials not found at {creds_path}. "
            "Create config/meta_credentials.json with your app_id, app_secret, "
            "and access_token. See README for setup instructions."
        )

    with open(creds_path, "r") as f:
        creds = json.load(f)

    required_keys = ["app_id", "app_secret", "access_token"]
    missing = [k for k in required_keys if k not in creds]
    if missing:
        raise ValueError(f"Missing keys in meta_credentials.json: {missing}")

    FacebookAdsApi.init(
        creds["app_id"],
        creds["app_secret"],
        creds["access_token"],
    )
    return creds


def _month_date_range(month_str):
    """Convert '2026-03' to start/end date dicts for API."""
    year, month = map(int, month_str.split("-"))
    last_day = calendar.monthrange(year, month)[1]
    return {
        "since": f"{year}-{month:02d}-01",
        "until": f"{year}-{month:02d}-{last_day}",
    }


def pull_meta_ads_data(ad_account_id, month_str, credentials_path=None):
    """
    Pull Meta Ads data for a given account and month.

    Args:
        ad_account_id: The ad account ID (e.g., "act_123456789")
        month_str: Month string like "2026-03"
        credentials_path: Optional path to credentials JSON

    Returns:
        Dict in the same format as parsers.parse_meta_business() so it
        plugs directly into the existing analysis pipeline.
    """
    _init_api(credentials_path)

    if not ad_account_id.startswith("act_"):
        ad_account_id = f"act_{ad_account_id}"

    date_range = _month_date_range(month_str)
    account = AdAccount(ad_account_id)

    # ── Pull account-level totals ──
    totals = _pull_account_totals(account, date_range)

    # ── Pull campaign-level breakdown ──
    by_campaign = _pull_campaign_breakdown(account, date_range)

    # ── Pull ad set-level breakdown ──
    by_ad_set = _pull_adset_breakdown(account, date_range)

    return {
        "totals": totals,
        "by_campaign": by_campaign,
        "by_ad_set": by_ad_set,
        "row_count": len(by_campaign),
        "columns_found": list(totals.keys()),
        "data_source": "meta_api",
    }


def _pull_account_totals(account, date_range):
    """Pull aggregate account-level metrics."""
    fields = [
        AdsInsights.Field.impressions,
        AdsInsights.Field.reach,
        AdsInsights.Field.clicks,
        AdsInsights.Field.ctr,
        AdsInsights.Field.cpc,
        AdsInsights.Field.cpm,
        AdsInsights.Field.spend,
        AdsInsights.Field.frequency,
        AdsInsights.Field.actions,
        AdsInsights.Field.cost_per_action_type,
    ]

    params = {
        "time_range": date_range,
        "level": "account",
    }

    insights = account.get_insights(fields=fields, params=params)

    totals = {}
    for row in insights:
        totals["impressions"] = int(row.get("impressions", 0))
        totals["reach"] = int(row.get("reach", 0))
        totals["clicks"] = int(row.get("clicks", 0))
        totals["spend"] = round(float(row.get("spend", 0)), 2)
        totals["frequency"] = round(float(row.get("frequency", 0)), 2)

        # Calculate CTR, CPC, CPM from raw values for accuracy
        if totals["impressions"] > 0:
            totals["ctr"] = round((totals["clicks"] / totals["impressions"]) * 100, 2)
            totals["cpm"] = round((totals["spend"] / totals["impressions"]) * 1000, 2)
        if totals["clicks"] > 0:
            totals["cpc"] = round(totals["spend"] / totals["clicks"], 2)

        # Extract leads/conversions from actions
        actions = row.get("actions", [])
        results = _extract_lead_actions(actions)
        totals["results"] = results

        # Cost per result
        if results > 0:
            totals["cost_per_result"] = round(totals["spend"] / results, 2)

    return totals


def _pull_campaign_breakdown(account, date_range):
    """Pull metrics broken down by campaign."""
    fields = [
        AdsInsights.Field.campaign_name,
        AdsInsights.Field.campaign_id,
        AdsInsights.Field.impressions,
        AdsInsights.Field.reach,
        AdsInsights.Field.clicks,
        AdsInsights.Field.spend,
        AdsInsights.Field.actions,
        AdsInsights.Field.frequency,
    ]

    params = {
        "time_range": date_range,
        "level": "campaign",
    }

    insights = account.get_insights(fields=fields, params=params)

    by_campaign = {}
    for row in insights:
        name = row.get("campaign_name", "Unknown")
        impressions = int(row.get("impressions", 0))
        clicks = int(row.get("clicks", 0))
        spend = round(float(row.get("spend", 0)), 2)

        actions = row.get("actions", [])
        results = _extract_lead_actions(actions)

        camp_data = {
            "impressions": impressions,
            "reach": int(row.get("reach", 0)),
            "clicks": clicks,
            "spend": spend,
            "results": results,
        }

        if impressions > 0:
            camp_data["ctr"] = round((clicks / impressions) * 100, 2)
            camp_data["cpm"] = round((spend / impressions) * 1000, 2)
        if clicks > 0:
            camp_data["cpc"] = round(spend / clicks, 2)
        if results > 0:
            camp_data["cost_per_result"] = round(spend / results, 2)

        by_campaign[name] = camp_data

    return by_campaign


def _pull_adset_breakdown(account, date_range):
    """Pull metrics broken down by ad set."""
    fields = [
        AdsInsights.Field.adset_name,
        AdsInsights.Field.adset_id,
        AdsInsights.Field.impressions,
        AdsInsights.Field.reach,
        AdsInsights.Field.clicks,
        AdsInsights.Field.spend,
        AdsInsights.Field.actions,
    ]

    params = {
        "time_range": date_range,
        "level": "adset",
    }

    insights = account.get_insights(fields=fields, params=params)

    by_ad_set = {}
    for row in insights:
        name = row.get("adset_name", "Unknown")
        impressions = int(row.get("impressions", 0))
        clicks = int(row.get("clicks", 0))
        spend = round(float(row.get("spend", 0)), 2)
        results = _extract_lead_actions(row.get("actions", []))

        adset_data = {
            "impressions": impressions,
            "reach": int(row.get("reach", 0)),
            "clicks": clicks,
            "spend": spend,
            "results": results,
        }

        if impressions > 0:
            adset_data["ctr"] = round((clicks / impressions) * 100, 2)
        if clicks > 0:
            adset_data["cpc"] = round(spend / clicks, 2)
        if results > 0:
            adset_data["cost_per_result"] = round(spend / results, 2)

        by_ad_set[name] = adset_data

    return by_ad_set


def _extract_lead_actions(actions):
    """
    Extract lead/conversion count from Meta actions array.
    Meta tracks many action types - we sum the ones relevant to home services.
    """
    if not actions:
        return 0

    lead_action_types = {
        "lead",
        "offsite_conversion.fb_pixel_lead",
        "onsite_conversion.lead_grouped",
        "onsite_conversion.messaging_conversation_started_7d",
        "contact_total",
        "onsite_conversion.messaging_first_reply",
        "offsite_conversion.fb_pixel_schedule",
        "offsite_conversion.fb_pixel_contact",
        "offsite_conversion.fb_pixel_complete_registration",
        "phone_call",
    }

    total = 0
    for action in actions:
        action_type = action.get("action_type", "")
        if action_type in lead_action_types:
            total += int(action.get("value", 0))

    # If no specific lead actions found, fall back to total actions
    if total == 0:
        for action in actions:
            if action.get("action_type") == "lead":
                total += int(action.get("value", 0))

    return total
