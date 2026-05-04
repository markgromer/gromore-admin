"""
Client Action Advisor

Takes analysis + suggestions from the analytics pipeline and produces:
1. Plain-English metric explanations ("Your click-through rate is 3.2% - this means...")
2. AI-generated action deliverables based on real account data
3. Priority-ranked action cards (max 2 per priority level)

Action cards are powered by AI that reads the actual campaign data, keyword
performance, competitor signals, and account metrics to produce specific,
ready-to-implement deliverables rather than generic how-to instructions.
"""
import json
import logging
import os

import requests as _requests

log = logging.getLogger(__name__)

# ── Mission metadata for gamified action plan ──

_CATEGORY_META = {
    "paid_advertising": {"icon": "bi-megaphone-fill", "color": "#6366f1", "skill": "Ad Optimization",
                         "platform_url": "", "platform_label": ""},
    "seo":              {"icon": "bi-search",         "color": "#059669", "skill": "Search Visibility",
                         "platform_url": "https://search.google.com/search-console", "platform_label": "Open Search Console"},
    "website":          {"icon": "bi-globe2",         "color": "#2563eb", "skill": "Website Performance",
                         "platform_url": "https://analytics.google.com", "platform_label": "Open Google Analytics"},
    "strategy":         {"icon": "bi-compass-fill",   "color": "#7c3aed", "skill": "Growth Strategy",
                         "platform_url": "", "platform_label": ""},
    "creative":         {"icon": "bi-palette-fill",   "color": "#db2777", "skill": "Creative Impact",
                         "platform_url": "https://business.facebook.com/adsmanager", "platform_label": "Open Ads Manager"},
    "budget":           {"icon": "bi-piggy-bank-fill","color": "#d97706", "skill": "Budget Strategy",
                         "platform_url": "", "platform_label": ""},
    "organic_social":   {"icon": "bi-people-fill",    "color": "#0891b2", "skill": "Social Engagement",
                         "platform_url": "https://business.facebook.com", "platform_label": "Open Meta Business Suite"},
}

_PLATFORM_META = {
    "google_ads": {"url": "https://ads.google.com", "label": "Open Google Ads"},
    "meta_ads": {"url": "https://business.facebook.com/adsmanager", "label": "Open Ads Manager"},
    "search_console": {"url": "https://search.google.com/search-console", "label": "Open Search Console"},
    "analytics": {"url": "https://analytics.google.com", "label": "Open Google Analytics"},
    "meta_business": {"url": "https://business.facebook.com", "label": "Open Meta Business Suite"},
}

MONTH_LEVELS = [
    (0,    1, "Rookie",          "Just getting started"),
    (200,  2, "Apprentice",      "Finding your stride"),
    (400,  3, "Strategist",      "Thinking like a marketer"),
    (700,  4, "Optimizer",       "Squeezing more from every dollar"),
    (1000, 5, "Growth Hacker",   "Your competitors should worry"),
    (1400, 6, "Marketing Pro",   "Running a tight ship"),
    (1800, 7, "Marketing Legend", "Nothing gets past you"),
]


_MISSION_SKILL_PROFILES = {
    "beginner": {
        "skill_level": "beginner",
        "label": "Starter Track",
        "summary": "Fewer missions, simpler language, and obvious first steps so new owners can build confidence fast.",
        "tagline": "Start small. Stack wins. Keep momentum.",
        "max_active": 3,
        "preview_steps": 3,
        "max_steps_hint": 4,
        "queue_title": "More Missions",
    },
    "intermediate": {
        "skill_level": "intermediate",
        "label": "Builder Track",
        "summary": "A compact queue with enough detail to move quickly without burying the user in operator noise.",
        "tagline": "Balanced pace, clearer priorities, stronger execution.",
        "max_active": 4,
        "preview_steps": 3,
        "max_steps_hint": 5,
        "queue_title": "Mission Queue",
    },
    "advanced": {
        "skill_level": "advanced",
        "label": "Operator Track",
        "summary": "A denser stack for owners who already know the tools and want tighter, faster operator-level missions.",
        "tagline": "Less hand-holding, more leverage.",
        "max_active": 6,
        "preview_steps": 4,
        "max_steps_hint": 5,
        "queue_title": "Extended Queue",
    },
}


def infer_mission_profile(completed_count=0, requested_level="auto"):
    requested = (requested_level or "auto").strip().lower()
    if requested in _MISSION_SKILL_PROFILES:
        profile = dict(_MISSION_SKILL_PROFILES[requested])
        profile["source"] = "manual"
        profile["requested_level"] = requested
        return profile

    if completed_count >= 8:
        resolved = "advanced"
    elif completed_count >= 3:
        resolved = "intermediate"
    else:
        resolved = "beginner"

    profile = dict(_MISSION_SKILL_PROFILES[resolved])
    profile["source"] = "auto"
    profile["requested_level"] = requested
    return profile


def _trim_copy(value, limit=160):
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


def _normalize_text_list(value):
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if not isinstance(value, (list, tuple)):
        return []
    items = []
    for raw in value:
        text = str(raw or "").strip()
        if text:
            items.append(text)
    return items


def _to_float(value, default=0.0):
    try:
        if value in (None, ""):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _has_channel_signal(value):
    if isinstance(value, dict):
        for nested in value.values():
            if isinstance(nested, dict):
                if _has_channel_signal(nested):
                    return True
            elif nested not in (None, "", 0, 0.0, False, []):
                return True
        return False
    if isinstance(value, (list, tuple, set)):
        return any(bool(item) for item in value)
    return value not in (None, "", 0, 0.0, False)


def _resolve_action_platform(title, category, relevant_data=None, detail=""):
    category = str(category or "").strip().lower()
    relevant_data = relevant_data or {}
    combined = " ".join(str(part or "") for part in (title, detail)).lower()

    has_meta = any(
        _has_channel_signal(relevant_data.get(key))
        for key in ("meta_campaigns", "meta_top_ads", "meta_kpis")
    )
    has_google = any(
        _has_channel_signal(relevant_data.get(key))
        for key in ("google_ads_campaigns", "google_ads_search_terms", "google_ads_kpis")
    )

    meta_words = ("facebook", "meta", "instagram", "ads manager")
    google_words = ("google ads", "ads.google.com", "search terms", "keyword", "keywords", "ppc", "cpc")
    if any(word in combined for word in meta_words) and not any(word in combined for word in google_words):
        return "meta_ads"
    if any(word in combined for word in google_words) and not any(word in combined for word in meta_words):
        return "google_ads"

    if category == "seo":
        return "search_console"
    if category == "website":
        return "analytics"
    if category == "organic_social":
        return "meta_business"
    if category == "strategy":
        if any(word in combined for word in ("traffic", "sessions", "analytics", "source/medium", "acquisition")):
            return "analytics"
        if any(word in combined for word in ("search console", "rank", "ranking", "impressions", "clicks", "organic")):
            return "search_console"
        if any(word in combined for word in ("campaign", "ad spend", "ads manager", "meta ads", "facebook ads", "instagram ads")) and has_meta and not has_google:
            return "meta_ads"
        if any(word in combined for word in ("campaign", "ad spend", "google ads", "ppc", "cpc")) and has_google:
            return "google_ads"
        return ""
    if category == "creative":
        if has_google and not has_meta:
            return "google_ads"
        return "meta_ads"
    if category in {"paid_advertising", "budget"}:
        if has_meta and not has_google:
            return "meta_ads"
        if has_google and not has_meta:
            return "google_ads"
        return ""
    return ""


def _platform_link(platform_key):
    meta = _PLATFORM_META.get(platform_key) or {}
    return meta.get("url", ""), meta.get("label", "")


def _build_action_items(suggestions, analysis_summary):
    paid_categories = {"paid_advertising", "budget", "creative"}
    seo_categories = {"seo"}
    web_categories = {"website"}
    organic_categories = {"organic_social"}

    google_ads_detail = analysis_summary.get("google_ads_detail", {})
    meta_detail = analysis_summary.get("meta_detail", {})
    seo_detail = analysis_summary.get("seo_detail", {})
    website_detail = analysis_summary.get("website_detail", {})
    fb_organic_detail = analysis_summary.get("facebook_organic_detail", {})
    kpis = analysis_summary.get("kpis", {})
    ad_intelligence = analysis_summary.get("ad_intelligence") or {}

    action_items = []
    for suggestion in suggestions:
        category = suggestion.get("category", "")
        title_lower = suggestion["title"].lower()
        detail_lower = str(suggestion.get("detail") or "").lower()
        paid_context = category in paid_categories or any(
            word in f"{title_lower} {detail_lower}"
            for word in (
                "google ads", "ads.google.com", "search term", "cpc", "cpa", "cost per click",
                "cost per lead", "paid", "ad spend", "campaign", "ad set", "ads manager",
                "meta ads", "facebook ads", "instagram ads",
            )
        )
        google_paid_hint = any(
            word in f"{title_lower} {detail_lower}"
            for word in ("google ads", "ads.google.com", "search term", "search terms", "keyword", "keywords", "quality score")
        )
        meta_paid_hint = any(
            word in f"{title_lower} {detail_lower}"
            for word in ("meta ads", "facebook ads", "instagram ads", "ads manager", "business.facebook.com/adsmanager")
        )
        generic_paid_context = bool(category in paid_categories and not google_paid_hint and not meta_paid_hint)
        item = {
            "title": suggestion["title"],
            "detail": suggestion["detail"],
            "category": category,
            "data_point": suggestion.get("data_point", ""),
            "relevant_data": {},
        }

        if paid_context and (generic_paid_context or google_paid_hint):
            item["relevant_data"]["google_ads_campaigns"] = (google_ads_detail.get("campaigns") or [])[:10]
            item["relevant_data"]["google_ads_search_terms"] = (google_ads_detail.get("search_terms") or [])[:30]
            item["relevant_data"]["google_ads_kpis"] = kpis.get("google_ads", {})

        if paid_context and (generic_paid_context or meta_paid_hint):
            item["relevant_data"]["meta_campaigns"] = (meta_detail.get("campaigns") or [])[:10]
            item["relevant_data"]["meta_top_ads"] = (meta_detail.get("top_ads") or [])[:10]
            item["relevant_data"]["meta_kpis"] = kpis.get("meta", {})

        if paid_context and ad_intelligence:
            item["relevant_data"]["ad_intelligence_findings"] = (ad_intelligence.get("findings") or [])[:8]
            item["relevant_data"]["ad_intelligence_next_actions"] = (ad_intelligence.get("next_best_actions") or [])[:5]
            item["relevant_data"]["ad_intelligence_summary"] = ad_intelligence.get("summary") or {}

        if category in seo_categories or "seo" in category or "keyword" in title_lower:
            item["relevant_data"]["seo_top_queries"] = (seo_detail.get("top_queries") or [])[:15]
            item["relevant_data"]["seo_keyword_opportunities"] = (seo_detail.get("keyword_opportunities") or [])[:15]
            item["relevant_data"]["seo_top_pages"] = (seo_detail.get("top_pages") or [])[:10]
            item["relevant_data"]["seo_kpis"] = kpis.get("gsc", {})

        if category in web_categories:
            item["relevant_data"]["website_kpis"] = kpis.get("ga", {})
            item["relevant_data"]["website_landing_pages"] = (website_detail.get("top_landing_pages") or [])[:10]
            item["relevant_data"]["website_traffic_sources"] = (website_detail.get("top_sources") or [])[:10]
            item["relevant_data"]["website_top_converting_sources"] = (website_detail.get("top_converting_sources") or [])[:10]
            item["relevant_data"]["website_organic_search"] = website_detail.get("organic_search") or {}
            item["relevant_data"]["website_device_breakdown"] = (website_detail.get("device_breakdown") or [])[:10]
            item["relevant_data"]["seo_top_pages"] = (seo_detail.get("top_pages") or [])[:10]
            item["relevant_data"]["seo_keyword_opportunities"] = (seo_detail.get("keyword_opportunities") or [])[:10]

        if category == "strategy":
            item["relevant_data"]["website_kpis"] = kpis.get("ga", {})
            item["relevant_data"]["website_landing_pages"] = (website_detail.get("top_landing_pages") or [])[:10]
            item["relevant_data"]["website_traffic_sources"] = (website_detail.get("top_sources") or [])[:10]
            item["relevant_data"]["website_top_converting_sources"] = (website_detail.get("top_converting_sources") or [])[:10]
            item["relevant_data"]["website_organic_search"] = website_detail.get("organic_search") or {}
            item["relevant_data"]["website_device_breakdown"] = (website_detail.get("device_breakdown") or [])[:10]
            item["relevant_data"]["seo_top_queries"] = (seo_detail.get("top_queries") or [])[:10]
            item["relevant_data"]["seo_top_pages"] = (seo_detail.get("top_pages") or [])[:10]
            item["relevant_data"]["google_ads_campaigns"] = (google_ads_detail.get("campaigns") or [])[:10]
            item["relevant_data"]["meta_campaigns"] = (meta_detail.get("campaigns") or [])[:10]
            item["relevant_data"]["google_ads_kpis"] = kpis.get("google_ads", {})
            item["relevant_data"]["meta_kpis"] = kpis.get("meta", {})

        if category in organic_categories:
            item["relevant_data"]["fb_organic_top_posts"] = (fb_organic_detail.get("top_posts") or [])[:10]
            item["relevant_data"]["fb_organic_kpis"] = kpis.get("facebook_organic", {})

        competitors = analysis_summary.get("competitor_watch") or {}
        if competitors:
            item["relevant_data"]["competitors"] = competitors

        item["relevant_data"] = {key: value for key, value in item["relevant_data"].items() if value}
        action_items.append(item)

    return action_items


def _format_exact_targets(category, relevant_data):
    targets = []

    if category == "website":
        for page in (relevant_data.get("website_landing_pages") or [])[:3]:
            path = page.get("page") or page.get("path") or page.get("url") or "this landing page"
            sessions = page.get("sessions")
            conversions = page.get("conversions")
            bounce = page.get("bounce_rate")
            parts = [str(path)]
            if sessions is not None:
                parts.append(f"{int(float(sessions))} sessions")
            if conversions is not None:
                parts.append(f"{int(float(conversions))} conversions")
            if bounce is not None:
                parts.append(f"{round(float(bounce), 1)}% bounce")
            targets.append(" - ".join(parts))

    if category in {"seo", "website"}:
        for row in (relevant_data.get("seo_keyword_opportunities") or [])[:3]:
            query = row.get("query") or row.get("keyword")
            if not query:
                continue
            page = row.get("page") or row.get("url") or "no clear page yet"
            impressions = row.get("impressions")
            position = row.get("position")
            parts = [f'"{query}"']
            if page:
                parts.append(str(page))
            if impressions is not None:
                parts.append(f"{int(float(impressions))} impressions")
            if position is not None:
                parts.append(f"position {round(float(position), 1)}")
            targets.append(" - ".join(parts))

    if category == "strategy":
        for row in (relevant_data.get("website_traffic_sources") or [])[:3]:
            source = str(row.get("source") or row.get("source_medium") or "").strip()
            if not source:
                continue
            sessions = row.get("sessions")
            conversions = row.get("conversions")
            parts = [source]
            if sessions is not None:
                parts.append(f"{int(float(sessions))} sessions")
            if conversions is not None:
                parts.append(f"{int(float(conversions))} conversions")
            targets.append(" - ".join(parts))

        for page in (relevant_data.get("website_landing_pages") or [])[:2]:
            path = page.get("page") or page.get("path") or page.get("url")
            if not path:
                continue
            sessions = page.get("sessions")
            conversions = page.get("conversions")
            parts = [str(path)]
            if sessions is not None:
                parts.append(f"{int(float(sessions))} sessions")
            if conversions is not None:
                parts.append(f"{int(float(conversions))} conversions")
            targets.append(" - ".join(parts))

        for campaign in (relevant_data.get("google_ads_campaigns") or [])[:2]:
            name = campaign.get("name") or campaign.get("campaign_name")
            if not name:
                continue
            metrics = campaign.get("metrics") or campaign
            spend = metrics.get("spend")
            results = metrics.get("results") or metrics.get("conversions")
            parts = [str(name)]
            if spend is not None:
                parts.append(f"${round(float(spend), 2)} spend")
            if results is not None:
                parts.append(f"{int(float(results))} results")
            targets.append(" - ".join(parts))

        for campaign in (relevant_data.get("meta_campaigns") or [])[:2]:
            name = campaign.get("name") or campaign.get("campaign_name")
            if not name:
                continue
            metrics = campaign.get("metrics") or campaign
            spend = metrics.get("spend")
            results = metrics.get("results") or metrics.get("conversions")
            parts = [str(name)]
            if spend is not None:
                parts.append(f"${round(float(spend), 2)} spend")
            if results is not None:
                parts.append(f"{int(float(results))} results")
            targets.append(" - ".join(parts))

    deduped = []
    for target in targets:
        cleaned = _trim_copy(target, 180)
        if cleaned and cleaned not in deduped:
            deduped.append(cleaned)
    return deduped[:4]


def _format_period_context(period):
    if not isinstance(period, dict):
        return ""
    if period.get("is_current_month") and period.get("elapsed_days") and period.get("days_in_month"):
        return (
            f"Current month is only day {int(period.get('elapsed_days'))} of "
            f"{int(period.get('days_in_month'))} ({period.get('progress_pct')}% complete)."
        )
    return ""


def _mission_confidence_from_evidence(suggestion, evidence, period):
    base = str(suggestion.get("confidence") or "medium").strip().lower()
    if base not in {"high", "medium", "low"}:
        base = "medium"
    if period.get("is_current_month") and period.get("early_month") and len(evidence) < 3:
        return "low"
    if len(evidence) >= 4 and base != "low":
        return "high"
    if len(evidence) >= 2 and base == "low":
        return "medium"
    return base


def _build_mission_intelligence(suggestion, action_item, analysis, brand=None):
    from webapp.vertical_intelligence import build_vertical_profile

    relevant_data = (action_item or {}).get("relevant_data") or {}
    category = str(suggestion.get("category") or "").strip().lower()
    period = (analysis or {}).get("period") or {}
    vertical_profile = build_vertical_profile(brand or {})
    evidence = []
    research = []

    period_context = _format_period_context(period)
    if period_context:
        evidence.append(period_context)

    data_point = str(suggestion.get("data_point") or "").strip()
    if data_point:
        evidence.append(f"Primary signal: {data_point}.")

    for source in (relevant_data.get("website_traffic_sources") or [])[:2]:
        name = source.get("source") or source.get("source_medium")
        sessions = _to_float(source.get("sessions"), 0)
        conversions = _to_float(source.get("conversions"), 0)
        if name and sessions:
            evidence.append(f"Traffic source {name}: {int(sessions)} sessions, {int(conversions)} conversions.")

    organic = relevant_data.get("website_organic_search") or {}
    if organic.get("sessions"):
        evidence.append(
            f"GA4 organic search: {int(_to_float(organic.get('sessions'), 0))} sessions, "
            f"{int(_to_float(organic.get('conversions'), 0))} conversions."
        )

    for page in (relevant_data.get("website_landing_pages") or [])[:2]:
        path = page.get("page") or page.get("path") or page.get("url")
        sessions = _to_float(page.get("sessions"), 0)
        bounce = _to_float(page.get("bounce_rate"), 0)
        conversions = _to_float(page.get("conversions"), 0)
        if path and sessions:
            evidence.append(f"Landing page {path}: {int(sessions)} sessions, {int(conversions)} conversions, {bounce:.1f}% bounce.")

    for query in (relevant_data.get("seo_keyword_opportunities") or relevant_data.get("seo_top_queries") or [])[:2]:
        q = query.get("query") or query.get("keyword")
        if q:
            evidence.append(
                f"Search query \"{q}\": {int(_to_float(query.get('impressions'), 0))} impressions, "
                f"{int(_to_float(query.get('clicks'), 0))} clicks, position {_to_float(query.get('position'), 0):.1f}."
            )

    if category == "organic_social":
        fb_kpis = relevant_data.get("fb_organic_kpis") or {}
        post_count = fb_kpis.get("post_count")
        if post_count is not None:
            evidence.append(f"Facebook Page posts: {int(_to_float(post_count, 0))} posts in this period.")
        reach = _to_float(fb_kpis.get("organic_impressions"), 0)
        engagements = _to_float(fb_kpis.get("post_engagements"), 0)
        if reach or engagements:
            evidence.append(f"Organic Facebook: {int(reach)} organic impressions, {int(engagements)} engagements.")
        for post in (relevant_data.get("fb_organic_top_posts") or [])[:2]:
            message = _trim_copy(post.get("message") or post.get("caption") or post.get("id") or "Top post", 80)
            likes = _to_float(post.get("likes"), 0)
            comments = _to_float(post.get("comments"), 0)
            shares = _to_float(post.get("shares"), 0)
            evidence.append(f"Top post {message}: {int(likes + comments + shares)} visible engagements.")

    if category in {"paid_advertising", "budget", "creative", "strategy"}:
        for finding in (relevant_data.get("ad_intelligence_findings") or [])[:2]:
            title = _trim_copy(finding.get("title") or finding.get("key") or "Paid media finding", 90)
            detail = _trim_copy(finding.get("detail") or "", 140)
            if title:
                evidence.append(f"Ad intelligence: {title}{f' - {detail}' if detail else ''}.")

        for campaign in (relevant_data.get("google_ads_campaigns") or relevant_data.get("meta_campaigns") or [])[:2]:
            metrics = campaign.get("metrics") or campaign
            name = campaign.get("name") or campaign.get("campaign_name")
            spend = _to_float(metrics.get("spend"), 0)
            results = _to_float(metrics.get("results") or metrics.get("conversions"), 0)
            if name and (spend or results):
                evidence.append(f"Campaign {name}: ${spend:.0f} spend, {int(results)} results.")

    for term in (relevant_data.get("google_ads_search_terms") or [])[:2]:
        term_name = term.get("search_term") or term.get("query") or term.get("term")
        cost = _to_float(term.get("cost") or term.get("spend"), 0)
        conversions = _to_float(term.get("conversions") or term.get("results"), 0)
        if term_name and cost:
            evidence.append(f"Search term \"{term_name}\": ${cost:.0f} spend, {int(conversions)} conversions.")

    if category in {"website", "strategy"}:
        research.extend([
            "Compare the weakest page against the best converting page before changing the offer.",
            "Check mobile first: above-the-fold CTA, load speed, click-to-call, and form length.",
        ])
    if category == "seo":
        research.extend([
            "Confirm the query has a matching page before creating anything new.",
            "Check title tag, H1, internal links, and whether the page answers the exact search intent.",
        ])
    if category in {"paid_advertising", "budget", "creative"}:
        research.extend([
            "Check whether the flagged campaign is active and still spending before pausing or scaling.",
            "Compare spend, results, CTR, CPC, and search terms or ad creative before changing budget.",
        ])
    if category in {"paid_advertising", "budget", "creative", "strategy"}:
        ad_angles = list(vertical_profile.get("ad_angles") or [])[:2]
        if ad_angles:
            research.append(f"Check the ad or offer against the vertical angle: {', '.join(ad_angles)}.")
    if category == "organic_social":
        buyer_paths = list(vertical_profile.get("buyer_paths") or [])[:2]
        if buyer_paths:
            research.append(f"Make the post useful for the actual buyer path: {', '.join(buyer_paths)}.")
    if category == "strategy":
        commercial_targets = list(vertical_profile.get("commercial_targets") or [])[:2]
        if commercial_targets:
            research.append(f"If this is commercial demand, segment it from residential flow first: {', '.join(commercial_targets)}.")
    if period.get("is_current_month") and period.get("early_month"):
        research.append("Because this is early in the month, confirm the issue is a rate, spend, or page-level problem before treating it as a volume problem.")

    confidence = _mission_confidence_from_evidence(suggestion, evidence, period)
    return {
        "diagnostics": evidence[:6],
        "research_questions": research[:4],
        "confidence": confidence.title(),
    }


def _pick_primary_target(rows, *keys):
    for row in rows or []:
        for key in keys:
            value = str(row.get(key) or "").strip()
            if value:
                return row, value
    return None, ""


def _pick_primary_campaign(rows):
    return _pick_primary_target(rows, "name", "campaign_name")


def _pick_primary_ad(rows):
    return _pick_primary_target(rows, "name", "ad_name")


def _pick_primary_query(rows):
    return _pick_primary_target(rows, "query", "keyword")


def _pick_primary_page(rows):
    return _pick_primary_target(rows, "page", "path", "url")


def _pick_primary_search_term(rows):
    return _pick_primary_target(rows, "search_term", "query", "term")


def _strategy_traffic_drop_steps(data_point, relevant_data):
    steps = []
    source_rows = list(relevant_data.get("website_traffic_sources") or [])
    landing_pages = list(relevant_data.get("website_landing_pages") or [])
    converting_sources = list(relevant_data.get("website_top_converting_sources") or [])
    seo_queries = list(relevant_data.get("seo_top_queries") or [])
    google_campaigns = list(relevant_data.get("google_ads_campaigns") or [])
    meta_campaigns = list(relevant_data.get("meta_campaigns") or [])

    source_row, source_name = _pick_primary_target(source_rows, "source", "source_medium")
    if source_row and source_name:
        steps.append(
            f"Go to analytics.google.com. Click \"Reports\", then \"Acquisition\", then \"Traffic acquisition\". Start with \"{source_name}\" because it drove about {int(_to_float(source_row.get('sessions'), 0))} sessions and {int(_to_float(source_row.get('conversions'), 0))} conversions in the current data."
        )
    else:
        steps.append(
            f"Go to analytics.google.com. Click \"Reports\", then \"Acquisition\", then \"Traffic acquisition\".{f' Current metric: {data_point}.' if data_point else ''} Sort by sessions so you can see which source is carrying the most traffic right now."
        )

    page_row, page_path = _pick_primary_target(landing_pages, "page", "path", "url")
    if page_row and page_path:
        steps.append(
            f"Then click \"Landing page\" or open Pages and screens, and compare the page \"{page_path}\" first. It already has about {int(_to_float(page_row.get('sessions'), 0))} sessions and {int(_to_float(page_row.get('conversions'), 0))} conversions, so any drop there matters."
        )

    query_row, query_name = _pick_primary_target(seo_queries, "query", "keyword")
    if query_row and query_name:
        steps.append(
            f"Go to search.google.com/search-console. Click \"Performance\" and check the query \"{query_name}\" first. It is one of the real search terms already in the account data, so confirm whether impressions, clicks, or position slipped."
        )

    top_paid = None
    paid_platform = ""
    if google_campaigns:
        top_paid = google_campaigns[0]
        paid_platform = "google"
    elif meta_campaigns:
        top_paid = meta_campaigns[0]
        paid_platform = "meta"

    if top_paid:
        campaign_name = top_paid.get("name") or top_paid.get("campaign_name") or "the top campaign"
        metrics = top_paid.get("metrics") or top_paid
        spend = _to_float(metrics.get("spend"), 0)
        results = _to_float(metrics.get("results") or metrics.get("conversions"), 0)
        if paid_platform == "google":
            steps.append(
                f"Open ads.google.com and inspect \"{campaign_name}\" next. It spent about ${spend:.0f} and produced {int(results)} results in the current month, so check whether budget, status, or search-term quality changed when traffic fell."
            )
        else:
            steps.append(
                f"Open business.facebook.com/adsmanager and inspect \"{campaign_name}\" next. It spent about ${spend:.0f} and produced {int(results)} results in the current month, so check whether spend, delivery, or audience changes line up with the traffic drop."
            )

    best_source_row, best_source_name = _pick_primary_target(converting_sources, "source", "source_medium")
    if best_source_row and best_source_name:
        steps.append(
            f"Write down what changed and compare it to \"{best_source_name}\", one of your current converting sources, so you know whether to restore a lost winner or shift attention somewhere else."
        )
    else:
        steps.append(
            "Write down the source, page, or campaign that dropped the hardest, then compare it to the sources still producing leads before you change budget or copy."
        )

    return steps[:5]


def _seo_execution_context(relevant_data):
    keyword_opportunities = list(relevant_data.get("seo_keyword_opportunities") or [])
    seo_pages = list(relevant_data.get("seo_top_pages") or [])
    landing_pages = list(relevant_data.get("website_landing_pages") or [])

    pages_with_traffic = []
    for row in seo_pages + landing_pages:
        page = str(row.get("page") or row.get("path") or row.get("url") or "").strip()
        if page and page not in pages_with_traffic:
            pages_with_traffic.append(page)

    impressions = [_to_float(row.get("impressions"), 0) for row in keyword_opportunities]
    best_impressions = max(impressions) if impressions else 0.0
    total_impressions = sum(impressions) if impressions else 0.0
    existing_page_matches = sum(1 for row in keyword_opportunities if str(row.get("page") or row.get("url") or "").strip())

    low_volume = bool(keyword_opportunities) and best_impressions < 150 and total_impressions < 400
    optimize_existing_first = bool(pages_with_traffic) and (low_volume or existing_page_matches > 0)

    if optimize_existing_first and low_volume:
        summary = (
            f"The best search opportunity is only around {int(best_impressions)} impressions, so this looks more like a low-demand problem "
            "than a missing-page problem. Improve the page you already have before building new local pages."
        )
    elif optimize_existing_first:
        summary = "Search Console already points to existing pages, so tighten those pages first instead of spinning up more local pages."
    else:
        summary = "There may be room for a new page, but only if the mission can name the query, the demand, and why an existing page is not enough."

    return {
        "keyword_opportunities": keyword_opportunities,
        "pages_with_traffic": pages_with_traffic,
        "best_impressions": best_impressions,
        "total_impressions": total_impressions,
        "low_volume": low_volume,
        "optimize_existing_first": optimize_existing_first,
        "summary": summary,
    }


def _looks_like_new_page_mission(*values):
    combined = " ".join(str(v or "") for v in values).lower()
    needles = (
        "new page",
        "create page",
        "build page",
        "local page",
        "location page",
        "city page",
        "area page",
    )
    return any(needle in combined for needle in needles)


def _rewrite_low_volume_seo_mission(card, relevant_data):
    seo_context = _seo_execution_context(relevant_data)
    exact_targets = _format_exact_targets("seo", relevant_data) or _format_exact_targets("website", relevant_data)
    page_list = seo_context.get("pages_with_traffic") or []
    primary_page = page_list[0] if page_list else "the main service page"
    best_impressions = int(seo_context.get("best_impressions") or 0)

    card["mission_name"] = "Tighten The Page You Have"
    card["execution_mode"] = "delegate"
    card["delegate_to"] = "developer"
    card["platform_url"] = ""
    card["platform_label"] = ""
    card["exact_targets"] = exact_targets
    card["why"] = (
        f"The problem is not a missing page yet. The best search opportunity is only around {best_impressions} impressions, "
        "so building more local pages is unlikely to move the needle right now."
    )
    card["reward"] = "You improve the page already getting seen in Google, which gives your current traffic a better shot at turning into calls and quote requests."
    card["impact"] = "Could lift clicks and leads from the traffic you already have without wasting time on low-demand pages."
    card["time"] = "10 minutes"
    card["delegate_message"] = "\n".join([
        "Hi - GroMore flagged an SEO update for us.",
        "",
        "Please do not build new city or local pages yet.",
        "The search volume is still too light to justify more pages, and Search Console is already pointing traffic toward pages we have.",
        "",
        "Please focus on improving these existing pages first:",
        *[f"- {target}" for target in (exact_targets or [_trim_copy(primary_page, 140)])],
        "",
        "What I need changed:",
        f"- Make the headline on {primary_page} clearly match the main service people are searching for.",
        "- Make the first call-to-action easy to see without scrolling.",
        "- Tighten the title tag, H1, and opening copy so the page matches the real search intent more clearly.",
        "- Keep the page focused on one clear action: call, quote request, or booking.",
        "",
        "When done, send me the page URLs you updated, a short list of what changed, and anything that still needs copy help.",
    ])
    card["steps"] = [
        "Copy the website update note below and send it today.",
        "Ask for updates to the page you already have before anyone builds a new city or local page.",
        "When the update is live, check that the headline, first call-to-action, and service wording are easier to understand.",
        "Mark the mission complete after the page is updated so GroMore can re-check clicks and leads on the next refresh.",
    ]


def _apply_mission_reality_checks(card, suggestion, action_item):
    category = str(suggestion.get("category") or "").strip().lower()
    title = str(card.get("mission_name") or card.get("title") or "")
    steps_text = " ".join(card.get("steps") or [])
    delegate_message = str(card.get("delegate_message") or "")
    relevant_data = (action_item or {}).get("relevant_data") or {}

    if category not in {"seo", "website"}:
        return

    seo_context = _seo_execution_context(relevant_data)
    if seo_context.get("optimize_existing_first") and _looks_like_new_page_mission(title, steps_text, delegate_message):
        _rewrite_low_volume_seo_mission(card, relevant_data)


def _apply_platform_reality_checks(card, suggestion, action_item, delegate_plan=None):
    category = str(suggestion.get("category") or "").strip().lower()
    relevant_data = (action_item or {}).get("relevant_data") or {}
    platform_key = _resolve_action_platform(
        suggestion.get("title", ""),
        category,
        relevant_data,
        suggestion.get("detail", ""),
    )

    if card.get("execution_mode") == "delegate" and category in {"website", "seo", "strategy", "creative"}:
        card["platform_url"] = ""
        card["platform_label"] = ""
        return

    platform_url, platform_label = _platform_link(platform_key)
    card["platform_url"] = platform_url
    card["platform_label"] = platform_label

    steps_text = " ".join(card.get("steps") or []).lower()
    if card.get("execution_mode") != "direct" or category not in {"paid_advertising", "budget", "creative"}:
        return

    google_words = ("ads.google.com", "google ads")
    meta_words = ("business.facebook.com/adsmanager", "ads manager", "meta business suite")
    mismatch = False
    if platform_key == "meta_ads" and any(word in steps_text for word in google_words):
        mismatch = True
    elif platform_key == "google_ads" and any(word in steps_text for word in meta_words):
        mismatch = True
    elif not platform_key and any(word in steps_text for word in (*google_words, *meta_words)):
        mismatch = True

    if mismatch:
        card["steps"] = _fallback_steps(suggestion, action_item=action_item, delegate_plan=delegate_plan)


def _needs_delegate_plan(title, category, relevant_data):
    title_lower = str(title or "").lower()
    category = str(category or "").strip().lower()
    exact_targets = _format_exact_targets(category, relevant_data)

    if category in {"website", "seo", "creative"}:
        return True

    delegate_words = ("page", "website", "landing", "seo", "headline", "design", "copy")
    direct_words = ("investigate", "diagnose", "check", "review", "compare", "audit", "traffic drop")

    if any(word in title_lower for word in direct_words):
        return False

    if category == "strategy":
        return bool(exact_targets) and any(word in title_lower for word in delegate_words)

    return any(word in title_lower for word in delegate_words)


def _build_delegate_plan(title, category, relevant_data):
    title_lower = str(title or "").lower()
    exact_targets = _format_exact_targets(category, relevant_data)
    needs_delegate = _needs_delegate_plan(title, category, relevant_data)
    if not needs_delegate:
        return {
            "execution_mode": "direct",
            "delegate_to": "",
            "delegate_message": "",
            "exact_targets": exact_targets,
        }

    if category == "creative" or any(word in title_lower for word in ("design", "image", "creative", "headline")):
        delegate_to = "designer"
        intro = "Hi - GroMore flagged a creative update for us. Please make these ad changes next:"
        close = "Goal: keep the same offer, but make the ad easier to notice and click."
    else:
        delegate_to = "developer"
        intro = "Hi - GroMore flagged these website updates for us. Please handle the items below:"
        close = "Goal: make the page easier to understand, easier to contact from, and stronger for the traffic it already gets."

    bullets = exact_targets or [_trim_copy(title or "Mission update", 140)]
    lines = [intro]
    for bullet in bullets:
        lines.append(f"- {bullet}")

    if category == "website":
        lines.append("- For each page above, make the main headline clearly say what service is being offered, put one clear call-to-action near the top, and make the contact path obvious without scrolling.")
        lines.append("- Keep the lead form simple: only ask for the fields needed to start the conversation, make the button text specific, and confirm the page works cleanly on mobile.")
        lines.append("- Keep the current offer unless something is wrong. If copy needs to change, keep the service, city, and intent that already bring traffic in.")
    elif category == "seo":
        lines.append("- Rewrite the title tag, H1, and opening copy so the page clearly matches what people are searching for without sounding stuffed or awkward.")
        lines.append("- Add stronger internal links into the page, make the main call-to-action easier to find, and keep the page focused on one main conversion goal.")
    elif category == "creative":
        lines.append("- Keep the offer the same, but refresh the image and headline so the main promise is obvious in one glance.")
    else:
        lines.append("- Reply with the ETA, any blockers, and whether this needs copy, design, or dev help before you make the update.")

    if delegate_to == "developer":
        lines.append("- After the changes are live, send back the exact URLs updated, what changed on each page, and anything that still needs copy or design review.")

    lines.append(close)
    return {
        "execution_mode": "delegate",
        "delegate_to": delegate_to,
        "delegate_message": "\n".join(lines),
        "exact_targets": exact_targets,
    }


def _parse_difficulty(time_str):
    """Return 1-3 star difficulty from a time estimate string."""
    if not time_str:
        return 2
    t = time_str.lower()
    if any(w in t for w in ("1 hour", "2 hour", "1-2", "3 hour")):
        return 3
    if any(w in t for w in ("30", "45")):
        return 2
    return 1


def build_client_dashboard(analysis, suggestions, brand, ai_model=None, include_deep_analysis=False, mission_profile=None):
    """
    Build the full client dashboard payload from raw analysis + suggestions.

    Returns dict with:
        - health: overall grade + score
        - channels: per-channel metric cards with explanations
        - actions: prioritized action cards with step-by-step instructions
        - kpi_status: target vs actual KPIs
    """
    from src.ad_intelligence import build_ad_intelligence
    from webapp.vertical_intelligence import build_vertical_profile

    if not (analysis.get("ad_intelligence") or {}).get("summary"):
        analysis = dict(analysis)
        analysis["ad_intelligence"] = build_ad_intelligence(analysis, brand)

    channels = {}

    ga = analysis.get("google_analytics")
    if ga:
        channels["website"] = _explain_website(ga)
        if ga.get("organic_search"):
            channels["organic_search"] = _explain_ga_organic(ga)

    meta = analysis.get("meta_business")
    if meta:
        channels["facebook_ads"] = _explain_meta(meta)

    fb_organic = analysis.get("facebook_organic")
    if fb_organic:
        channels["facebook_organic"] = _explain_facebook_organic(fb_organic)

    google_ads = analysis.get("google_ads")
    if google_ads:
        channels["google_ads"] = _explain_google_ads(google_ads)

    gsc = analysis.get("search_console")
    if gsc:
        channels["seo"] = _explain_seo(gsc)

    actions = _build_action_cards(analysis, suggestions, brand, ai_model=ai_model, mission_profile=mission_profile)
    kpi_status = _explain_kpis(analysis)

    overall_score = analysis.get("overall_score")
    overall_grade = analysis.get("overall_grade", "N/A")

    result = {
        "health": {
            "grade": overall_grade,
            "score": overall_score,
            "label": _grade_label(overall_grade),
        },
        "health_summary": _build_health_summary(analysis, actions, overall_grade, overall_score),
        "channels": channels,
        "actions": actions,
        "kpi_status": kpi_status,
        "ad_intelligence": analysis.get("ad_intelligence") or {},
        "vertical_profile": build_vertical_profile(brand),
        "highlights": analysis.get("highlights", []),
        "concerns": analysis.get("concerns", []),
    }

    result["health_cluster"] = _build_health_cluster(result)

    if include_deep_analysis:
        result["ai_analysis"] = _generate_ai_analysis_brief(
            analysis,
            suggestions,
            brand,
            ai_model=ai_model,
        )

    return result


# ── Metric Explanations ──

def _explain_website(ga):
    metrics = ga.get("metrics", {})
    scores = ga.get("scores", {})
    mom = ga.get("month_over_month", {})

    cards = []

    sessions = metrics.get("sessions", 0)
    sessions_mom = mom.get("sessions", {})
    trend = _trend_text(sessions_mom)
    cards.append({
        "metric": "Website Visitors",
        "value": f"{sessions:,}",
        "status": _score_to_status(scores.get("sessions", "no_data")),
        "explanation": (
            f"Your website had {sessions:,} visits this month{trend}. "
            "This counts every time someone landed on your site from any source: "
            "Google searches, ads, social media, or typing your URL directly."
        ),
    })

    bounce = round(float(metrics.get("bounce_rate", 0)), 1)
    cards.append({
        "metric": "Bounce Rate",
        "value": f"{bounce}%",
        "status": _score_to_status(scores.get("bounce_rate", "no_data")),
        "explanation": (
            f"{bounce}% of visitors left your site without clicking anything else. "
            + ("This is higher than it should be. Your landing pages may be loading slowly, "
               "or visitors aren't finding what they need right away."
               if bounce > 50
               else "This is within a healthy range - most visitors are exploring your site.")
        ),
    })

    duration = metrics.get("avg_session_duration", 0)
    cards.append({
        "metric": "Time on Site",
        "value": f"{int(duration)}s",
        "status": _score_to_status(scores.get("avg_session_duration", "no_data")),
        "explanation": (
            f"Visitors spend an average of {int(duration)} seconds on your site. "
            + ("That's less than 2 minutes, which usually means people aren't finding "
               "enough reason to stick around. Adding photos of your work, reviews, "
               "and detailed service info helps keep visitors engaged."
               if duration < 120
               else "That's a solid amount of time, meaning visitors are reading your "
                    "content and exploring your services.")
        ),
    })

    conv_rate = round(float(metrics.get("conversion_rate", 0)), 1)
    conversions = metrics.get("conversions", 0)
    if conv_rate > 0 or conversions > 0:
        cards.append({
            "metric": "Website Conversions",
            "value": f"{conversions:,} ({conv_rate}%)",
            "status": _score_to_status(scores.get("conversion_rate", "no_data")),
            "explanation": (
                f"Out of every 100 visitors, about {conv_rate} filled out a form, "
                f"called, or took action. You got {conversions:,} total conversions this month. "
                + ("This rate could be higher. Make sure your phone number is clickable, "
                   "your contact form is short (name, phone, service needed), and you have "
                   "clear 'Get a Quote' buttons on every page."
                   if conv_rate < 5
                   else "This is a strong conversion rate. Your site is doing a good job "
                        "turning visitors into leads.")
            ),
        })

    device_rows = ga.get("device_breakdown") or []
    if device_rows:
        top_device = max(device_rows, key=lambda row: _to_float(row.get("sessions"), 0))
        device_name = str(top_device.get("device") or top_device.get("deviceCategory") or "device").title()
        device_sessions = int(_to_float(top_device.get("sessions"), 0))
        device_cvr = _to_float(top_device.get("conversion_rate"), 0)
        mobile = next(
            (row for row in device_rows if str(row.get("device") or row.get("deviceCategory") or "").lower() == "mobile"),
            None,
        )
        mobile_status = "neutral"
        if mobile and _to_float(mobile.get("sessions"), 0) >= 25:
            mobile_bounce = _to_float(mobile.get("bounce_rate"), 0)
            mobile_cvr = _to_float(mobile.get("conversion_rate"), 0)
            if mobile_bounce >= 65 or (conv_rate > 0 and mobile_cvr < conv_rate * 0.7):
                mobile_status = "warning"
        cards.append({
            "metric": "Device Mix",
            "value": f"{device_name}: {device_sessions:,}",
            "status": mobile_status,
            "explanation": (
                f"{device_name} is the top device type this month with {device_sessions:,} sessions "
                f"and a {device_cvr:.1f}% conversion rate. Warren uses this to spot mobile-specific "
                "contact-flow problems instead of judging the website as one flat number."
            ),
        })

    top_events = ga.get("top_events") or []
    if top_events:
        event = top_events[0]
        event_name = str(event.get("event") or event.get("eventName") or "top event").replace("_", " ")
        event_count = int(_to_float(event.get("event_count"), 0))
        cards.append({
            "metric": "Top Website Action",
            "value": f"{event_count:,}",
            "status": "neutral",
            "explanation": (
                f"The most-tracked action is {event_name} with {event_count:,} events. "
                "This helps Warren separate real lead intent from plain traffic."
            ),
        })

    return {"title": "Your Website", "icon": "bi-globe", "cards": cards}


def _explain_ga_organic(ga):
    organic = ga.get("organic_search") or {}
    sessions = int(_to_float(organic.get("sessions"), 0))
    users = int(_to_float(organic.get("users"), 0))
    conversions = int(_to_float(organic.get("conversions"), 0))
    conversion_rate = _to_float(organic.get("conversion_rate"), 0)
    sources = organic.get("sources") or []
    top_source = sources[0].get("source") if sources and isinstance(sources[0], dict) else "organic search"

    cards = [{
        "metric": "Organic Website Sessions",
        "value": f"{sessions:,}",
        "status": "good" if sessions > 0 else "warning",
        "explanation": (
            f"GA4 shows {sessions:,} sessions from organic search this month"
            + (f", led by {top_source}" if top_source else "")
            + ". Warren uses this alongside Search Console because Search Console can lag and may show zero before GA4 does."
        ),
    }]

    if conversions > 0 or conversion_rate > 0:
        cards.append({
            "metric": "Organic Website Leads",
            "value": f"{conversions:,} ({conversion_rate:.1f}%)",
            "status": "good" if conversion_rate >= 3 else "caution",
            "explanation": (
                f"Organic search drove {conversions:,} tracked conversions from {users:,} users. "
                "This separates organic traffic volume from whether those visitors are turning into leads."
            ),
        })

    return {"title": "Organic Search from GA4", "icon": "bi-search", "cards": cards}


def _explain_meta(meta):
    metrics = meta.get("metrics", {})
    scores = meta.get("scores", {})

    cards = []

    spend = metrics.get("spend", 0)
    results = metrics.get("results", 0)
    cpr = round(spend / results, 2) if results > 0 else 0

    cards.append({
        "metric": "Ad Spend",
        "value": f"${spend:,.2f}",
        "status": "neutral",
        "explanation": (
            f"You spent ${spend:,.2f} on Facebook/Instagram ads this month"
            + (f" and got {results:,} leads, costing ${cpr:.2f} each."
               if results > 0
               else ". No leads were tracked yet - make sure your conversion tracking is set up.")
        ),
    })

    ctr = round(float(metrics.get("ctr", 0)), 1)
    cards.append({
        "metric": "Click Rate",
        "value": f"{ctr}%",
        "status": _score_to_status(scores.get("ctr", "no_data")),
        "explanation": (
            f"Out of everyone who saw your ads, {ctr}% clicked on them. "
            + ("This is lower than ideal. Your ad images or text may not be grabbing attention. "
               "Try using real photos of your work instead of stock images, and make your "
               "headline about what the customer gets, not what you do."
               if ctr < 1.0
               else "This is a healthy click rate - your ads are resonating with your audience.")
        ),
    })

    cpc = metrics.get("cpc", 0)
    cards.append({
        "metric": "Cost Per Click",
        "value": f"${cpc:.2f}",
        "status": _score_to_status(scores.get("cpc", "no_data")),
        "explanation": (
            f"Each click on your ad costs ${cpc:.2f}. "
            + ("This is on the higher side. You can bring it down by improving your ad quality "
               "score (better images, more relevant text) or adjusting your audience targeting."
               if cpc > 2.5
               else "This is a reasonable cost per click for your industry.")
        ),
    })

    frequency = metrics.get("frequency", 0)
    if frequency > 0:
        cards.append({
            "metric": "Ad Frequency",
            "value": f"{frequency:.1f}x",
            "status": "warning" if frequency > 3.5 else "good",
            "explanation": (
                f"On average, each person in your audience saw your ads {frequency:.1f} times. "
                + ("This is very high - people are seeing the same ads too many times and "
                   "starting to ignore them. You need fresh ad creative (new images, new text) "
                   "or a larger audience."
                   if frequency > 4
                   else "This is within a healthy range."
                   if frequency <= 3
                   else "Getting close to ad fatigue territory. Consider refreshing your creative soon.")
            ),
        })

    return {"title": "Facebook & Instagram Ads", "icon": "bi-meta", "cards": cards}


def _explain_facebook_organic(fb_organic):
    metrics = fb_organic.get("metrics") or {}
    top_posts = fb_organic.get("top_posts") or []
    post_count = int(_to_float(fb_organic.get("post_count", 0), 0))
    period = fb_organic.get("period") or metrics.get("period") or {}
    debug = metrics.get("_debug") or {}

    cards = []

    followers = metrics.get("followers") or 0
    fans = metrics.get("fans") or 0
    new_fans = metrics.get("new_fans") or 0
    net_fans = metrics.get("net_fans") or 0
    tracked_page_count = int(_to_float(metrics.get("tracked_page_count"), 0))
    if tracked_page_count > 1:
        cards.append({
            "metric": "Tracked Facebook Pages",
            "value": str(tracked_page_count),
            "status": "good",
            "explanation": (
                f"Warren is combining signals from {tracked_page_count} Facebook pages for this brand. "
                "This supports businesses that use one main page for organic/Messenger and a secondary page for ads or lead forms."
            ),
        })

    cards.append({
        "metric": "Page Followers",
        "value": f"{followers:,}",
        "status": "good" if net_fans > 0 else ("warning" if net_fans < 0 else "neutral"),
        "explanation": (
            f"Your Facebook page has {followers:,} followers."
            + (f" You gained {net_fans:,} net new followers this month - your audience is growing!"
               if net_fans > 0
               else f" You lost {abs(net_fans):,} followers this month. Review your content mix to keep people engaged."
               if net_fans < 0
               else "")
        ),
    })

    organic_impressions = metrics.get("organic_impressions") or 0
    total_reach = metrics.get("reach") or organic_impressions
    display_reach = organic_impressions if organic_impressions > 0 else total_reach
    if display_reach > 0 or followers > 0:
        reach_pct = round((display_reach / followers) * 100, 1) if followers > 0 else 0
        reach_label = "organically" if organic_impressions > 0 else "in total"
        cards.append({
            "metric": "Organic Reach",
            "value": f"{display_reach:,}",
            "status": "good" if reach_pct > 30 else ("warning" if reach_pct < 15 else "neutral"),
            "explanation": (
                f"Your posts were seen {display_reach:,} times {reach_label} (without paying). "
                + (f"That's {reach_pct}% of your followers."
                   if followers > 0 else "")
                + (" Great reach - your content is getting shared and picked up by the algorithm."
                   if reach_pct > 30
                   else " Try posting more engaging content (questions, before/after photos, videos) to boost this."
                   if reach_pct < 15 and followers > 0
                   else "")
            ),
        })

    post_engagements = metrics.get("post_engagements") or 0
    engagement_rate = metrics.get("engagement_rate") or 0
    cards.append({
        "metric": "Engagement",
        "value": f"{post_engagements:,}",
        "status": "good" if engagement_rate > 2 else ("warning" if engagement_rate < 1 else "neutral"),
        "explanation": (
            f"Your posts received {post_engagements:,} total engagements (likes, comments, shares). "
            + (f"That's a {engagement_rate:.1f}% engagement rate. "
               if engagement_rate > 0 else "")
            + ("This is strong for a local business page."
               if engagement_rate > 2
               else "This is below average. Posting more consistently and using photos/videos from your actual work can help."
               if engagement_rate < 1
               else "Solid engagement - keep the content coming.")
        ),
    })

    post_clicks = metrics.get("post_clicks") or metrics.get("clicks") or sum((post.get("clicks") or 0) for post in top_posts)
    if post_clicks > 0 or post_count > 0:
        clicks_per_post = round(post_clicks / max(post_count, 1), 1) if post_count > 0 else 0
        click_status = "good" if post_clicks >= max(10, post_count) else ("warning" if post_count >= 6 and post_clicks == 0 else "neutral")
        cards.append({
            "metric": "Website Clicks from Social",
            "value": f"{post_clicks:,}",
            "status": click_status,
            "explanation": (
                f"Your organic Facebook posts drove {post_clicks:,} clicks toward your website this month. "
                + (f"That works out to about {clicks_per_post:.1f} clicks per post. " if post_count > 0 else "")
                + ("People are moving from social attention to site traffic, which is the right direction."
                   if click_status == "good"
                   else "You are getting engagement, but not much traffic back to the site yet. Tighten the offer, CTA, and link placement in your posts."
                   if click_status == "warning"
                   else "Keep testing stronger offers and clearer reasons to click through to the site.")
            ),
        })

    monthly_post_target = 12
    expected_posts = monthly_post_target
    is_current_month = bool(period.get("is_current_month"))
    if is_current_month:
        elapsed = _to_float(period.get("elapsed_days"), 0)
        days = _to_float(period.get("days_in_month"), 0)
        if elapsed > 0 and days > 0:
            expected_posts = round(monthly_post_target * (elapsed / days), 1)
    on_post_pace = post_count >= max(1, expected_posts * 0.75)
    if post_count >= monthly_post_target:
        post_status = "good"
        post_note = "Great consistency. Regular posting keeps your page active in the algorithm."
    elif is_current_month and on_post_pace:
        post_status = "neutral"
        post_note = f"You are on pace for this point in the month. The paced target is about {expected_posts:.1f} posts by now."
    elif is_current_month:
        post_status = "warning"
        post_note = f"You are behind the current-month posting pace. The paced target is about {expected_posts:.1f} posts by now."
    elif post_count < 8:
        post_status = "warning"
        post_note = "Aim for at least 3 posts per week (12+/month). Consistency matters more than perfection."
    else:
        post_status = "neutral"
        post_note = "Decent posting pace. A few more posts per week could help grow your reach."

    cards.append({
        "metric": "Posts This Month",
        "value": str(post_count),
        "status": post_status,
        "explanation": f"You published {post_count} posts this month. {post_note}",
    })

    # Top post highlight
    if top_posts:
        best = top_posts[0]
        best_eng = best.get("engagement_rate", 0)
        best_type = best.get("type", "post")
        best_msg = (best.get("message") or "")[:80]
        if best_eng > 0:
            cards.append({
                "metric": "Top Post",
                "value": f"{best_eng:.1f}% engagement",
                "status": "good" if best_eng > 3 else "neutral",
                "explanation": (
                    f"Your best-performing post was a {best_type}"
                    + (f': "{best_msg}..."' if best_msg else "")
                    + f" with {best_eng:.1f}% engagement. "
                    "Look at what made this one work and create more content like it."
                ),
            })

    page_views = metrics.get("page_views") or 0
    if page_views > 0:
        cards.append({
            "metric": "Page Views",
            "value": f"{page_views:,}",
            "status": "neutral",
            "explanation": (
                f"Your Facebook page was viewed {page_views:,} times. "
                "These are people actively looking at your business page, "
                "so make sure your page info, services, and contact details are up to date."
            ),
        })

    # Diagnostic card: if we have followers but all insights are zero, likely a permissions issue
    organic_impressions = metrics.get("organic_impressions") or 0
    post_engagements = metrics.get("post_engagements") or 0
    insights_found = debug.get("insights_metrics_found", [])
    insights_status = debug.get("insights_status", "unknown")
    if followers > 0 and organic_impressions == 0 and post_engagements == 0 and post_count == 0:
        if insights_status in ("empty_response", "not_attempted") or insights_status.startswith("http_"):
            hint = (
                "We can see your page info but could not pull engagement data. "
                "This usually means Facebook permissions need updating. "
                "Go to Connections, disconnect Meta, then reconnect and make sure you approve ALL permissions "
                "(pages_read_engagement, read_insights, pages_show_list, pages_manage_metadata, pages_messaging, leads_retrieval). "
                "Also confirm your Meta app has Advanced Access for these permissions in the App Dashboard."
            )
        else:
            hint = (
                "Your page had no organic reach, engagement, or posts this period. "
                "If you have been posting, check that the correct Facebook Page is linked in your brand settings."
            )
        if insights_found:
            hint += f" (Metrics returned by API: {', '.join(insights_found)})"
        elif insights_status and insights_status != "unknown":
            hint += f" (Insights API status: {insights_status})"
        cards.append({
            "metric": "Data Status",
            "value": "Limited",
            "status": "bad",
            "explanation": hint,
        })

    return {"title": "Facebook Organic", "icon": "bi-facebook", "cards": cards}


def _explain_google_ads(google_ads):
    metrics = google_ads.get("metrics", {})
    scores = google_ads.get("scores", {})

    cards = []

    spend = metrics.get("spend", 0)
    conversions = metrics.get("results", 0)
    cpa = metrics.get("cost_per_result", 0)

    cards.append({
        "metric": "Ad Spend",
        "value": f"${spend:,.2f}",
        "status": "neutral",
        "explanation": (
            f"You spent ${spend:,.2f} on Google Ads this month"
            + (f" and got {int(conversions):,} leads at ${cpa:.2f} each."
               if conversions > 0
               else ". No conversions tracked yet - check that your conversion tracking is working.")
        ),
    })

    ctr = round(float(metrics.get("ctr", 0)), 1)
    cards.append({
        "metric": "Click Rate",
        "value": f"{ctr}%",
        "status": _score_to_status(scores.get("ctr", "no_data")),
        "explanation": (
            f"{ctr}% of people who saw your Google ad clicked on it. "
            + ("This is below average. Your ad copy may not match what people are searching for. "
               "Make sure your headlines include the exact service + city people are looking for."
               if ctr < 4
               else "Good click rate - your ads are relevant to what people are searching.")
        ),
    })

    cpc = metrics.get("cpc", 0)
    cards.append({
        "metric": "Cost Per Click",
        "value": f"${cpc:.2f}",
        "status": _score_to_status(scores.get("cpc", "no_data")),
        "explanation": (
            f"Each click costs ${cpc:.2f}. Google Ads tend to be more expensive per click "
            "than Facebook because people searching on Google have higher intent - they're "
            "actively looking for your service right now."
        ),
    })

    if cpa > 0:
        cards.append({
            "metric": "Cost Per Lead",
            "value": f"${cpa:.2f}",
            "status": _score_to_status(scores.get("cost_per_result", "no_data")),
            "explanation": (
                f"Each lead from Google Ads costs ${cpa:.2f}. "
                + ("This is higher than ideal. Check which keywords are eating budget "
                   "without converting, and pause or adjust them."
                   if cpa > 50
                   else "This is a solid cost per lead for paid search.")
            ),
        })

    campaign_analysis = google_ads.get("campaign_analysis", [])
    underperforming = [c for c in campaign_analysis if c.get("status") == "underperforming"]
    if underperforming:
        names = ", ".join(c.get("name", "Unknown") for c in underperforming[:3])
        cards.append({
            "metric": "Underperforming Campaigns",
            "value": f"{len(underperforming)}",
            "status": "bad",
            "explanation": (
                f"These campaigns are below benchmark: {names}. "
                "They're spending money but not getting enough leads. "
                "Check the action steps below for what to fix."
            ),
        })

    return {"title": "Google Ads", "icon": "bi-google", "cards": cards}


def _explain_seo(gsc):
    metrics = gsc.get("metrics", {})
    scores = gsc.get("scores", {})

    cards = []

    clicks = metrics.get("clicks", 0)
    impressions = metrics.get("impressions", 0)
    avg_pos = metrics.get("avg_position", 0)

    cards.append({
        "metric": "Google Searches Showing You",
        "value": f"{impressions:,}",
        "status": "neutral",
        "explanation": (
            f"Your website appeared in Google search results {impressions:,} times this month. "
            "This is free visibility - the more people see you in search results, "
            "the more potential customers can find you."
        ),
    })

    cards.append({
        "metric": "Clicks from Google",
        "value": f"{clicks:,}",
        "status": _score_to_status(scores.get("clicks", "no_data")),
        "explanation": (
            f"{clicks:,} people clicked through from Google search to your website. "
            + (f"That's a {round(clicks/impressions*100, 1)}% click rate from searches. "
               if impressions > 0 else "")
            + ("More clicks means more free leads without paying for ads."
               if clicks > 0
               else "Focus on improving your Google rankings to start getting free traffic.")
        ),
    })

    if avg_pos > 0:
        page = "page 1" if avg_pos <= 10 else f"page {int((avg_pos - 1) // 10) + 1}"
        cards.append({
            "metric": "Average Search Position",
            "value": f"#{avg_pos:.1f}",
            "status": _score_to_status(scores.get("avg_position", "no_data")),
            "explanation": (
                f"For your top search terms, you rank around position {avg_pos:.1f} ({page}). "
                + ("Most clicks go to the top 3 results. Being on page 2+ means most people "
                   "never see your listing."
                   if avg_pos > 10
                   else "You're showing up on page 1 for your most important searches, which is where you want to be."
                   if avg_pos <= 10
                   else "")
            ),
        })

    opportunities = gsc.get("keyword_opportunities", [])
    if opportunities:
        top = opportunities[:3]
        kw_list = ", ".join(f'"{o["query"]}"' for o in top)
        cards.append({
            "metric": "SEO Opportunities",
            "value": f"{len(opportunities)}",
            "status": "info",
            "explanation": (
                f"Found {len(opportunities)} keywords where you rank close to page 1. "
                f"Top opportunities: {kw_list}. With some targeted work on these pages, "
                "you could start showing up higher and getting more free clicks."
            ),
        })

    query_pages = gsc.get("query_pages") or []
    if query_pages:
        mapped = [row for row in query_pages if row.get("query") and row.get("page")]
        if mapped:
            top = max(mapped, key=lambda row: _to_float(row.get("impressions"), 0))
            query = top.get("query")
            page = top.get("page")
            cards.append({
                "metric": "Query to Page Match",
                "value": f"{len(mapped)}",
                "status": "info",
                "explanation": (
                    f"Warren can now see which pages rank for which searches. "
                    f"Start with \"{query}\" on {page}; it has enough search data to guide the next content or title update."
                ),
            })

    return {"title": "SEO (Free Google Traffic)", "icon": "bi-search", "cards": cards}


# ── Action Cards with AI-Generated Deliverables ──

def _build_action_cards(analysis, suggestions, brand, ai_model=None, mission_profile=None):
    """Convert top suggestions into action cards with AI-generated deliverables.

    Generate up to 10 action items per load (high priority first, then medium,
    then low). The monthly cap of 20 is enforced at the route level by tracking
    completed items in the database, so we generate a healthy pool here.
    """
    high_cards = []
    medium_cards = []
    low_cards = []

    for s in suggestions:
        if s["priority"] == "high" and len(high_cards) < 5:
            high_cards.append(s)
        elif s["priority"] == "medium" and len(medium_cards) < 5:
            medium_cards.append(s)
        elif s["priority"] == "low" and len(low_cards) < 3:
            low_cards.append(s)

    selected = high_cards + medium_cards + low_cards
    if not selected:
        return []

    from webapp.ai_assistant import _summarize_analysis_for_ai

    analysis_summary = _summarize_analysis_for_ai(analysis)
    action_items = _build_action_items(selected, analysis_summary)

    # Build basic card structure first
    actions = []
    for index, s in enumerate(selected):
        cat_key = s.get("category", "")
        cat_meta = _CATEGORY_META.get(
            cat_key,
            {"icon": "bi-star-fill", "color": "#6b7280", "skill": "Marketing",
             "platform_url": "", "platform_label": ""},
        )
        xp = 150 if s["priority"] == "high" else (100 if s["priority"] == "medium" else 75)

        action_item = action_items[index] if index < len(action_items) else {"relevant_data": {}}
        platform_key = _resolve_action_platform(
            s.get("title", ""),
            cat_key,
            (action_item.get("relevant_data") or {}),
            s.get("detail", ""),
        )
        platform_url, platform_label = _platform_link(platform_key)
        if not platform_url:
            platform_url = cat_meta.get("platform_url", "")
            platform_label = cat_meta.get("platform_label", "")

        card = {
            "title": _client_friendly_title(s["title"]),
            "priority": "Do This Now" if s["priority"] == "high" else "Worth Doing Soon",
            "priority_class": "danger" if s["priority"] == "high" else "warning",
            "category": _client_friendly_category(s["category"]),
            "what": _plain_english_what(s),
            "steps": [],
            "impact": "",
            "time": "",
            "data_point": s.get("data_point", ""),
            # Mission metadata
            "mission_name": "",
            "why": "",
            "reward": "",
            "icon": cat_meta["icon"],
            "icon_color": cat_meta["color"],
            "skill": cat_meta["skill"],
            "platform_url": platform_url,
            "platform_label": platform_label,
            "xp": xp,
            "difficulty": 0,
            "execution_mode": "direct",
            "delegate_to": "",
            "delegate_message": "",
            "exact_targets": [],
            "diagnostics": [],
            "research_questions": [],
            "confidence": str(s.get("confidence") or "medium").title(),
        }
        actions.append(card)

    # Generate AI deliverables using actual account data
    ai_actions = _generate_ai_actions(
        selected,
        analysis,
        brand,
        ai_model=ai_model,
        mission_profile=mission_profile,
        action_items=action_items,
    )
    if ai_actions:
        for i, card in enumerate(actions):
            if i < len(ai_actions):
                ai = ai_actions[i]
                card["steps"] = ai.get("steps", [])
                card["impact"] = ai.get("impact", "")
                card["time"] = ai.get("time", "")
                card["mission_name"] = ai.get("mission_name", "")
                card["why"] = ai.get("why", "")
                card["reward"] = ai.get("reward", "")
                card["execution_mode"] = str(ai.get("execution_mode") or card["execution_mode"] or "direct")
                card["delegate_to"] = str(ai.get("delegate_to") or card["delegate_to"] or "")
                card["delegate_message"] = str(ai.get("delegate_message") or card["delegate_message"] or "")
                card["exact_targets"] = _normalize_text_list(ai.get("exact_targets") or card["exact_targets"])

    # Fallback: if AI didn't return steps, generate basic ones from the suggestion data
    for i, card in enumerate(actions):
        action_item = action_items[i] if i < len(action_items) else {"relevant_data": {}}
        delegate_plan = _build_delegate_plan(
            card["title"],
            selected[i].get("category", "") if i < len(selected) else "",
            action_item.get("relevant_data") or {},
        )
        if not card.get("exact_targets"):
            card["exact_targets"] = delegate_plan["exact_targets"]
        if not card.get("delegate_message"):
            card["delegate_message"] = delegate_plan["delegate_message"]
        if not card.get("delegate_to"):
            card["delegate_to"] = delegate_plan["delegate_to"]
        if not card.get("execution_mode") or card.get("execution_mode") == "direct":
            card["execution_mode"] = delegate_plan["execution_mode"]
        if not card["steps"] and i < len(selected):
            s = selected[i]
            card["steps"] = _fallback_steps(s, action_item=action_item, delegate_plan=delegate_plan)
            if not card["time"]:
                card["time"] = "15-30 minutes"
        # Fallback mission name
        if not card["mission_name"]:
            card["mission_name"] = card["title"]
        _apply_mission_reality_checks(card, selected[i], action_item)
        _apply_platform_reality_checks(card, selected[i], action_item, delegate_plan=delegate_plan)
        mission_intel = _build_mission_intelligence(selected[i], action_item, analysis, brand=brand)
        card["diagnostics"] = mission_intel["diagnostics"]
        card["research_questions"] = mission_intel["research_questions"]
        card["confidence"] = mission_intel["confidence"]
        # Calculate difficulty from time
        card["difficulty"] = _parse_difficulty(card["time"])

    return actions


def _fallback_steps(suggestion, action_item=None, delegate_plan=None):
    """Build detailed, click-by-click steps a fifth grader could follow.

    These are used when the AI prompt fails or no API key is set. Each step
    tells the user exactly where to click, what to look for, and what to do.
    """
    title = suggestion.get("title", "")
    category = suggestion.get("category", "")
    data_point = suggestion.get("data_point", "")
    detail = suggestion.get("detail", "")
    title_lower = title.lower()
    relevant_data = (action_item or {}).get("relevant_data") or {}
    delegate_plan = delegate_plan or {"execution_mode": "direct", "delegate_message": "", "exact_targets": []}
    platform_key = _resolve_action_platform(title, category, relevant_data, detail)

    dp = data_point  # short alias

    if delegate_plan.get("execution_mode") == "delegate":
        steps = ["Copy the developer brief below and send it today."]
        for target in delegate_plan.get("exact_targets") or []:
            steps.append(f"Have them update this exact target: {target}.")
        steps.append("Once the update is live, mark the mission complete so GroMore can re-check performance on the next refresh.")
        return steps[:5]

    if category == "strategy" and any(word in title_lower for word in ("traffic", "sessions", "traffic drop", "dropped")):
        return _strategy_traffic_drop_steps(dp, relevant_data)

    # --- Google Ads: CPC / Cost related ---
    if platform_key == "google_ads" and any(w in title_lower for w in ("cost per click", "cpc", "lower cost")):
        wasted_row, wasted_term = _pick_primary_search_term(relevant_data.get("google_ads_search_terms") or [])
        campaign_row, campaign_name = _pick_primary_campaign(relevant_data.get("google_ads_campaigns") or [])
        return [
            f"Go to ads.google.com. Click \"Campaigns\" in the left sidebar. Click \"Keywords\" then \"Search terms\" at the top.{f' Your CPC right now is {dp}.' if dp else ''}",
            (f"Start with the search term \"{wasted_term}\" if it is still spending without conversions. Check the box next to it and any similar wasted terms." if wasted_term else "Sort the list by \"Cost\" (highest first). Find search terms that have spent money but show 0 conversions. Check the box next to each one."),
            "Click the blue \"Add as negative keyword\" button at the top. Choose \"Account level\" so they're blocked everywhere.",
            "Now click \"Keywords\" (not search terms). Sort by \"Cost/conv.\" highest first. Any keyword over 2x your target CPA, click the green dot and change it to \"Paused.\"",
            (f"Click into \"{campaign_name}\" if that is your main spender. Open \"Settings\" and lower the daily budget by 10-15% only if it is still eating spend without enough conversions." if campaign_name else "Click on your highest-spending campaign. Click \"Settings.\" Lower the daily budget by 10-15% and move that money to your best-converting campaign instead."),
        ]

    # --- Meta: duplicate / clone ad ---
    if platform_key == "meta_ads" and any(w in title_lower for w in ("clone", "duplicate")) and "ad" in title_lower:
        return [
            f"Go to business.facebook.com/adsmanager. Click \"Ads\" at the top.{f' Current note: {dp}.' if dp else ''}",
            "Sort by \"Results\" or \"CTR (link)\" and find the ad already bringing in the best response at the lowest cost.",
            "Check the box next to that ad. Click \"Duplicate.\" Keep it in the same campaign and ad set unless you already know you need a separate test.",
            "In the duplicated ad, change just one thing first - either the primary text, headline, or image - so you can see exactly what improved or got worse.",
            "Click \"Publish\" and leave the original ad running. Check results in 3-5 days before making another change.",
        ]

    # --- Paid advertising optimization, routed by real platform ---
    if category == "paid_advertising" and platform_key == "google_ads":
        campaign_row, campaign_name = _pick_primary_campaign(relevant_data.get("google_ads_campaigns") or [])
        search_term_row, search_term = _pick_primary_search_term(relevant_data.get("google_ads_search_terms") or [])
        return [
            (f"Go to ads.google.com. Click \"Campaigns\" on the left side and open \"{campaign_name}\" first.{f' Data point: {dp}.' if dp else ''}" if campaign_name else f"Go to ads.google.com. Click \"Campaigns\" on the left side.{f' Data point: {dp}.' if dp else ''} Sort by \"Cost\" to see which campaign spends the most."),
            (f"Inside \"{campaign_name}\", click \"Ad groups\" and look for any group spending money without conversions." if campaign_name else "Click the campaign name that's spending the most. Click \"Ad groups\" to see all ad groups inside it. Look for any with a high cost but 0 conversions."),
            "For ad groups with 0 conversions: click the green dot next to it and choose \"Paused.\" This stops wasting money on ads that don't work.",
            (f"Go back to the campaign. Click \"Keywords\" then \"Search terms\" and review \"{search_term}\" first if it is not converting. Add it as a negative keyword if it does not belong." if search_term else "Go back to the campaign. Click \"Keywords\" then \"Search terms.\" Add anything irrelevant as a negative keyword (check the box, then click \"Add as negative keyword\")."),
            "Click \"Ads & assets.\" If any ad has a CTR below 2%, click the pencil icon and rewrite the headline to include your main service + city name.",
        ]

    if category == "paid_advertising" and platform_key == "meta_ads":
        campaign_row, campaign_name = _pick_primary_campaign(relevant_data.get("meta_campaigns") or [])
        return [
            (f"Go to business.facebook.com/adsmanager. Click \"Campaigns\" at the top and open \"{campaign_name}\" first.{f' Data point: {dp}.' if dp else ''}" if campaign_name else f"Go to business.facebook.com/adsmanager. Click \"Campaigns\" at the top.{f' Data point: {dp}.' if dp else ''} Sort by \"Amount spent\" so the biggest spenders are at the top."),
            (f"Inside \"{campaign_name}\", click \"Ad sets\" and pause any set that has spent hard but still has 0 leads or a cost per result far above your target." if campaign_name else "Open the campaign spending the most. Click \"Ad sets\" and pause any ad set that has spent hard but still has 0 leads or a cost per result far above your target."),
            "Click into the ad sets still working. Open \"Edit\" and move a little more budget toward the one getting leads at the lowest cost.",
            "Click \"Ads\" and compare \"CTR (link)\" and \"Cost per result.\" Duplicate the best ad if you need a new variation, and pause weak ads that keep spending without leads.",
            "Check results again in 3-5 days so you can keep the winners running and cut the losers faster.",
        ]

    # --- Facebook / Meta Ads ---
    if any(w in title_lower for w in ("facebook", "meta", "instagram", "roas")):
        return [
            f"Go to business.facebook.com/adsmanager. Click \"Campaigns\" at the top.{f' Current metric: {dp}.' if dp else ''} Sort by \"Cost per result\" (click the column header).",
            "Find any campaign where the cost per result is more than double your goal. Click the toggle switch on the left side to turn it OFF.",
            "For campaigns that ARE working: click the campaign name, then click into the ad set level. Click \"Edit.\" Under \"Budget,\" increase the daily budget by $5-10.",
            "Still in the ad set, scroll down to \"Placements.\" Switch to \"Manual placements\" and uncheck anything except Facebook Feed, Instagram Feed, and Instagram Stories.",
            "Click \"Ads\" at the top. Look at each ad's \"CTR (link).\" If any ad has under 1% CTR, click the pencil icon and change the image or headline.",
        ]

    # --- SEO / Search Console ---
    if category == "seo" or any(w in title_lower for w in ("seo", "ranking", "organic", "search console")):
        query_row, query_name = _pick_primary_query((relevant_data.get("seo_keyword_opportunities") or []) + (relevant_data.get("seo_top_queries") or []))
        page_row, page_path = _pick_primary_page(relevant_data.get("seo_top_pages") or [])
        return [
            f"Go to search.google.com/search-console. Click \"Performance\" on the left side.{f' Current data: {dp}.' if dp else ''} Make sure \"Average position\" is checked at the top.",
            (f"Click the \"Pages\" tab and inspect \"{page_path}\" first if it is already earning impressions. That page is one of your best current SEO opportunities." if page_path else "Click the \"Pages\" tab. Sort by \"Impressions\" (highest first). Find pages with lots of impressions but very few clicks - those need better titles."),
            (f"Go to your website editor and update \"{page_path}\" so the title tag and headline match the query people actually searched for." if page_path else "Click on a page with high impressions but low clicks. Go to your website editor and change that page's title tag to include the exact keyword people searched."),
            (f"Now click the \"Queries\" tab and review \"{query_name}\" first. If it is close to page 1, add a section to the matching page that answers that exact search intent more clearly." if query_name else "Now click the \"Queries\" tab in Search Console. Look for keywords in positions 8-20 (page 1-2 of Google). These are close to ranking. Write a new section on your page about that exact topic."),
            "Go to your website. Make sure every service page has at least 500 words, your city name in the title, and a clear \"Call Now\" or \"Get a Quote\" button at the top.",
        ]

    # --- Website / Analytics ---
    if category == "website" or any(w in title_lower for w in ("website", "landing page", "conversion", "bounce", "analytics")):
        exact_targets = _format_exact_targets("website", relevant_data)
        if exact_targets:
            steps = []
            for target in exact_targets[:3]:
                steps.append(f"Update this exact page next: {target}.")
            steps.append("On each page, put the phone CTA or quote form above the fold and make the headline match the service intent.")
            steps.append("When the edits are live, compare lead volume and bounce rate after 7 days.")
            return steps[:5]
        return [
            f"Go to analytics.google.com. Click \"Reports\" on the left, then \"Pages and screens.\"{f' Current metric: {dp}.' if dp else ''} Sort by \"Views\" to see your most visited pages.",
            "Look at the \"Bounce rate\" column. Find any page with a bounce rate over 70%. That means most people leave without doing anything. Those pages need fixing first.",
            "Open your website in a new tab. Go to each high-bounce page. Ask yourself: is there a phone number or form visible without scrolling? If not, add one at the very top.",
            "Check if your pages load in under 3 seconds. Go to pagespeed.web.dev, paste each page URL, and click \"Analyze.\" Fix anything it flags as red.",
            "On every page, add a clear button that says exactly what you want them to do: \"Call Now,\" \"Get a Free Quote,\" or \"Book Online.\" Put it above the fold (visible without scrolling).",
        ]

    # --- Budget / Spend efficiency ---
    if category == "budget":
        if platform_key == "meta_ads":
            campaign_row, campaign_name = _pick_primary_campaign(relevant_data.get("meta_campaigns") or [])
            return [
                f"Go to business.facebook.com/adsmanager. Click \"Campaigns\" at the top.{f' Budget data: {dp}.' if dp else ''} Write down how much each campaign spent this month and how many leads it produced.",
                "Divide each campaign's spend by its leads. The campaign with the lowest cost per lead is your best use of budget. The one with the highest cost per lead, or 0 leads, is your weakest.",
                (f"Start with \"{campaign_name}\" if that is the main active campaign and verify whether it still deserves the budget it is getting." if campaign_name else "Open your weakest campaign. If it is still spending without results, lower the budget or pause it so it stops eating money."),
                "Move a small amount of that budget into the campaign already producing leads at the best cost. Keep the move modest so you can watch what happens.",
                "Set a reminder for 7 days from now and compare cost per lead again before making the next shift.",
            ]
        if platform_key != "google_ads":
            return [
                f"Open the ad platform this account is actually using.{f' Budget data: {dp}.' if dp else ''} Pull up the campaigns that spent money this month.",
                "Write down spend and leads for each campaign so you can see which one is cheapest and which one is wasting money.",
                "Reduce budget on the weak campaign that is spending without leads or has the highest cost per lead.",
                "Move a small amount of that budget into the campaign already producing leads at the best cost.",
                "Check the same numbers again in 7 days before making another budget move.",
            ]
        return [
            f"Go to ads.google.com. Click \"Campaigns\" on the left.{f' Budget data: {dp}.' if dp else ''} Write down how much each campaign spent this month and how many leads it got.",
            "Divide each campaign's spend by its leads. The one with the LOWEST cost per lead is your best campaign. The one with the HIGHEST cost per lead (or 0 leads) is your worst.",
            "Click on your worst campaign (highest cost per lead). Click \"Settings.\" Lower the daily budget by 20%. Write down the dollar amount you saved.",
            "Now click on your best campaign (lowest cost per lead). Click \"Settings.\" Add the money you just saved to this campaign's daily budget.",
            "Set a calendar reminder for 7 days from now to check again. Look at the same numbers. If the change helped, keep it. If not, reverse it.",
        ]

    # --- Creative / Ad copy ---
    if category == "creative" and platform_key == "google_ads":
        campaign_row, campaign_name = _pick_primary_campaign(relevant_data.get("google_ads_campaigns") or [])
        return [
            (f"Go to ads.google.com. Open \"{campaign_name}\" and click \"Ads & assets\".{f' Creative data: {dp}.' if dp else ''}" if campaign_name else f"Go to ads.google.com. Click \"Ads & assets\" inside your top-spending campaign.{f' Creative data: {dp}.' if dp else ''}"),
            "Sort the ads by CTR and write down what the best headline says. That is the angle already getting attention.",
            "Edit the weakest ad and rewrite the headline so it sounds closer to the winner, but test one new promise, hook, or offer angle.",
            "Keep the landing page and offer aligned with the ad so the click feels consistent after people land.",
            "Save the change and compare CTR and conversions again after enough traffic comes through.",
        ]

    if category == "creative":
        ad_row, ad_name = _pick_primary_ad(relevant_data.get("meta_top_ads") or [])
        return [
            f"Go to your ads platform (ads.google.com or business.facebook.com/adsmanager).{f' Creative data: {dp}.' if dp else ''} Click into your top-spending campaign, then click \"Ads\" or \"Ads & assets.\"",
            (f"Start with the ad \"{ad_name}\" if it is one of your top current performers. Write down what its headline and image are doing better than the rest." if ad_name else "Look at each ad's CTR (Click-Through Rate). Find the ad with the HIGHEST CTR. That's your winning style. Write down what its headline and image look like."),
            "Find ads with the LOWEST CTR. Click the pencil/edit icon. Rewrite the headline to match the style of your winning ad, but test a different angle (urgency, price, guarantee).",
            "For image ads: make sure the image shows your actual work, team, or a real before/after. Remove any ad that uses a generic stock photo. Replace with a real photo from your phone.",
            "Duplicate your best-performing ad. Change ONLY the headline (keep the image). This lets you test which words get more clicks without losing what already works.",
        ]

    # --- Organic social ---
    if category == "organic_social":
        return [
            f"Go to business.facebook.com. Click your page name, then \"Insights\" on the left side.{f' Data point: {dp}.' if dp else ''} Click \"Posts\" and sort by \"Reach\" (highest first).",
            "Look at your top 3 posts by reach. Write down what they have in common: was it a photo, video, question, or tip? That type of post is what your audience likes.",
            "Open your phone. Take a photo or short video (under 60 seconds) of your work, your team, or a customer result. Something real, NOT a stock image or graphic.",
            "Write a post using this exact format: Start with a question or bold statement. Then 2-3 short sentences. End with a call to action (\"Comment below\" or \"DM us\").",
            "Post it NOW. Don't overthink it. Then set a reminder to post again in 3 days. Consistency beats perfection.",
        ]

    # --- Catch-all with platform detection ---
    url, label = _platform_link(platform_key)

    steps = []
    if url:
        steps.append(f"Go to {url}. Log in and find the section related to \"{title}.\"{f' Your current number is {dp}.' if dp else ''}")

    if detail:
        sentences = [s.strip() for s in detail.replace(". ", ".\n").split("\n") if s.strip() and len(s.strip()) > 15]
        for s in sentences[:2]:
            if not s.endswith("."):
                s += "."
            steps.append(s)

    steps.append(f"Make one specific change today. Write down what you changed and what the number was before, so you can check if it helped next week.")
    if url:
        steps.append(f"Set a reminder for 7 days from now. Go back to {url} and compare the numbers to see if your change made a difference.")

    return steps[:5]


def _generate_ai_actions(suggestions, analysis, brand, ai_model=None, mission_profile=None, action_items=None):
    """Call AI to generate specific deliverables for each action card.

    Instead of 'go to Google Ads and click...', this produces the actual work:
    real ad headlines to test, real keywords to pause, real negative keywords,
    specific audience changes, actual content recommendations tied to data.

    Returns a list of step-lists (one per suggestion), or empty list on failure.
    """
    api_key = (brand.get("openai_api_key") or "").strip()
    try:
        from flask import current_app
        if not api_key:
            api_key = (current_app.config.get("OPENAI_API_KEY", "") or "").strip()
    except RuntimeError:
        if not api_key:
            api_key = os.environ.get("OPENAI_API_KEY", "").strip()

    if not api_key:
        return []

    model = (
        (ai_model or "").strip()
        or (brand.get("openai_model_analysis") or "").strip()
        or (brand.get("openai_model") or "").strip()
        or "gpt-4o"
    )

    from src.ad_intelligence import build_ad_intelligence
    from webapp.ai_assistant import _summarize_analysis_for_ai
    from webapp.vertical_intelligence import build_vertical_profile
    if not (analysis.get("ad_intelligence") or {}).get("summary"):
        analysis = dict(analysis)
        analysis["ad_intelligence"] = build_ad_intelligence(analysis, brand)
    analysis_summary = _summarize_analysis_for_ai(analysis)

    if action_items is None:
        action_items = _build_action_items(suggestions, analysis_summary)

    for item in action_items:
        category = item.get("category", "")
        exact_targets = _format_exact_targets(category, item.get("relevant_data") or {})
        if exact_targets:
            item["detail"] = _trim_copy(
                f"{item.get('detail', '')} Exact targets from the connected data: {'; '.join(exact_targets)}",
                900,
            )

    client_info = analysis_summary.get("client", {})

    prompt_data = {
        "client": {
            "name": client_info.get("name"),
            "industry": client_info.get("industry"),
            "service_area": client_info.get("service_area"),
            "services": client_info.get("primary_services"),
            "budget": client_info.get("monthly_budget"),
            "goals": client_info.get("goals"),
            "competitors": client_info.get("competitors"),
            "target_audience": client_info.get("target_audience"),
            "active_offers": client_info.get("active_offers"),
        },
        "vertical_profile": build_vertical_profile(brand),
        "mission_profile": mission_profile or infer_mission_profile(),
        "highlights": analysis_summary.get("highlights", []),
        "concerns": analysis_summary.get("concerns", []),
        "period": analysis_summary.get("period", {}),
        "ad_intelligence": analysis_summary.get("ad_intelligence") or {},
        "action_items": action_items,
    }

    skill_level = ((mission_profile or {}).get("skill_level") or "beginner").strip().lower()
    if skill_level == "advanced":
        audience_block = (
            "AUDIENCE: An owner or operator who already knows the major tools. "
            "Keep the missions tighter and less tutorial-heavy, but still name the exact menu, button, or field when a mistake would be costly. "
            "Prefer concise operator language over hand-holding."
        )
        step_rule = "Write 3-5 steps per mission."
    elif skill_level == "intermediate":
        audience_block = (
            "AUDIENCE: A business owner who has used these tools before, but is not a full-time operator. "
            "Be clear and specific without sounding overwhelming. Name the exact menus and buttons for important actions."
        )
        step_rule = "Write 4-5 steps per mission."
    else:
        audience_block = (
            "AUDIENCE: A business owner who has NEVER been inside Google Ads before. "
            "Write every step so a literal fifth grader could follow it. "
            "Name every button, every menu, every tab. "
            "If you say 'click', say exactly what words are on the button. "
            "If you say 'change', say the exact old value and exact new value. "
            "NEVER assume they know where anything is."
        )
        step_rule = "Write 4-6 steps per mission."

    system = (
        "You are the senior paid-media and SEO strategist inside GroMore. "
        "You have completed a deep-dive analysis of this account. "
        "Now produce MISSIONS the business owner can execute right now.\n\n"

        + audience_block + "\n\n"

        "CRITICAL EXECUTION RULES:\n"
        "- The user should NEVER have to hunt through analytics, Search Console, or ad dashboards for targets that are already in the connected data.\n"
        "- Name the exact page, query, campaign, ad, keyword, or search term from the provided data whenever one exists.\n"
        "- If the work belongs with a developer, designer, or assistant, do NOT send the owner into the tool to figure it out. Write a delegation-ready mission and include a ready-to-send handoff note.\n"
        "- If relevant_data only includes Meta campaigns or Meta ads and no Google Ads campaigns/search terms, NEVER send the owner to ads.google.com or mention Google Ads.\n"
        "- If relevant_data only includes Google Ads campaigns/search terms and no Meta campaigns/top ads, NEVER send the owner to business.facebook.com or Ads Manager.\n"
        "- If category is organic_social, keep the mission about organic Page posting, engagement, reach, and top posts. Do not use paid campaign spend or lead results as evidence for that mission.\n"
        "- If the SEO data already points to existing pages and the search volume is light, do NOT recommend new city or local pages. Rewrite the current page instead.\n"
        "- If period says early_month is true, do NOT create missions from low current-month volume alone. Use rate, spend, page, query, or campaign evidence, and call out lower confidence when sample size is thin.\n"
        "- Use ad_intelligence as the primary paid-media diagnosis when it is present. Its findings already normalize Google Ads and Meta evidence into waste, scale, creative, and data-gap signals.\n"
        "- Use vertical_profile to adapt chatbot, ads, commercial, and content guidance to the actual service vertical. Do not assume one niche.\n"
        "- If vertical_profile shows commercial targets, distinguish commercial account missions from residential lead missions.\n"
        "- Every mission must include a diagnosis first: what signal triggered it, what exact evidence supports it, what might be a false positive, and then the fix.\n"
        "- Avoid verbs like 'look for', 'review', 'find', 'evaluate', or 'assess' unless the exact thing to inspect is named in the same sentence.\n\n"

        "OUTPUT FORMAT (JSON only):\n"
        "{\"actions\": [\n"
        "  {\n"
        "    \"mission_name\": \"3-6 word punchy verb phrase\",\n"
        "    \"micro_steps\": [\"step 1\", \"step 2\", ...],\n"
        "    \"exact_targets\": [\"exact page/query/campaign names from the data\"],\n"
        "    \"execution_mode\": \"direct\" or \"delegate\",\n"
        "    \"delegate_to\": \"developer\" or \"designer\" or \"assistant\" or \"\",\n"
        "    \"delegate_message\": \"ready-to-send note if the owner should hand this off\",\n"
        "    \"why\": \"one sentence: how this is costing them money or losing them leads\",\n"
        "    \"reward\": \"one sentence: what improves when they finish\",\n"
        "    \"impact\": \"one sentence with specific projected numbers\",\n"
        "    \"time\": \"15 minutes\"\n"
        "  }\n"
        "]}\n"
        "One object per action_item, same order as input.\n\n"

        "MISSION NAME RULES:\n"
        "- 3-6 words, starts with a verb. Punchy and specific.\n"
        "- GOOD: \"Kill the $340 Money Drain\", \"Stop Paying for Junk Clicks\", "
        "\"Fix Your Broken Landing Page\", \"Launch a High-Converting Ad\"\n"
        "- BAD: \"Optimize Campaign Performance\", \"Improve Your SEO\", \"Tune Underperforming Campaigns\"\n\n"

        "MICRO-STEP RULES - THIS IS THE MOST IMPORTANT PART:\n"
        + step_rule + " Each step = ONE specific action.\n\n"

        "Each step MUST include ALL of these:\n"
        "1. The exact URL to go to (ads.google.com, business.facebook.com/adsmanager, etc.)\n"
        "2. The exact menu/tab/button to click, using the exact words shown on screen\n"
        "3. The exact thing to type, change, pause, or enable\n"
        "4. WHY this specific thing (reference a campaign name, keyword, dollar amount, or metric from the data)\n\n"

        "STEP EXAMPLES THAT ARE CORRECT:\n"
        "- 'Go to ads.google.com. Click \"Keywords\" in the left sidebar, then click \"Search terms\" at the top. Sort the list by \"Cost\" (click the column header). Find any search term that spent over $20 but has 0 conversions. Check the box next to it, then click \"Add as negative keyword\" and choose \"Account level.\"'\n"
        "- 'Go to ads.google.com. Click \"Campaigns\" on the left. Find \"SDL Search Campaign\" (it spent $340 and got 0 leads). Click the green dot under \"Status\" and change it to \"Paused.\"'\n"
        "- 'Go to business.facebook.com/adsmanager. Click on your active campaign. Click \"Ad sets\" at the top. Click \"Edit\" on the ad set. Scroll down to \"Budget.\" Change the daily budget from $15 to $25 because this ad set has the lowest cost per lead at $12.'\n"
        "- 'Go to search.google.com/search-console. Click \"Performance\" on the left. Click the \"Queries\" tab. Find \"plumber near me\" - you are at position 14 with 800 impressions. Go to your website and add a new page titled \"Plumber Near Me in [Your City]\" with at least 500 words about that service.'\n\n"
        "- 'Copy the developer note below and send it to your website developer. Update /emergency-plumbing - 92 sessions, 0 conversions, 81% bounce. Put the phone CTA above the fold, tighten the H1 to match emergency service intent, and shorten the quote form to name, phone, and service.'\n\n"

        "STEP EXAMPLES THAT ARE WRONG (NEVER WRITE THESE):\n"
        "- 'Go to ads.google.com and log in to your Google Ads account.' (FILLER. They know how to log in.)\n"
        "- 'Your current number: CPC: $7.84. Look for this in your dashboard to confirm.' (That's just restating data. It's not a step.)\n"
        "- 'Add negative keywords weekly, improve Quality Score, and split high-cost broad groups into tighter exact and phrase match groups.' (Three vague actions crammed into one sentence. No specifics on WHICH keywords, WHICH groups.)\n"
        "- 'Review your campaigns and pause underperformers' (WHICH campaigns? Name them.)\n"
        "- 'Reallocate spend toward top converters' (Move how much? From which campaign to which?)\n"
        "- 'Optimize your landing pages' (WHICH page? Change what text to what?)\n"
        "- 'Consider testing new ad copy' (Don't suggest it - WRITE the actual headline for them.)\n"
        "- 'Tighten targeting' (Change what setting? To what value?)\n"
        "- 'These campaigns are under target: SDL Search Campaign' (That's a fact, not a step.)\n"
        "- 'Average CPC is $7.84, above benchmark.' (That's data, not an action.)\n"
        "- Any step that starts with 'Review', 'Consider', 'Look into', 'Assess', or 'Evaluate'\n\n"

        "USE THE DATA: You have relevant_data attached to each action item. "
        "Use actual campaign names, actual keyword names, actual dollar amounts, actual search terms from the data. "
        "If data says campaign X spent $Y with Z conversions, reference those exact numbers. "
        "If search_terms data shows wasted terms, name those exact terms. "
        "NEVER write a generic step when you have specific data available.\n\n"

        "WHY field: One sentence using a specific dollar amount or lead count. "
        "Example: \"You burned $340 last month on clicks that never turned into a phone call.\"\n\n"

        "REWARD field: Concrete result, not vague improvement. "
        "Example: \"$340/month gets redirected to keywords that actually generate calls.\"\n\n"

        "IMPACT: Specific projected numbers. "
        "Example: 'Could save $340/month and generate about 5 more leads at $38 each.'\n\n"

        "TIME: '5 minutes', '10 minutes', '15 minutes', '30 minutes'. Not 'varies'.\n\n"

        "FINAL CHECK before returning: Read each step out loud. "
        "If a step does not tell the user EXACTLY which button to click and EXACTLY what to type or change, rewrite it. "
        "If a step just restates a metric or describes a problem, delete it and replace it with an action. "
        "No filler steps like 'log in to your account' or 'check your dashboard.' "
        "Every step must CHANGE something or CREATE something."
    )

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    try:
        resp = _requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json={
                "model": model,
                "temperature": 0.4,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": json.dumps(prompt_data)},
                ],
                "response_format": {"type": "json_object"},
            },
            timeout=60,
        )

        if resp.status_code != 200:
            log.warning("AI action generation failed (%s): %s", resp.status_code, resp.text[:200])
            return []

        data = resp.json()
        content = (
            (data.get("choices") or [{}])[0]
            .get("message", {})
            .get("content", "")
        )

        parsed = json.loads(content)

        # Extract the actions array from the response
        actions_list = parsed
        if isinstance(parsed, dict):
            actions_list = parsed.get("actions", [])
            if not actions_list:
                for v in parsed.values():
                    if isinstance(v, list):
                        actions_list = v
                        break

        if not isinstance(actions_list, list):
            return []

        # Each item should be {"mission_name": "...", "micro_steps": [...], "why": "...", "reward": "...", "impact": "...", "time": "..."}
        result = []
        for item in actions_list:
            if isinstance(item, dict):
                # Accept both "micro_steps" (new) and "steps" (old) keys
                steps = item.get("micro_steps") or item.get("steps") or []
                result.append({
                    "steps": [str(s) for s in steps if s],
                    "impact": str(item.get("impact", "")),
                    "time": str(item.get("time", "")),
                    "mission_name": str(item.get("mission_name", "")),
                    "exact_targets": _normalize_text_list(item.get("exact_targets")),
                    "execution_mode": str(item.get("execution_mode", "direct") or "direct"),
                    "delegate_to": str(item.get("delegate_to", "")),
                    "delegate_message": str(item.get("delegate_message", "")),
                    "why": str(item.get("why", "")),
                    "reward": str(item.get("reward", "")),
                })
            elif isinstance(item, list):
                # Fallback: plain list of strings (old format)
                result.append({
                    "steps": [str(s) for s in item if s],
                    "impact": "",
                    "time": "",
                    "exact_targets": [],
                    "execution_mode": "direct",
                    "delegate_to": "",
                    "delegate_message": "",
                })
            else:
                result.append({"steps": [], "impact": "", "time": "", "exact_targets": [], "execution_mode": "direct", "delegate_to": "", "delegate_message": ""})

        return result

    except Exception as e:
        log.warning("AI action generation error: %s", e)
        return []


def _generate_ai_analysis_brief(analysis, suggestions, brand, ai_model=None):
    """Generate a deeper cross-source analysis brief for the Action Plan page."""
    api_key = (brand.get("openai_api_key") or "").strip()
    try:
        from flask import current_app
        if not api_key:
            api_key = (current_app.config.get("OPENAI_API_KEY", "") or "").strip()
    except RuntimeError:
        if not api_key:
            api_key = os.environ.get("OPENAI_API_KEY", "").strip()

    if not api_key:
        return ""

    model = (
        (ai_model or "").strip()
        or (brand.get("openai_model_analysis") or "").strip()
        or (brand.get("openai_model") or "").strip()
        or "gpt-4o"
    )

    from webapp.ai_assistant import _summarize_analysis_for_ai
    analysis_summary = _summarize_analysis_for_ai(analysis)

    payload = {
        "analysis": analysis_summary,
        "suggestions": suggestions,
    }

    system = (
        "You are a principal growth operator writing a short executive analysis for a business owner. "
        "Use only data in context. No generic filler, no speculation, no platform blame without proof. "
        "Return concise markdown with sections: Top Risks, Best Opportunities, 30-Day Action Plan, What To Watch Weekly. "
        "Each bullet must include at least one concrete data point (metric, campaign, keyword, query, spend, CPA, CTR, or position)."
    )

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    try:
        resp = _requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json={
                "model": model,
                "temperature": 0.3,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": json.dumps(payload)},
                ],
            },
            timeout=45,
        )
        if resp.status_code != 200:
            log.warning("AI analysis brief failed (%s): %s", resp.status_code, resp.text[:200])
            return ""

        data = resp.json()
        content = (
            (data.get("choices") or [{}])[0]
            .get("message", {})
            .get("content", "")
        )
        return (content or "").strip()
    except Exception as e:
        log.warning("AI analysis brief error: %s", e)
        return ""


def _explain_kpis(analysis):
    """Build plain-English KPI status cards."""
    kpi = analysis.get("kpi_status", {})
    targets = kpi.get("targets", {})
    actual = kpi.get("actual", {})
    evaluation = kpi.get("evaluation", {})

    cards = []

    if targets.get("cpa") and actual.get("blended_cpa"):
        cpa_eval = evaluation.get("cpa", {})
        on_track = cpa_eval.get("on_track", False)
        cards.append({
            "label": "Cost Per Lead",
            "target": f"${targets['cpa']:.2f}",
            "actual": f"${actual['blended_cpa']:.2f}",
            "on_track": on_track,
            "explanation": (
                f"Your target is ${targets['cpa']:.2f} per lead and you're at ${actual['blended_cpa']:.2f}. "
                + ("You're beating your target. Nice work."
                   if on_track
                   else "You're above target. Check which campaigns are driving up costs "
                        "and see the action steps below.")
            ),
        })

    if targets.get("leads") and actual.get("paid_leads") is not None:
        leads_eval = evaluation.get("leads", {})
        on_track = leads_eval.get("on_track", False)
        expected_to_date = leads_eval.get("expected_to_date")
        is_current_month = leads_eval.get("is_current_month", False)
        pace_label = leads_eval.get("pace_label") or ("On pace" if on_track else "Behind pace")
        elapsed_days = leads_eval.get("elapsed_days")
        cards.append({
            "label": "Total Leads",
            "target": f"{int(targets['leads'])}",
            "actual": f"{int(actual['paid_leads'])}",
            "on_track": on_track,
            "status_label": pace_label,
            "explanation": (
                (
                    f"You have {int(actual['paid_leads'])} leads against a monthly target of {int(targets['leads'])}. "
                    f"By day {int(elapsed_days or 0)}, the paced target is {expected_to_date:.1f}, so you're {pace_label.lower()}."
                    if is_current_month and expected_to_date is not None
                    else f"Target is {int(targets['leads'])} leads and you got {int(actual['paid_leads'])}. "
                )
                + (
                    " Keep spend stable and focus on lead quality before making major changes."
                    if on_track and is_current_month
                    else " You're hitting your lead target."
                    if on_track
                    else " Consider increasing budget on your best-performing campaigns or launching new ad variations."
                )
            ),
        })

    if targets.get("roas") and actual.get("blended_roas"):
        roas_eval = evaluation.get("roas", {})
        on_track = roas_eval.get("on_track", False)
        cards.append({
            "label": "Return on Ad Spend",
            "target": f"{targets['roas']:.1f}x",
            "actual": f"{actual['blended_roas']:.1f}x",
            "on_track": on_track,
            "explanation": (
                f"For every $1 you spend on ads, you're making ${actual['blended_roas']:.2f} back. "
                f"Your target is ${targets['roas']:.1f}x. "
                + ("You're above target - your ads are profitable."
                   if on_track
                   else "Below target. Focus on reducing cost per lead or improving close rate.")
            ),
        })

    if not cards:
        paid_spend = _to_float(actual.get("paid_spend"), 0)
        paid_leads = _to_float(actual.get("paid_leads"), 0)
        blended_cpa = actual.get("blended_cpa")
        if paid_spend > 0 or paid_leads > 0:
            actual_parts = [f"${paid_spend:,.2f} spend", f"{paid_leads:.0f} paid leads"]
            if blended_cpa:
                actual_parts.append(f"${float(blended_cpa):.2f} blended CPL")
            cards.append({
                "label": "Paid Efficiency",
                "target": "Set lead and CPL targets",
                "actual": " / ".join(actual_parts),
                "on_track": None,
                "status_label": "Observed",
                "explanation": (
                    "No KPI targets are set yet, so Warren is showing the available paid signals: "
                    "spend, leads, and blended cost per lead. Add target leads and target CPL to turn this into a true score."
                ),
            })

    return cards


def _build_health_summary(analysis, actions, overall_grade, overall_score):
    kpi = analysis.get("kpi_status", {}) if isinstance(analysis, dict) else {}
    evaluation = kpi.get("evaluation", {}) if isinstance(kpi, dict) else {}
    actual = kpi.get("actual", {}) if isinstance(kpi, dict) else {}
    targets = kpi.get("targets", {}) if isinstance(kpi, dict) else {}

    leads_eval = evaluation.get("leads") if isinstance(evaluation.get("leads"), dict) else None
    cpa_eval = evaluation.get("cpa") if isinstance(evaluation.get("cpa"), dict) else None
    roas_eval = evaluation.get("roas") if isinstance(evaluation.get("roas"), dict) else None

    tone = "neutral"
    label = "Needs more data"
    summary = "Connect more data sources or wait for more activity before the dashboard can call the month clearly."
    numbers = []

    if leads_eval:
        paid_leads = int(actual.get("paid_leads") or 0)
        target_leads = int(targets.get("leads") or 0)
        expected_to_date = leads_eval.get("expected_to_date")
        pace_status = leads_eval.get("pace_status") or "full_month"

        numbers.append(f"{paid_leads} leads so far")
        if target_leads:
            numbers.append(f"{target_leads} target this month")
        if expected_to_date is not None and leads_eval.get("is_current_month"):
            numbers.append(f"{expected_to_date:.1f} paced target by today")

        if pace_status == "ahead":
            tone = "positive"
            label = "Ahead of pace"
            summary = (
                f"The numbers say lead volume is running ahead of plan. You have {paid_leads} leads so far, "
                f"which is above the paced target of {expected_to_date:.1f} for this point in the month."
            )
        elif pace_status == "on_track":
            tone = "good"
            label = "On pace"
            summary = (
                f"The numbers say lead volume is on track. You have {paid_leads} leads so far against a paced target of {expected_to_date:.1f}."
            )
        elif pace_status == "watch":
            tone = "caution"
            label = "Watch closely"
            summary = (
                f"The numbers say lead flow is a little behind pace. You have {paid_leads} leads so far against a paced target of {expected_to_date:.1f}."
            )
        elif pace_status == "at_risk":
            tone = "warning"
            label = "Needs attention"
            summary = (
                f"The numbers say lead flow is behind pace. You have {paid_leads} leads so far against a paced target of {expected_to_date:.1f}."
            )
        elif leads_eval.get("on_track"):
            tone = "good"
            label = "On target"
            summary = f"The numbers say you are hitting your lead target with {paid_leads} leads against a goal of {target_leads}."
        else:
            tone = "warning"
            label = "Off target"
            summary = f"The numbers say you are below your lead target with {paid_leads} leads against a goal of {target_leads}."
    elif cpa_eval and cpa_eval.get("on_track") is not None:
        tone = "good" if cpa_eval.get("on_track") else "warning"
        label = "Efficient" if cpa_eval.get("on_track") else "Needs attention"
        summary = (
            f"The numbers say cost efficiency is {'healthy' if cpa_eval.get('on_track') else 'slipping'}. "
            f"Your blended cost per lead is ${cpa_eval.get('actual'):.2f} against a target of ${cpa_eval.get('target'):.2f}."
        )

    action_titles = []
    for action in actions or []:
        title = (action.get("mission_name") or action.get("title") or "").strip()
        if title:
            action_titles.append(title)
        if len(action_titles) == 2:
            break

    if not action_titles:
        if tone in {"positive", "good"}:
            action_titles = ["Keep budget stable", "Watch lead quality before making changes"]
        elif tone == "caution":
            action_titles = ["Review your top-performing campaigns", "Refresh one underperforming ad or offer"]
        elif tone == "warning":
            action_titles = ["Shift spend to your best campaign", "Launch one new ad variation this week"]
        else:
            action_titles = ["Connect your data sources", "Wait for more month-to-date data"]

    meter_pct_map = {
        "positive": 88,
        "good": 74,
        "caution": 52,
        "warning": 28,
        "neutral": 40,
    }

    return {
        "tone": tone,
        "label": label,
        "summary": summary,
        "numbers": numbers,
        "actions": action_titles,
        "meter_pct": meter_pct_map.get(tone, 40),
        "grade": overall_grade,
        "score": overall_score,
        "grade_label": _grade_label(overall_grade),
        "cpa_on_track": cpa_eval.get("on_track") if cpa_eval else None,
        "roas_on_track": roas_eval.get("on_track") if roas_eval else None,
    }


_CLUSTER_STATUS_PCT = {
    "great": 92,
    "good": 76,
    "ok": 56,
    "warning": 34,
    "bad": 18,
}


def ensure_dashboard_health_cluster(dashboard):
    if not isinstance(dashboard, dict):
        return dashboard
    if not isinstance(dashboard.get("health_cluster"), dict):
        dashboard["health_cluster"] = _build_health_cluster(dashboard)
    return dashboard


def _build_health_cluster(dashboard):
    channels = dashboard.get("channels") if isinstance(dashboard.get("channels"), dict) else {}
    health_summary = dashboard.get("health_summary") if isinstance(dashboard.get("health_summary"), dict) else {}
    cards = [
        _build_channel_cluster_card(
            channels,
            key="paid_ads",
            label="Paid Ads",
            kicker="Google and Meta",
            channel_keys=("google_ads", "facebook_ads"),
            metric_priority=("Cost Per Lead", "Click Rate", "Cost Per Click"),
            empty_detail="Connect Google Ads or Meta to read paid traffic health here.",
            positive_detail="Paid traffic is holding up across click quality and lead cost.",
            caution_detail="Paid traffic is active, but at least one channel is leaking efficiency.",
            warning_detail="Paid traffic needs attention before you scale budgets.",
            next_steps={
                "positive": "Protect the best campaign before increasing spend.",
                "good": "Keep budgets stable and test one new winner.",
                "caution": "Refresh one weak ad, offer, or audience this week.",
                "warning": "Cut waste and fix tracking or CPL before adding spend.",
                "neutral": "Connect ads data so this gauge can call it clearly.",
            },
        ),
        _build_channel_cluster_card(
            channels,
            key="organic",
            label="Organic",
            kicker="SEO, content, and social",
            channel_keys=("organic_search", "seo", "facebook_organic"),
            metric_priority=("Organic Website Sessions", "Clicks from Google", "Website Clicks from Social", "Organic Reach", "Engagement", "Posts This Month"),
            empty_detail="Search and content signals are too thin to call yet.",
            positive_detail="Organic visibility is healthy across search and social content.",
            caution_detail="Organic visibility is moving, but consistency, traffic quality, or ranking strength is slipping.",
            warning_detail="Organic traffic and content momentum need work across search or social.",
            next_steps={
                "positive": "Double down on the topics and posts already earning attention.",
                "good": "Keep publishing and tighten one page that is close to ranking.",
                "caution": "Ship a few more local posts or refresh one service page this month.",
                "warning": "Improve publishing consistency and local search coverage first.",
                "neutral": "Connect Search Console or post more consistently to light this up.",
            },
        ),
        _build_channel_cluster_card(
            channels,
            key="website",
            label="Website",
            kicker="Conversion health",
            channel_keys=("website",),
            metric_priority=("Website Conversions", "Bounce Rate", "Time on Site", "Website Visitors"),
            empty_detail="The website gauge needs GA4 data before it can judge conversion health.",
            positive_detail="Visitors are sticking around and the site is helping turn traffic into leads.",
            caution_detail="The site is usable, but conversion friction is holding it back.",
            warning_detail="The site is losing too many visitors before they turn into leads.",
            next_steps={
                "positive": "Protect the landing page that is converting best and test one new CTA.",
                "good": "Keep the main CTA visible and tighten one form or offer.",
                "caution": "Shorten the path to contact and sharpen the main call to action.",
                "warning": "Fix landing-page friction before buying more traffic.",
                "neutral": "Connect GA4 so this gauge can read visitor quality and conversions.",
            },
        ),
        _build_kpi_cluster_card(dashboard, health_summary),
    ]
    return {
        "title": "Gauge Cluster",
        "summary": health_summary.get("summary") or "Your KPI snapshot is ready when enough data is available.",
        "cards": cards,
    }


def _build_channel_cluster_card(
    channels,
    *,
    key,
    label,
    kicker,
    channel_keys,
    metric_priority,
    empty_detail,
    positive_detail,
    caution_detail,
    warning_detail,
    next_steps,
):
    statuses = []
    for channel_key in channel_keys:
        channel = channels.get(channel_key) if isinstance(channels.get(channel_key), dict) else {}
        for card in channel.get("cards") or []:
            status = (card.get("status") or "").strip()
            if status and status != "neutral":
                statuses.append(status)

    score_pct = _average_cluster_score(statuses)
    tone = _cluster_tone(score_pct)
    primary_metric = _cluster_primary_metric(channels, channel_keys, metric_priority)

    if score_pct is None:
        detail = empty_detail
    elif tone in {"positive", "good"}:
        detail = positive_detail
    elif tone == "caution":
        detail = caution_detail
    else:
        detail = warning_detail

    return {
        "key": key,
        "label": label,
        "kicker": kicker,
        "tone": tone,
        "score_pct": score_pct or 0,
        "display_score": str(int(round(score_pct))) if score_pct is not None else "--",
        "state_label": _cluster_state_label(tone, has_data=score_pct is not None),
        "primary_metric": primary_metric,
        "detail": detail,
        "next_step": next_steps.get(tone) or next_steps.get("neutral") or "Connect more data to unlock this view.",
    }


def _build_kpi_cluster_card(dashboard, health_summary):
    kpi = dashboard.get("kpi_status") if isinstance(dashboard.get("kpi_status"), dict) else {}
    evaluation = kpi.get("evaluation") if isinstance(kpi.get("evaluation"), dict) else {}
    actual = kpi.get("actual") if isinstance(kpi.get("actual"), dict) else {}
    targets = kpi.get("targets") if isinstance(kpi.get("targets"), dict) else {}

    leads_eval = evaluation.get("leads") if isinstance(evaluation.get("leads"), dict) else None
    cpa_eval = evaluation.get("cpa") if isinstance(evaluation.get("cpa"), dict) else None
    roas_eval = evaluation.get("roas") if isinstance(evaluation.get("roas"), dict) else None

    score_parts = []
    if leads_eval:
        pace_map = {
            "ahead": 88,
            "on_track": 74,
            "watch": 52,
            "at_risk": 28,
        }
        pace_status = leads_eval.get("pace_status")
        if pace_status in pace_map:
            score_parts.append(pace_map[pace_status])
        elif leads_eval.get("on_track") is not None:
            score_parts.append(74 if leads_eval.get("on_track") else 28)
    if cpa_eval and cpa_eval.get("on_track") is not None:
        score_parts.append(76 if cpa_eval.get("on_track") else 30)
    if roas_eval and roas_eval.get("on_track") is not None:
        score_parts.append(78 if roas_eval.get("on_track") else 30)

    has_configured_targets = bool(leads_eval or cpa_eval or roas_eval)
    if not score_parts:
        status_points = {
            "positive": 86,
            "good": 74,
            "neutral": 58,
            "warning": 34,
            "bad": 24,
            "caution": 48,
        }
        channels = dashboard.get("channels") if isinstance(dashboard.get("channels"), dict) else {}
        paid_metric_tokens = (
            "ad spend",
            "click rate",
            "cost per click",
            "cost per lead",
            "underperforming campaigns",
        )
        for channel_key in ("google_ads", "facebook_ads"):
            channel = channels.get(channel_key) if isinstance(channels.get(channel_key), dict) else {}
            for card in channel.get("cards") or []:
                metric = str(card.get("metric") or "").lower()
                if any(token in metric for token in paid_metric_tokens):
                    score_parts.append(status_points.get(card.get("status"), 58))

    score_pct = round(sum(score_parts) / len(score_parts)) if score_parts else None
    tone = _cluster_tone(score_pct)

    primary_metric = "No KPI target is connected yet"
    if leads_eval:
        primary_metric = f"{int(actual.get('paid_leads') or 0)} leads vs {int(targets.get('leads') or 0)} target"
    elif cpa_eval and cpa_eval.get("actual") is not None and cpa_eval.get("target") is not None:
        primary_metric = f"${float(cpa_eval.get('actual') or 0):.2f} CPL vs ${float(cpa_eval.get('target') or 0):.2f} target"
    elif _to_float(actual.get("paid_spend"), 0) > 0 or _to_float(actual.get("paid_leads"), 0) > 0:
        paid_spend = _to_float(actual.get("paid_spend"), 0)
        paid_leads = _to_float(actual.get("paid_leads"), 0)
        blended_cpa = actual.get("blended_cpa")
        primary_metric = f"${paid_spend:,.0f} spend, {paid_leads:.0f} paid leads"
        if blended_cpa:
            primary_metric += f", ${float(blended_cpa):.2f} CPL"
    elif health_summary.get("grade"):
        primary_metric = f"Overall grade: {health_summary.get('grade')}"

    actions = [item for item in (health_summary.get("actions") or []) if item]
    if score_pct is None:
        detail = "The KPI gauge needs lead or cost targets before it can judge the month clearly."
    elif not has_configured_targets:
        detail = (
            "No KPI targets are set, so this gauge is reading available paid efficiency signals "
            "like CPC, CTR, CPL, spend, and leads."
        )
    else:
        detail = health_summary.get("summary") or "This bucket rolls up pace, cost, and return against your stated targets."

    if actions:
        next_step = actions[0]
    elif score_pct is None:
        next_step = "Connect targets so this gauge can tell you what to fix next."
    elif not has_configured_targets:
        next_step = "Set lead and CPL targets in My Business so Warren can judge this against your actual goal."
    else:
        next_step = "Use the Action Plan to work the biggest KPI constraint first."

    return {
        "key": "kpis",
        "label": "KPIs",
        "kicker": "Targets and pacing",
        "tone": tone,
        "score_pct": score_pct or 0,
        "display_score": str(int(round(score_pct))) if score_pct is not None else "--",
        "state_label": _cluster_state_label(tone, has_data=score_pct is not None),
        "primary_metric": primary_metric,
        "detail": detail,
        "next_step": next_step,
    }


def _average_cluster_score(statuses):
    values = [_CLUSTER_STATUS_PCT.get(status) for status in statuses if _CLUSTER_STATUS_PCT.get(status) is not None]
    if not values:
        return None
    return round(sum(values) / len(values))


def _cluster_tone(score_pct):
    if score_pct is None:
        return "neutral"
    if score_pct >= 82:
        return "positive"
    if score_pct >= 68:
        return "good"
    if score_pct >= 48:
        return "caution"
    return "warning"


def _cluster_state_label(tone, *, has_data):
    if not has_data:
        return "No data"
    return {
        "positive": "Strong",
        "good": "Healthy",
        "caution": "Watch",
        "warning": "Fix",
        "neutral": "No data",
    }.get(tone, "Watch")


def _cluster_primary_metric(channels, channel_keys, metric_priority):
    for metric in metric_priority:
        for channel_key in channel_keys:
            channel = channels.get(channel_key) if isinstance(channels.get(channel_key), dict) else {}
            for card in channel.get("cards") or []:
                if (card.get("metric") or "") == metric:
                    return f"{metric}: {card.get('value') or 'N/A'}"

    for channel_key in channel_keys:
        channel = channels.get(channel_key) if isinstance(channels.get(channel_key), dict) else {}
        for card in channel.get("cards") or []:
            value = card.get("value") or "N/A"
            metric = card.get("metric") or "Signal"
            return f"{metric}: {value}"

    return "No live metric yet"


# ── Helpers ──

def _score_to_status(score):
    return {
        "excellent": "great",
        "good": "good",
        "average": "ok",
        "below_average": "warning",
        "poor": "bad",
        "no_data": "neutral",
    }.get(score, "neutral")


def _grade_label(grade):
    return {
        "A": "Excellent - your marketing is performing well across the board",
        "B": "Good - solid performance with some room to improve",
        "C": "Average - several areas need attention to get better results",
        "D": "Below Average - significant improvements needed to hit your goals",
        "F": "Needs Work - major changes required across multiple channels",
        "N/A": "Not enough data to grade yet",
    }.get(grade, "")


def _trend_text(mom_data):
    change = mom_data.get("change_pct")
    if change is None:
        return ""
    direction = "up" if change > 0 else "down"
    return f" ({direction} {abs(change):.0f}% from last month)"


def _client_friendly_title(title):
    """Strip jargon from suggestion titles."""
    replacements = {
        "CTR": "Click Rate",
        "CPC": "Cost Per Click",
        "CPA": "Cost Per Lead",
        "CPM": "Ad View Cost",
        "Meta Ad": "Facebook Ad",
        "Meta ": "Facebook ",
        "MoM": "Month-over-Month",
        "ROAS": "Return on Ad Spend",
        "GSC": "Google Search",
    }
    result = title
    for old, new in replacements.items():
        result = result.replace(old, new)
    return result


def _client_friendly_category(category):
    return {
        "paid_advertising": "Paid Ads",
        "seo": "SEO (Free Traffic)",
        "website": "Your Website",
        "strategy": "Strategy",
        "creative": "Ad Creative",
        "budget": "Budget",
    }.get(category, category.replace("_", " ").title())


def _plain_english_what(suggestion):
    """Create a 1-2 sentence plain-English summary of what needs to happen."""
    detail = suggestion["detail"]
    # Dejargon
    detail = detail.replace("CTR", "click rate")
    detail = detail.replace("CPC", "cost per click")
    detail = detail.replace("CPA", "cost per lead")
    detail = detail.replace("CPM", "cost per thousand views")
    detail = detail.replace("MoM", "compared to last month")
    detail = detail.replace("ROAS", "return on ad spend")
    detail = detail.replace("RSA", "responsive search ad")

    # Take first 2 sentences max
    sentences = detail.split(". ")
    if len(sentences) > 2:
        return ". ".join(sentences[:2]) + "."
    return detail
