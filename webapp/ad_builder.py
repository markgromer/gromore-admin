"""
Ad Builder - AI-Powered Ad Copy Generator

Generates complete, ready-to-paste ad packages for Google Ads and Facebook/Instagram
using the client's actual performance data, competitor intelligence, and brand context.

Each ad package includes all copy, image guidance, targeting suggestions,
and step-by-step implementation instructions (where to paste what).
"""
import json
import logging
import os

import requests as _requests

log = logging.getLogger(__name__)


def _normalize_strategy(value):
    return (value or "").strip().lower()


def _data_availability(context):
    seo = (context or {}).get("seo") or {}
    google_ads = (context or {}).get("google_ads") or {}
    meta_ads = (context or {}).get("meta_ads") or {}
    competitor = (context or {}).get("competitor_watch") or {}
    performance = (context or {}).get("performance") or {}

    def _has_list(path):
        cur = seo
        for part in path.split("."):
            if not isinstance(cur, dict):
                return False
            cur = cur.get(part)
        return isinstance(cur, list) and len(cur) > 0

    return {
        "has_gsc_top_queries": _has_list("top_queries"),
        "has_gsc_keyword_opportunities": _has_list("keyword_opportunities"),
        "has_gsc_top_pages": _has_list("top_pages"),
        "has_google_ads_campaigns": bool((google_ads.get("campaigns") or [])[:1]),
        "has_google_ads_search_terms": bool((google_ads.get("search_terms") or [])[:1]),
        "has_meta_campaigns": bool((meta_ads.get("campaigns") or [])[:1]),
        "has_meta_top_ads": bool((meta_ads.get("top_ads") or [])[:1]),
        "has_competitor_watch": bool(competitor),
        "has_kpis": bool((performance.get("kpis") or {}) if isinstance(performance, dict) else performance),
    }



def _load_knowledge(platform, fmt, industry=None):
    """Load ad knowledge context from the database (examples, best practices, master prompt)."""
    try:
        from flask import current_app
        db = getattr(current_app, "db", None)
        if not db:
            return ""
        from webapp.ad_knowledge import build_ad_knowledge_context
        return build_ad_knowledge_context(db, platform, fmt, industry=industry)
    except Exception as e:
        log.warning("Failed to load ad knowledge: %s", e)
        return ""


def generate_google_ads(analysis, brand, strategy=None):
    """Generate a Google ad package based on selected strategy.

    Strategy values: search | display | performance_max | video
    """
    api_key = _get_api_key(brand)
    if not api_key:
        return None

    model = (
        (brand or {}).get("openai_model_ads")
        or (brand or {}).get("openai_model")
        or "gpt-4o-mini"
    )

    context = _build_ad_context(analysis, brand)
    context["data_available"] = _data_availability(context)

    strategy_n = _normalize_strategy(strategy)
    if not strategy_n:
        strategy_n = "search"

    # Load knowledge base for the specific platform/format
    fmt_map = {"search": "search_rsa", "display": "display", "performance_max": "performance_max", "pmax": "performance_max", "video": "video"}
    knowledge = _load_knowledge("google", fmt_map.get(strategy_n, "search_rsa"), industry=(brand or {}).get("industry"))

    if strategy_n == "display":
        return _generate_google_display_ads(api_key, context, model, knowledge)
    if strategy_n in ("performance_max", "pmax", "performance max"):
        return _generate_google_pmax(api_key, context, model, knowledge)
    if strategy_n == "video":
        return _generate_google_video_ads(api_key, context, model, knowledge)
    return _generate_google_search_rsa(api_key, context, model, knowledge)


def _evidence_rules(prefix=""):
    p = prefix
    return (
        f"{p}Evidence rules:\n"
        f"{p}- You MUST include a 'data_used' array listing which context fields you used, chosen ONLY from this allowed set:\n"
        f"{p}  ['seo.top_queries','seo.keyword_opportunities','seo.top_pages','google_ads.campaigns','google_ads.search_terms','meta_ads.campaigns','meta_ads.top_ads','competitor_watch','performance.kpis']\n"
        f"{p}- If a field is not present or is empty in the context, you MUST NOT include it in data_used.\n"
        f"{p}- Do not invent competitor names, metrics, or claims. If unknown, keep copy generic but still specific to services + service area.\n"
    )


def _generate_google_search_rsa(api_key, context, model, knowledge=""):
    system = (
        "You are the AI ad copy engine inside GroMore, a platform for local service businesses. "
        "Generate a complete Google Search Responsive Search Ad (RSA) package ready to copy and paste.\n\n"
        "The business owner will paste these directly into Google Ads. Make every character count.\n\n"
        "Return ONLY valid JSON with this exact structure:\n"
        "{\n"
        '  "format": "google_search_rsa",\n'
        '  "strategy": "search",\n'
        '  "rationale": "2-4 sentences: why Search is the right play right now based on context (or state what is missing).",\n'
        '  "data_used": ["list of strings from the allowed set"],\n'
        '  "campaign_target": "Which campaign or ad group this ad should go in (based on the data, or General if unclear)",\n'
        '  "headlines": ["15 headlines, each UNDER 30 characters. Mix: service+city, benefits, offers, urgency, trust signals"],\n'
        '  "descriptions": ["4 descriptions, each UNDER 90 characters. Include CTA, differentiators, and social proof"],\n'
        '  "sitelinks": [{"title": "under 25 chars", "description": "under 35 chars", "url_hint": "/page-to-link-to"}],\n'
        '  "keywords_to_target": ["5-10 high-intent keywords based on the data"],\n'
        '  "negative_keywords": ["5-10 negative keywords to exclude based on the data"],\n'
        '  "implementation": ["Step-by-step paste instructions: exactly where in Google Ads to go and what to paste where"]\n'
        "}\n\n"
        "Rules:\n"
        "- Headlines MUST be under 30 characters. Count carefully. This is a hard limit.\n"
        "- Descriptions MUST be under 90 characters.\n"
        "- Use the business service area and services from context.\n"
        "- If seo.top_queries or seo.keyword_opportunities exists AND you use them, keywords_to_target MUST be aligned to those.\n"
        "- If you do NOT use seo.top_queries or seo.keyword_opportunities (Search Console not used), then keywords_to_target MUST be derived ONLY from google_ads.search_terms (in-account query/search-term data) if present.\n"
        "- In that no-SEO fallback case, your rationale MUST explicitly say: Search Console was not used, and keywords were chosen from in-account Google Ads search terms for the selected month.\n"
        "- If both SEO query data and google_ads.search_terms are missing/empty, set keywords_to_target to [] and state clearly in rationale that query data is unavailable.\n"
        "- negative_keywords should block low-intent and irrelevant traffic implied by context (jobs, free, DIY, wholesale, etc.).\n"
        "- Include at least 2 headlines with primary service + city/service area.\n"
        "- Include at least 1 headline with a number if present in context; otherwise omit numeric claim.\n"
        "- Include at least 1 headline with urgency if it is true for the business; otherwise avoid false urgency.\n"
        "- Sitelinks should point to logical pages (services, reviews, contact, areas served).\n"
        "- Implementation steps must be literal click-path instructions.\n"
        "- Use sentence case for headlines, not ALL CAPS.\n"
        "- No generic filler. Every headline and description should earn its spot.\n\n"
        + _evidence_rules()
    )

    return _call_ai(api_key, system, context, "google_ads_search", model, knowledge)


def _generate_google_display_ads(api_key, context, model, knowledge=""):
    system = (
        "You are the AI ad copy engine inside GroMore, a platform for local service businesses. "
        "Generate a complete Google Display ad package (Responsive Display Ad style) ready to copy and paste.\n\n"
        "Return ONLY valid JSON with this exact structure:\n"
        "{\n"
        '  "format": "google_display_rda",\n'
        '  "strategy": "display",\n'
        '  "rationale": "2-4 sentences: why Display makes sense (awareness or retargeting) based on context.",\n'
        '  "data_used": ["list of strings from the allowed set"],\n'
        '  "campaign_target": "Which campaign/ad group or new campaign recommendation",\n'
        '  "short_headlines": ["5 short headlines, under 30 chars"],\n'
        '  "long_headline": "1 long headline, under 90 chars",\n'
        '  "descriptions": ["5 descriptions, under 90 chars"],\n'
        '  "image_guidance": {"primary": "what image to use", "backup": "backup image", "sizes": ["1200x628","1080x1080"], "tips": ["2-4 tips"]},\n'
        '  "audience": {"type": "retargeting or prospecting", "signals": ["3-6 signals to use"], "exclude": ["what to exclude"]},\n'
        '  "implementation": ["Step-by-step: where to create a Display campaign/ad and what to paste where"]\n'
        "}\n\n"
        "Rules:\n"
        "- Copy must be consistent with the brand voice from context.\n"
        "- If seo.top_pages exists, recommend retargeting those page visitors for services with purchase intent.\n"
        "- If competitor_watch exists, position against gaps without naming competitors unless provided.\n"
        "- No invented guarantees or claims.\n\n"
        + _evidence_rules()
    )
    return _call_ai(api_key, system, context, "google_ads_display", model, knowledge)


def _generate_google_pmax(api_key, context, model, knowledge=""):
    system = (
        "You are the AI ad copy engine inside GroMore, a platform for local service businesses. "
        "Generate a Performance Max asset group package ready to paste into Google Ads.\n\n"
        "Return ONLY valid JSON with this exact structure:\n"
        "{\n"
        '  "format": "google_performance_max",\n'
        '  "strategy": "performance_max",\n'
        '  "rationale": "2-4 sentences: when PMax is appropriate and what you will optimize for.",\n'
        '  "data_used": ["list of strings from the allowed set"],\n'
        '  "campaign_target": "Existing PMax campaign to use or new campaign recommendation",\n'
        '  "asset_groups": [\n'
        '    {\n'
        '      "name": "Asset Group Name",\n'
        '      "final_url_hint": "/best-landing-page",\n'
        '      "headlines": ["5 headlines under 30 chars"],\n'
        '      "long_headlines": ["2 long headlines under 90 chars"],\n'
        '      "descriptions": ["4 descriptions under 90 chars"],\n'
        '      "callouts": ["6 callouts under 25 chars"],\n'
        '      "sitelinks": [{"title": "under 25 chars", "description": "under 35 chars", "url_hint": "/page"}],\n'
        '      "image_guidance": {"must_have": ["2-4 images to create"], "avoid": ["2-3 things"], "sizes": ["1200x628","1080x1080","1200x1200"], "tips": ["2-4 tips"]},\n'
        '      "audience_signals": ["3-6 audience signals to seed PMax"]\n'
        '    }\n'
        '  ],\n'
        '  "implementation": ["Step-by-step: where to create PMax, how to add asset group, what to paste where"]\n'
        "}\n\n"
        "Rules:\n"
        "- Do not promise exact performance.\n"
        "- If seo.keyword_opportunities exists, use them as audience_signals themes.\n"
        "- If google_ads.campaigns exists, align the asset group to what has worked (high CTR, low CPA) but do not invent numbers.\n\n"
        + _evidence_rules()
    )
    return _call_ai(api_key, system, context, "google_ads_pmax", model, knowledge)


def _generate_google_video_ads(api_key, context, model, knowledge=""):
    system = (
        "You are the AI ad copy engine inside GroMore, a platform for local service businesses. "
        "Generate a YouTube video ad package (scripts + copy + targeting guidance) ready to implement.\n\n"
        "Return ONLY valid JSON with this exact structure:\n"
        "{\n"
        '  "format": "google_video",\n'
        '  "strategy": "video",\n'
        '  "rationale": "2-4 sentences: why video is appropriate (awareness, education, remarketing).",\n'
        '  "data_used": ["list of strings from the allowed set"],\n'
        '  "campaign_target": "Existing video campaign to use or new campaign recommendation",\n'
        '  "scripts": {"bumper_6s": "6s script", "in_stream_15s": "15s script", "in_stream_30s": "30s script"},\n'
        '  "headlines": ["5 headlines under 30 chars"],\n'
        '  "descriptions": ["2 descriptions under 90 chars"],\n'
        '  "creative_guidance": {"shots": ["6-10 shot list"], "on_screen_text": ["3-6 lines"], "cta": "single CTA"},\n'
        '  "targeting": {"audiences": ["3-6"], "placements": ["optional"], "geo": "service area"},\n'
        '  "implementation": ["Step-by-step: YouTube/Google Ads video campaign creation + where to paste"]\n'
        "}\n\n"
        "Rules:\n"
        "- Scripts must be realistic for a local business: simple, direct, no hype.\n"
        "- If seo.top_queries exists, mirror the language people use in the hook.\n"
        "- No invented awards or review counts.\n\n"
        + _evidence_rules()
    )
    return _call_ai(api_key, system, context, "google_ads_video", model, knowledge)



def generate_facebook_ads(analysis, brand, strategy=None):
    """Generate a complete Facebook/Instagram ad package, tailored to the selected strategy/objective."""
    api_key = _get_api_key(brand)
    if not api_key:
        return None

    model = (
        (brand or {}).get("openai_model_ads")
        or (brand or {}).get("openai_model")
        or "gpt-4o-mini"
    )

    context = _build_ad_context(analysis, brand)
    context["data_available"] = _data_availability(context)

    # Strategy-specific prompt guidance
    strategy_map = {
        "awareness": "Maximize reach and brand recall. Use best practices for Facebook Awareness campaigns. Explain when and why to invest in awareness.",
        "engagement": "Drive likes, comments, shares, and social proof. Use best practices for Engagement campaigns.",
        "leads": "Drive signups, form fills, or inquiries. Use best practices for Lead Generation campaigns.",
        "sales": "Drive purchases, bookings, or direct sales. Use best practices for Sales/Conversion campaigns.",
    }
    strategy_text = strategy_map.get(_normalize_strategy(strategy), "If the strategy is unclear, default to Awareness best practices.")

    system = (
        f"You are the AI ad copy engine inside GroMore, a platform for local service businesses. "
        f"Generate a complete Facebook/Instagram ad package ready to copy and paste, tailored for the following objective: {strategy or 'Awareness'}\n\n"
        f"Objective intent: {strategy_text}\n\n"
        "Return ONLY valid JSON with this exact structure:\n"
        "{\n"
        '  "format": "meta_ads",\n'
        '  "strategy": "awareness|engagement|leads|sales",\n'
        '  "rationale": "2-4 sentences: why this objective fits the current context. If data is missing, say what you would check.",\n'
        '  "data_used": ["list of strings from the allowed set"],\n'
        '  "campaign_target": "Which campaign this ad should go in (based on the data, or new campaign recommendation)",\n'
        '  "ad_variations": [\n'
        "    {\n"
        '      "name": "Variation A - [brief label]",\n'
        '      "primary_text": "The main ad copy (appears above the image). 2-4 sentences. Hook + value + CTA.",\n'
        '      "headline": "Bold headline below the image. Under 40 chars.",\n'
        '      "description": "One line below headline. Under 30 chars.",\n'
        '      "angle": "Brief note on what angle this variation takes"\n'
        "    }\n"
        "  ],\n"
        '  "cta_button": "LEARN_MORE or GET_QUOTE or CALL_NOW or BOOK_NOW or SIGN_UP",\n'
        '  "image_guidance": {\n'
        '    "primary": "Specific description of the ideal image (e.g., Before/after of a completed job, team photo in uniform, etc.)",\n'
        '    "backup": "Alternative image option",\n'
        '    "specs": "1080x1080 for feed, 1080x1920 for stories",\n'
        '    "tips": ["2-3 specific tips for the image based on what works in their industry"]\n'
        "  },\n"
        '  "audience_suggestions": {\n'
        '    "location": "Radius or zip codes based on their service area",\n'
        '    "age_range": "Recommended age range",\n'
        '    "interests": ["3-5 relevant interests to target"],\n'
        '    "custom_audience": "Recommendation for custom/lookalike audiences"\n'
        "  },\n"
        '  "implementation": ["Step-by-step: where to go in Ads Manager, what to paste where, how to set up the ad"]\n'
        "}\n\n"
        "Rules:\n"
        "- Generate exactly 3 ad variations with different angles (social proof, urgency, value/offer)\n"
        "- Primary text should be conversational, not corporate. Write like a real person.\n"
        "- Use the client's actual city, services, offers, and competitive advantages\n"
        "- Reference real data: if their best campaign has high CTR, build on what's working\n"
        "- If competitor data exists, exploit gaps (services competitors don't offer, areas they don't serve)\n"
        "- Image guidance should be specific to their industry and what converts best\n"
        "- Implementation steps should be literal: 'Go to Ads Manager > Campaign Name > Ad Set > Create Ad > paste this in Primary Text'\n"
        "- No hashtags unless they're industry-standard\n"
        "- No emojis in headlines. Emojis OK in primary text if natural.\n"
        "- Never use 'we' - the business owner is running this, use 'I/my' or their business name\n\n"
        + _evidence_rules()
    )

    knowledge = _load_knowledge("meta", "feed", industry=(brand or {}).get("industry"))
    return _call_ai(api_key, system, context, "facebook_ads", model, knowledge)


def _get_api_key(brand=None):
    """Get OpenAI API key from app config or environment."""
    brand_key = ((brand or {}).get("openai_api_key") or "").strip()
    if brand_key:
        return brand_key
    try:
        from flask import current_app
        return (current_app.config.get("OPENAI_API_KEY", "") or "").strip()
    except RuntimeError:
        return os.environ.get("OPENAI_API_KEY", "").strip()


def _build_ad_context(analysis, brand):
    """Build the context payload for ad generation."""
    from webapp.ai_assistant import _summarize_analysis_for_ai
    summary = _summarize_analysis_for_ai(analysis)

    client = summary.get("client", {})
    # Build structured competitor profiles if available
    competitor_profiles = client.get("competitor_profiles") or []
    competitor_info = []
    for cp in competitor_profiles:
        parts = [cp.get("name", "")]
        if cp.get("website"):
            parts.append(f"website: {cp['website']}")
        if cp.get("facebook_url"):
            parts.append(f"facebook: {cp['facebook_url']}")
        if cp.get("google_maps_url"):
            parts.append(f"GMB: {cp['google_maps_url']}")
        if cp.get("notes"):
            parts.append(f"notes: {cp['notes']}")
        competitor_info.append(" | ".join(parts))

    return {
        "business": {
            "name": brand.get("display_name") or client.get("name"),
            "industry": client.get("industry"),
            "service_area": client.get("service_area"),
            "services": client.get("primary_services") or [],
            "target_audience": client.get("target_audience"),
            "active_offers": client.get("active_offers"),
            "brand_voice": client.get("brand_voice"),
            "competitors": client.get("competitors"),
            "competitor_profiles": competitor_info if competitor_info else None,
        },
        "performance": {
            "kpis": summary.get("kpis", {}),
            "highlights": summary.get("highlights", []),
            "concerns": summary.get("concerns", []),
        },
        "google_ads": summary.get("google_ads_detail", {}),
        "meta_ads": summary.get("meta_detail", {}),
        "seo": summary.get("seo_detail", {}),
        "competitor_watch": summary.get("competitor_watch", {}),
    }


def _call_ai(api_key, system, context, label, model, knowledge=""):
    """Make the OpenAI API call and parse the response."""
    if knowledge:
        system = knowledge + "\n\n" + system

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    try:
        resp = _requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json={
                "model": (model or "gpt-4o-mini"),
                "temperature": 0.4,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": json.dumps(context)},
                ],
                "response_format": {"type": "json_object"},
            },
            timeout=60,
        )

        if resp.status_code != 200:
            log.warning("Ad builder AI failed (%s) for %s: %s", resp.status_code, label, resp.text[:200])
            return None

        data = resp.json()
        content = (
            (data.get("choices") or [{}])[0]
            .get("message", {})
            .get("content", "")
        )

        return json.loads(content)

    except Exception as e:
        log.warning("Ad builder error (%s): %s", label, e)
        return None
