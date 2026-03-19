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


def generate_google_ads(analysis, brand):
    """Generate a complete Google Ads package: responsive search ad copy.

    Returns dict with:
        - headlines: list of 15 headlines (max 30 chars each)
        - descriptions: list of 4 descriptions (max 90 chars each)
        - sitelinks: list of 4 sitelink suggestions
        - keywords_to_target: list of recommended keywords
        - negative_keywords: list of negatives to add
        - campaign_target: which campaign/ad group to apply to
        - implementation: step-by-step paste instructions
    """
    api_key = _get_api_key()
    if not api_key:
        return None

    context = _build_ad_context(analysis, brand)

    system = (
        "You are the AI ad copy engine inside GroMore, a platform for local service businesses. "
        "Generate a complete Google Responsive Search Ad package ready to copy and paste.\n\n"
        "The business owner will paste these directly into Google Ads. Make every character count.\n\n"
        "Return ONLY valid JSON with this exact structure:\n"
        "{\n"
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
        "- Use the client's actual city/service area, services, and competitive advantages\n"
        "- Reference real competitor weaknesses or gaps from the data if available\n"
        "- Include at least 2 headlines with the primary service + city\n"
        "- Include at least 1 headline with a number (years in business, reviews, etc.)\n"
        "- Include at least 1 headline with urgency (Same-Day, 24/7, Today, etc.)\n"
        "- Sitelinks should point to logical pages (services, reviews, contact, areas served)\n"
        "- Implementation steps should tell them exactly: Campaign > Ad Group > Ads > New RSA > paste headline 1 here, etc.\n"
        "- Use sentence case for headlines, not ALL CAPS\n"
        "- No generic filler. Every headline and description should earn its spot."
    )

    return _call_ai(api_key, system, context, "google_ads")


def generate_facebook_ads(analysis, brand):
    """Generate a complete Facebook/Instagram ad package.

    Returns dict with:
        - primary_text: the main ad copy (2-3 variations)
        - headline: the headline below the image (2-3 variations)
        - description: the description text (2-3 variations)
        - cta_button: recommended CTA button type
        - image_guidance: specific instructions for what image to use
        - audience_suggestions: targeting recommendations
        - campaign_target: which campaign to apply to
        - implementation: step-by-step paste instructions
    """
    api_key = _get_api_key()
    if not api_key:
        return None

    context = _build_ad_context(analysis, brand)

    system = (
        "You are the AI ad copy engine inside GroMore, a platform for local service businesses. "
        "Generate a complete Facebook/Instagram ad package ready to copy and paste.\n\n"
        "Return ONLY valid JSON with this exact structure:\n"
        "{\n"
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
        "- Never use 'we' - the business owner is running this, use 'I/my' or their business name"
    )

    return _call_ai(api_key, system, context, "facebook_ads")


def _get_api_key():
    """Get OpenAI API key from app config or environment."""
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


def _call_ai(api_key, system, context, label):
    """Make the OpenAI API call and parse the response."""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    try:
        resp = _requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json={
                "model": "gpt-4o-mini",
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
