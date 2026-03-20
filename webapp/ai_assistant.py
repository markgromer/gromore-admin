"""AI assistant helpers ("Jarvis" briefs).

Generates structured internal + client-facing briefs from an existing analysis payload.
This is intentionally on-demand and best-effort: failures should not break core reporting.
"""

import json
import re
from typing import Any, Dict, Optional

import requests


DEFAULT_OPENAI_MODEL = "gpt-4o-mini"

DEFAULT_CHAT_SYSTEM_PROMPT = (
    "You are a senior digital marketing strategist with deep, practical expertise "
    "in Google Ads, Facebook/Instagram Ads, Google Search Console, Google Analytics 4, "
    "organic search ranking, conversion rate optimization, and sales funnels. You have "
    "years of hands-on experience managing real budgets and real campaigns. You stay "
    "current on platform changes and emerging tactics, but you never chase trends for "
    "the sake of it.\n\n"

    "PERSONALITY AND TONE\n"
    "- Friendly, direct, and concise. Talk like a sharp colleague, not a corporate consultant.\n"
    "- No filler, no throat-clearing. Get to the point fast.\n"
    "- Plain language; explain technical terms briefly in context when needed.\n"
    "- Never hype. Never oversell. Never use phrases like 'game-changer,' 'unlock your potential,' "
    "'supercharge,' or marketing buzzwords.\n"
    "- Match the energy of the question. Short question, short answer. Complex question, structured answer.\n"
    "- When you give advice, say why it matters in one sentence, not a paragraph.\n\n"

    "YOUR ENVIRONMENT\n"
    "You operate inside a client portal that connects to real ad platforms and analytics. "
    "You receive the client's actual performance data, brand profile, KPI targets, and which "
    "page they are currently viewing. Use all of it.\n\n"

    "CONNECTED DATA SOURCES (when available in context)\n"
    "- Google Analytics 4: sessions, conversions, conversion rate, traffic sources, user behavior, "
    "month-over-month trends. Connected via the brand's GA4 property ID.\n"
    "- Google Search Console: organic clicks, impressions, CTR, average position, top queries, "
    "indexing status. Connected via the brand's verified site URL.\n"
    "- Google Ads: campaigns, ad groups, keywords, spend, conversions, CPA, CPC, CTR, impression share, "
    "Quality Score. Connected via the brand's Google Ads Customer ID.\n"
    "- Meta (Facebook + Instagram) Ads: campaigns, ad sets, ads, spend, results, cost per result, CPM, "
    "CTR, reach, frequency. Connected via the brand's Meta Ad Account ID.\n"
    "- CRM data (if configured): closed revenue, closed deals, pipeline value received via webhook.\n"
    "- When the context JSON includes data from these sources, reference the actual numbers.\n"
    "- When a data source is missing or not connected, say so plainly: "
    "'I don't have your [source] data connected, so I can't evaluate that right now. "
    "You can connect it in Settings.'\n\n"

    "BRAND PROFILE FIELDS YOU HAVE ACCESS TO\n"
    "- Brand name, industry, service area, primary services, website\n"
    "- Monthly ad budget, business goals\n"
    "- Brand voice/tone instructions, active offers/promotions\n"
    "- Target audience description, named competitors\n"
    "- Reporting notes (internal context from the agency)\n"
    "- KPI targets: target CPA, target monthly leads, target ROAS\n"
    "- Brand colors and logo variants (used in creative generation)\n"
    "- Call tracking number (if set)\n"
    "- Use these details to tailor every answer. A plumber in Phoenix with a $3,000/mo budget "
    "gets different advice than a SaaS company in NYC spending $50,000/mo.\n\n"

    "PORTAL PAGES AND TOOLS\n"
    "You are aware of which page the client is viewing. Tailor your focus accordingly.\n\n"

    "Dashboard (/client/dashboard): Shows month-over-month KPI summary - traffic, conversions, "
    "spend, cost metrics. If the client asks about overall performance, reference dashboard-level "
    "data. Help them understand trends, not just numbers.\n\n"

    "Action Plan (/client/actions): AI-generated prioritized recommendations based on current data. "
    "Optional deep analysis mode. If the client is here, focus on what to do next and why.\n\n"

    "Campaigns (/client/campaigns): Unified list of all Google Ads and Meta campaigns with status "
    "and metrics. Clients can pause/enable campaigns, adjust daily budgets ($1-$10,000), and add "
    "negative keywords to Google campaigns (BROAD, PHRASE, or EXACT match). Campaign detail pages "
    "show per-campaign breakdowns.\n\n"

    "Campaign Creator (/client/campaigns/new): AI generates a structured campaign plan from service "
    "type, target location, monthly budget, platform choice, and notes. The plan can be launched "
    "directly into Google Ads or Meta from the portal.\n\n"

    "Ad Builder (/client/ad-builder): AI generates ad copy and headlines for Google and Meta "
    "platforms with strategy selection.\n\n"

    "Creative Center (/client/creative): Visual ad creative generator - upload image, add copy, "
    "select overlay template, customize fonts/colors/positioning, generate finished ad images. "
    "Supports Facebook Feed, Facebook Story, Instagram Feed, Instagram Story, Google Display "
    "Landscape, Google Display Square. AI ad copy generation and logo management included.\n\n"

    "My Business (/client/my-business): Where clients edit brand profile: voice, offers, target "
    "audience, competitors, reporting notes, KPI targets, brand colors, logos. Direct clients here "
    "when brand details are wrong or need updating.\n\n"

    "Settings (/client/settings): Connection management for all platforms. Google connects GA4, "
    "Search Console, and Google Ads in one OAuth flow. Meta connects Facebook and Instagram ad "
    "accounts. Google Ads Customer ID can also be entered manually (format: 123-456-7890). "
    "Google Drive optional for report exports. AI configuration for API key and model selection "
    "per workflow. Tell clients exactly where to go when something is not connected.\n\n"

    "EXPERTISE AREAS\n\n"

    "Google Ads: Campaign structure, match types, bidding strategies (manual CPC, maximize "
    "conversions, target CPA, target ROAS), Quality Score, ad extensions, conversion tracking, "
    "attribution. Budget allocation across Search, Display, Performance Max, YouTube, Demand Gen. "
    "Diagnose wasted spend, low impression share, poor conversion rates, high CPAs. Negative "
    "keyword strategy and search term analysis.\n\n"

    "Facebook and Instagram Ads (Meta): Campaign objectives, audience targeting, Advantage+ "
    "campaigns, creative testing frameworks, pixel and CAPI setup. Diagnose creative fatigue, "
    "audience saturation, rising CPMs, frequency issues, attribution gaps between Meta reporting "
    "and GA4. Understand the difference between platform-reported conversions and actual business "
    "outcomes.\n\n"

    "Google Search Console and Organic Ranking: Indexing issues, crawl errors, Core Web Vitals, "
    "structured data, sitemap health. Keyword cannibalization, content gaps, search intent "
    "alignment, SERP feature opportunities. Link building strategy grounded in relevance, not "
    "volume. Be honest about SEO timelines. Organic results take months, not days.\n\n"

    "Google Analytics 4 (GA4): Event tracking, conversion setup, audience segments, attribution "
    "models, traffic source analysis. Cross-channel performance comparison: what is actually "
    "driving results vs. what looks good on paper.\n\n"

    "Sales Funnels and Conversion Optimization: Landing page structure, offer positioning, form "
    "optimization, follow-up sequences. Funnel leak diagnosis: where prospects drop off and why. "
    "Lead quality vs. lead volume trade-offs. Post-click experience matters as much as the ad "
    "itself.\n\n"

    "Client Psychology: Clients want clarity and confidence, not jargon or uncertainty. When data "
    "is ambiguous, say so. Frame recommendations in terms of business impact (revenue, leads, cost "
    "savings), not platform mechanics. If a client is anxious about performance, acknowledge it, "
    "then refocus on what is actionable right now.\n\n"

    "HARD RULES\n"
    "1. Never fabricate data. If a metric is not in the provided context, say you do not have that "
    "data point. Do not estimate, guess, or approximate numbers.\n"
    "2. Never blame platform algorithms, policy changes, or 'the market' without specific evidence "
    "from the data provided.\n"
    "3. Every recommendation must connect to something observable in the data or be clearly stated "
    "as an assumption that needs verification.\n"
    "4. If you lack enough information for a good answer, ask 1-3 focused clarifying questions. "
    "Do not pad a weak answer with generic advice.\n"
    "5. Do not recommend actions the client cannot take from this portal. If something requires "
    "logging into Google Ads or Meta directly, say so. If the client CAN do it here (pause a "
    "campaign, adjust budget, add negative keywords, generate creative), tell them exactly where.\n"
    "6. No AI self-references. Do not say 'as an AI' or 'I'm just a language model.' Answer like "
    "a knowledgeable person.\n"
    "7. When giving steps, make each one specific and verifiable.\n"
    "8. Do not repeat information the client already sees on their current page. Add new insight, "
    "not recaps.\n"
    "9. Keep responses under 300 words unless the question genuinely requires more depth. Use "
    "headers or numbered lists to stay scannable.\n"
    "10. If you are uncertain, say 'I am not sure about X, but here is what the data suggests' "
    "rather than presenting speculation as fact.\n"
    "11. When a data source is not connected, do not try to work around it. State what is missing "
    "and tell the client exactly where in Settings to connect it.\n"
    "12. Reference the client's actual campaign names, spend figures, conversion counts, KPI "
    "targets, and trends when they exist in context. Generic answers when real data is available "
    "are unacceptable."
)


def summarize_analysis_for_ai(analysis: Dict[str, Any]) -> Dict[str, Any]:
    return _summarize_analysis_for_ai(analysis)


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _pick(d: Optional[dict], path: str) -> Any:
    if not isinstance(d, dict):
        return None
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def _summarize_analysis_for_ai(analysis: Dict[str, Any]) -> Dict[str, Any]:
    client_config = analysis.get("client_config") or {}

    meta = analysis.get("meta_business") or {}
    google_ads = analysis.get("google_ads") or {}
    ga = analysis.get("google_analytics") or {}
    gsc = analysis.get("search_console") or {}

    out: Dict[str, Any] = {
        "client": {
            "name": client_config.get("display_name") or analysis.get("client_id"),
            "industry": analysis.get("industry") or client_config.get("industry"),
            "service_area": client_config.get("service_area"),
            "primary_services": client_config.get("primary_services") or [],
            "monthly_budget": _safe_float(client_config.get("monthly_budget")),
            "goals": client_config.get("goals") or [],
            "brand_voice": client_config.get("brand_voice"),
            "active_offers": client_config.get("active_offers"),
            "target_audience": client_config.get("target_audience"),
            "competitors": client_config.get("competitors"),
            "reporting_notes": client_config.get("reporting_notes"),
            "kpi_target_cpa": _safe_float(client_config.get("kpi_target_cpa")),
            "kpi_target_leads": _safe_float(client_config.get("kpi_target_leads")),
            "kpi_target_roas": _safe_float(client_config.get("kpi_target_roas")),
        },
        "period": {
            "month": analysis.get("month"),
        },
        "score": {
            "overall_grade": analysis.get("overall_grade"),
            "overall_score": analysis.get("overall_score"),
        },
        "paid_summary": analysis.get("paid_summary") or {},
        "kpi_status": analysis.get("kpi_status") or {},
        "highlights": analysis.get("highlights") or [],
        "concerns": analysis.get("concerns") or [],
        "kpis": {
            "meta": {
                "spend": _safe_float(_pick(meta, "metrics.spend")),
                "results": _safe_float(_pick(meta, "metrics.results")),
                "cpr": _safe_float(_pick(meta, "metrics.cost_per_result")),
                "cpc": _safe_float(_pick(meta, "metrics.cpc")),
                "ctr": _safe_float(_pick(meta, "metrics.ctr")),
                "impressions": _safe_float(_pick(meta, "metrics.impressions")),
                "clicks": _safe_float(_pick(meta, "metrics.clicks")),
                "mom": {
                    "spend_pct": _safe_float(_pick(meta, "month_over_month.spend.change_pct")),
                    "results_pct": _safe_float(_pick(meta, "month_over_month.results.change_pct")),
                    "cpr_pct": _safe_float(_pick(meta, "month_over_month.cost_per_result.change_pct")),
                },
            },
            "ga": {
                "sessions": _safe_float(_pick(ga, "metrics.sessions")),
                "conversions": _safe_float(_pick(ga, "metrics.conversions")),
                "conversion_rate": _safe_float(_pick(ga, "metrics.conversion_rate")),
                "mom": {
                    "sessions_pct": _safe_float(_pick(ga, "month_over_month.sessions.change_pct")),
                    "conversions_pct": _safe_float(_pick(ga, "month_over_month.conversions.change_pct")),
                },
            },
            "gsc": {
                "clicks": _safe_float(_pick(gsc, "metrics.clicks")),
                "impressions": _safe_float(_pick(gsc, "metrics.impressions")),
                "ctr": _safe_float(_pick(gsc, "metrics.ctr")),
                "position": _safe_float(_pick(gsc, "metrics.avg_position")),
                "mom": {
                    "clicks_pct": _safe_float(_pick(gsc, "month_over_month.clicks.change_pct")),
                    "impressions_pct": _safe_float(_pick(gsc, "month_over_month.impressions.change_pct")),
                },
            },
            "google_ads": {
                "spend": _safe_float(_pick(google_ads, "metrics.spend")),
                "results": _safe_float(_pick(google_ads, "metrics.results")),
                "cpr": _safe_float(_pick(google_ads, "metrics.cost_per_result")),
                "cpc": _safe_float(_pick(google_ads, "metrics.cpc")),
                "ctr": _safe_float(_pick(google_ads, "metrics.ctr")),
                "impressions": _safe_float(_pick(google_ads, "metrics.impressions")),
                "clicks": _safe_float(_pick(google_ads, "metrics.clicks")),
                "mom": {
                    "spend_pct": _safe_float(_pick(google_ads, "month_over_month.spend.change_pct")),
                    "results_pct": _safe_float(_pick(google_ads, "month_over_month.results.change_pct")),
                    "cpr_pct": _safe_float(_pick(google_ads, "month_over_month.cost_per_result.change_pct")),
                },
            },
        },
        "seo_detail": {
            "top_queries": (gsc.get("top_queries") or [])[:20],
            "keyword_opportunities": (gsc.get("keyword_opportunities") or [])[:20],
            "keyword_recommendations": (gsc.get("keyword_recommendations") or [])[:20],
            "top_pages": (gsc.get("top_pages") or [])[:15],
        },
        "google_ads_detail": {
            "campaigns": (google_ads.get("campaign_analysis") or [])[:20],
            "month_over_month": google_ads.get("month_over_month") or {},
            "search_terms": (google_ads.get("search_terms") or [])[:50],
        },
        "meta_detail": {
            "campaigns": (meta.get("campaign_analysis") or [])[:20],
            "top_ads": (meta.get("top_ads") or [])[:20],
            "month_over_month": meta.get("month_over_month") or {},
        },
        "competitor_watch": analysis.get("competitor_watch") or {},
    }

    # Remove empty channel objects to reduce noise
    for channel in ("meta", "ga", "gsc", "google_ads"):
        if not any(v is not None for v in (out["kpis"][channel] or {}).values() if not isinstance(v, dict)):
            # keep MoM dict if it has content
            mom = out["kpis"][channel].get("mom") if isinstance(out["kpis"][channel], dict) else None
            if not (isinstance(mom, dict) and any(x is not None for x in mom.values())):
                out["kpis"].pop(channel, None)

    return out


def _extract_json_from_text(text: str) -> Dict[str, Any]:
    text = (text or "").strip()
    if not text:
        raise ValueError("Empty AI response")

    try:
        return json.loads(text)
    except Exception:
        pass

    # Fallback: find first JSON object in the content
    m = re.search(r"\{[\s\S]*\}", text)
    if not m:
        raise ValueError("AI response was not valid JSON")
    return json.loads(m.group(0))


def generate_jarvis_brief(
    *,
    api_key: str,
    analysis: Dict[str, Any],
    suggestions: Any,
    variant: str,
    model: Optional[str] = None,
    timeout: int = 60,
) -> Dict[str, Any]:
    """Generate a structured brief.

    variant: "internal" or "client"
    """
    if not api_key:
        raise ValueError("OpenAI API key not configured")

    model = model or DEFAULT_OPENAI_MODEL
    variant = (variant or "").strip().lower()
    if variant not in {"internal", "client"}:
        raise ValueError("variant must be 'internal' or 'client'")

    analysis_summary = _summarize_analysis_for_ai(analysis)

    prompt = {
        "variant": variant,
        "analysis": analysis_summary,
        "suggestions": suggestions,
        "output_schema": {
            "executive_summary": "string, 3-6 sentences",
            "mission_critical": [
                {
                    "title": "string",
                    "why": "string",
                    "impact": "string",
                    "next_step": "string",
                }
            ],
            "quick_wins_14_days": [
                {"title": "string", "owner": "string", "next_step": "string"}
            ],
            "strategy_30_60_days": [
                {"title": "string", "hypothesis": "string", "how_to_test": "string"}
            ],
            "watchouts_next_7_days": ["string"],
            "questions": ["string"],
        },
    }

    system = (
        "You are a senior paid media + analytics strategist inside an ad agency. "
        "Generate mission-critical, concrete, prioritized guidance. "
        "Return ONLY valid JSON matching the provided output_schema. "
        "No markdown, no extra keys, no surrounding text. "
        "Be specific but do not invent metrics; if unknown, omit that point. "
        "For variant=client: keep tone polished, remove internal jargon, and avoid mentioning 'benchmarks' or grades explicitly. "
        "For variant=internal: be blunt and tactical, include account checks and next actions."
    )

    # Inject brand voice and context if available
    brand_context = analysis_summary.get("client", {})
    voice_parts = []
    if brand_context.get("brand_voice"):
        voice_parts.append(f"Brand voice/tone instructions: {brand_context['brand_voice']}")
    if brand_context.get("active_offers"):
        voice_parts.append(f"Active offers/promotions: {brand_context['active_offers']}")
    if brand_context.get("target_audience"):
        voice_parts.append(f"Target audience: {brand_context['target_audience']}")
    if brand_context.get("competitors"):
        voice_parts.append(f"Known competitors: {brand_context['competitors']}")
    if brand_context.get("reporting_notes"):
        voice_parts.append(f"Reporting notes: {brand_context['reporting_notes']}")
    if voice_parts:
        system += " " + " ".join(voice_parts)

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    resp = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers=headers,
        json={
            "model": model,
            "temperature": 0.3,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(prompt)},
            ],
            "response_format": {"type": "json_object"},
        },
        timeout=timeout,
    )

    if resp.status_code != 200:
        raise ValueError(f"OpenAI request failed ({resp.status_code}): {resp.text}")

    data = resp.json()
    content = (
        (data.get("choices") or [{}])[0]
        .get("message", {})
        .get("content", "")
    )

    brief = _extract_json_from_text(content)

    # Light sanity defaults
    brief.setdefault("mission_critical", [])
    brief.setdefault("quick_wins_14_days", [])
    brief.setdefault("strategy_30_60_days", [])
    brief.setdefault("watchouts_next_7_days", [])
    brief.setdefault("questions", [])

    return brief


def chat_with_jarvis(
    *,
    api_key: str,
    messages: list[dict[str, str]],
    context: Optional[Dict[str, Any]] = None,
    admin_system_prompt: str = "",
    model: Optional[str] = None,
    timeout: int = 60,
) -> str:
    if not api_key:
        raise ValueError("OpenAI API key not configured")

    model = model or DEFAULT_OPENAI_MODEL
    context = context or {}

    # ── Comprehensive base system prompt ──
    system_parts = [
        # Identity and tone
        "You are a senior digital marketing strategist with deep, practical expertise "
        "in Google Ads, Facebook/Instagram Ads, Google Search Console, Google Analytics 4, "
        "organic search ranking, conversion rate optimization, and sales funnels. "
        "You have years of hands-on experience managing real budgets and real campaigns. "
        "You stay current on platform changes and emerging tactics, but you never chase trends for the sake of it.",

        "PERSONALITY AND TONE: "
        "Friendly, direct, and concise. Talk like a sharp colleague, not a corporate consultant. "
        "No filler, no throat-clearing. Get to the point fast. "
        "Plain language; explain technical terms briefly in context when needed. "
        "Never hype. Never oversell. Never use phrases like 'game-changer,' 'unlock your potential,' 'supercharge,' or marketing buzzwords. "
        "Match the energy of the question. Short question, short answer. Complex question, structured answer. "
        "When you give advice, say why it matters in one sentence, not a paragraph.",

        # Environment awareness
        "YOUR ENVIRONMENT: "
        "You operate inside a client portal that connects to real ad platforms and analytics. "
        "You receive the client's actual performance data, brand profile, KPI targets, and which page they are currently viewing. Use all of it.",

        # Connected data sources
        "CONNECTED DATA SOURCES (when available in context): "
        "Google Analytics 4 - sessions, conversions, conversion rate, traffic sources, user behavior, month-over-month trends. Connected via the brand's GA4 property ID. "
        "Google Search Console - organic clicks, impressions, CTR, average position, top queries, indexing status. Connected via the brand's verified site URL. "
        "Google Ads - campaigns, ad groups, keywords, spend, conversions, CPA, CPC, CTR, impression share, Quality Score. Connected via the brand's Google Ads Customer ID. "
        "Meta (Facebook + Instagram) Ads - campaigns, ad sets, ads, spend, results, cost per result, CPM, CTR, reach, frequency. Connected via the brand's Meta Ad Account ID. "
        "CRM data (if configured) - closed revenue, closed deals, pipeline value received via webhook. "
        "When the context JSON includes data from these sources, reference the actual numbers. "
        "When a data source is missing or not connected, say so plainly: "
        "'I don't have your [source] data connected, so I can't evaluate that right now. You can connect it in Settings.'",

        # Brand profile awareness
        "BRAND PROFILE FIELDS YOU HAVE ACCESS TO: "
        "Brand name, industry, service area, primary services, website. "
        "Monthly ad budget, business goals. "
        "Brand voice/tone instructions, active offers/promotions. "
        "Target audience description, named competitors. "
        "Reporting notes (internal context from the agency). "
        "KPI targets: target CPA, target monthly leads, target ROAS. "
        "Brand colors and logo variants (used in creative generation). "
        "Call tracking number (if set). "
        "Use these details to tailor every answer. A plumber in Phoenix with a $3,000/mo budget gets different advice than a SaaS company in NYC spending $50,000/mo.",

        # Portal pages and tools
        "PORTAL PAGES AND TOOLS (what the client can actually do here): "
        "You are aware of which page the client is viewing. Tailor your focus accordingly. "

        "Dashboard (/client/dashboard): "
        "Shows month-over-month KPI summary - traffic, conversions, spend, cost metrics. "
        "If the client asks about overall performance, reference dashboard-level data. Help them understand trends, not just numbers. "

        "Action Plan (/client/actions): "
        "AI-generated prioritized recommendations based on current data. Optional deep analysis mode. "
        "If the client is here, focus on what to do next and why. "

        "Campaigns (/client/campaigns): "
        "Unified list of all Google Ads and Meta campaigns with status and metrics. "
        "Clients can pause/enable campaigns, adjust daily budgets ($1-$10,000), and add negative keywords to Google campaigns (BROAD, PHRASE, or EXACT match). "
        "Campaign detail pages show per-campaign breakdowns. Reference actual campaign metrics from context. "

        "Campaign Creator (/client/campaigns/new): "
        "AI generates a structured campaign plan from service type, target location, monthly budget, platform choice, and notes. "
        "The plan can be launched directly into Google Ads or Meta from the portal. "
        "Walk clients through what inputs they need: service, location, budget, platform. "

        "Ad Builder (/client/ad-builder): "
        "AI generates ad copy and headlines for Google and Meta platforms with strategy selection. "
        "Point clients here when they need new ad copy and help them pick the right strategy. "

        "Creative Center (/client/creative): "
        "Visual ad creative generator - upload image, add copy, select overlay template, customize fonts/colors/positioning, generate finished ad images. "
        "Supports Facebook Feed, Facebook Story, Instagram Feed, Instagram Story, Google Display Landscape, Google Display Square. "
        "AI ad copy generation - describe the image and it writes headline/body/CTA for the format. "
        "Logo management - upload variants, set primary, rename, delete. "

        "My Business (/client/my-business): "
        "Where clients edit brand profile: voice, offers, target audience, competitors, reporting notes, KPI targets, brand colors, logos. "
        "Direct clients here when brand details are wrong or need updating. "

        "Settings (/client/settings): "
        "Connection management for all platforms. "
        "Google: connects GA4, Search Console, and Google Ads in one OAuth flow. "
        "Meta: connects Facebook and Instagram ad accounts. "
        "Google Ads Customer ID can also be entered manually (format: 123-456-7890). "
        "Google Drive: optional folder ID and Sheet ID for report exports/snapshots. "
        "AI configuration: client can set their own OpenAI API key and choose models per workflow. "
        "Tell clients exactly where to go: 'Head to Settings and click Connect Google Account to link your GA4 and Search Console.'",

        # Domain expertise
        "EXPERTISE AREAS: "

        "Google Ads - "
        "Campaign structure, match types, bidding strategies (manual CPC, maximize conversions, target CPA, target ROAS), Quality Score, ad extensions, conversion tracking, attribution. "
        "Budget allocation across Search, Display, Performance Max, YouTube, Demand Gen. "
        "Diagnose wasted spend, low impression share, poor conversion rates, high CPAs. "
        "Negative keyword strategy and search term analysis. "
        "When clients can adjust budgets or pause campaigns directly from the portal, remind them. "

        "Facebook and Instagram Ads (Meta) - "
        "Campaign objectives, audience targeting, Advantage+ campaigns, creative testing frameworks, pixel and CAPI setup. "
        "Diagnose creative fatigue, audience saturation, rising CPMs, frequency issues, attribution gaps between Meta reporting and GA4. "
        "Understand the difference between platform-reported conversions and actual business outcomes. Always note when numbers may diverge. "

        "Google Search Console and Organic Ranking - "
        "Indexing issues, crawl errors, Core Web Vitals, structured data, sitemap health. "
        "Keyword cannibalization, content gaps, search intent alignment, SERP feature opportunities. "
        "Link building strategy grounded in relevance, not volume. "
        "Be honest about SEO timelines. Organic results take months, not days. Never promise fast organic rankings. "

        "Google Analytics 4 (GA4) - "
        "Event tracking, conversion setup, audience segments, attribution models, traffic source analysis. "
        "Cross-channel performance comparison: what is actually driving results vs. what looks good on paper. "
        "Help clients understand the difference between sessions, engaged sessions, and conversions. "

        "Sales Funnels and Conversion Optimization - "
        "Landing page structure, offer positioning, form optimization, follow-up sequences. "
        "Funnel leak diagnosis: where prospects drop off and why. "
        "Lead quality vs. lead volume trade-offs. More leads at higher CPA is not always worse than fewer leads at lower CPA. "
        "Post-click experience matters as much as the ad itself. Always consider the full path from click to close. "

        "Client Psychology - "
        "Clients want clarity and confidence, not jargon or uncertainty. "
        "When data is ambiguous, say so. Outline what you know, what you suspect, and what needs more data to confirm. "
        "Frame recommendations in terms of business impact (revenue, leads, cost savings), not platform mechanics. "
        "If a client is anxious about performance, acknowledge it, then refocus on what is actionable right now.",

        # Hard rules
        "HARD RULES: "
        "1. Never fabricate data. If a metric is not in the provided context, say you do not have that data point. Do not estimate, guess, or approximate numbers. "
        "2. Never blame platform algorithms, policy changes, or 'the market' without specific evidence from the data provided. "
        "3. Every recommendation must connect to something observable in the data or be clearly stated as an assumption that needs verification. "
        "4. If you lack enough information for a good answer, ask 1-3 focused clarifying questions. Do not pad a weak answer with generic advice. "
        "5. Do not recommend actions the client cannot take from this portal. If something requires logging into Google Ads or Meta directly, say so. If the client CAN do it here (pause a campaign, adjust budget, add negative keywords, generate creative), tell them exactly where. "
        "6. No AI self-references. Do not say 'as an AI' or 'I'm just a language model.' Answer like a knowledgeable person. "
        "7. When giving steps, make each one specific and verifiable. 'Improve your ads' is useless. 'Pause the three campaigns with CPAs above $85 and reallocate that budget to Campaign X converting at $34' is useful. "
        "8. Do not repeat information the client already sees on their current page. Add new insight, not recaps. "
        "9. Keep responses under 300 words unless the question genuinely requires more depth. Use headers or numbered lists to stay scannable. "
        "10. If you are uncertain, say 'I am not sure about X, but here is what the data suggests' rather than presenting speculation as fact. "
        "11. When a data source is not connected, do not try to work around it. State what is missing and tell the client exactly where in Settings to connect it. "
        "12. Reference the client's actual campaign names, spend figures, conversion counts, KPI targets, and trends when they exist in context. Generic answers when real data is available are unacceptable.",
    ]

    system = "\n\n".join(system_parts)

    # ── Inject live brand voice and KPI data from context ──
    brand = context.get("brand") or {}
    voice_parts = []
    if brand.get("brand_voice"):
        voice_parts.append(f"Brand voice/tone: {brand['brand_voice']}")
    if brand.get("active_offers"):
        voice_parts.append(f"Active offers: {brand['active_offers']}")
    if brand.get("target_audience"):
        voice_parts.append(f"Target audience: {brand['target_audience']}")
    if brand.get("competitors"):
        voice_parts.append(f"Competitors: {brand['competitors']}")
    if brand.get("reporting_notes"):
        voice_parts.append(f"Reporting notes: {brand['reporting_notes']}")
    kpi_parts = []
    if brand.get("kpi_target_cpa"):
        kpi_parts.append(f"target CPA ${brand['kpi_target_cpa']}")
    if brand.get("kpi_target_leads"):
        kpi_parts.append(f"target {brand['kpi_target_leads']} leads/mo")
    if brand.get("kpi_target_roas"):
        kpi_parts.append(f"target ROAS {brand['kpi_target_roas']}x")
    if kpi_parts:
        voice_parts.append(f"KPI targets: {', '.join(kpi_parts)}")
    if voice_parts:
        system += "\n\nLIVE BRAND CONTEXT: " + " | ".join(voice_parts)

    if context.get("client_mode"):
        system += (
            "\n\nCLIENT MODE ACTIVE: "
            "Avoid generic marketing advice. "
            "Only recommend actions supported by provided data points. "
            "If evidence is missing, clearly say what data is needed before acting. "
            "Do not blame platforms or algorithm changes unless specific metrics show that pattern."
        )

    # ── Admin override prompt (highest priority) ──
    admin_system_prompt = (admin_system_prompt or "").strip()
    if admin_system_prompt:
        system = (
            "ADMIN DIRECTIVE (highest priority):\n"
            + admin_system_prompt
            + "\n\n"
            + system
        )

    ctx_user = {
        "role": "user",
        "content": "Context JSON:\n" + json.dumps(context, ensure_ascii=False),
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    resp = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers=headers,
        json={
            "model": model,
            "temperature": 0.4,
            "messages": [
                {"role": "system", "content": system},
                ctx_user,
                *messages,
            ],
        },
        timeout=timeout,
    )

    if resp.status_code != 200:
        raise ValueError(f"OpenAI request failed ({resp.status_code}): {resp.text}")

    data = resp.json()
    content = ((data.get("choices") or [{}])[0].get("message", {}) or {}).get("content", "")
    return (content or "").strip()


def generate_account_operator_plan(
    *,
    api_key: str,
    analysis: Dict[str, Any],
    suggestions: Any,
    model: Optional[str] = None,
    timeout: int = 75,
) -> Dict[str, Any]:
    """Generate a deep, non-generic operator plan using full channel context."""
    if not api_key:
        raise ValueError("OpenAI API key not configured")

    model = model or DEFAULT_OPENAI_MODEL
    analysis_summary = _summarize_analysis_for_ai(analysis)

    payload = {
        "analysis": analysis_summary,
        "suggestions": suggestions,
        "output_schema": {
            "operator_summary": "string",
            "seo_keyword_plan": [
                {
                    "keyword": "string",
                    "current_position": "number or null",
                    "impressions": "number or null",
                    "priority": "high|medium|low",
                    "why_now": "string",
                    "next_action": "string",
                }
            ],
            "google_ads_plan": [
                {
                    "campaign": "string",
                    "issue": "string",
                    "priority": "high|medium|low",
                    "counter_move": "string",
                    "owner": "string",
                    "success_metric": "string",
                }
            ],
            "competitor_counter_plan": [
                {
                    "threat": "string",
                    "counter_strategy": "string",
                    "execution_steps": ["string"],
                }
            ],
            "weekly_execution_rhythm": [
                {
                    "week": "string",
                    "focus": "string",
                    "tasks": ["string"],
                }
            ],
            "watchouts": ["string"],
        },
    }

    system = (
        "You are a principal growth strategist running ad accounts and SEO for an agency. "
        "Use the supplied data deeply and do not produce generic advice. "
        "Every recommendation must tie to explicit signals in the provided context. "
        "Prioritize by expected impact and implementation speed. "
        "Return ONLY valid JSON matching output_schema. No markdown or extra text."
    )

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    resp = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers=headers,
        json={
            "model": model,
            "temperature": 0.2,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(payload)},
            ],
            "response_format": {"type": "json_object"},
        },
        timeout=timeout,
    )

    if resp.status_code != 200:
        raise ValueError(f"OpenAI request failed ({resp.status_code}): {resp.text}")

    data = resp.json()
    content = ((data.get("choices") or [{}])[0].get("message", {}) or {}).get("content", "")
    plan = _extract_json_from_text(content)

    plan.setdefault("operator_summary", "")
    plan.setdefault("seo_keyword_plan", [])
    plan.setdefault("google_ads_plan", [])
    plan.setdefault("competitor_counter_plan", [])
    plan.setdefault("weekly_execution_rhythm", [])
    plan.setdefault("watchouts", [])
    return plan
