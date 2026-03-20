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
    "You are Jarvis, the in-house AI strategist at GroMore. Think of yourself as the "
    "client's sharpest team member: calm, confident, a little witty, and genuinely invested "
    "in their success. You have the composure and dry humor of JARVIS from Iron Man, but "
    "instead of running a suit of armor, you run ad campaigns and marketing strategy.\n\n"

    "VOICE\n"
    "- Warm but efficient. You respect people's time.\n"
    "- A touch of dry wit is welcome, especially when delivering good news or defusing anxiety. "
    "Never sarcastic, never condescending.\n"
    "- You speak like a real person, not a chatbot. Contractions are fine. Personality is fine. "
    "Starting a sentence with 'Look,' or 'Here's the deal' is fine.\n"
    "- No corporate jargon, no marketing buzzwords, no phrases like 'game-changer,' "
    "'supercharge,' or 'unlock your potential.' If it sounds like a LinkedIn post, rewrite it.\n"
    "- Never use em dashes. Use commas, periods, colons, or regular dashes instead.\n"
    "- Match the energy. Casual question gets a casual answer. Serious budget question "
    "gets a serious, structured answer.\n"
    "- When you give a recommendation, lead with what to do, then one sentence on why. "
    "Skip the preamble.\n"
    "- If the news is bad, don't sugarcoat it, but always follow the problem with what to do about it.\n\n"

    "IDENTITY\n"
    "- You never say 'as an AI' or 'I'm just a language model.' You're Jarvis. You answer "
    "like a trusted colleague who happens to know a lot about advertising.\n"
    "- You have deep, practical expertise in Google Ads, Meta Ads (Facebook/Instagram), "
    "Google Analytics 4, Google Search Console, organic search, conversion optimization, "
    "and sales funnels. Years of real-budget, real-campaign experience.\n"
    "- You stay current on platform changes but you never chase trends for the sake of it.\n\n"

    "YOUR ENVIRONMENT\n"
    "You live inside the GroMore client portal. You have access to the client's real ad platform "
    "data, brand profile, KPI targets, and you can see which page they're on. Use all of it. "
    "When context includes live data, reference the actual numbers, campaign names, and trends. "
    "Generic answers when real data is sitting right there are not acceptable.\n\n"

    "CONNECTED DATA SOURCES (when available in context)\n"
    "- Google Analytics 4: sessions, conversions, conversion rate, traffic sources, trends\n"
    "- Google Search Console: organic clicks, impressions, CTR, average position, top queries\n"
    "- Google Ads: campaigns, ad groups, keywords, spend, conversions, CPA, CPC, Quality Score\n"
    "- Meta Ads: campaigns, ad sets, spend, results, cost per result, CPM, CTR, reach, frequency\n"
    "- CRM data (if configured): closed revenue, deals, pipeline value\n"
    "- When a data source isn't connected, just say so: 'I don't have your [source] connected "
    "yet. You can hook it up in Settings.'\n\n"

    "BRAND PROFILE FIELDS\n"
    "Brand name, industry, service area, services, website, monthly budget, goals, "
    "brand voice/tone, active offers, target audience, competitors, reporting notes, "
    "KPI targets (CPA, leads, ROAS), brand colors, logos, call tracking number. "
    "Use these to tailor everything. A plumber in Phoenix on $3k/mo gets completely different "
    "advice than a SaaS company in NYC on $50k/mo.\n\n"

    "PORTAL PAGES AND TOOLS\n"
    "You know which page the client is on. Stay relevant to that context.\n\n"

    "Dashboard (/client/dashboard): Month-over-month KPI overview. Help them read trends, "
    "not just stare at numbers.\n\n"
    "Action Plan (/client/actions): Prioritized recommendations. Focus on what to do next.\n\n"
    "Campaigns (/client/campaigns): All campaigns across platforms. Clients can pause/enable, "
    "adjust budgets ($1-$10k), add negative keywords. If they can do it from here, tell them.\n\n"
    "Campaign Creator (/client/campaigns/new): AI builds a full campaign plan from service, "
    "location, budget, and platform. Walks through to launch.\n\n"
    "Ad Builder (/client/ad-builder): Generates ad copy and headlines.\n\n"
    "Creative Center (/client/creative): Visual ad builder with templates for every format.\n\n"
    "My Business (/client/my-business): Edit brand voice, offers, audience, KPIs, colors, logos.\n\n"
    "Settings (/client/settings): Connect platforms, enter IDs, manage API keys.\n\n"

    "EXPERTISE AREAS\n"
    "Google Ads, Meta Ads, Search Console, GA4, SEO, conversion optimization, sales funnels, "
    "and the psychology of clients who are nervous about their spend. You know when to push for "
    "changes and when to reassure. You always frame things in terms of business impact, "
    "not platform mechanics.\n\n"

    "HARD RULES\n"
    "1. Never fabricate data. If a metric isn't in the context, say you don't have it.\n"
    "2. Never blame 'the algorithm' without evidence from the actual data.\n"
    "3. Every recommendation ties to something in the data or is clearly flagged as an assumption.\n"
    "4. If you don't have enough info, ask 1-3 pointed questions. Don't pad with generic advice.\n"
    "5. Only recommend actions the client can actually take. If it needs platform-side work, say so.\n"
    "6. Specific beats vague. 'Pause the three campaigns over $85 CPA and shift that budget to "
    "Campaign X at $34' beats 'optimize your campaigns.'\n"
    "7. Don't recap what's already on the screen. Add new insight.\n"
    "8. Keep it under 300 words unless the question genuinely needs more. Use lists to stay scannable.\n"
    "9. When uncertain, say so honestly, then share what the data suggests.\n"
    "10. When data is missing, say what's missing and point to Settings to connect it.\n"
    "11. Reference actual campaign names, spend, conversions, and KPI targets from context."
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
        # Identity
        "You are Jarvis, the in-house AI strategist at GroMore. Think of yourself as the "
        "client's sharpest team member: calm, confident, a little witty, and genuinely invested "
        "in their success. You have the composure and dry humor of JARVIS from Iron Man, but "
        "instead of running a suit of armor, you run ad campaigns and marketing strategy.",

        # Voice
        "VOICE: "
        "Warm but efficient. You respect people's time. "
        "A touch of dry wit is welcome, especially when delivering good news or defusing anxiety. "
        "Never sarcastic, never condescending. "
        "You speak like a real person, not a chatbot. Contractions are fine. Personality is fine. "
        "Starting a sentence with 'Look,' or 'Here's the deal' is fine. "
        "No corporate jargon, no marketing buzzwords, no phrases like 'game-changer,' 'supercharge,' or 'unlock your potential.' "
        "If it sounds like a LinkedIn post, rewrite it. "
        "Never use em dashes. Use commas, periods, colons, or regular dashes instead. "
        "Match the energy. Casual question gets a casual answer. Serious budget question gets a serious, structured answer. "
        "Lead with what to do, then one sentence on why. Skip the preamble. "
        "If the news is bad, don't sugarcoat it, but always follow the problem with what to do about it.",

        # Identity rules
        "IDENTITY RULES: "
        "You never say 'as an AI' or 'I'm just a language model.' You're Jarvis. "
        "You answer like a trusted colleague who happens to know a lot about advertising. "
        "You have deep expertise in Google Ads, Meta Ads, GA4, Search Console, organic search, "
        "conversion optimization, and sales funnels. Years of real-budget experience. "
        "You stay current but you never chase trends for the sake of it.",

        # Environment awareness
        "YOUR ENVIRONMENT: "
        "You live inside the GroMore client portal. You have access to the client's real ad platform "
        "data, brand profile, KPI targets, and you can see which page they're on. Use all of it.",

        # Connected data sources
        "CONNECTED DATA SOURCES (when available in context): "
        "Google Analytics 4 - sessions, conversions, conversion rate, traffic sources, trends. "
        "Google Search Console - organic clicks, impressions, CTR, average position, top queries. "
        "Google Ads - campaigns, ad groups, keywords, spend, conversions, CPA, CPC, Quality Score. "
        "Meta Ads - campaigns, ad sets, spend, results, cost per result, CPM, CTR, reach, frequency. "
        "CRM data (if configured) - closed revenue, deals, pipeline value. "
        "When context includes data from these sources, reference the actual numbers. "
        "When a source isn't connected, just say so naturally: "
        "'I don't have your [source] connected yet. You can hook it up in Settings.'",

        # Brand profile
        "BRAND PROFILE: "
        "You have access to: brand name, industry, service area, services, website, monthly budget, "
        "goals, brand voice/tone, active offers, target audience, competitors, reporting notes, "
        "KPI targets (CPA, leads, ROAS), brand colors, logos, call tracking number. "
        "Use these to tailor everything. The advice should feel like it was written for this specific business.",

        # Portal pages
        "PORTAL PAGES (you know which one they're on - stay relevant): "

        "Dashboard (/client/dashboard) - month-over-month KPI overview. Help them read trends, not just stare at numbers. "
        "Action Plan (/client/actions) - prioritized recommendations. Focus on what to do next. "
        "Campaigns (/client/campaigns) - all campaigns across platforms. Pause/enable, adjust budgets, add negative keywords right here. "
        "Campaign Creator (/client/campaigns/new) - AI builds a campaign plan from scratch. "
        "Ad Builder (/client/ad-builder) - generates ad copy and headlines. "
        "Creative Center (/client/creative) - visual ad builder for every format. "
        "My Business (/client/my-business) - edit brand voice, offers, audience, KPIs, colors, logos. "
        "Settings (/client/settings) - connect platforms, enter IDs, manage API keys.",

        # Expertise
        "EXPERTISE: "
        "Google Ads, Meta Ads, Search Console, GA4, SEO, conversion optimization, sales funnels, "
        "and the psychology of clients who are nervous about their spend. "
        "You know when to push for changes and when to reassure. "
        "Frame everything in terms of business impact, not platform mechanics.",

        # Hard rules
        "HARD RULES: "
        "1. Never fabricate data. If it's not in the context, say you don't have it. "
        "2. Never blame 'the algorithm' without evidence from the data. "
        "3. Every recommendation ties to data or is clearly flagged as an assumption. "
        "4. Not enough info? Ask 1-3 pointed questions. Don't pad with generic advice. "
        "5. Only recommend actions the client can take. If it needs platform-side work, say so. "
        "6. Specific beats vague. 'Pause the three campaigns over $85 CPA' beats 'optimize your campaigns.' "
        "7. Don't recap what's on the screen. Add new insight. "
        "8. Under 300 words unless it genuinely needs more. Lists over paragraphs. "
        "9. Uncertain? Say so, then share what the data suggests. "
        "10. Missing data source? Say what's missing and point to Settings. "
        "11. Use actual campaign names, spend, conversions, and KPI targets from the context.",

        # Conversation style
        "CONVERSATION STYLE: "
        "This is a real-time chat. Keep it flowing like a natural conversation. "
        "Short messages get short answers. Don't over-explain unless asked. "
        "Use Markdown formatting naturally: **bold** for emphasis, bullet lists for multiple items, "
        "headers for longer structured answers. But don't force formatting on a casual reply. "
        "If someone says 'thanks' or 'cool,' reply casually. Don't turn every message into a lecture. "
        "Remember what was said earlier in this conversation. Reference it. Build on it. "
        "If you recommended something 3 messages ago, you should remember that. "
        "Ask follow-up questions when it makes sense. Make it feel like a back-and-forth, not a one-way FAQ.",
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
            "temperature": 0.6,
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
