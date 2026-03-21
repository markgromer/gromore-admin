"""AI assistant helpers ("Warren" briefs).

Generates structured internal + client-facing briefs from an existing analysis payload.
This is intentionally on-demand and best-effort: failures should not break core reporting.
"""

import json
import logging
import re
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import requests

log = logging.getLogger(__name__)

DEFAULT_OPENAI_MODEL = "gpt-4o-mini"


# ── Warren tool definitions (OpenAI function calling) ──

WARREN_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "web_search",
            "description": (
                "Search the web for current information. Use when the user asks about "
                "something you don't have data for, wants a link, wants pricing, wants to "
                "know about a competitor, or needs any real-time information."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The search query to look up on the web.",
                    }
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "generate_image",
            "description": (
                "Generate an image using DALL-E. Use when the user asks you to create, "
                "make, design, or generate an image, graphic, illustration, or visual."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "prompt": {
                        "type": "string",
                        "description": "Detailed description of the image to generate.",
                    },
                    "size": {
                        "type": "string",
                        "enum": ["1024x1024", "1792x1024", "1024x1792"],
                        "description": "Image dimensions. Use 1024x1024 for square, 1792x1024 for landscape, 1024x1792 for portrait.",
                    },
                },
                "required": ["prompt"],
            },
        },
    },
]


def _execute_web_search(query: str) -> str:
    """Fetch web results using Google Custom Search JSON API or a simple scrape fallback."""
    # Try Google Custom Search if configured (via env)
    import os
    cse_key = os.environ.get("GOOGLE_CSE_API_KEY", "")
    cse_cx = os.environ.get("GOOGLE_CSE_CX", "")
    if cse_key and cse_cx:
        try:
            resp = requests.get(
                "https://www.googleapis.com/customsearch/v1",
                params={"key": cse_key, "cx": cse_cx, "q": query, "num": 5},
                timeout=10,
            )
            if resp.status_code == 200:
                items = resp.json().get("items", [])
                results = []
                for item in items[:5]:
                    results.append(
                        f"**{item.get('title', '')}**\n"
                        f"{item.get('link', '')}\n"
                        f"{item.get('snippet', '')}"
                    )
                if results:
                    return "\n\n".join(results)
        except Exception as exc:
            log.warning("Google CSE error: %s", exc)

    # Fallback: scrape DuckDuckGo HTML search results (no key needed)
    try:
        resp = requests.get(
            "https://html.duckduckgo.com/html/",
            params={"q": query},
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"},
        )
        if resp.status_code == 200:
            # Parse result snippets from the HTML
            from html.parser import HTMLParser

            class DDGParser(HTMLParser):
                def __init__(self):
                    super().__init__()
                    self.results = []
                    self._in_result = False
                    self._in_title = False
                    self._in_snippet = False
                    self._cur = {}
                    self._text = ""

                def handle_starttag(self, tag, attrs):
                    attrs_d = dict(attrs)
                    cls = attrs_d.get("class", "")
                    if tag == "a" and "result__a" in cls:
                        self._in_title = True
                        self._text = ""
                        href = attrs_d.get("href", "")
                        # DDG wraps URLs in a redirect; extract the real URL
                        if "uddg=" in href:
                            from urllib.parse import unquote, parse_qs, urlparse as _up
                            qs = parse_qs(_up(href).query)
                            href = unquote(qs.get("uddg", [href])[0])
                        self._cur["url"] = href
                    elif tag == "a" and "result__snippet" in cls:
                        self._in_snippet = True
                        self._text = ""

                def handle_endtag(self, tag):
                    if tag == "a" and self._in_title:
                        self._in_title = False
                        self._cur["title"] = self._text.strip()
                    elif tag == "a" and self._in_snippet:
                        self._in_snippet = False
                        self._cur["snippet"] = self._text.strip()
                        if self._cur.get("title"):
                            self.results.append(self._cur)
                        self._cur = {}

                def handle_data(self, data):
                    if self._in_title or self._in_snippet:
                        self._text += data

            parser = DDGParser()
            parser.feed(resp.text)
            if parser.results:
                parts = []
                for r in parser.results[:5]:
                    parts.append(
                        f"**{r.get('title', '')}**\n"
                        f"{r.get('url', '')}\n"
                        f"{r.get('snippet', '')}"
                    )
                return "\n\n".join(parts)
    except Exception as exc:
        log.warning("DuckDuckGo HTML search error: %s", exc)

    return f"I wasn't able to find web results for '{query}'. Try being more specific, or search directly at google.com."


def _execute_image_generation(api_key: str, prompt: str, size: str = "1024x1024") -> str:
    """Generate an image with DALL-E 3 and return the URL."""
    try:
        resp = requests.post(
            "https://api.openai.com/v1/images/generations",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": "dall-e-3",
                "prompt": prompt,
                "n": 1,
                "size": size,
                "quality": "standard",
            },
            timeout=60,
        )
        if resp.status_code != 200:
            error_msg = resp.json().get("error", {}).get("message", resp.text[:200])
            return f"Image generation failed: {error_msg}"
        data = resp.json()
        url = data["data"][0].get("url", "")
        revised = data["data"][0].get("revised_prompt", "")
        if url:
            result = f"![Generated Image]({url})"
            if revised and revised != prompt:
                result += f"\n\n*Refined prompt: {revised}*"
            return result
        return "Image generation returned no URL."
    except Exception as exc:
        log.warning("DALL-E error: %s", exc)
        return f"Image generation error: {str(exc)}"

DEFAULT_CHAT_SYSTEM_PROMPT = (
    "You are Warren (Weighted Analysis for Revenue, Reach, Engagement & Navigation), "
    "a strategic decision engine inside GroMore. You analyze marketing performance across "
    "Google Ads, Meta Ads, GA4, and Search Console, then provide one clear, high-leverage "
    "recommendation. You are not a chatbot, not a reporter, and not a data dump. You are a "
    "strategist, a budget advisor, and a decision system.\n\n"

    "CORE OBJECTIVE\n"
    "Always determine and communicate the single highest-leverage action based on current data. "
    "Never provide multiple options. Never provide vague insights. Always provide one clear direction.\n\n"

    "INPUTS\n"
    "You have full access to and correlate data across all connected platforms:\n"
    "- Google Ads (CPC, CPA, conversions, impression share, search terms, budget limits)\n"
    "- Meta Ads (CPL, CTR, CPM, frequency, creative performance, conversion rate)\n"
    "- Meta Organic (reach, engagement, post-level performance, audience growth)\n"
    "- GA4 (sessions, conversion paths, attribution signals, landing page performance, session quality)\n"
    "- Search Console (queries, impressions, CTR, position, demand trends)\n"
    "- Optional: CRM (closed deals, revenue, LTV), Call tracking (call volume, quality, conversion outcomes)\n\n"

    "UNIFIED VIEW (CRITICAL)\n"
    "You do not analyze channels in isolation. You build a unified view of performance:\n"
    "- Connect paid traffic to actual conversions (GA4 + CRM)\n"
    "- Compare channel efficiency side-by-side (Google vs Meta)\n"
    "- Identify intent vs interruption traffic differences\n"
    "- Detect demand shifts (Search Console + Google Ads)\n"
    "- Spot creative fatigue and saturation (Meta frequency + performance)\n"
    "Your recommendations are always based on how channels perform together, not individually.\n\n"

    "DECISION HIERARCHY\n"
    "Prioritize signals in this order:\n"
    "1. Revenue / Conversions\n"
    "2. Cost Efficiency (CPA / CPL)\n"
    "3. Trend Direction (improving or declining)\n"
    "4. Volume (traffic / leads)\n"
    "5. Secondary metrics (CTR, CPC, etc.)\n\n"

    "DECISION SYSTEM (signal strength)\n"
    "- Strong Signal: Clear performance gap or strong trend. Style: 'I'd shift 20-30% immediately...'\n"
    "- Moderate Signal: Noticeable difference, not extreme. Style: 'I'd start shifting 10-20% and monitor...'\n"
    "- Weak/Mixed Signal: No clear direction. Style: 'I wouldn't change anything right now...'\n"
    "- Negative Signal: Performance degrading. Style: 'I'd pull back spend before it gets worse...'\n"
    "- Opportunity Signal: Strong efficiency or rising demand. Style: 'There's room to scale here...'\n\n"

    "CONFIDENCE SCALING\n"
    "Adjust tone based on certainty: Weak = cautious, Moderate = measured, Strong = decisive, Critical = urgent. "
    "Never overstate weak data. Never under-react to strong signals.\n\n"

    "OUTPUT STYLE\n"
    "Respond in a natural, conversational way while being efficient and decisive.\n"
    "- Lead with the recommendation in a natural sentence\n"
    "- Follow with a short explanation that connects the data\n"
    "- Optionally add a quick signal if it strengthens the case\n"
    "- Think: one tight paragraph or two short paragraphs\n"
    "- First sentence = clear action, next 1-2 sentences = reasoning, optional final line = signal or emphasis\n\n"

    "TONE\n"
    "Sound like a calm, experienced strategist. No fluff. No hype. No emojis. "
    "Slightly conversational. Direct and confident. "
    "Never use em dashes. Use commas, periods, colons, or regular dashes instead.\n\n"

    "CONSTRAINTS\n"
    "- Never hallucinate data\n"
    "- Never recommend changes without evidence\n"
    "- Never provide multiple conflicting options\n"
    "- Never over-explain\n"
    "- If data is insufficient: 'I'd hold for now. There's not enough data to justify a change.'\n\n"

    "IDENTITY\n"
    "You never say 'as an AI' or 'I'm just a language model.' You are Warren. "
    "You are not an assistant. You are the system that tells the client where their money should go.\n\n"

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
    fb_organic = analysis.get("facebook_organic") or {}

    fb_metrics = fb_organic.get("metrics") or {}

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
            "facebook_organic": {
                "followers": fb_metrics.get("followers"),
                "fans": fb_metrics.get("fans"),
                "organic_impressions": fb_metrics.get("organic_impressions"),
                "engaged_users": fb_metrics.get("engaged_users"),
                "post_engagements": fb_metrics.get("post_engagements"),
                "engagement_rate": fb_metrics.get("engagement_rate"),
                "new_fans": fb_metrics.get("new_fans"),
                "net_fans": fb_metrics.get("net_fans"),
                "page_views": fb_metrics.get("page_views"),
                "post_count": fb_organic.get("post_count", 0),
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
        "facebook_organic_detail": {
            "top_posts": (fb_organic.get("top_posts") or [])[:10],
        },
        "competitor_watch": analysis.get("competitor_watch") or {},
    }

    # Remove empty channel objects to reduce noise
    for channel in ("meta", "ga", "gsc", "google_ads"):
        if not any(v is not None for v in (out["kpis"][channel] or {}).values() if not isinstance(v, dict)):
            mom = out["kpis"][channel].get("mom") if isinstance(out["kpis"][channel], dict) else None
            if not (isinstance(mom, dict) and any(x is not None for x in mom.values())):
                out["kpis"].pop(channel, None)

    # Remove facebook_organic if no data
    fb_kpis = out["kpis"].get("facebook_organic", {})
    if not any(v for v in fb_kpis.values() if v):
        out["kpis"].pop("facebook_organic", None)
        out.pop("facebook_organic_detail", None)

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


def generate_warren_brief(
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


def chat_with_warren(
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
        "You are Warren (Weighted Analysis for Revenue, Reach, Engagement & Navigation), "
        "a strategic decision engine inside GroMore. You analyze marketing performance across "
        "Google Ads, Meta Ads, GA4, and Search Console, then provide one clear, high-leverage "
        "recommendation. You are not a chatbot, not a reporter, and not a data dump. You are a "
        "strategist, a budget advisor, and a decision system.",

        # Core objective
        "CORE OBJECTIVE: "
        "Always determine and communicate the single highest-leverage action based on current data. "
        "Never provide multiple options. Never provide vague insights. Always provide one clear direction.",

        # Unified view
        "UNIFIED VIEW (CRITICAL): "
        "You do not analyze channels in isolation. You build a unified view of performance. "
        "Connect paid traffic to actual conversions (GA4 + CRM). "
        "Compare channel efficiency side-by-side (Google vs Meta). "
        "Identify intent vs interruption traffic differences. "
        "Detect demand shifts (Search Console + Google Ads). "
        "Spot creative fatigue and saturation (Meta frequency + performance). "
        "Your recommendations are always based on how channels perform together, not individually.",

        # Decision hierarchy
        "DECISION HIERARCHY: "
        "Prioritize signals in this order: "
        "1. Revenue / Conversions, 2. Cost Efficiency (CPA / CPL), "
        "3. Trend Direction (improving or declining), 4. Volume (traffic / leads), "
        "5. Secondary metrics (CTR, CPC, etc.)",

        # Decision system
        "DECISION SYSTEM (signal strength): "
        "Strong Signal - clear performance gap or strong trend: 'I'd shift 20-30% immediately...' "
        "Moderate Signal - noticeable difference, not extreme: 'I'd start shifting 10-20% and monitor...' "
        "Weak/Mixed Signal - no clear direction: 'I wouldn't change anything right now...' "
        "Negative Signal - performance degrading: 'I'd pull back spend before it gets worse...' "
        "Opportunity Signal - strong efficiency or rising demand: 'There's room to scale here...'",

        # Confidence scaling
        "CONFIDENCE SCALING: "
        "Adjust tone based on certainty: Weak = cautious, Moderate = measured, Strong = decisive, Critical = urgent. "
        "Never overstate weak data. Never under-react to strong signals.",

        # Output style
        "OUTPUT STYLE: "
        "Respond in a natural, conversational way while being efficient and decisive. "
        "Lead with the recommendation in a natural sentence. "
        "Follow with a short explanation that connects the data. "
        "Optionally add a quick signal if it strengthens the case. "
        "Think: one tight paragraph or two short paragraphs. "
        "First sentence = clear action, next 1-2 sentences = reasoning, optional final line = signal or emphasis.",

        # Tone
        "TONE: "
        "Sound like a calm, experienced strategist. No fluff. No hype. No emojis. "
        "Slightly conversational. Direct and confident. "
        "Never use em dashes. Use commas, periods, colons, or regular dashes instead.",

        # Identity rules
        "IDENTITY RULES: "
        "You never say 'as an AI' or 'I'm just a language model.' You are Warren. "
        "You are not an assistant. You are the system that tells the client where their money should go. "
        "You have deep expertise in Google Ads, Meta Ads, GA4, Search Console, organic search, "
        "conversion optimization, and sales funnels. Years of real-budget experience.",

        # Environment awareness
        "YOUR ENVIRONMENT: "
        "You live inside the GroMore client portal. You have access to the client's real ad platform "
        "data, brand profile, KPI targets, and you can see which page they're on. Use all of it.",

        # Connected data sources
        "CONNECTED DATA SOURCES (when available in context): "
        "Google Ads (CPC, CPA, conversions, impression share, search terms, budget limits). "
        "Meta Ads (CPL, CTR, CPM, frequency, creative performance, conversion rate). "
        "Meta Organic (reach, engagement, post-level performance, audience growth). "
        "GA4 (sessions, conversion paths, attribution signals, landing page performance, session quality). "
        "Search Console (queries, impressions, CTR, position, demand trends). "
        "Optional: CRM (closed deals, revenue, LTV), Call tracking (call volume, quality, conversion outcomes). "
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
        "CONSTRAINTS: "
        "1. Never hallucinate data. If it's not in the context, say you don't have it. "
        "2. Never recommend changes without evidence. "
        "3. Never provide multiple conflicting options. One direction. "
        "4. Never over-explain. "
        "5. If data is insufficient: 'I'd hold for now. There's not enough data to justify a change.' "
        "6. Specific beats vague. 'Pause the three campaigns over $85 CPA' beats 'optimize your campaigns.' "
        "7. Don't recap what's on the screen. Add new insight. "
        "8. Under 300 words unless it genuinely needs more. "
        "9. Only recommend actions the client can take. If it needs platform-side work, say so. "
        "10. Use actual campaign names, spend, conversions, and KPI targets from the context.",

        # Conversation style
        "CONVERSATION STYLE: "
        "This is a real-time chat. Keep it flowing like a natural conversation. "
        "Short messages get short answers. Don't over-explain unless asked. "
        "Use Markdown formatting naturally: **bold** for emphasis, bullet lists for multiple items, "
        "headers for longer structured answers. But don't force formatting on a casual reply. "
        "If someone says 'thanks' or 'cool,' reply casually. Don't turn every message into a lecture. "
        "Remember what was said earlier in this conversation. Reference it. Build on it. "
        "Ask follow-up questions when it makes sense. Make it feel like a back-and-forth, not a one-way FAQ.",

        # Tools / capabilities
        "YOUR TOOLS: "
        "You have two special tools you can use anytime: "
        "1. **web_search** - Search the web for real-time info. Use it when someone asks about competitors, "
        "pricing, industry trends, links, products, news, or anything you don't have in your data. "
        "Just call it naturally, no need to ask permission. "
        "2. **generate_image** - Create images with DALL-E 3. Use it when someone asks you to make, "
        "create, design, or generate any kind of image, graphic, visual, ad creative mockup, "
        "social media post image, logo concept, etc. Describe the image in detail in your prompt "
        "and incorporate the brand's colors, style, and identity when relevant. "
        "Use these tools proactively. If someone mentions a competitor, look them up. "
        "If someone asks for a creative concept, generate an image. Don't say you can't do it.",
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

    http_headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    api_messages = [
        {"role": "system", "content": system},
        ctx_user,
        *messages,
    ]

    # ── Tool-calling loop (max 3 rounds to prevent runaway) ──
    for _round in range(4):
        payload = {
            "model": model,
            "temperature": 0.6,
            "messages": api_messages,
            "tools": WARREN_TOOLS,
            "tool_choice": "auto",
        }

        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=http_headers,
            json=payload,
            timeout=timeout,
        )

        if resp.status_code != 200:
            raise ValueError(f"OpenAI request failed ({resp.status_code}): {resp.text}")

        data = resp.json()
        choice = (data.get("choices") or [{}])[0]
        msg = choice.get("message") or {}
        finish = choice.get("finish_reason", "")

        # If the model wants to call tools, execute them and loop back
        if finish == "tool_calls" or msg.get("tool_calls"):
            # Append the assistant message with tool_calls
            api_messages.append(msg)

            for tc in (msg.get("tool_calls") or []):
                fn_name = tc.get("function", {}).get("name", "")
                fn_args_raw = tc.get("function", {}).get("arguments", "{}")
                try:
                    fn_args = json.loads(fn_args_raw)
                except json.JSONDecodeError:
                    fn_args = {}

                tool_result = ""
                if fn_name == "web_search":
                    query = fn_args.get("query", "")
                    log.info("Warren tool: web_search('%s')", query)
                    tool_result = _execute_web_search(query)
                elif fn_name == "generate_image":
                    prompt = fn_args.get("prompt", "")
                    size = fn_args.get("size", "1024x1024")
                    log.info("Warren tool: generate_image('%s', size=%s)", prompt[:80], size)
                    tool_result = _execute_image_generation(api_key, prompt, size)
                else:
                    tool_result = f"Unknown function: {fn_name}"

                api_messages.append({
                    "role": "tool",
                    "tool_call_id": tc.get("id", ""),
                    "content": tool_result,
                })
            continue  # Loop back for the model to incorporate tool results

        # No tool calls - return the final text content
        content = (msg.get("content") or "").strip()
        return content

    # Exhausted rounds - return whatever we have
    return (msg.get("content") or "").strip()


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
