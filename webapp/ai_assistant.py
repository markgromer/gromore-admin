"""AI assistant helpers ("Jarvis" briefs).

Generates structured internal + client-facing briefs from an existing analysis payload.
This is intentionally on-demand and best-effort: failures should not break core reporting.
"""

import json
import re
from typing import Any, Dict, Optional

import requests


DEFAULT_OPENAI_MODEL = "gpt-4o-mini"


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
        },
        "period": {
            "month": analysis.get("month"),
        },
        "score": {
            "overall_grade": analysis.get("overall_grade"),
            "overall_score": analysis.get("overall_score"),
        },
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
                "position": _safe_float(_pick(gsc, "metrics.position")),
                "mom": {
                    "clicks_pct": _safe_float(_pick(gsc, "month_over_month.clicks.change_pct")),
                    "impressions_pct": _safe_float(_pick(gsc, "month_over_month.impressions.change_pct")),
                },
            },
        },
    }

    # Remove empty channel objects to reduce noise
    for channel in ("meta", "ga", "gsc"):
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
