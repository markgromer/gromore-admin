"""Controlled SEO research via Perplexity-capable providers.

Research calls are intentionally opt-in, cached, and capped per brand so
dashboard loads do not create surprise token spend.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import requests


OPENROUTER_CHAT_URL = "https://openrouter.ai/api/v1/chat/completions"
PERPLEXITY_CHAT_URL = "https://api.perplexity.ai/chat/completions"
DEFAULT_OPENROUTER_MODEL = "perplexity/sonar"
DEFAULT_PERPLEXITY_MODEL = "sonar"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "enabled"}


def _int_value(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(float(str(value).strip()))
    except Exception:
        parsed = default
    return max(minimum, min(maximum, parsed))


def _setting(db: Any, key: str, env_key: str = "", default: str = "") -> str:
    value = ""
    if db:
        try:
            value = (db.get_setting(key, "") or "").strip()
        except Exception:
            value = ""
    return value or (os.environ.get(env_key or key.upper(), default) or default).strip()


def _cache_key(brand_id: int, fingerprint: str) -> str:
    digest = hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()[:24]
    return f"seo_research_cache_{brand_id}_{digest}"


def _usage_key(brand_id: int, day: Optional[str] = None) -> str:
    return f"seo_research_usage_{brand_id}_{day or _utcnow().strftime('%Y-%m-%d')}"


def _extract_json(text: str) -> Dict[str, Any]:
    text = (text or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        pass

    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, flags=re.I | re.S)
    if fenced:
        try:
            parsed = json.loads(fenced.group(1))
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            pass

    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        try:
            parsed = json.loads(text[start : end + 1])
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            pass
    return {"summary": text[:2000]}


def seo_research_config(db: Any, brand: Dict[str, Any]) -> Dict[str, Any]:
    brand = brand or {}
    provider = (
        brand.get("seo_research_provider")
        or _setting(db, "seo_research_provider", default="openrouter")
        or "openrouter"
    ).strip().lower()
    if provider not in {"openrouter", "perplexity", "off"}:
        provider = "openrouter"

    enabled = _truthy(brand.get("seo_research_enabled"))
    daily_limit = _int_value(
        brand.get("seo_research_daily_limit") or _setting(db, "seo_research_daily_limit", default="5"),
        5,
        0,
        100,
    )
    cache_days = _int_value(
        brand.get("seo_research_cache_days") or _setting(db, "seo_research_cache_days", default="14"),
        14,
        1,
        90,
    )
    max_results = _int_value(
        brand.get("seo_research_max_results") or _setting(db, "seo_research_max_results", default="8"),
        8,
        3,
        20,
    )

    if provider == "perplexity":
        model = (
            brand.get("seo_research_model")
            or _setting(db, "seo_research_model", default=DEFAULT_PERPLEXITY_MODEL)
            or DEFAULT_PERPLEXITY_MODEL
        ).strip()
        if model.startswith("perplexity/"):
            model = model.split("/", 1)[1]
        api_key = (
            (brand.get("seo_perplexity_api_key") or "").strip()
            or _setting(db, "perplexity_api_key", "PERPLEXITY_API_KEY")
        )
    elif provider == "off":
        model = ""
        api_key = ""
    else:
        provider = "openrouter"
        model = (
            brand.get("seo_research_model")
            or _setting(db, "seo_research_model", default=DEFAULT_OPENROUTER_MODEL)
            or DEFAULT_OPENROUTER_MODEL
        ).strip()
        if "/" not in model:
            model = f"perplexity/{model}"
        api_key = (
            (brand.get("ai_openrouter_api_key") or "").strip()
            or (brand.get("ai_provider_api_key") or "").strip()
            or _setting(db, "openrouter_api_key", "OPENROUTER_API_KEY")
        )

    return {
        "enabled": enabled and provider != "off",
        "provider": provider,
        "model": model,
        "api_key": api_key,
        "daily_limit": daily_limit,
        "cache_days": cache_days,
        "max_results": max_results,
        "configured": bool(api_key and provider != "off"),
    }


def _read_cached(db: Any, key: str) -> Optional[Dict[str, Any]]:
    if not db:
        return None
    try:
        raw = db.get_setting(key, "")
        payload = json.loads(raw) if raw else {}
        expires_at = payload.get("expires_at")
        if expires_at and datetime.fromisoformat(expires_at) > _utcnow():
            result = payload.get("result") or {}
            if isinstance(result, dict):
                result = dict(result)
                result["cached"] = True
                result["cached_at"] = payload.get("created_at", "")
                return result
    except Exception:
        return None
    return None


def _write_cached(db: Any, key: str, result: Dict[str, Any], cache_days: int) -> None:
    if not db:
        return
    now = _utcnow()
    payload = {
        "created_at": now.isoformat(timespec="seconds"),
        "expires_at": (now + timedelta(days=cache_days)).isoformat(timespec="seconds"),
        "result": result,
    }
    try:
        db.save_setting(key, json.dumps(payload))
    except Exception:
        pass


def _usage_count(db: Any, brand_id: int) -> int:
    if not db:
        return 0
    try:
        raw = db.get_setting(_usage_key(brand_id), "")
        payload = json.loads(raw) if raw else {}
        return int(payload.get("count") or 0)
    except Exception:
        return 0


def _increment_usage(db: Any, brand_id: int) -> None:
    if not db:
        return
    key = _usage_key(brand_id)
    count = _usage_count(db, brand_id) + 1
    try:
        db.save_setting(key, json.dumps({"count": count, "date": _utcnow().strftime("%Y-%m-%d")}))
    except Exception:
        pass


def _compact_rows(rows: Any, limit: int) -> Any:
    if not isinstance(rows, list):
        return []
    compacted = []
    for item in rows[:limit]:
        if isinstance(item, dict):
            compacted.append({k: item.get(k) for k in ("query", "page", "clicks", "impressions", "ctr", "position") if k in item})
        else:
            compacted.append(item)
    return compacted


def build_seo_research_prompt(brand: Dict[str, Any], seo_data: Optional[Dict[str, Any]], query: str, max_results: int) -> str:
    brand = brand or {}
    seo_data = seo_data or {}
    context = {
        "business": brand.get("display_name") or brand.get("name") or "",
        "industry": brand.get("industry") or "",
        "service_area": brand.get("service_area") or "",
        "primary_services": brand.get("primary_services") or "",
        "website": brand.get("website") or "",
        "competitors": brand.get("competitors") or "",
        "focus_question": query or "Find practical SEO and local search opportunities for this business.",
        "search_console": {
            "totals": seo_data.get("totals") or {},
            "top_queries": _compact_rows(seo_data.get("top_queries"), max_results),
            "opportunity_queries": _compact_rows(seo_data.get("opportunity_queries"), max_results),
            "top_pages": _compact_rows(seo_data.get("top_pages"), max_results),
        },
    }
    return (
        "You are Warren's SEO research agent for a local service business. "
        "Use current web/search knowledge only where your model/provider supports it. "
        "Return concise, business-owner-ready strategy, not generic SEO homework. "
        "Prioritize actions WARREN can turn into site pages, blog topics, GBP posts, ad landing pages, and missions. "
        "Do not invent exact rankings or traffic numbers that are not provided.\n\n"
        f"Context JSON:\n{json.dumps(context, ensure_ascii=True)}\n\n"
        "Return ONLY valid JSON with these keys:\n"
        "- summary: 2-4 sentence plain-English readout\n"
        "- market_read: array of current search/competitor observations\n"
        "- content_gaps: array of specific missing pages/topics with why they matter\n"
        "- questions_to_answer: array of customer questions the site should answer\n"
        "- pages_to_create_or_update: array of objects with page, intent, priority, reason\n"
        "- local_seo_angles: array of GBP/local trust actions\n"
        "- paid_vs_organic_notes: array explaining how organic opportunities can lower blended CPA\n"
        "- mission_candidates: array of objects with title, why_it_matters, first_steps\n"
        "- risks: array of assumptions or items to verify\n"
        "- sources: array of source names or URLs used, if available"
    )


def _post_chat(config: Dict[str, Any], prompt: str) -> Dict[str, Any]:
    provider = config["provider"]
    url = PERPLEXITY_CHAT_URL if provider == "perplexity" else OPENROUTER_CHAT_URL
    headers = {
        "Authorization": f"Bearer {config['api_key']}",
        "Content-Type": "application/json",
    }
    if provider == "openrouter":
        headers["HTTP-Referer"] = "https://warren.local"
        headers["X-Title"] = "WARREN SEO Research"

    payload = {
        "model": config["model"],
        "messages": [
            {"role": "system", "content": "Return valid JSON only. Be specific, current, and practical."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
        "max_tokens": 1400,
    }
    response = requests.post(url, headers=headers, json=payload, timeout=45)
    if response.status_code >= 400:
        raise RuntimeError(f"{provider} returned HTTP {response.status_code}: {response.text[:240]}")
    data = response.json()
    content = (((data.get("choices") or [{}])[0].get("message") or {}).get("content") or "").strip()
    parsed = _extract_json(content)
    return {
        "research": parsed,
        "usage": data.get("usage") or {},
        "raw_excerpt": content[:500] if not parsed else "",
    }


def run_seo_research(
    db: Any,
    brand: Dict[str, Any],
    seo_data: Optional[Dict[str, Any]] = None,
    query: str = "",
    force: bool = False,
) -> Dict[str, Any]:
    brand = brand or {}
    brand_id = int(brand.get("id") or 0)
    config = seo_research_config(db, brand)
    if not config["enabled"]:
        return {"ok": False, "error": "SEO research is not enabled for this brand.", "config": _public_config(config)}
    if not config["configured"]:
        return {"ok": False, "error": f"{config['provider'].title()} API key is not configured.", "config": _public_config(config)}

    prompt = build_seo_research_prompt(brand, seo_data, query, config["max_results"])
    fingerprint = json.dumps(
        {
            "brand_id": brand_id,
            "provider": config["provider"],
            "model": config["model"],
            "query": query,
            "prompt": prompt,
        },
        sort_keys=True,
    )
    key = _cache_key(brand_id, fingerprint)
    if not force:
        cached = _read_cached(db, key)
        if cached:
            return {"ok": True, **cached, "config": _public_config(config)}

    used = _usage_count(db, brand_id)
    if config["daily_limit"] and used >= config["daily_limit"]:
        return {
            "ok": False,
            "error": f"Daily SEO research limit reached ({used}/{config['daily_limit']}). Use cached results or raise the cap in settings.",
            "config": _public_config(config),
        }

    result = _post_chat(config, prompt)
    _increment_usage(db, brand_id)
    payload = {
        "cached": False,
        "provider": config["provider"],
        "model": config["model"],
        "query": query,
        "research": result.get("research") or {},
        "usage": result.get("usage") or {},
        "raw_excerpt": result.get("raw_excerpt") or "",
        "generated_at": _utcnow().isoformat(timespec="seconds"),
    }
    _write_cached(db, key, payload, config["cache_days"])
    return {"ok": True, **payload, "config": _public_config(config)}


def _public_config(config: Dict[str, Any]) -> Dict[str, Any]:
    public = dict(config)
    public.pop("api_key", None)
    return public
