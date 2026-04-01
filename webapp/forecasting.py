"""Forecasting helpers for the Market Forecaster agent.

Goal: build a lightweight, explainable forecast using only data already in the system.
- Uses historical month data from src.database (monthly_data in agency.db)
- Computes seasonal baseline (same month last year(s)) + recent momentum adjustment
- Produces simple confidence bands from historical variance

This module is best-effort: if data is missing, callers should gracefully degrade.
"""

from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger(__name__)


def month_add(month: str, delta_months: int) -> str:
    """Add months to YYYY-MM."""
    year, mon = [int(x) for x in month.split("-")]
    idx = (year * 12 + (mon - 1)) + int(delta_months)
    out_year = idx // 12
    out_mon = (idx % 12) + 1
    return f"{out_year:04d}-{out_mon:02d}"


def month_now() -> str:
    return datetime.now().strftime("%Y-%m")


def _recent_months(*, end_month: str, count: int) -> List[str]:
    months: List[str] = []
    for offset in range(max(0, count - 1), -1, -1):
        months.append(month_add(end_month, -offset))
    return months


def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _safe_div(n: Optional[float], d: Optional[float]) -> Optional[float]:
    if n is None or d in (None, 0):
        return None
    return n / d


def _median(values: List[float]) -> Optional[float]:
    if not values:
        return None
    vals = sorted(values)
    mid = len(vals) // 2
    if len(vals) % 2 == 1:
        return float(vals[mid])
    return float((vals[mid - 1] + vals[mid]) / 2)


def _stdev(values: List[float]) -> Optional[float]:
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    var = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
    return math.sqrt(var)


def available_months_for_client(client_id: str, *, limit: int = 60) -> List[str]:
    """Return available months (YYYY-MM) from src.database.monthly_data."""
    try:
        from src.database import get_connection

        conn = get_connection()
        rows = conn.execute(
            """SELECT DISTINCT month FROM monthly_data
               WHERE client_id = ?
               ORDER BY month DESC
               LIMIT ?""",
            (client_id, limit),
        ).fetchall()
        conn.close()
        return [r[0] for r in rows]
    except Exception as exc:
        log.debug("available_months_for_client failed: %s", exc)
        return []


def load_current_data_for_month(client_id: str, month: str) -> Dict[str, Any]:
    """Load all sources for client/month from src.database.monthly_data."""
    current_data: Dict[str, Any] = {}
    try:
        from src.database import get_monthly_data

        rows = get_monthly_data(client_id, month)
        for r in rows:
            src = r.get("source")
            if not src:
                continue
            try:
                current_data[src] = json.loads(r.get("data_json") or "{}")
            except Exception:
                current_data[src] = {}
    except Exception as exc:
        log.debug("load_current_data_for_month failed: %s", exc)

    return current_data


def build_analysis_summary_for_month(*, brand: dict, month: str) -> Optional[Dict[str, Any]]:
    """Rebuild analysis (src.analytics) from stored monthly_data and summarize for AI."""
    try:
        from src.analytics import build_full_analysis
        from webapp.ai_assistant import summarize_analysis_for_ai

        client_id = brand.get("slug")
        if not client_id:
            return None

        # Mirror webapp.report_runner._brand_to_client_config (kept local to avoid import cycles)
        goals = brand.get("goals", "[]")
        if isinstance(goals, str):
            try:
                goals = json.loads(goals)
            except Exception:
                goals = []

        client_config = {
            "display_name": brand.get("display_name", ""),
            "industry": brand.get("industry", "plumbing"),
            "monthly_budget": brand.get("monthly_budget", 0),
            "primary_services": [
                s.strip() for s in (brand.get("primary_services", "") or "").split(",") if s.strip()
            ],
            "service_area": brand.get("service_area", ""),
            "goals": goals,
            "brand_voice": brand.get("brand_voice", ""),
            "active_offers": brand.get("active_offers", ""),
            "target_audience": brand.get("target_audience", ""),
            "competitors": brand.get("competitors", ""),
            "reporting_notes": brand.get("reporting_notes", ""),
            "kpi_target_cpa": brand.get("kpi_target_cpa"),
            "kpi_target_leads": brand.get("kpi_target_leads"),
            "kpi_target_roas": brand.get("kpi_target_roas"),
        }

        current_data = load_current_data_for_month(client_id, month)
        if not current_data:
            return None

        analysis = build_full_analysis(client_id, month, current_data, client_config)
        return summarize_analysis_for_ai(analysis)
    except Exception as exc:
        log.debug("build_analysis_summary_for_month failed: %s", exc)
        return None


@dataclass
class ForecastMetric:
    p50: Optional[float]
    low: Optional[float]
    high: Optional[float]


def build_paid_kpi_series(*, brand: dict, months_back: int = 36) -> List[Dict[str, Any]]:
    """Return a list of monthly KPI points usable for seasonality/trend.

    Each point:
      {"month": "YYYY-MM", "paid_spend": float|None, "paid_leads": float|None, "blended_cpa": float|None,
       "ga_sessions": float|None, "gsc_impressions": float|None, "auction_pressure": str|None}

    Note: best-effort. Missing channels become None.
    """
    client_id = brand.get("slug")
    if not client_id:
        return []

    stored_months_desc = available_months_for_client(client_id, limit=max(12, months_back))
    stored_months = list(reversed(stored_months_desc))  # ascending
    if months_back and len(stored_months) > months_back:
        stored_months = stored_months[-months_back:]

    months = list(stored_months)

    # Atlas should be able to use live connected APIs for history too, not only
    # previously stored monthly_data. When stored history is thin, build a small
    # rolling history directly from the connected sources.
    if len(months) < min(9, months_back):
        target_months = _recent_months(end_month=month_now(), count=min(max(months_back, 9), 12))
        months = sorted(set(months).union(target_months))

    series: List[Dict[str, Any]] = []
    for m in months:
        summary = build_analysis_summary_for_month(brand=brand, month=m)
        if not summary:
            try:
                from flask import current_app
                from webapp.report_runner import build_analysis_and_suggestions_for_brand

                db = current_app.db
                analysis, _ = build_analysis_and_suggestions_for_brand(db, brand, m)
                if analysis:
                    from webapp.ai_assistant import summarize_analysis_for_ai

                    summary = summarize_analysis_for_ai(analysis)
            except Exception as exc:
                log.debug("Live history fallback failed for %s %s: %s", client_id, m, exc)
        if not summary:
            continue
        kpis = summary.get("kpis", {}) or {}

        meta_spend = _to_float(((kpis.get("meta") or {}).get("spend")))
        google_spend = _to_float(((kpis.get("google_ads") or {}).get("spend")))
        paid_spend = (meta_spend or 0.0) + (google_spend or 0.0)
        paid_spend = paid_spend if paid_spend > 0 else None

        meta_leads = _to_float(((kpis.get("meta") or {}).get("results")))
        google_leads = _to_float(((kpis.get("google_ads") or {}).get("results")))
        paid_leads = (meta_leads or 0.0) + (google_leads or 0.0)
        paid_leads = paid_leads if paid_leads > 0 else None

        blended_cpa = None
        if paid_spend is not None and paid_leads is not None and paid_leads > 0:
            blended_cpa = round(paid_spend / paid_leads, 2)

        ga_sessions = _to_float(((kpis.get("ga") or {}).get("sessions")))
        gsc_impr = _to_float(((kpis.get("gsc") or {}).get("impressions")))

        # Lightweight "auction pressure" classification based on cost changes if present.
        pressure = None
        cpc_google = _to_float(((kpis.get("google_ads") or {}).get("cpc")))
        cpc_meta = _to_float(((kpis.get("meta") or {}).get("cpc")))
        if (cpc_google and cpc_google > 0) or (cpc_meta and cpc_meta > 0):
            pressure = "unknown"
        # We do not overfit here; the agent prompt will interpret.

        series.append(
            {
                "month": m,
                "paid_spend": paid_spend,
                "paid_leads": paid_leads,
                "blended_cpa": blended_cpa,
                "ga_sessions": ga_sessions,
                "gsc_impressions": gsc_impr,
                "auction_pressure": pressure,
            }
        )

    return series


def seasonal_forecast_next_month(*, series: List[Dict[str, Any]], target_month: str) -> Dict[str, ForecastMetric]:
    """Explainable seasonal naive forecast for paid_leads and paid_spend.

    - Baseline: average of same calendar month in prior years
    - Momentum adjustment: ratio of recent 3-month median vs trailing 12-month median
    - Confidence: low/high from stdev of same-month history (or +-20% fallback)
    """

    def same_calendar_month_points(metric: str) -> List[float]:
        mon = target_month.split("-")[1]
        out: List[float] = []
        for p in series:
            if str(p.get("month", "")).endswith(f"-{mon}"):
                v = _to_float(p.get(metric))
                if v is not None and v > 0:
                    out.append(float(v))
        return out

    def recent_points(metric: str, n: int) -> List[float]:
        vals = []
        for p in series[-n:]:
            v = _to_float(p.get(metric))
            if v is not None and v > 0:
                vals.append(float(v))
        return vals

    def trailing_points(metric: str, n: int) -> List[float]:
        vals = []
        for p in series[-n:]:
            v = _to_float(p.get(metric))
            if v is not None and v > 0:
                vals.append(float(v))
        return vals

    forecasts: Dict[str, ForecastMetric] = {}

    for metric in ("paid_leads", "paid_spend"):
        seasonal_vals = same_calendar_month_points(metric)
        baseline = sum(seasonal_vals) / len(seasonal_vals) if seasonal_vals else None

        recent_med = _median(recent_points(metric, 3))
        trailing_med = _median(trailing_points(metric, 12))
        momentum = None
        if recent_med is not None and trailing_med not in (None, 0):
            momentum = max(0.7, min(1.3, recent_med / trailing_med))

        p50 = baseline
        if p50 is not None and momentum is not None:
            p50 = p50 * momentum

        sd = _stdev(seasonal_vals)
        if p50 is None:
            forecasts[metric] = ForecastMetric(p50=None, low=None, high=None)
            continue

        if sd is not None and sd > 0:
            low = max(0.0, p50 - sd)
            high = p50 + sd
        else:
            low = max(0.0, p50 * 0.8)
            high = p50 * 1.2

        forecasts[metric] = ForecastMetric(
            p50=round(p50, 2),
            low=round(low, 2),
            high=round(high, 2),
        )

    # Derived blended_cpa
    spend = forecasts.get("paid_spend")
    leads = forecasts.get("paid_leads")
    cpa_p50 = _safe_div(spend.p50 if spend else None, leads.p50 if leads else None)
    cpa_low = _safe_div(spend.low if spend else None, leads.high if leads else None)
    cpa_high = _safe_div(spend.high if spend else None, leads.low if leads else None)

    forecasts["blended_cpa"] = ForecastMetric(
        p50=round(cpa_p50, 2) if cpa_p50 is not None else None,
        low=round(cpa_low, 2) if cpa_low is not None else None,
        high=round(cpa_high, 2) if cpa_high is not None else None,
    )

    return forecasts
