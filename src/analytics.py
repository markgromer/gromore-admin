"""
Analytics / Interpretation Engine

Takes parsed data from all three sources, compares against industry benchmarks
and prior months, and produces a structured analysis with scored metrics.
"""
import calendar
import json
from datetime import date, datetime
from pathlib import Path
from . import database as db


def load_benchmarks():
    benchmarks_path = Path(__file__).parent.parent / "config" / "benchmarks.json"
    with open(benchmarks_path, "r") as f:
        return json.load(f)


def pct_change(current, previous):
    """Calculate percentage change. Returns None if previous is 0 or None."""
    if not previous or previous == 0:
        return None
    return round(((current - previous) / previous) * 100, 1)


def _month_progress(month, today=None):
    today = today or date.today()
    try:
        month_dt = datetime.strptime(month, "%Y-%m")
    except (TypeError, ValueError):
        return {
            "month": month,
            "is_current_month": False,
            "elapsed_days": None,
            "days_in_month": None,
            "progress_pct": None,
            "early_month": False,
        }
    days_in_month = calendar.monthrange(month_dt.year, month_dt.month)[1]
    is_current = today.year == month_dt.year and today.month == month_dt.month
    elapsed_days = max(1, min(today.day, days_in_month)) if is_current else days_in_month
    progress_pct = round(elapsed_days / days_in_month * 100, 1) if days_in_month else None
    return {
        "month": month,
        "is_current_month": is_current,
        "elapsed_days": elapsed_days,
        "days_in_month": days_in_month,
        "progress_pct": progress_pct,
        "early_month": bool(is_current and elapsed_days <= 7),
    }


def _attach_period(analysis, month):
    if isinstance(analysis, dict):
        analysis["period"] = _month_progress(month)
    return analysis


def _suppress_early_month_volume_drop(month, metric, change):
    if change is None or change >= 0:
        return False
    if metric not in {"sessions", "users", "new_users", "conversions", "pageviews", "clicks", "impressions", "reach", "results"}:
        return False
    progress = _month_progress(month)
    return bool(progress.get("early_month"))


def _is_organic_source(source_name):
    text = str(source_name or "").strip().lower()
    if not text:
        return False
    return (
        "organic" in text
        or "/ organic" in text
        or text.endswith(" organic")
        or text in {"google", "bing", "yahoo", "duckduckgo"}
    )


def score_metric(value, benchmark, higher_is_better=True):
    """
    Score a metric against benchmark.
    Returns: 'excellent', 'good', 'average', 'below_average', 'poor'
    """
    if value is None or benchmark is None:
        return "no_data"

    if higher_is_better:
        ratio = value / benchmark if benchmark != 0 else 0
    else:
        ratio = benchmark / value if value != 0 else 0

    if ratio >= 1.3:
        return "excellent"
    elif ratio >= 1.1:
        return "good"
    elif ratio >= 0.9:
        return "average"
    elif ratio >= 0.7:
        return "below_average"
    else:
        return "poor"


def _to_float(value, default=0.0):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _build_lead_pacing(month, target_leads, actual_leads, today=None):
    today = today or date.today()
    actual_leads = round(_to_float(actual_leads, 0.0), 2)
    target_leads = round(_to_float(target_leads, 0.0), 2)
    if target_leads <= 0:
        return None

    pacing = {
        "is_current_month": False,
        "expected_to_date": target_leads,
        "elapsed_days": None,
        "days_in_month": None,
        "pace_ratio": None,
        "status": "full_month",
        "label": "Full-month target",
        "on_track": actual_leads >= target_leads,
    }

    try:
        month_dt = datetime.strptime(month, "%Y-%m")
    except (TypeError, ValueError):
        return pacing

    days_in_month = calendar.monthrange(month_dt.year, month_dt.month)[1]
    pacing["days_in_month"] = days_in_month

    if today.year == month_dt.year and today.month == month_dt.month:
        elapsed_days = max(1, min(today.day, days_in_month))
        expected_to_date = round(target_leads * (elapsed_days / days_in_month), 2)
        pace_ratio = round(actual_leads / expected_to_date, 2) if expected_to_date > 0 else None

        if actual_leads >= expected_to_date * 1.15:
            status = "ahead"
            label = "Ahead of pace"
            on_track = True
        elif actual_leads >= expected_to_date * 0.9:
            status = "on_track"
            label = "On pace"
            on_track = True
        elif actual_leads >= expected_to_date * 0.75:
            status = "watch"
            label = "Slightly behind pace"
            on_track = False
        else:
            status = "at_risk"
            label = "Behind pace"
            on_track = False

        pacing.update({
            "is_current_month": True,
            "expected_to_date": expected_to_date,
            "elapsed_days": elapsed_days,
            "pace_ratio": pace_ratio,
            "status": status,
            "label": label,
            "on_track": on_track,
        })

    return pacing


def _parse_competitors(raw_competitors):
    if not raw_competitors:
        return []
    if isinstance(raw_competitors, list):
        return [str(item).strip() for item in raw_competitors if str(item).strip()]
    return [item.strip() for item in str(raw_competitors).split(",") if item.strip()]


def _competitor_tokens(name):
    lowered = (name or "").lower().replace("https://", "").replace("http://", "")
    lowered = lowered.replace("www.", "").replace(".com", " ").replace(".net", " ").replace(".org", " ")
    return [token for token in lowered.replace("-", " ").split() if len(token) >= 3]


def _build_keyword_recommendations(gsc_data, opportunities):
    recommendations = []
    top_queries = gsc_data.get("top_queries", []) if isinstance(gsc_data, dict) else []

    for row in (opportunities or [])[:8]:
        query = str(row.get("query", "")).strip()
        if not query:
            continue
        recommendations.append({
            "keyword": query,
            "impressions": int(row.get("impressions", 0) or 0),
            "clicks": int(row.get("clicks", 0) or 0),
            "ctr": row.get("ctr", 0) or 0,
            "position": row.get("position", 0) or 0,
            "reason": "High-impression query ranking below top 3",
            "recommended_action": "Create or improve a dedicated page and strengthen internal links for this term.",
        })

    if len(recommendations) < 8:
        low_ctr_queries = sorted(
            [
                q for q in top_queries
                if (q.get("impressions", 0) or 0) >= 50 and (q.get("ctr", 0) or 0) <= 2.0
            ],
            key=lambda item: item.get("impressions", 0),
            reverse=True,
        )
        for row in low_ctr_queries:
            if len(recommendations) >= 8:
                break
            query = str(row.get("query", "")).strip()
            if not query or any(r.get("keyword") == query for r in recommendations):
                continue
            recommendations.append({
                "keyword": query,
                "impressions": int(row.get("impressions", 0) or 0),
                "clicks": int(row.get("clicks", 0) or 0),
                "ctr": row.get("ctr", 0) or 0,
                "position": row.get("position", 0) or 0,
                "reason": "High visibility but low click-through rate",
                "recommended_action": "Rewrite title/meta description to better match search intent and increase clicks.",
            })

    return recommendations


def _build_competitor_watch(client_config, gsc_analysis, meta_analysis, google_ads_analysis):
    competitors = _parse_competitors((client_config or {}).get("competitors"))
    if not competitors:
        return None

    signals = []
    counter_moves = []
    matched_queries = []

    gsc_top_queries = (gsc_analysis or {}).get("top_queries", []) if isinstance(gsc_analysis, dict) else []
    for competitor in competitors:
        tokens = _competitor_tokens(competitor)
        if not tokens:
            continue
        for query_row in gsc_top_queries:
            query = str(query_row.get("query", "")).lower()
            if query and any(token in query for token in tokens):
                matched_queries.append({
                    "competitor": competitor,
                    "query": query_row.get("query", ""),
                    "impressions": int(query_row.get("impressions", 0) or 0),
                    "clicks": int(query_row.get("clicks", 0) or 0),
                    "position": query_row.get("position", 0),
                })

    if matched_queries:
        total_impressions = sum(item.get("impressions", 0) for item in matched_queries)
        signals.append({
            "severity": "high" if total_impressions >= 100 else "medium",
            "title": "Competitor-branded demand detected in Google search",
            "detail": f"Detected {len(matched_queries)} competitor-related queries with {total_impressions} impressions.",
        })
        counter_moves.append({
            "priority": "high",
            "title": "Build competitor comparison pages",
            "detail": "Publish pages that position your offer against competitor alternatives and include clear proof points, reviews, and pricing clarity.",
        })
        counter_moves.append({
            "priority": "high",
            "title": "Defend branded and high-intent terms",
            "detail": "Protect your most valuable branded/service keywords with exact-match coverage and message-match landing pages.",
        })

    ads_cpc_pressure = False
    ads_pressure_details = []

    if isinstance(google_ads_analysis, dict):
        cpc_mom = (google_ads_analysis.get("month_over_month", {}).get("cpc", {}) or {}).get("change_pct")
        if cpc_mom is not None and cpc_mom >= 15:
            ads_cpc_pressure = True
            ads_pressure_details.append(f"Google Ads CPC up {cpc_mom}%")

    if isinstance(meta_analysis, dict):
        meta_cpm_mom = (meta_analysis.get("month_over_month", {}).get("cpm", {}) or {}).get("change_pct")
        if meta_cpm_mom is not None and meta_cpm_mom >= 20:
            ads_cpc_pressure = True
            ads_pressure_details.append(f"Meta CPM up {meta_cpm_mom}%")

    if ads_cpc_pressure:
        signals.append({
            "severity": "medium",
            "title": "Paid auction pressure is increasing",
            "detail": ", ".join(ads_pressure_details),
        })
        counter_moves.append({
            "priority": "medium",
            "title": "Counter rising auction costs",
            "detail": "Tighten match types, expand negative keywords weekly, and shift spend to campaigns with best conversion efficiency.",
        })

    if not signals:
        signals.append({
            "severity": "low",
            "title": "No major competitor pressure signal this month",
            "detail": "Continue monitoring branded search overlap and paid CPC/CPM trends monthly.",
        })
        counter_moves.append({
            "priority": "low",
            "title": "Keep a monthly competitor pulse",
            "detail": "Track competitor query overlap, ad cost trends, and messaging changes each month to catch early shifts.",
        })

    return {
        "competitors": competitors,
        "signals": signals,
        "matched_queries": sorted(matched_queries, key=lambda item: item.get("impressions", 0), reverse=True)[:12],
        "counter_moves": counter_moves,
    }


def analyze_google_analytics(ga_data, prev_ga_data, benchmarks_website, month=None):
    """Analyze Google Analytics data against benchmarks and prior month."""
    if not ga_data:
        return None

    totals = ga_data.get("totals", {})
    prev_totals = prev_ga_data.get("totals", {}) if prev_ga_data else {}

    analysis = {
        "metrics": {},
        "month_over_month": {},
        "scores": {},
        "highlights": [],
        "concerns": [],
    }

    # Analyze each key metric
    metrics_config = {
        "sessions": {"higher_is_better": True, "benchmark_key": None},
        "users": {"higher_is_better": True, "benchmark_key": None},
        "new_users": {"higher_is_better": True, "benchmark_key": None},
        "bounce_rate": {"higher_is_better": False, "benchmark_key": "bounce_rate_good"},
        "pages_per_session": {"higher_is_better": True, "benchmark_key": "pages_per_session_good"},
        "avg_session_duration": {"higher_is_better": True, "benchmark_key": "avg_session_duration_good"},
        "conversions": {"higher_is_better": True, "benchmark_key": None},
        "conversion_rate": {"higher_is_better": True, "benchmark_key": "conversion_rate_good"},
        "pageviews": {"higher_is_better": True, "benchmark_key": None},
    }

    for metric, config in metrics_config.items():
        current_val = totals.get(metric)
        prev_val = prev_totals.get(metric)

        if current_val is not None:
            analysis["metrics"][metric] = current_val

            # Month over month
            if prev_val is not None:
                change = pct_change(current_val, prev_val)
                analysis["month_over_month"][metric] = {
                    "previous": prev_val,
                    "current": current_val,
                    "change_pct": change,
                }

                # Flag significant changes
                if change is not None and not _suppress_early_month_volume_drop(month, metric, change):
                    if config["higher_is_better"]:
                        if change >= 15:
                            analysis["highlights"].append(
                                f"{metric.replace('_', ' ').title()} increased {change}% month-over-month"
                            )
                        elif change <= -15:
                            analysis["concerns"].append(
                                f"{metric.replace('_', ' ').title()} decreased {abs(change)}% month-over-month"
                            )
                    else:
                        if change <= -10:
                            analysis["highlights"].append(
                                f"{metric.replace('_', ' ').title()} improved (dropped {abs(change)}%) month-over-month"
                            )
                        elif change >= 10:
                            analysis["concerns"].append(
                                f"{metric.replace('_', ' ').title()} worsened (increased {change}%) month-over-month"
                            )

            # Benchmark scoring
            if config["benchmark_key"] and benchmarks_website:
                benchmark_val = benchmarks_website.get(config["benchmark_key"])
                if benchmark_val is not None:
                    analysis["scores"][metric] = score_metric(
                        current_val, benchmark_val, config["higher_is_better"]
                    )

    # Source/medium analysis
    by_source = ga_data.get("by_source", {})
    if by_source:
        # Find top traffic sources
        source_sessions = {k: v.get("sessions", 0) for k, v in by_source.items()}
        sorted_sources = sorted(source_sessions.items(), key=lambda x: x[1], reverse=True)
        analysis["top_sources"] = [
            {
                "source": source_name,
                "sessions": sessions_count,
                "conversions": (by_source.get(source_name) or {}).get("conversions", 0),
            }
            for source_name, sessions_count in sorted_sources[:10]
        ]

        # Find best converting sources
        source_conversions = {
            k: v.get("conversions", 0) for k, v in by_source.items() if v.get("conversions", 0) > 0
        }
        sorted_conversions = sorted(source_conversions.items(), key=lambda x: x[1], reverse=True)
        analysis["top_converting_sources"] = [
            {"source": s, "conversions": c} for s, c in sorted_conversions[:5]
        ]

        organic_sources = [
            {
                "source": source_name,
                "sessions": int((source_data or {}).get("sessions") or 0),
                "users": int((source_data or {}).get("users") or 0),
                "conversions": int((source_data or {}).get("conversions") or 0),
            }
            for source_name, source_data in by_source.items()
            if _is_organic_source(source_name)
        ]
        organic_sources.sort(key=lambda row: row.get("sessions", 0), reverse=True)
        organic_sessions = sum(row.get("sessions", 0) for row in organic_sources)
        organic_users = sum(row.get("users", 0) for row in organic_sources)
        organic_conversions = sum(row.get("conversions", 0) for row in organic_sources)
        if organic_sources or organic_sessions > 0:
            analysis["organic_search"] = {
                "sessions": organic_sessions,
                "users": organic_users,
                "conversions": organic_conversions,
                "conversion_rate": round((organic_conversions / organic_sessions) * 100, 2) if organic_sessions else 0.0,
                "sources": organic_sources[:10],
            }

    by_page = ga_data.get("by_page", [])
    if by_page:
        analysis["top_landing_pages"] = sorted(
            by_page,
            key=lambda row: (row.get("sessions", 0), row.get("conversions", 0)),
            reverse=True,
        )[:10]

    by_device = ga_data.get("by_device") or []
    if by_device:
        analysis["device_breakdown"] = sorted(
            by_device,
            key=lambda row: row.get("sessions", 0),
            reverse=True,
        )[:10]

    by_city = ga_data.get("by_city") or []
    if by_city:
        analysis["top_cities"] = sorted(
            by_city,
            key=lambda row: row.get("sessions", 0),
            reverse=True,
        )[:10]

    top_events = ga_data.get("top_events") or []
    if top_events:
        analysis["top_events"] = sorted(
            top_events,
            key=lambda row: row.get("event_count", 0),
            reverse=True,
        )[:15]

    return _attach_period(analysis, month)


def analyze_meta_business(meta_data, prev_meta_data, benchmarks_meta, industry, month=None):
    """Analyze Meta Business Suite data against benchmarks and prior month."""
    if not meta_data:
        return None

    totals = meta_data.get("totals", {})
    prev_totals = prev_meta_data.get("totals", {}) if prev_meta_data else {}
    industry_benchmarks = benchmarks_meta.get(industry, {})

    analysis = {
        "metrics": {},
        "month_over_month": {},
        "scores": {},
        "highlights": [],
        "concerns": [],
        "campaign_analysis": [],
        "top_ads": [],
    }

    # Key metrics
    metrics_config = {
        "impressions": {"higher_is_better": True, "benchmark_key": None},
        "reach": {"higher_is_better": True, "benchmark_key": None},
        "clicks": {"higher_is_better": True, "benchmark_key": None},
        "ctr": {"higher_is_better": True, "benchmark_key": "ctr"},
        "cpc": {"higher_is_better": False, "benchmark_key": "cpc"},
        "cpm": {"higher_is_better": False, "benchmark_key": "cpm"},
        "spend": {"higher_is_better": None, "benchmark_key": None},
        "results": {"higher_is_better": True, "benchmark_key": None},
        "cost_per_result": {"higher_is_better": False, "benchmark_key": None},
        "frequency": {"higher_is_better": None, "benchmark_key": "frequency_cap"},
    }

    for metric, config in metrics_config.items():
        current_val = totals.get(metric)
        prev_val = prev_totals.get(metric)

        if current_val is not None:
            analysis["metrics"][metric] = current_val

            if prev_val is not None and config["higher_is_better"] is not None:
                change = pct_change(current_val, prev_val)
                analysis["month_over_month"][metric] = {
                    "previous": prev_val,
                    "current": current_val,
                    "change_pct": change,
                }
                if change is not None and not _suppress_early_month_volume_drop(month, metric, change):
                    if config["higher_is_better"]:
                        if change >= 15:
                            analysis["highlights"].append(
                                f"Meta {metric.upper().replace('_', ' ')} increased {change}% MoM"
                            )
                        elif change <= -15:
                            analysis["concerns"].append(
                                f"Meta {metric.upper().replace('_', ' ')} decreased {abs(change)}% MoM"
                            )
                    else:
                        if change <= -10:
                            analysis["highlights"].append(
                                f"Meta {metric.upper().replace('_', ' ')} improved (dropped {abs(change)}%) MoM"
                            )
                        elif change >= 15:
                            analysis["concerns"].append(
                                f"Meta {metric.upper().replace('_', ' ')} worsened (increased {change}%) MoM"
                            )

            if config["benchmark_key"] and industry_benchmarks:
                benchmark_val = industry_benchmarks.get(config["benchmark_key"])
                if benchmark_val is not None:
                    higher = config["higher_is_better"]
                    if metric == "frequency":
                        higher = False  # Lower frequency is better (below cap)
                    if higher is not None:
                        analysis["scores"][metric] = score_metric(
                            current_val, benchmark_val, higher
                        )

    # Frequency warning
    freq = totals.get("frequency", 0)
    freq_cap = industry_benchmarks.get("frequency_cap", 3.5)
    if freq > freq_cap:
        analysis["concerns"].append(
            f"Ad frequency ({freq}) exceeds recommended cap ({freq_cap}). Audience fatigue risk."
        )

    # Campaign-level analysis
    by_campaign = meta_data.get("by_campaign", {})
    for camp_name, camp_data in by_campaign.items():
        camp_analysis = {"name": camp_name, "metrics": camp_data, "status": "ok"}

        # Flag underperforming campaigns
        camp_ctr = camp_data.get("ctr", 0)
        camp_cpc = camp_data.get("cpc", 0)
        camp_cpr = camp_data.get("cost_per_result")

        if industry_benchmarks:
            if camp_ctr < industry_benchmarks.get("ctr", 0) * 0.7:
                camp_analysis["status"] = "underperforming"
                camp_analysis["issue"] = f"CTR ({camp_ctr}%) well below benchmark ({industry_benchmarks.get('ctr')}%)"
            if camp_cpc > industry_benchmarks.get("cpc", 999) * 1.3:
                camp_analysis["status"] = "underperforming"
                camp_analysis["issue"] = f"CPC (${camp_cpc}) well above benchmark (${industry_benchmarks.get('cpc')})"

        analysis["campaign_analysis"].append(camp_analysis)

    top_ads = meta_data.get("top_ads", [])
    if top_ads:
        analysis["top_ads"] = top_ads[:10]

    return _attach_period(analysis, month)


def analyze_google_ads(google_ads_data, prev_google_ads_data, benchmarks_ads, industry, month=None):
    """Analyze Google Ads data against benchmarks and prior month."""
    if not google_ads_data:
        return None

    totals = google_ads_data.get("totals", {})
    prev_totals = prev_google_ads_data.get("totals", {}) if prev_google_ads_data else {}
    industry_benchmarks = benchmarks_ads.get(industry, {})

    analysis = {
        "metrics": {},
        "month_over_month": {},
        "scores": {},
        "highlights": [],
        "concerns": [],
        "campaign_analysis": [],
        "search_terms": [],
    }

    search_terms = google_ads_data.get("search_terms") or []
    if isinstance(search_terms, list):
        analysis["search_terms"] = search_terms[:50]

    metrics_config = {
        "impressions": {"higher_is_better": True, "benchmark_key": None},
        "clicks": {"higher_is_better": True, "benchmark_key": None},
        "spend": {"higher_is_better": None, "benchmark_key": None},
        "ctr": {"higher_is_better": True, "benchmark_key": "ctr"},
        "cpc": {"higher_is_better": False, "benchmark_key": "cpc"},
        "results": {"higher_is_better": True, "benchmark_key": None},
        "cost_per_result": {"higher_is_better": False, "benchmark_key": "cpa"},
    }

    for metric, config in metrics_config.items():
        current_val = totals.get(metric)
        prev_val = prev_totals.get(metric)

        if current_val is not None:
            analysis["metrics"][metric] = current_val

            if prev_val is not None and config["higher_is_better"] is not None:
                change = pct_change(current_val, prev_val)
                analysis["month_over_month"][metric] = {
                    "previous": prev_val,
                    "current": current_val,
                    "change_pct": change,
                }

            if config["benchmark_key"] and industry_benchmarks:
                benchmark_val = industry_benchmarks.get(config["benchmark_key"])
                if benchmark_val is not None:
                    analysis["scores"][metric] = score_metric(
                        current_val, benchmark_val, config["higher_is_better"]
                    )

    by_campaign = google_ads_data.get("by_campaign", {})
    for campaign_name, campaign_data in by_campaign.items():
        campaign_analysis = {"name": campaign_name, "metrics": campaign_data, "status": "ok"}
        ctr = campaign_data.get("ctr", 0)
        cpc = campaign_data.get("cpc", 0)
        cpr = campaign_data.get("cost_per_result", 0)

        if industry_benchmarks:
            if ctr and ctr < industry_benchmarks.get("ctr", 0) * 0.7:
                campaign_analysis["status"] = "underperforming"
                campaign_analysis["issue"] = (
                    f"CTR ({ctr}%) below benchmark ({industry_benchmarks.get('ctr')}%)"
                )
            if cpc and cpc > industry_benchmarks.get("cpc", 999) * 1.3:
                campaign_analysis["status"] = "underperforming"
                campaign_analysis["issue"] = (
                    f"CPC (${cpc}) above benchmark (${industry_benchmarks.get('cpc')})"
                )
            if cpr and cpr > industry_benchmarks.get("cpa", 999) * 1.3:
                campaign_analysis["status"] = "underperforming"
                campaign_analysis["issue"] = (
                    f"CPA (${cpr}) above benchmark (${industry_benchmarks.get('cpa')})"
                )

        analysis["campaign_analysis"].append(campaign_analysis)

    return _attach_period(analysis, month)


def analyze_search_console(gsc_data, prev_gsc_data, benchmarks_seo, industry, month=None):
    """Analyze Search Console data against benchmarks and prior month."""
    if not gsc_data:
        return None

    totals = gsc_data.get("totals", {})
    prev_totals = prev_gsc_data.get("totals", {}) if prev_gsc_data else {}
    industry_benchmarks = benchmarks_seo.get(industry, {})

    analysis = {
        "metrics": {},
        "month_over_month": {},
        "scores": {},
        "highlights": [],
        "concerns": [],
    }

    metrics_config = {
        "clicks": {"higher_is_better": True, "benchmark_key": None},
        "impressions": {"higher_is_better": True, "benchmark_key": None},
        "ctr": {"higher_is_better": True, "benchmark_key": "avg_ctr"},
        "avg_position": {"higher_is_better": False, "benchmark_key": "target_position"},
    }

    for metric, config in metrics_config.items():
        current_val = totals.get(metric)
        prev_val = prev_totals.get(metric)

        if current_val is not None:
            analysis["metrics"][metric] = current_val

            if prev_val is not None:
                change = pct_change(current_val, prev_val)
                analysis["month_over_month"][metric] = {
                    "previous": prev_val,
                    "current": current_val,
                    "change_pct": change,
                }
                if change is not None and not _suppress_early_month_volume_drop(month, metric, change):
                    if config["higher_is_better"]:
                        if change >= 15:
                            analysis["highlights"].append(
                                f"Organic {metric} increased {change}% MoM"
                            )
                        elif change <= -15:
                            analysis["concerns"].append(
                                f"Organic {metric} decreased {abs(change)}% MoM"
                            )
                    else:
                        if change <= -5:  # Position going down (lower) is good
                            analysis["highlights"].append(
                                f"Search position improved from {prev_val} to {current_val} (top queries)"
                            )
                        elif change >= 5:
                            analysis["concerns"].append(
                                f"Search position worsened from {prev_val} to {current_val} (top queries)"
                            )

            if config["benchmark_key"] and industry_benchmarks:
                benchmark_val = industry_benchmarks.get(config["benchmark_key"])
                if benchmark_val is not None:
                    analysis["scores"][metric] = score_metric(
                        current_val, benchmark_val, config["higher_is_better"]
                    )

    # Top queries analysis
    analysis["top_queries"] = gsc_data.get("top_queries", [])[:20]

    # Opportunity analysis
    opportunities = gsc_data.get("opportunity_queries", [])
    analysis["keyword_opportunities"] = opportunities[:10]
    analysis["keyword_recommendations"] = _build_keyword_recommendations(gsc_data, opportunities)

    if len(opportunities) > 5:
        analysis["highlights"].append(
            f"Found {len(opportunities)} keyword opportunities (high impressions, position 4-20)"
        )

    # Top pages
    analysis["top_pages"] = gsc_data.get("top_pages", [])[:15]
    analysis["query_pages"] = gsc_data.get("query_pages", [])[:50]

    return _attach_period(analysis, month)


def build_full_analysis(client_id, month, current_data, client_config):
    """
    Build a complete analysis combining all data sources.

    Args:
        client_id: Client identifier
        month: Month string (YYYY-MM)
        current_data: Dict with parsed data from parsers.load_client_data()
        client_config: Client config from clients.json

    Returns:
        Dict with full analysis results
    """
    benchmarks = load_benchmarks()
    industry = client_config.get("industry", "plumbing")
    prev_month = db.get_previous_month(month)

    # Load previous month data from database
    prev_data = {}
    for source in ["google_analytics", "meta_business", "search_console", "google_ads", "crm_revenue"]:
        prev_rows = db.get_monthly_data(client_id, prev_month, source)
        if prev_rows:
            prev_data[source] = json.loads(prev_rows[0]["data_json"])

    # Run analyses
    website_benchmarks = benchmarks.get("website", {})
    industry_website = website_benchmarks.get(industry, website_benchmarks.get("_default", {}))
    ga_analysis = analyze_google_analytics(
        current_data.get("google_analytics"),
        prev_data.get("google_analytics"),
        industry_website,
        month,
    )

    meta_analysis = analyze_meta_business(
        current_data.get("meta_business"),
        prev_data.get("meta_business"),
        benchmarks.get("meta_ads", {}),
        industry,
        month,
    )

    gsc_analysis = analyze_search_console(
        current_data.get("search_console"),
        prev_data.get("search_console"),
        benchmarks.get("seo", {}),
        industry,
        month,
    )

    google_ads_analysis = analyze_google_ads(
        current_data.get("google_ads"),
        prev_data.get("google_ads"),
        benchmarks.get("google_ads", {}),
        industry,
        month,
    )

    # Aggregate highlights and concerns
    all_highlights = []
    all_concerns = []
    for analysis in [ga_analysis, meta_analysis, gsc_analysis, google_ads_analysis]:
        if analysis:
            all_highlights.extend(analysis.get("highlights", []))
            all_concerns.extend(analysis.get("concerns", []))

    # Overall health score (simple weighted scoring)
    total_score = 0
    score_count = 0
    score_values = {"excellent": 5, "good": 4, "average": 3, "below_average": 2, "poor": 1}
    for analysis in [ga_analysis, meta_analysis, gsc_analysis, google_ads_analysis]:
        if analysis:
            for metric, score in analysis.get("scores", {}).items():
                if score in score_values:
                    total_score += score_values[score]
                    score_count += 1

    overall_score = round(total_score / score_count, 1) if score_count > 0 else None
    overall_grade = "N/A"
    if overall_score:
        if overall_score >= 4.5:
            overall_grade = "A"
        elif overall_score >= 3.5:
            overall_grade = "B"
        elif overall_score >= 2.5:
            overall_grade = "C"
        elif overall_score >= 1.5:
            overall_grade = "D"
        else:
            overall_grade = "F"

    # Calculate ROAS if we have spend and revenue/results
    roas_data = {}
    meta_spend = 0
    if meta_analysis:
        meta_spend = meta_analysis.get("metrics", {}).get("spend", 0)

    google_ads_spend = 0
    if google_ads_analysis:
        google_ads_spend = google_ads_analysis.get("metrics", {}).get("spend", 0)

    total_spend = meta_spend + google_ads_spend

    meta_results = 0
    if meta_analysis:
        meta_results = meta_analysis.get("metrics", {}).get("results", 0)

    google_ads_results = 0
    if google_ads_analysis:
        google_ads_results = google_ads_analysis.get("metrics", {}).get("results", 0)

    ga_conversions = 0
    if ga_analysis:
        ga_conversions = ga_analysis.get("metrics", {}).get("conversions", 0)

    total_conversions = meta_results + google_ads_results + ga_conversions
    if total_spend > 0 and total_conversions > 0:
        roas_data["total_spend"] = total_spend
        roas_data["total_conversions"] = total_conversions
        roas_data["cost_per_conversion"] = round(total_spend / total_conversions, 2)

    crm_revenue = current_data.get("crm_revenue") or {}
    attributed_revenue = _to_float((crm_revenue.get("totals") or {}).get("revenue"), 0.0)
    blended_roas = round(attributed_revenue / total_spend, 2) if total_spend > 0 and attributed_revenue > 0 else None
    if attributed_revenue > 0:
        roas_data["attributed_revenue"] = round(attributed_revenue, 2)
    if blended_roas is not None:
        roas_data["blended_roas"] = blended_roas

    # KPI target intelligence (brand-specific, not generic)
    target_cpa = _to_float(client_config.get("kpi_target_cpa"), 0.0)
    target_leads = _to_float(client_config.get("kpi_target_leads"), 0.0)
    target_roas = _to_float(client_config.get("kpi_target_roas"), 0.0)

    paid_spend = round(total_spend, 2)
    paid_leads = round(meta_results + google_ads_results, 2)
    blended_cpa = round((paid_spend / paid_leads), 2) if paid_spend > 0 and paid_leads > 0 else None

    kpi_status = {
        "targets": {
            "cpa": target_cpa if target_cpa > 0 else None,
            "leads": target_leads if target_leads > 0 else None,
            "roas": target_roas if target_roas > 0 else None,
        },
        "actual": {
            "paid_spend": paid_spend,
            "paid_leads": paid_leads,
            "blended_cpa": blended_cpa,
            "attributed_revenue": round(attributed_revenue, 2),
            "blended_roas": blended_roas,
        },
        "evaluation": {},
    }

    if target_cpa > 0 and blended_cpa is not None:
        cpa_gap_pct = round(((blended_cpa - target_cpa) / target_cpa) * 100, 1)
        cpa_on_track = blended_cpa <= target_cpa
        kpi_status["evaluation"]["cpa"] = {
            "target": target_cpa,
            "actual": blended_cpa,
            "gap_pct": cpa_gap_pct,
            "on_track": cpa_on_track,
        }
        if cpa_on_track:
            all_highlights.append(
                f"Blended paid CPA is ${blended_cpa} vs target ${target_cpa} ({abs(cpa_gap_pct)}% better than target)"
            )
        else:
            all_concerns.append(
                f"Blended paid CPA is ${blended_cpa} vs target ${target_cpa} ({cpa_gap_pct}% above target)"
            )

    if target_leads > 0:
        lead_pacing = _build_lead_pacing(month, target_leads, paid_leads)
        comparison_target = lead_pacing.get("expected_to_date") if lead_pacing else target_leads
        lead_gap_pct = round(((paid_leads - comparison_target) / comparison_target) * 100, 1) if comparison_target else None
        leads_on_track = bool(lead_pacing.get("on_track")) if lead_pacing else paid_leads >= target_leads
        kpi_status["evaluation"]["leads"] = {
            "target": target_leads,
            "actual": paid_leads,
            "gap_pct": lead_gap_pct,
            "on_track": leads_on_track,
            "expected_to_date": lead_pacing.get("expected_to_date") if lead_pacing else None,
            "elapsed_days": lead_pacing.get("elapsed_days") if lead_pacing else None,
            "days_in_month": lead_pacing.get("days_in_month") if lead_pacing else None,
            "pace_ratio": lead_pacing.get("pace_ratio") if lead_pacing else None,
            "pace_status": lead_pacing.get("status") if lead_pacing else "full_month",
            "pace_label": lead_pacing.get("label") if lead_pacing else "Full-month target",
            "is_current_month": bool(lead_pacing.get("is_current_month")) if lead_pacing else False,
        }
        if lead_pacing and lead_pacing.get("is_current_month"):
            expected_to_date = lead_pacing.get("expected_to_date") or 0
            pace_label = (lead_pacing.get("label") or "On pace").lower()
            if leads_on_track:
                all_highlights.append(
                    f"Paid leads are {paid_leads} against a paced target of {expected_to_date} by day {lead_pacing.get('elapsed_days')} ({pace_label})"
                )
            else:
                all_concerns.append(
                    f"Paid leads are {paid_leads} against a paced target of {expected_to_date} by day {lead_pacing.get('elapsed_days')} ({pace_label})"
                )
        elif leads_on_track:
            all_highlights.append(
                f"Paid leads are {paid_leads} vs target {target_leads} (+{lead_gap_pct}%)"
            )
        else:
            all_concerns.append(
                f"Paid leads are {paid_leads} vs target {target_leads} ({abs(lead_gap_pct)}% below target)"
            )

    if target_roas > 0:
        if blended_roas is not None:
            roas_gap_pct = round(((blended_roas - target_roas) / target_roas) * 100, 1)
            roas_on_track = blended_roas >= target_roas
            kpi_status["evaluation"]["roas"] = {
                "target": target_roas,
                "actual": blended_roas,
                "gap_pct": roas_gap_pct,
                "on_track": roas_on_track,
            }
            if roas_on_track:
                all_highlights.append(
                    f"Blended ROAS is {blended_roas}x vs target {target_roas}x (+{roas_gap_pct}%)"
                )
            else:
                all_concerns.append(
                    f"Blended ROAS is {blended_roas}x vs target {target_roas}x ({abs(roas_gap_pct)}% below target)"
                )
        else:
            kpi_status["evaluation"]["roas"] = {
                "target": target_roas,
                "actual": None,
                "gap_pct": None,
                "on_track": None,
                "note": "ROAS target set; add monthly revenue in Revenue Tracking to score ROAS.",
            }

    paid_summary = {
        "channels": {
            "meta": {
                "spend": round(meta_spend, 2),
                "results": round(meta_results, 2),
            },
            "google_ads": {
                "spend": round(google_ads_spend, 2),
                "results": round(google_ads_results, 2),
            },
        },
        "total_paid_spend": paid_spend,
        "total_paid_leads": paid_leads,
        "blended_cpa": blended_cpa,
    }

    competitor_watch = _build_competitor_watch(
        client_config,
        gsc_analysis,
        meta_analysis,
        google_ads_analysis,
    )

    facebook_organic = current_data.get("facebook_organic")
    if isinstance(facebook_organic, dict):
        facebook_organic = dict(facebook_organic)
        facebook_period = _month_progress(month)
        facebook_organic["period"] = facebook_period
        metrics = dict(facebook_organic.get("metrics") or {})
        metrics["period"] = facebook_period
        facebook_organic["metrics"] = metrics

    analysis_payload = {
        "client_id": client_id,
        "month": month,
        "client_config": client_config,
        "industry": industry,
        "google_analytics": ga_analysis,
        "meta_business": meta_analysis,
        "google_ads": google_ads_analysis,
        "search_console": gsc_analysis,
        "facebook_organic": facebook_organic,
        "highlights": all_highlights,
        "concerns": all_concerns,
        "overall_score": overall_score,
        "overall_grade": overall_grade,
        "roas": roas_data,
        "paid_summary": paid_summary,
        "kpi_status": kpi_status,
        "competitor_watch": competitor_watch,
        "period": _month_progress(month),
        "has_previous_month": bool(prev_data),
    }

    try:
        from .ad_intelligence import build_ad_intelligence
        analysis_payload["ad_intelligence"] = build_ad_intelligence(analysis_payload, client_config)
    except Exception:
        analysis_payload["ad_intelligence"] = {}

    return analysis_payload
