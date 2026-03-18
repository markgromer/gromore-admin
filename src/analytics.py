"""
Analytics / Interpretation Engine

Takes parsed data from all three sources, compares against industry benchmarks
and prior months, and produces a structured analysis with scored metrics.
"""
import json
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


def analyze_google_analytics(ga_data, prev_ga_data, benchmarks_website):
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
                if change is not None:
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

    by_page = ga_data.get("by_page", [])
    if by_page:
        analysis["top_landing_pages"] = sorted(
            by_page,
            key=lambda row: (row.get("sessions", 0), row.get("conversions", 0)),
            reverse=True,
        )[:10]

    return analysis


def analyze_meta_business(meta_data, prev_meta_data, benchmarks_meta, industry):
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
                if change is not None:
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

    return analysis


def analyze_google_ads(google_ads_data, prev_google_ads_data, benchmarks_ads, industry):
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
    }

    metrics_config = {
        "impressions": {"higher_is_better": True, "benchmark_key": None},
        "clicks": {"higher_is_better": True, "benchmark_key": None},
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

    return analysis


def analyze_search_console(gsc_data, prev_gsc_data, benchmarks_seo, industry):
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
                if change is not None:
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
                                f"Average position improved from {prev_val} to {current_val}"
                            )
                        elif change >= 5:
                            analysis["concerns"].append(
                                f"Average position worsened from {prev_val} to {current_val}"
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

    if len(opportunities) > 5:
        analysis["highlights"].append(
            f"Found {len(opportunities)} keyword opportunities (high impressions, position 4-20)"
        )

    # Top pages
    analysis["top_pages"] = gsc_data.get("top_pages", [])[:15]

    return analysis


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
    for source in ["google_analytics", "meta_business", "search_console", "google_ads"]:
        prev_rows = db.get_monthly_data(client_id, prev_month, source)
        if prev_rows:
            prev_data[source] = json.loads(prev_rows[0]["data_json"])

    # Run analyses
    ga_analysis = analyze_google_analytics(
        current_data.get("google_analytics"),
        prev_data.get("google_analytics"),
        benchmarks.get("website", {})
    )

    meta_analysis = analyze_meta_business(
        current_data.get("meta_business"),
        prev_data.get("meta_business"),
        benchmarks.get("meta_ads", {}),
        industry
    )

    gsc_analysis = analyze_search_console(
        current_data.get("search_console"),
        prev_data.get("search_console"),
        benchmarks.get("seo", {}),
        industry
    )

    google_ads_analysis = analyze_google_ads(
        current_data.get("google_ads"),
        prev_data.get("google_ads"),
        benchmarks.get("google_ads", {}),
        industry,
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
        lead_gap_pct = round(((paid_leads - target_leads) / target_leads) * 100, 1)
        leads_on_track = paid_leads >= target_leads
        kpi_status["evaluation"]["leads"] = {
            "target": target_leads,
            "actual": paid_leads,
            "gap_pct": lead_gap_pct,
            "on_track": leads_on_track,
        }
        if leads_on_track:
            all_highlights.append(
                f"Paid leads are {paid_leads} vs target {target_leads} (+{lead_gap_pct}%)"
            )
        else:
            all_concerns.append(
                f"Paid leads are {paid_leads} vs target {target_leads} ({abs(lead_gap_pct)}% below target)"
            )

    if target_roas > 0:
        kpi_status["evaluation"]["roas"] = {
            "target": target_roas,
            "actual": None,
            "gap_pct": None,
            "on_track": None,
            "note": "ROAS target set; revenue feed not connected yet for true ROAS calculation.",
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

    return {
        "client_id": client_id,
        "month": month,
        "client_config": client_config,
        "industry": industry,
        "google_analytics": ga_analysis,
        "meta_business": meta_analysis,
        "google_ads": google_ads_analysis,
        "search_console": gsc_analysis,
        "highlights": all_highlights,
        "concerns": all_concerns,
        "overall_score": overall_score,
        "overall_grade": overall_grade,
        "roas": roas_data,
        "paid_summary": paid_summary,
        "kpi_status": kpi_status,
        "has_previous_month": bool(prev_data),
    }
