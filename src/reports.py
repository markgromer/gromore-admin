"""
Report Generator

Produces HTML (and optionally PDF) reports from analysis and suggestion data.
Two report types:
  1. Internal - detailed, tactical, for the ad account team
  2. Client - clean, professional, simplified language
"""
import os
from pathlib import Path
from datetime import datetime
from decimal import Decimal
from jinja2 import Environment, FileSystemLoader, select_autoescape


TEMPLATES_DIR = Path(__file__).parent / "templates"
REPORTS_DIR = Path(__file__).parent.parent / "reports"


def _get_jinja_env():
    def _finalize_display_value(value):
        if isinstance(value, float):
            # Keep 2 decimal places max for all float display
            rounded = round(value, 2)
            # Drop trailing zeros: 3.10 -> 3.1, 4.00 -> 4
            if rounded == int(rounded):
                return int(rounded)
            return rounded
        if isinstance(value, Decimal):
            rounded = round(float(value), 2)
            if rounded == int(rounded):
                return int(rounded)
            return rounded
        return value

    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        autoescape=select_autoescape(["html", "htm", "xml"]),
        finalize=_finalize_display_value,
    )
    env.filters["pct"] = lambda v: f"{round(float(v), 1)}" if v is not None else "0"
    env.filters["money"] = lambda v: f"{round(float(v), 2):,.2f}" if v is not None else "0.00"
    env.filters["pos"] = lambda v: f"{round(float(v), 1)}" if v is not None else "N/A"
    return env


def _month_display(month_str):
    """Convert '2026-03' to 'March 2026'."""
    try:
        dt = datetime.strptime(month_str, "%Y-%m")
        return dt.strftime("%B %Y")
    except ValueError:
        return month_str


def _round_report_values(value, places=2):
    """Recursively round floats for report rendering precision."""
    if isinstance(value, float):
        return round(value, places)
    if isinstance(value, list):
        return [_round_report_values(v, places) for v in value]
    if isinstance(value, dict):
        return {k: _round_report_values(v, places) for k, v in value.items()}
    return value


def generate_internal_report(analysis, suggestions_internal, output_dir=None, branding=None):
    """
    Generate the internal team report (HTML).

    Args:
        analysis: Output from analytics.build_full_analysis()
        suggestions_internal: Output from suggestions.format_suggestions_for_internal()
        output_dir: Optional custom output directory
        branding: Optional dict with agency_name, agency_logo_url, agency_website, agency_color
    """
    rounded_analysis = _round_report_values(analysis, places=3)

    client_id = rounded_analysis["client_id"]
    month = rounded_analysis["month"]
    client_config = rounded_analysis["client_config"]

    if output_dir is None:
        output_dir = REPORTS_DIR / client_id / month
    else:
        output_dir = Path(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    env = _get_jinja_env()
    template = env.get_template("internal_report.html")

    # Build template context
    context = {
        "client_name": client_config.get("display_name", client_id),
        "month": month,
        "month_display": _month_display(month),
        "industry": rounded_analysis.get("industry", ""),
        "monthly_budget": client_config.get("monthly_budget", 0),
        "generated_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "overall_grade": rounded_analysis.get("overall_grade", "N/A"),
        "overall_score": rounded_analysis.get("overall_score"),
        "highlights": rounded_analysis.get("highlights", []),
        "concerns": rounded_analysis.get("concerns", []),
        "roas": _dict_to_obj(rounded_analysis.get("roas", {})),
        "paid_summary": _dict_to_obj(rounded_analysis.get("paid_summary", {})),
        "kpi_status": _dict_to_obj(rounded_analysis.get("kpi_status", {})),
        "competitor_watch": _dict_to_obj(rounded_analysis.get("competitor_watch")) if rounded_analysis.get("competitor_watch") else None,
        "ga": _dict_to_obj(rounded_analysis.get("google_analytics")) if rounded_analysis.get("google_analytics") else None,
        "meta": _dict_to_obj(rounded_analysis.get("meta_business")) if rounded_analysis.get("meta_business") else None,
        "google_ads": _dict_to_obj(rounded_analysis.get("google_ads")) if rounded_analysis.get("google_ads") else None,
        "gsc": _dict_to_obj(rounded_analysis.get("search_console")) if rounded_analysis.get("search_console") else None,
        "suggestions": suggestions_internal,
        "ai_brief": rounded_analysis.get("ai_brief_internal"),
        "branding": branding or {},
    }

    html = template.render(**context)
    output_path = output_dir / "internal_report.html"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"  Internal report: {output_path}")
    return str(output_path)


def generate_client_report(analysis, suggestions_client, output_dir=None, branding=None):
    """
    Generate the client-facing report (HTML).

    Args:
        analysis: Output from analytics.build_full_analysis()
        suggestions_client: Output from suggestions.format_suggestions_for_client()
        output_dir: Optional custom output directory
        branding: Optional dict with agency_name, agency_logo_url, agency_website, agency_color
    """
    rounded_analysis = _round_report_values(analysis, places=3)

    client_id = rounded_analysis["client_id"]
    month = rounded_analysis["month"]
    client_config = rounded_analysis["client_config"]

    if output_dir is None:
        output_dir = REPORTS_DIR / client_id / month
    else:
        output_dir = Path(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    env = _get_jinja_env()
    template = env.get_template("client_report.html")

    # Calculate client-friendly KPIs
    meta = rounded_analysis.get("meta_business")
    google_ads = rounded_analysis.get("google_ads")
    ga = rounded_analysis.get("google_analytics")
    gsc = rounded_analysis.get("search_console")
    paid_summary = rounded_analysis.get("paid_summary", {})
    kpi_status = rounded_analysis.get("kpi_status", {})
    roas_data = rounded_analysis.get("roas", {})
    crm_revenue = rounded_analysis.get("crm_revenue", {})

    # --- Leads: use paid_summary to avoid double-counting GA4 conversions ---
    paid_leads = paid_summary.get("total_paid_leads", 0)
    crm_leads = paid_summary.get("crm_leads", 0)
    total_leads = paid_summary.get("total_leads", 0)
    ga_conversions = ga.get("metrics", {}).get("conversions", 0) if ga else 0

    # If paid_summary is empty, fall back to summing channel results
    if not paid_leads:
        if meta:
            paid_leads += meta.get("metrics", {}).get("results", 0)
        if google_ads:
            paid_leads += google_ads.get("metrics", {}).get("results", 0)
    if not crm_leads:
        crm_totals = (crm_revenue or {}).get("totals") or {}
        crm_leads = max(
            crm_totals.get("leads") or 0,
            crm_totals.get("lead_count") or 0,
            crm_totals.get("new_leads") or 0,
            crm_totals.get("requests") or 0,
            crm_totals.get("new_clients") or 0,
            crm_totals.get("closed_deals") or 0,
        )
    if not total_leads:
        total_leads = max(paid_leads or 0, crm_leads or 0)

    total_spend = paid_summary.get("total_paid_spend", 0)
    if not total_spend:
        if meta:
            total_spend += meta.get("metrics", {}).get("spend", 0)
        if google_ads:
            total_spend += google_ads.get("metrics", {}).get("spend", 0)

    cost_per_lead = round(total_spend / total_leads, 2) if total_leads > 0 and total_spend > 0 else None

    # MoM for paid leads - weighted average across channels
    leads_change = None
    if meta:
        results_mom = meta.get("month_over_month", {}).get("results", {})
        if results_mom.get("change_pct") is not None:
            leads_change = results_mom["change_pct"]
    if leads_change is None and google_ads:
        ads_results_mom = google_ads.get("month_over_month", {}).get("results", {})
        if ads_results_mom.get("change_pct") is not None:
            leads_change = ads_results_mom["change_pct"]

    cpl_change = None

    website_sessions = None
    sessions_change = None
    if ga:
        website_sessions = ga.get("metrics", {}).get("sessions")
        sessions_mom = ga.get("month_over_month", {}).get("sessions", {})
        if sessions_mom.get("change_pct") is not None:
            sessions_change = sessions_mom["change_pct"]

    # --- Revenue / ROAS ---
    attributed_revenue = roas_data.get("attributed_revenue")
    blended_roas = roas_data.get("blended_roas")

    # --- Channel efficiency comparison ---
    channel_efficiency = []
    if meta and meta.get("metrics", {}).get("spend"):
        m = meta["metrics"]
        channel_efficiency.append({
            "name": "Facebook / Instagram",
            "spend": m.get("spend", 0),
            "leads": m.get("results", 0),
            "cpl": m.get("cost_per_result", 0),
            "clicks": m.get("clicks", 0),
            "cpc": m.get("cpc", 0),
        })
    if google_ads and google_ads.get("metrics", {}).get("spend"):
        g = google_ads["metrics"]
        channel_efficiency.append({
            "name": "Google Ads",
            "spend": g.get("spend", 0),
            "leads": g.get("results", 0),
            "cpl": g.get("cost_per_result", 0),
            "clicks": g.get("clicks", 0),
            "cpc": g.get("cpc", 0),
        })
    if gsc and gsc.get("metrics", {}).get("clicks"):
        channel_efficiency.append({
            "name": "Organic Search (SEO)",
            "spend": 0,
            "leads": 0,
            "cpl": 0,
            "clicks": gsc["metrics"].get("clicks", 0),
            "cpc": 0,
        })

    # --- "Why it moved" drivers ---
    drivers = []
    if meta:
        mom = meta.get("month_over_month", {})
        for metric, label in [("results", "Meta leads"), ("spend", "Meta spend"),
                              ("cpc", "Meta cost-per-click"), ("ctr", "Meta click rate")]:
            entry = mom.get(metric, {})
            pct = entry.get("change_pct")
            if pct is not None and abs(pct) >= 10:
                drivers.append({
                    "metric": label,
                    "change_pct": pct,
                    "direction": "up" if pct > 0 else "down",
                    "previous": entry.get("previous"),
                    "current": entry.get("current"),
                })
    if google_ads:
        mom = google_ads.get("month_over_month", {})
        for metric, label in [("results", "Google Ads conversions"), ("spend", "Google Ads spend"),
                              ("cpc", "Google Ads CPC"), ("ctr", "Google Ads click rate")]:
            entry = mom.get(metric, {})
            pct = entry.get("change_pct")
            if pct is not None and abs(pct) >= 10:
                drivers.append({
                    "metric": label,
                    "change_pct": pct,
                    "direction": "up" if pct > 0 else "down",
                    "previous": entry.get("previous"),
                    "current": entry.get("current"),
                })
    if ga:
        mom = ga.get("month_over_month", {})
        for metric, label in [("sessions", "Website sessions"), ("conversions", "Website conversions"),
                              ("bounce_rate", "Bounce rate")]:
            entry = mom.get(metric, {})
            pct = entry.get("change_pct")
            if pct is not None and abs(pct) >= 10:
                drivers.append({
                    "metric": label,
                    "change_pct": pct,
                    "direction": "up" if pct > 0 else "down",
                    "previous": entry.get("previous"),
                    "current": entry.get("current"),
                })
    # Sort by magnitude of change
    drivers.sort(key=lambda d: abs(d["change_pct"]), reverse=True)

    # --- Tracking confidence ---
    connected_sources = []
    missing_sources = []
    for src, data, label in [
        ("meta", meta, "Facebook/Instagram Ads"),
        ("google_ads", google_ads, "Google Ads"),
        ("ga", ga, "Google Analytics"),
        ("gsc", gsc, "Google Search Console"),
    ]:
        if data and data.get("metrics"):
            connected_sources.append(label)
        else:
            missing_sources.append(label)
    tracking_confidence = round(len(connected_sources) / 4 * 100)

    has_revenue = attributed_revenue is not None and attributed_revenue > 0

    # --- AI brief watchouts ---
    ai_brief = rounded_analysis.get("ai_brief_client")
    watchouts = []
    if ai_brief and ai_brief.get("watchouts_next_7_days"):
        watchouts = ai_brief["watchouts_next_7_days"]

    context = {
        "client_name": client_config.get("display_name", client_id),
        "month": month,
        "month_display": _month_display(month),
        "generated_date": datetime.now().strftime("%Y-%m-%d"),
        "overall_grade": rounded_analysis.get("overall_grade", "N/A"),
        # Scorecard: split into paid leads, website conversions, total
        "paid_leads": paid_leads if paid_leads > 0 else None,
        "crm_leads": crm_leads if crm_leads > 0 else None,
        "ga_conversions": ga_conversions if ga_conversions > 0 else None,
        "total_leads": total_leads if total_leads > 0 else ((paid_leads + ga_conversions) if (paid_leads + ga_conversions) > 0 else None),
        "total_spend": total_spend if total_spend > 0 else None,
        "cost_per_lead": cost_per_lead,
        "leads_change": leads_change,
        "cpl_change": cpl_change,
        "website_sessions": website_sessions,
        "sessions_change": sessions_change,
        # Revenue
        "attributed_revenue": attributed_revenue,
        "blended_roas": blended_roas,
        "has_revenue": has_revenue,
        # Channel efficiency
        "channel_efficiency": [_dict_to_obj(c) for c in channel_efficiency],
        # Drivers
        "drivers": [_dict_to_obj(d) for d in drivers[:8]],
        # Concerns and watchouts
        "highlights": rounded_analysis.get("highlights", []),
        "concerns": rounded_analysis.get("concerns", []),
        "watchouts": watchouts,
        # Tracking confidence
        "tracking_confidence": tracking_confidence,
        "connected_sources": connected_sources,
        "missing_sources": missing_sources,
        # Existing
        "paid_summary": _dict_to_obj(paid_summary),
        "kpi_status": _dict_to_obj(kpi_status),
        "competitor_watch": _dict_to_obj(rounded_analysis.get("competitor_watch")) if rounded_analysis.get("competitor_watch") else None,
        "ga": _dict_to_obj(ga) if ga else None,
        "meta": _dict_to_obj(meta) if meta else None,
        "google_ads": _dict_to_obj(google_ads) if google_ads else None,
        "gsc": _dict_to_obj(gsc) if gsc else None,
        "client_suggestions": suggestions_client,
        "ai_brief": ai_brief,
        "branding": branding or {},
    }

    html = template.render(**context)
    output_path = output_dir / "client_report.html"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"  Client report:   {output_path}")
    return str(output_path)


class _DictObj(dict):
    """Dict subclass that also allows attribute access, for cleaner template use."""
    def __init__(self, d):
        super().__init__()
        if isinstance(d, dict):
            for k, v in d.items():
                if isinstance(v, dict):
                    self[k] = _DictObj(v)
                elif isinstance(v, list):
                    self[k] = [_DictObj(i) if isinstance(i, dict) else i for i in v]
                else:
                    self[k] = v

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        self[name] = value


def _dict_to_obj(d):
    if d is None:
        return None
    if isinstance(d, dict):
        return _DictObj(d)
    return d
