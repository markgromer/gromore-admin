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
from jinja2 import Environment, FileSystemLoader


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
        autoescape=True,
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

    total_leads = 0
    total_spend = 0
    cost_per_lead = None
    leads_change = None
    cpl_change = None

    if meta:
        total_leads += meta.get("metrics", {}).get("results", 0)
        total_spend += meta.get("metrics", {}).get("spend", 0)
        # MoM for leads
        results_mom = meta.get("month_over_month", {}).get("results", {})
        if results_mom.get("change_pct") is not None:
            leads_change = results_mom["change_pct"]

    if google_ads:
        total_leads += google_ads.get("metrics", {}).get("results", 0)
        total_spend += google_ads.get("metrics", {}).get("spend", 0)
        if leads_change is None:
            ads_results_mom = google_ads.get("month_over_month", {}).get("results", {})
            if ads_results_mom.get("change_pct") is not None:
                leads_change = ads_results_mom["change_pct"]

    if ga:
        total_leads += ga.get("metrics", {}).get("conversions", 0)

    if total_leads > 0 and total_spend > 0:
        cost_per_lead = round(total_spend / total_leads, 3)

    website_sessions = None
    sessions_change = None
    if ga:
        website_sessions = ga.get("metrics", {}).get("sessions")
        sessions_mom = ga.get("month_over_month", {}).get("sessions", {})
        if sessions_mom.get("change_pct") is not None:
            sessions_change = sessions_mom["change_pct"]

    context = {
        "client_name": client_config.get("display_name", client_id),
        "month": month,
        "month_display": _month_display(month),
        "generated_date": datetime.now().strftime("%Y-%m-%d"),
        "overall_grade": rounded_analysis.get("overall_grade", "N/A"),
        "total_leads": total_leads if total_leads > 0 else None,
        "total_spend": total_spend if total_spend > 0 else None,
        "cost_per_lead": cost_per_lead,
        "leads_change": leads_change,
        "cpl_change": cpl_change,
        "website_sessions": website_sessions,
        "sessions_change": sessions_change,
        "highlights": rounded_analysis.get("highlights", []),
        "concerns": rounded_analysis.get("concerns", []),
        "paid_summary": _dict_to_obj(rounded_analysis.get("paid_summary", {})),
        "kpi_status": _dict_to_obj(rounded_analysis.get("kpi_status", {})),
        "competitor_watch": _dict_to_obj(rounded_analysis.get("competitor_watch")) if rounded_analysis.get("competitor_watch") else None,
        "ga": _dict_to_obj(ga) if ga else None,
        "meta": _dict_to_obj(meta) if meta else None,
        "google_ads": _dict_to_obj(google_ads) if google_ads else None,
        "gsc": _dict_to_obj(gsc) if gsc else None,
        "client_suggestions": suggestions_client,
        "ai_brief": rounded_analysis.get("ai_brief_client"),
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
