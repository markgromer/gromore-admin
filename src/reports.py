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
from jinja2 import Environment, FileSystemLoader


TEMPLATES_DIR = Path(__file__).parent / "templates"
REPORTS_DIR = Path(__file__).parent.parent / "reports"


def _get_jinja_env():
    return Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        autoescape=True,
    )


def _month_display(month_str):
    """Convert '2026-03' to 'March 2026'."""
    try:
        dt = datetime.strptime(month_str, "%Y-%m")
        return dt.strftime("%B %Y")
    except ValueError:
        return month_str


def generate_internal_report(analysis, suggestions_internal, output_dir=None):
    """
    Generate the internal team report (HTML).

    Args:
        analysis: Output from analytics.build_full_analysis()
        suggestions_internal: Output from suggestions.format_suggestions_for_internal()
        output_dir: Optional custom output directory
    """
    client_id = analysis["client_id"]
    month = analysis["month"]
    client_config = analysis["client_config"]

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
        "industry": analysis.get("industry", ""),
        "monthly_budget": client_config.get("monthly_budget", 0),
        "generated_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "overall_grade": analysis.get("overall_grade", "N/A"),
        "overall_score": analysis.get("overall_score"),
        "highlights": analysis.get("highlights", []),
        "concerns": analysis.get("concerns", []),
        "roas": _dict_to_obj(analysis.get("roas", {})),
        "ga": _dict_to_obj(analysis.get("google_analytics")) if analysis.get("google_analytics") else None,
        "meta": _dict_to_obj(analysis.get("meta_business")) if analysis.get("meta_business") else None,
        "gsc": _dict_to_obj(analysis.get("search_console")) if analysis.get("search_console") else None,
        "suggestions": suggestions_internal,
        "ai_brief": analysis.get("ai_brief_internal"),
    }

    html = template.render(**context)
    output_path = output_dir / "internal_report.html"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"  Internal report: {output_path}")
    return str(output_path)


def generate_client_report(analysis, suggestions_client, output_dir=None):
    """
    Generate the client-facing report (HTML).

    Args:
        analysis: Output from analytics.build_full_analysis()
        suggestions_client: Output from suggestions.format_suggestions_for_client()
        output_dir: Optional custom output directory
    """
    client_id = analysis["client_id"]
    month = analysis["month"]
    client_config = analysis["client_config"]

    if output_dir is None:
        output_dir = REPORTS_DIR / client_id / month
    else:
        output_dir = Path(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    env = _get_jinja_env()
    template = env.get_template("client_report.html")

    # Calculate client-friendly KPIs
    meta = analysis.get("meta_business")
    ga = analysis.get("google_analytics")
    gsc = analysis.get("search_console")

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

    if ga:
        total_leads += ga.get("metrics", {}).get("conversions", 0)

    if total_leads > 0 and total_spend > 0:
        cost_per_lead = total_spend / total_leads

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
        "overall_grade": analysis.get("overall_grade", "N/A"),
        "total_leads": total_leads if total_leads > 0 else None,
        "total_spend": total_spend if total_spend > 0 else None,
        "cost_per_lead": cost_per_lead,
        "leads_change": leads_change,
        "cpl_change": cpl_change,
        "website_sessions": website_sessions,
        "sessions_change": sessions_change,
        "highlights": analysis.get("highlights", []),
        "concerns": analysis.get("concerns", []),
        "ga": _dict_to_obj(ga) if ga else None,
        "meta": _dict_to_obj(meta) if meta else None,
        "gsc": _dict_to_obj(gsc) if gsc else None,
        "client_suggestions": suggestions_client,
        "ai_brief": analysis.get("ai_brief_client"),
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
