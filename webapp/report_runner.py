"""
Report runner - bridge between the web app and the existing analytics pipeline.

Takes a brand from the web DB, maps it to the config format the existing
src/ pipeline expects, pulls data (API or CSV), runs analysis, generates reports.
"""
import json
import sys
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.parsers import load_client_data
from src.database import init_db, store_monthly_data, get_monthly_data, get_previous_month
from src.analytics import build_full_analysis
from src.suggestions import (
    generate_suggestions,
    format_suggestions_for_internal,
    format_suggestions_for_client,
)
from src.reports import generate_internal_report, generate_client_report


def _brand_to_client_config(brand):
    """Convert a web DB brand dict to the client config dict the pipeline expects."""
    import json as _json
    goals = brand.get("goals", "[]")
    if isinstance(goals, str):
        try:
            goals = _json.loads(goals)
        except (ValueError, TypeError):
            goals = []

    return {
        "display_name": brand["display_name"],
        "industry": brand.get("industry", "plumbing"),
        "monthly_budget": brand.get("monthly_budget", 0),
        "primary_services": [
            s.strip() for s in (brand.get("primary_services", "") or "").split(",") if s.strip()
        ],
        "service_area": brand.get("service_area", ""),
        "goals": goals,
        "api": {
            "ga4_property_id": brand.get("ga4_property_id", ""),
            "gsc_site_url": brand.get("gsc_site_url", ""),
            "gsc_enabled": bool(brand.get("gsc_site_url")),
            "meta_ad_account_id": brand.get("meta_ad_account_id", ""),
        },
    }


def run_report_for_brand(db, brand, month):
    """
    Run the full pipeline for one brand/month.
    Returns {"success": True/False, "error": str, "report_id": int}
    """
    slug = brand["slug"]
    client_config = _brand_to_client_config(brand)

    # Try loading data from CSV imports first
    import_dir = BASE_DIR / "data" / "imports" / slug / month
    data = {}

    if import_dir.exists():
        try:
            data = load_client_data(slug, month, import_dir=str(BASE_DIR / "data" / "imports"))
        except Exception as e:
            return {"success": False, "error": f"CSV parse error: {str(e)}"}

    # Try API pull if we have connections
    connections = db.get_brand_connections(brand["id"])
    if connections:
        try:
            from webapp.api_bridge import pull_api_data_for_brand
            api_data = pull_api_data_for_brand(brand, connections, month)
            # Merge: API data fills in gaps from CSV
            for key in ("google_analytics", "meta_business", "search_console"):
                if key in api_data and api_data[key] and key not in data:
                    data[key] = api_data[key]
        except Exception:
            pass  # best-effort; CSV-first proof of concept

    if not data:
        return {"success": False, "error": "No data available (no CSV imports and no API connections)"}

    # Store in analytics DB
    try:
        init_db()
        for source, source_data in data.items():
            store_monthly_data(slug, month, source, source_data)
    except Exception:
        pass

    # Run analysis (build_full_analysis fetches prev data internally from the DB)
    analysis = build_full_analysis(slug, month, data, client_config)

    # Generate suggestions
    suggestions = generate_suggestions(analysis)
    suggestions_internal = format_suggestions_for_internal(suggestions)
    suggestions_client = format_suggestions_for_client(suggestions)

    # Generate reports
    reports_dir = BASE_DIR / "reports" / slug / month
    reports_dir.mkdir(parents=True, exist_ok=True)

    internal_path = generate_internal_report(analysis, suggestions_internal, output_dir=str(reports_dir))
    client_path = generate_client_report(analysis, suggestions_client, output_dir=str(reports_dir))

    # Record in web DB
    report_id = db.create_report(brand["id"], month, internal_path, client_path)

    return {"success": True, "report_id": report_id, "error": ""}
