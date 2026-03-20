"""
Google Search Console API Integration

Pulls search performance data directly from the Search Console API.

Setup:
  1. Same Google Cloud service account used for GA4 works here too
  2. Enable "Google Search Console API" in Cloud Console
  3. Add the service account email as a user in Search Console
     (Search Console > Settings > Users and permissions > Add user)
  4. Set permission to "Full" or "Restricted"
  5. Add the site URL to the client config in clients.json
"""
from googleapiclient.discovery import build
from google.oauth2 import service_account
from pathlib import Path
import calendar


CREDENTIALS_PATH = Path(__file__).parent.parent / "config" / "google_credentials.json"


def _get_service(credentials_path=None):
    """Create an authenticated Search Console API service."""
    creds_path = credentials_path or CREDENTIALS_PATH
    if not Path(creds_path).exists():
        raise FileNotFoundError(
            f"Google credentials not found at {creds_path}. "
            "Download your service account JSON key from Google Cloud Console "
            "and save it to config/google_credentials.json"
        )

    credentials = service_account.Credentials.from_service_account_file(
        str(creds_path),
        scopes=["https://www.googleapis.com/auth/webmasters.readonly"],
    )
    return build("searchconsole", "v1", credentials=credentials)


def _month_date_range(month_str):
    """Convert '2026-03' to start/end date strings."""
    year, month = map(int, month_str.split("-"))
    start_date = f"{year}-{month:02d}-01"
    last_day = calendar.monthrange(year, month)[1]
    end_date = f"{year}-{month:02d}-{last_day}"
    return start_date, end_date


def pull_search_console_data(site_url, month_str, credentials_path=None):
    """
    Pull Search Console data for a given site and month.

    Args:
        site_url: The site URL as registered in Search Console
                  (e.g., "https://aceplumbing.com/" or "sc-domain:aceplumbing.com")
        month_str: Month string like "2026-03"
        credentials_path: Optional path to credentials JSON

    Returns:
        Dict in the same format as parsers.parse_search_console() so it
        plugs directly into the existing analysis pipeline.
    """
    service = _get_service(credentials_path)
    start_date, end_date = _month_date_range(month_str)

    # ── Pull query-level data ──
    query_data = _pull_by_dimension(service, site_url, start_date, end_date, "query", row_limit=500)

    # ── Pull page-level data ──
    page_data = _pull_by_dimension(service, site_url, start_date, end_date, "page", row_limit=200)

    # ── Pull aggregate totals ──
    totals = _pull_totals(service, site_url, start_date, end_date)

    # Override avg_position: top 5 queries by impressions, best 3 positions from those
    if len(query_data) >= 3:
        sorted_by_imp = sorted(query_data, key=lambda r: r.get("impressions", 0), reverse=True)
        top5 = sorted_by_imp[:5]
        best3_positions = sorted([r.get("position", 0) for r in top5])[:3]
        totals["avg_position"] = round(sum(best3_positions) / len(best3_positions), 1)

    # ── Build top queries list ──
    top_queries = []
    for row in sorted(query_data, key=lambda r: r["clicks"], reverse=True)[:50]:
        top_queries.append({
            "query": row["query"],
            "clicks": row["clicks"],
            "impressions": row["impressions"],
            "ctr": row["ctr"],
            "position": row["position"],
        })

    # ── Build top pages list ──
    top_pages = []
    for row in sorted(page_data, key=lambda r: r["clicks"], reverse=True)[:30]:
        top_pages.append({
            "page": row["page"],
            "clicks": row["clicks"],
            "impressions": row["impressions"],
            "ctr": row["ctr"],
            "position": row["position"],
        })

    # ── Build opportunity queries (high impressions, position 4-20) ──
    opportunity_queries = []
    for row in query_data:
        if 4 <= row["position"] <= 20 and row["impressions"] >= 10:
            opportunity_queries.append({
                "query": row["query"],
                "clicks": row["clicks"],
                "impressions": row["impressions"],
                "ctr": row["ctr"],
                "position": row["position"],
            })
    opportunity_queries.sort(key=lambda r: r["impressions"], reverse=True)
    opportunity_queries = opportunity_queries[:20]

    return {
        "totals": totals,
        "top_queries": top_queries,
        "top_pages": top_pages,
        "opportunity_queries": opportunity_queries,
        "row_count": len(query_data),
        "columns_found": ["query", "page", "clicks", "impressions", "ctr", "position"],
        "data_source": "search_console_api",
    }


def _pull_totals(service, site_url, start_date, end_date):
    """Pull aggregate totals without dimension breakdown."""
    request = {
        "startDate": start_date,
        "endDate": end_date,
        "type": "web",
    }

    response = service.searchanalytics().query(
        siteUrl=site_url, body=request
    ).execute()

    rows = response.get("rows", [])
    if rows:
        row = rows[0]
        total_clicks = int(row.get("clicks", 0))
        total_impressions = int(row.get("impressions", 0))
        ctr = round(row.get("ctr", 0) * 100, 2)
        position = round(row.get("position", 0), 1)
        return {
            "clicks": total_clicks,
            "impressions": total_impressions,
            "ctr": ctr,
            "avg_position": position,
        }

    return {"clicks": 0, "impressions": 0, "ctr": 0, "avg_position": 0}


def _pull_by_dimension(service, site_url, start_date, end_date, dimension, row_limit=500):
    """Pull data broken down by a single dimension (query or page)."""
    request = {
        "startDate": start_date,
        "endDate": end_date,
        "dimensions": [dimension],
        "type": "web",
        "rowLimit": row_limit,
    }

    response = service.searchanalytics().query(
        siteUrl=site_url, body=request
    ).execute()

    results = []
    for row in response.get("rows", []):
        key_value = row["keys"][0]
        results.append({
            dimension: key_value,
            "clicks": int(row.get("clicks", 0)),
            "impressions": int(row.get("impressions", 0)),
            "ctr": round(row.get("ctr", 0) * 100, 2),
            "position": round(row.get("position", 0), 1),
        })

    return results


def pull_search_console_by_device(site_url, month_str, credentials_path=None):
    """Pull data broken down by device type for additional reporting detail."""
    service = _get_service(credentials_path)
    start_date, end_date = _month_date_range(month_str)

    request = {
        "startDate": start_date,
        "endDate": end_date,
        "dimensions": ["device"],
        "type": "web",
    }

    response = service.searchanalytics().query(
        siteUrl=site_url, body=request
    ).execute()

    devices = {}
    for row in response.get("rows", []):
        device = row["keys"][0]
        devices[device] = {
            "clicks": int(row.get("clicks", 0)),
            "impressions": int(row.get("impressions", 0)),
            "ctr": round(row.get("ctr", 0) * 100, 2),
            "position": round(row.get("position", 0), 1),
        }

    return devices
