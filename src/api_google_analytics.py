"""
Google Analytics 4 (GA4) API Integration

Pulls data directly from GA4 using the Google Analytics Data API v1beta.
No more CSV exports needed - just configure credentials and property ID.

Setup:
  1. Go to Google Cloud Console > APIs & Services > Enable "Google Analytics Data API"
  2. Create a Service Account (or OAuth credentials)
  3. Download the JSON key file
  4. Grant the service account "Viewer" access in GA4 Admin > Property > Property Access Management
  5. Save the JSON key to config/google_credentials.json
  6. Add the GA4 property ID to the client config in clients.json
"""
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest,
    DateRange,
    Dimension,
    Metric,
    OrderBy,
    FilterExpression,
    Filter,
)
from google.oauth2 import service_account
from pathlib import Path
from datetime import datetime, timedelta
import calendar


CREDENTIALS_PATH = Path(__file__).parent.parent / "config" / "google_credentials.json"


def _get_client(credentials_path=None):
    """Create an authenticated GA4 Data API client."""
    creds_path = credentials_path or CREDENTIALS_PATH
    if not Path(creds_path).exists():
        raise FileNotFoundError(
            f"Google credentials not found at {creds_path}. "
            "Download your service account JSON key from Google Cloud Console "
            "and save it to config/google_credentials.json"
        )

    credentials = service_account.Credentials.from_service_account_file(
        str(creds_path),
        scopes=["https://www.googleapis.com/auth/analytics.readonly"],
    )
    return BetaAnalyticsDataClient(credentials=credentials)


def _month_date_range(month_str):
    """Convert '2026-03' to start/end date strings for API request."""
    year, month = map(int, month_str.split("-"))
    start_date = f"{year}-{month:02d}-01"
    last_day = calendar.monthrange(year, month)[1]
    end_date = f"{year}-{month:02d}-{last_day}"
    return start_date, end_date


def pull_ga4_data(property_id, month_str, credentials_path=None):
    """
    Pull GA4 data for a given property and month.

    Args:
        property_id: GA4 property ID (e.g., "properties/123456789")
        month_str: Month string like "2026-03"
        credentials_path: Optional path to credentials JSON

    Returns:
        Dict in the same format as parsers.parse_google_analytics() so it
        plugs directly into the existing analysis pipeline.
    """
    client = _get_client(credentials_path)
    start_date, end_date = _month_date_range(month_str)

    # Ensure property ID is in the right format
    if not property_id.startswith("properties/"):
        property_id = f"properties/{property_id}"

    # ── Pull aggregate metrics ──
    totals = _pull_aggregate_metrics(client, property_id, start_date, end_date)

    # ── Pull by source/medium breakdown ──
    by_source = _pull_source_medium_breakdown(client, property_id, start_date, end_date)

    return {
        "totals": totals,
        "by_source": by_source,
        "row_count": sum(len(v) if isinstance(v, list) else 1 for v in by_source.values()),
        "columns_found": list(totals.keys()),
        "data_source": "ga4_api",
    }


def _pull_aggregate_metrics(client, property_id, start_date, end_date):
    """Pull total/aggregate metrics for the month."""
    request = RunReportRequest(
        property=property_id,
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        metrics=[
            Metric(name="sessions"),
            Metric(name="totalUsers"),
            Metric(name="newUsers"),
            Metric(name="bounceRate"),
            Metric(name="screenPageViewsPerSession"),
            Metric(name="averageSessionDuration"),
            Metric(name="conversions"),
            Metric(name="screenPageViews"),
            Metric(name="engagedSessions"),
            Metric(name="engagementRate"),
            Metric(name="sessionConversionRate"),
        ],
    )

    response = client.run_report(request)

    totals = {}
    if response.rows:
        row = response.rows[0]
        metric_names = [
            "sessions", "users", "new_users", "bounce_rate",
            "pages_per_session", "avg_session_duration", "conversions",
            "pageviews", "engaged_sessions", "engagement_rate", "conversion_rate",
        ]
        for i, name in enumerate(metric_names):
            val = row.metric_values[i].value
            if name in ("bounce_rate", "pages_per_session", "engagement_rate", "conversion_rate"):
                totals[name] = round(float(val) * 100 if float(val) <= 1 else float(val), 2)
            elif name == "avg_session_duration":
                totals[name] = round(float(val), 1)
            else:
                totals[name] = int(float(val))

    return totals


def _pull_source_medium_breakdown(client, property_id, start_date, end_date):
    """Pull metrics broken down by source/medium."""
    request = RunReportRequest(
        property=property_id,
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="sessionSourceMedium")],
        metrics=[
            Metric(name="sessions"),
            Metric(name="totalUsers"),
            Metric(name="newUsers"),
            Metric(name="bounceRate"),
            Metric(name="screenPageViewsPerSession"),
            Metric(name="averageSessionDuration"),
            Metric(name="conversions"),
        ],
        order_bys=[
            OrderBy(metric=OrderBy.MetricOrderBy(metric_name="sessions"), desc=True)
        ],
        limit=50,
    )

    response = client.run_report(request)

    by_source = {}
    metric_keys = [
        "sessions", "users", "new_users", "bounce_rate",
        "pages_per_session", "avg_session_duration", "conversions",
    ]

    for row in response.rows:
        source_medium = row.dimension_values[0].value
        source_data = {}
        for i, key in enumerate(metric_keys):
            val = row.metric_values[i].value
            if key in ("bounce_rate", "pages_per_session"):
                source_data[key] = round(float(val) * 100 if float(val) <= 1 else float(val), 2)
            elif key == "avg_session_duration":
                source_data[key] = round(float(val), 1)
            else:
                source_data[key] = int(float(val))
        by_source[source_medium] = source_data

    return by_source


def pull_ga4_landing_pages(property_id, month_str, credentials_path=None):
    """Pull top landing pages data - useful for additional reporting detail."""
    client = _get_client(credentials_path)
    start_date, end_date = _month_date_range(month_str)

    if not property_id.startswith("properties/"):
        property_id = f"properties/{property_id}"

    request = RunReportRequest(
        property=property_id,
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="landingPagePlusQueryString")],
        metrics=[
            Metric(name="sessions"),
            Metric(name="conversions"),
            Metric(name="bounceRate"),
            Metric(name="averageSessionDuration"),
        ],
        order_bys=[
            OrderBy(metric=OrderBy.MetricOrderBy(metric_name="sessions"), desc=True)
        ],
        limit=30,
    )

    response = client.run_report(request)

    landing_pages = []
    for row in response.rows:
        landing_pages.append({
            "page": row.dimension_values[0].value,
            "sessions": int(float(row.metric_values[0].value)),
            "conversions": int(float(row.metric_values[1].value)),
            "bounce_rate": round(float(row.metric_values[2].value) * 100, 2),
            "avg_session_duration": round(float(row.metric_values[3].value), 1),
        })

    return landing_pages


def pull_ga4_conversions_by_source(property_id, month_str, credentials_path=None):
    """Pull conversion breakdown by source to understand which channels convert best."""
    client = _get_client(credentials_path)
    start_date, end_date = _month_date_range(month_str)

    if not property_id.startswith("properties/"):
        property_id = f"properties/{property_id}"

    request = RunReportRequest(
        property=property_id,
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[
            Dimension(name="sessionSourceMedium"),
            Dimension(name="eventName"),
        ],
        metrics=[
            Metric(name="eventCount"),
        ],
        dimension_filter=FilterExpression(
            filter=Filter(
                field_name="eventName",
                in_list_filter=Filter.InListFilter(
                    values=["generate_lead", "phone_call", "form_submit",
                            "contact", "purchase", "schedule", "book_appointment"]
                ),
            )
        ),
        order_bys=[
            OrderBy(metric=OrderBy.MetricOrderBy(metric_name="eventCount"), desc=True)
        ],
        limit=50,
    )

    response = client.run_report(request)

    conversions = []
    for row in response.rows:
        conversions.append({
            "source_medium": row.dimension_values[0].value,
            "event_name": row.dimension_values[1].value,
            "count": int(float(row.metric_values[0].value)),
        })

    return conversions
