"""
Data ingestion parsers for Google Analytics, Meta Business Suite, and Google Search Console.

Each parser normalizes the CSV data into a consistent dictionary structure for storage
and analysis, handling common column name variations from different export formats.
"""
import pandas as pd
import os
from pathlib import Path


# ── Column mapping tables (export formats vary, so we map common variations) ──

GA_COLUMN_MAP = {
    # Sessions
    "sessions": "sessions",
    "ga:sessions": "sessions",
    # Users
    "users": "users",
    "total users": "users",
    "ga:users": "users",
    # New Users
    "new users": "new_users",
    "ga:newusers": "new_users",
    "new_users": "new_users",
    # Bounce Rate
    "bounce rate": "bounce_rate",
    "ga:bouncerate": "bounce_rate",
    "bounce_rate": "bounce_rate",
    # Pages per Session
    "pages / session": "pages_per_session",
    "pages/session": "pages_per_session",
    "ga:pageviewspersession": "pages_per_session",
    "pages_per_session": "pages_per_session",
    # Avg Session Duration
    "avg. session duration": "avg_session_duration",
    "average session duration": "avg_session_duration",
    "avg session duration": "avg_session_duration",
    "ga:avgsessionduration": "avg_session_duration",
    "avg_session_duration": "avg_session_duration",
    # Source/Medium
    "source / medium": "source_medium",
    "source/medium": "source_medium",
    "session source / medium": "source_medium",
    "source_medium": "source_medium",
    # Conversions
    "conversions": "conversions",
    "goal completions": "conversions",
    "ga:goalcompletionsall": "conversions",
    "key events": "conversions",
    # Revenue
    "revenue": "revenue",
    "ga:transactionrevenue": "revenue",
    "purchase revenue": "revenue",
    "total revenue": "revenue",
    # Date
    "date": "date",
    "ga:date": "date",
    "date range": "date",
    # Page views
    "pageviews": "pageviews",
    "views": "pageviews",
    "ga:pageviews": "pageviews",
    "screen_page_views": "pageviews",
    # Engaged sessions
    "engaged sessions": "engaged_sessions",
    "engagement rate": "engagement_rate",
    "average engagement time": "avg_engagement_time",
    # Conversion rate
    "session conversion rate": "conversion_rate",
    "conversion rate": "conversion_rate",
    "ga:goalconversionrateall": "conversion_rate",
}

META_COLUMN_MAP = {
    # Campaign
    "campaign name": "campaign_name",
    "campaign": "campaign_name",
    # Ad Set
    "ad set name": "ad_set_name",
    "ad set": "ad_set_name",
    "adset name": "ad_set_name",
    # Ad Name
    "ad name": "ad_name",
    # Impressions
    "impressions": "impressions",
    # Reach
    "reach": "reach",
    # Clicks
    "link clicks": "clicks",
    "clicks (all)": "clicks_all",
    "clicks": "clicks",
    # CTR
    "ctr (link click-through rate)": "ctr",
    "ctr (all)": "ctr_all",
    "ctr": "ctr",
    # CPC
    "cpc (cost per link click)": "cpc",
    "cpc (all)": "cpc_all",
    "cost per result": "cost_per_result",
    "cpc": "cpc",
    # CPM
    "cpm (cost per 1,000 impressions)": "cpm",
    "cpm": "cpm",
    # Spend
    "amount spent": "spend",
    "spend": "spend",
    "total spend": "spend",
    # Results
    "results": "results",
    "leads": "results",
    "conversions": "results",
    # Cost per result
    "cost per result": "cost_per_result",
    "cost per lead": "cost_per_result",
    # Frequency
    "frequency": "frequency",
    # Date
    "reporting starts": "date_start",
    "reporting ends": "date_end",
    "date": "date_start",
    "day": "date_start",
    # Result type
    "result type": "result_type",
    "result indicator": "result_type",
}

GSC_COLUMN_MAP = {
    "top queries": "query",
    "query": "query",
    "queries": "query",
    "top pages": "page",
    "page": "page",
    "pages": "page",
    "url": "page",
    "clicks": "clicks",
    "impressions": "impressions",
    "ctr": "ctr",
    "position": "position",
    "average position": "position",
    "country": "country",
    "device": "device",
    "date": "date",
}


def _normalize_columns(df, column_map):
    """Normalize column names using the provided mapping."""
    rename_dict = {}
    for col in df.columns:
        col_lower = col.strip().lower()
        if col_lower in column_map:
            rename_dict[col] = column_map[col_lower]
    df = df.rename(columns=rename_dict)
    return df


def _clean_numeric(series):
    """Clean numeric columns: remove %, $, commas, and convert to float."""
    if series.dtype == object:
        cleaned = series.astype(str).str.replace("%", "", regex=False)
        cleaned = cleaned.str.replace("$", "", regex=False)
        cleaned = cleaned.str.replace(",", "", regex=False)
        cleaned = cleaned.str.strip()
        return pd.to_numeric(cleaned, errors="coerce")
    return pd.to_numeric(series, errors="coerce")


def parse_google_analytics(filepath):
    """
    Parse a Google Analytics CSV export.

    Returns a dict with:
      - totals: aggregate metrics for the period
      - by_source: breakdown by source/medium
      - raw_rows: list of row dicts for detailed analysis
    """
    df = pd.read_csv(filepath, skiprows=_detect_header_row(filepath))
    df = _normalize_columns(df, GA_COLUMN_MAP)

    numeric_cols = [
        "sessions", "users", "new_users", "bounce_rate", "pages_per_session",
        "avg_session_duration", "conversions", "revenue", "pageviews",
        "engaged_sessions", "engagement_rate", "conversion_rate"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = _clean_numeric(df[col])

    totals = {}
    for col in numeric_cols:
        if col in df.columns:
            if col in ("bounce_rate", "pages_per_session", "engagement_rate", "conversion_rate"):
                totals[col] = round(df[col].mean(), 2)
            elif col == "avg_session_duration":
                totals[col] = round(df[col].mean(), 1)
            else:
                totals[col] = int(df[col].sum())

    # By source/medium breakdown
    by_source = {}
    if "source_medium" in df.columns:
        grouped = df.groupby("source_medium")
        for name, group in grouped:
            source_data = {}
            for col in numeric_cols:
                if col in group.columns:
                    if col in ("bounce_rate", "pages_per_session", "engagement_rate", "conversion_rate"):
                        source_data[col] = round(group[col].mean(), 2)
                    else:
                        source_data[col] = int(group[col].sum())
            by_source[str(name)] = source_data

    return {
        "totals": totals,
        "by_source": by_source,
        "row_count": len(df),
        "columns_found": list(df.columns),
    }


def parse_meta_business(filepath):
    """
    Parse a Meta Business Suite / Ads Manager CSV export.

    Returns a dict with:
      - totals: aggregate metrics
      - by_campaign: breakdown by campaign
      - by_ad_set: breakdown by ad set
      - top_ads: top performing ads
    """
    df = pd.read_csv(filepath, skiprows=_detect_header_row(filepath))
    df = _normalize_columns(df, META_COLUMN_MAP)

    numeric_cols = [
        "impressions", "reach", "clicks", "clicks_all", "ctr", "ctr_all",
        "cpc", "cpc_all", "cpm", "spend", "results", "cost_per_result", "frequency"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = _clean_numeric(df[col])

    # Aggregate totals
    totals = {}
    sum_cols = ["impressions", "reach", "clicks", "clicks_all", "spend", "results"]
    for col in sum_cols:
        if col in df.columns:
            totals[col] = int(df[col].sum())

    # Calculated metrics
    if totals.get("impressions", 0) > 0:
        totals["ctr"] = round((totals.get("clicks", 0) / totals["impressions"]) * 100, 2)
        totals["cpm"] = round((totals.get("spend", 0) / totals["impressions"]) * 1000, 2)
    if totals.get("clicks", 0) > 0:
        totals["cpc"] = round(totals.get("spend", 0) / totals["clicks"], 2)
    if totals.get("results", 0) > 0:
        totals["cost_per_result"] = round(totals.get("spend", 0) / totals["results"], 2)
    if totals.get("reach", 0) > 0:
        totals["frequency"] = round(totals.get("impressions", 0) / totals["reach"], 2)

    # By campaign
    by_campaign = {}
    if "campaign_name" in df.columns:
        for name, group in df.groupby("campaign_name"):
            camp = {}
            for col in sum_cols:
                if col in group.columns:
                    camp[col] = int(group[col].sum())
            # Calculated
            if camp.get("impressions", 0) > 0:
                camp["ctr"] = round((camp.get("clicks", 0) / camp["impressions"]) * 100, 2)
                camp["cpm"] = round((camp.get("spend", 0) / camp["impressions"]) * 1000, 2)
            if camp.get("clicks", 0) > 0:
                camp["cpc"] = round(camp.get("spend", 0) / camp["clicks"], 2)
            if camp.get("results", 0) > 0:
                camp["cost_per_result"] = round(camp.get("spend", 0) / camp["results"], 2)
            by_campaign[str(name)] = camp

    # By Ad Set
    by_ad_set = {}
    if "ad_set_name" in df.columns:
        for name, group in df.groupby("ad_set_name"):
            ad_set = {}
            for col in sum_cols:
                if col in group.columns:
                    ad_set[col] = int(group[col].sum())
            if ad_set.get("impressions", 0) > 0:
                ad_set["ctr"] = round((ad_set.get("clicks", 0) / ad_set["impressions"]) * 100, 2)
            if ad_set.get("clicks", 0) > 0:
                ad_set["cpc"] = round(ad_set.get("spend", 0) / ad_set["clicks"], 2)
            if ad_set.get("results", 0) > 0:
                ad_set["cost_per_result"] = round(ad_set.get("spend", 0) / ad_set["results"], 2)
            by_ad_set[str(name)] = ad_set

    return {
        "totals": totals,
        "by_campaign": by_campaign,
        "by_ad_set": by_ad_set,
        "row_count": len(df),
        "columns_found": list(df.columns),
    }


def parse_search_console(filepath):
    """
    Parse a Google Search Console CSV export.

    Returns a dict with:
      - totals: aggregate clicks, impressions, avg CTR, avg position
      - top_queries: top 50 queries by clicks
      - top_pages: top 30 pages by clicks
      - opportunity_queries: high-impression, low-CTR queries (quick wins)
    """
    df = pd.read_csv(filepath, skiprows=_detect_header_row(filepath))
    df = _normalize_columns(df, GSC_COLUMN_MAP)

    for col in ["clicks", "impressions", "ctr", "position"]:
        if col in df.columns:
            df[col] = _clean_numeric(df[col])

    # If CTR is in decimal form (0.05 = 5%), convert to percentage
    if "ctr" in df.columns and df["ctr"].max() <= 1.0:
        df["ctr"] = df["ctr"] * 100

    totals = {}
    if "clicks" in df.columns:
        totals["clicks"] = int(df["clicks"].sum())
    if "impressions" in df.columns:
        totals["impressions"] = int(df["impressions"].sum())
    if "ctr" in df.columns:
        # Weighted CTR
        if totals.get("impressions", 0) > 0:
            totals["ctr"] = round((totals.get("clicks", 0) / totals["impressions"]) * 100, 2)
        else:
            totals["ctr"] = 0.0
    if "position" in df.columns:
        # Weighted avg position
        if "impressions" in df.columns and totals.get("impressions", 0) > 0:
            totals["avg_position"] = round(
                (df["position"] * df["impressions"]).sum() / df["impressions"].sum(), 1
            )
        else:
            totals["avg_position"] = round(df["position"].mean(), 1)

    # Top queries
    top_queries = []
    if "query" in df.columns:
        query_df = df.groupby("query").agg({
            col: "sum" if col in ("clicks", "impressions") else "mean"
            for col in ["clicks", "impressions", "ctr", "position"]
            if col in df.columns
        }).reset_index()

        if "clicks" in query_df.columns:
            query_df = query_df.sort_values("clicks", ascending=False)

        # Recalculate CTR from totals
        if "clicks" in query_df.columns and "impressions" in query_df.columns:
            mask = query_df["impressions"] > 0
            query_df.loc[mask, "ctr"] = (
                query_df.loc[mask, "clicks"] / query_df.loc[mask, "impressions"] * 100
            ).round(2)

        for _, row in query_df.head(50).iterrows():
            q = {"query": row.get("query", "")}
            for col in ["clicks", "impressions", "ctr", "position"]:
                if col in row.index:
                    val = row[col]
                    q[col] = int(val) if col in ("clicks", "impressions") else round(float(val), 2)
            top_queries.append(q)

    # Top pages
    top_pages = []
    if "page" in df.columns:
        page_df = df.groupby("page").agg({
            col: "sum" if col in ("clicks", "impressions") else "mean"
            for col in ["clicks", "impressions", "ctr", "position"]
            if col in df.columns
        }).reset_index()
        if "clicks" in page_df.columns:
            page_df = page_df.sort_values("clicks", ascending=False)
        if "clicks" in page_df.columns and "impressions" in page_df.columns:
            mask = page_df["impressions"] > 0
            page_df.loc[mask, "ctr"] = (
                page_df.loc[mask, "clicks"] / page_df.loc[mask, "impressions"] * 100
            ).round(2)
        for _, row in page_df.head(30).iterrows():
            p = {"page": row.get("page", "")}
            for col in ["clicks", "impressions", "ctr", "position"]:
                if col in row.index:
                    val = row[col]
                    p[col] = int(val) if col in ("clicks", "impressions") else round(float(val), 2)
            top_pages.append(p)

    # Opportunity queries: high impressions but low CTR (position 4-20)
    opportunity_queries = []
    if "query" in df.columns and "impressions" in df.columns and "position" in df.columns:
        opp_df = df[
            (df["position"] >= 4) & (df["position"] <= 20) & (df["impressions"] >= 10)
        ].copy()
        if len(opp_df) > 0:
            opp_df = opp_df.sort_values("impressions", ascending=False)
            for _, row in opp_df.head(20).iterrows():
                o = {"query": row.get("query", "")}
                for col in ["clicks", "impressions", "ctr", "position"]:
                    if col in row.index:
                        val = row[col]
                        o[col] = int(val) if col in ("clicks", "impressions") else round(float(val), 2)
                opportunity_queries.append(o)

    return {
        "totals": totals,
        "top_queries": top_queries,
        "top_pages": top_pages,
        "opportunity_queries": opportunity_queries,
        "row_count": len(df),
        "columns_found": list(df.columns),
    }


def _detect_header_row(filepath, max_rows=10):
    """
    Some exports have metadata rows before the header.
    Try to detect where the actual header row is.
    """
    with open(filepath, "r", encoding="utf-8-sig", errors="replace") as f:
        lines = []
        for i in range(max_rows):
            line = f.readline()
            if not line:
                break
            lines.append(line.strip())

    # Heuristic: the header row usually has the most commas
    # and contains recognizable column names
    known_terms = {
        "sessions", "users", "clicks", "impressions", "campaign", "query",
        "page", "ctr", "cpc", "spend", "source", "medium", "date", "position",
        "bounce", "conversions", "reach", "results", "revenue"
    }

    best_row = 0
    best_score = 0
    for i, line in enumerate(lines):
        parts = [p.strip().lower().strip('"') for p in line.split(",")]
        score = sum(1 for p in parts if any(term in p for term in known_terms))
        if score > best_score:
            best_score = score
            best_row = i

    return best_row if best_row > 0 else 0


def load_client_data(client_id, month, import_dir=None):
    """
    Load all available data files for a client/month.
    Returns a dict with keys: google_analytics, meta_business, search_console.
    Each value is the parsed dict or None if the file wasn't found.
    """
    if import_dir is None:
        import_dir = Path(__file__).parent.parent / "data" / "imports"

    client_dir = Path(import_dir) / client_id / month
    result = {}

    # Google Analytics
    ga_files = ["google_analytics.csv", "ga.csv", "analytics.csv", "ga4.csv"]
    for fname in ga_files:
        fpath = client_dir / fname
        if fpath.exists():
            result["google_analytics"] = parse_google_analytics(str(fpath))
            break

    # Meta Business Suite
    meta_files = ["meta_business.csv", "meta.csv", "facebook.csv", "fb_ads.csv", "meta_ads.csv", "ads_manager.csv"]
    for fname in meta_files:
        fpath = client_dir / fname
        if fpath.exists():
            result["meta_business"] = parse_meta_business(str(fpath))
            break

    # Google Search Console
    gsc_files = ["search_console.csv", "gsc.csv", "search.csv", "google_search.csv"]
    for fname in gsc_files:
        fpath = client_dir / fname
        if fpath.exists():
            result["search_console"] = parse_search_console(str(fpath))
            break

    return result
