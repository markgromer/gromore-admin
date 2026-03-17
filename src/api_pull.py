"""
Unified API Data Pulling

Coordinates pulling from all three APIs for a given client.
Falls back to CSV imports if APIs aren't configured.
"""
import json
from pathlib import Path


CONFIG_DIR = Path(__file__).parent.parent / "config"


def pull_all_api_data(client_id, month, client_config):
    """
    Pull data from all configured APIs for a client.

    Checks which APIs are configured in the client config and pulls from each.
    Falls back gracefully - if an API isn't configured, it's skipped.

    Args:
        client_id: Client identifier
        month: Month string (YYYY-MM)
        client_config: Client config dict from clients.json

    Returns:
        Dict with keys: google_analytics, meta_business, search_console
        Each value is the parsed data dict or None if not available.
        Also returns a 'sources' dict indicating which were pulled via API.
    """
    api_config = client_config.get("api", {})
    result = {}
    sources = {}

    # ── Google Analytics 4 ──
    ga4_property = api_config.get("ga4_property_id")
    if ga4_property:
        try:
            from .api_google_analytics import pull_ga4_data
            print(f"  Pulling GA4 data (property: {ga4_property})...")
            result["google_analytics"] = pull_ga4_data(ga4_property, month)
            sources["google_analytics"] = "api"
            print(f"    OK - {result['google_analytics'].get('totals', {}).get('sessions', 0)} sessions")
        except ImportError:
            print("    SKIP - google-analytics-data package not installed")
            print("    Run: pip install google-analytics-data")
        except FileNotFoundError as e:
            print(f"    SKIP - {e}")
        except Exception as e:
            print(f"    ERROR - {e}")

    # ── Google Search Console ──
    gsc_site = api_config.get("gsc_site_url") or client_config.get("website")
    gsc_enabled = api_config.get("gsc_enabled", True)
    if gsc_site and gsc_enabled and ga4_property:
        # Only try GSC if Google credentials exist (same service account)
        try:
            from .api_search_console import pull_search_console_data
            print(f"  Pulling Search Console data ({gsc_site})...")
            result["search_console"] = pull_search_console_data(gsc_site, month)
            sources["search_console"] = "api"
            print(f"    OK - {result['search_console'].get('totals', {}).get('clicks', 0)} clicks")
        except ImportError:
            print("    SKIP - google-api-python-client package not installed")
            print("    Run: pip install google-api-python-client google-auth")
        except FileNotFoundError as e:
            print(f"    SKIP - {e}")
        except Exception as e:
            print(f"    ERROR - {e}")

    # ── Meta Marketing API ──
    meta_account = api_config.get("meta_ad_account_id")
    if meta_account:
        try:
            from .api_meta import pull_meta_ads_data
            print(f"  Pulling Meta Ads data (account: {meta_account})...")
            result["meta_business"] = pull_meta_ads_data(meta_account, month)
            sources["meta_business"] = "api"
            print(f"    OK - ${result['meta_business'].get('totals', {}).get('spend', 0)} spend")
        except ImportError:
            print("    SKIP - facebook-business package not installed")
            print("    Run: pip install facebook-business")
        except FileNotFoundError as e:
            print(f"    SKIP - {e}")
        except Exception as e:
            print(f"    ERROR - {e}")

    return result, sources


def merge_api_and_csv_data(api_data, csv_data, api_sources):
    """
    Merge API-pulled data with CSV imports.
    API data takes priority over CSV for the same source.
    CSV fills in any gaps where API isn't configured.
    """
    merged = {}

    for source in ["google_analytics", "meta_business", "search_console"]:
        if source in api_data and api_sources.get(source) == "api":
            merged[source] = api_data[source]
        elif source in csv_data:
            merged[source] = csv_data[source]

    return merged


def check_api_setup(client_config):
    """
    Check which APIs are configured for a client and report status.
    Useful for diagnostics.
    """
    api_config = client_config.get("api", {})
    status = {
        "ga4": {
            "configured": bool(api_config.get("ga4_property_id")),
            "property_id": api_config.get("ga4_property_id"),
            "credentials_exist": (CONFIG_DIR / "google_credentials.json").exists(),
        },
        "search_console": {
            "configured": bool(api_config.get("gsc_site_url") or client_config.get("website")),
            "site_url": api_config.get("gsc_site_url") or client_config.get("website"),
            "credentials_exist": (CONFIG_DIR / "google_credentials.json").exists(),
        },
        "meta": {
            "configured": bool(api_config.get("meta_ad_account_id")),
            "account_id": api_config.get("meta_ad_account_id"),
            "credentials_exist": (CONFIG_DIR / "meta_credentials.json").exists(),
        },
    }

    # Check package availability
    try:
        import google.analytics.data_v1beta
        status["ga4"]["package_installed"] = True
    except ImportError:
        status["ga4"]["package_installed"] = False

    try:
        import googleapiclient
        status["search_console"]["package_installed"] = True
    except ImportError:
        status["search_console"]["package_installed"] = False

    try:
        import facebook_business
        status["meta"]["package_installed"] = True
    except ImportError:
        status["meta"]["package_installed"] = False

    return status
