"""
Google Business Profile Integration

Fetches GMB account info, location details, and verification status
using the Google Business Profile API (v1) and My Business Verifications API.
Requires the business.manage OAuth scope.
"""
import logging
import requests

logger = logging.getLogger(__name__)

GBP_BASE = "https://mybusinessbusinessinformation.googleapis.com/v1"
VERIFY_BASE = "https://mybusinessverifications.googleapis.com/v1"
ACCOUNT_BASE = "https://mybusinessaccountmanagement.googleapis.com/v1"


def _headers(access_token):
    return {"Authorization": f"Bearer {access_token}"}


class GBPAccessError(Exception):
    """Raised when the Business Profile API is not enabled or access is denied."""
    pass


def get_accounts(access_token):
    """List all GBP accounts the authenticated user has access to."""
    resp = requests.get(
        f"{ACCOUNT_BASE}/accounts",
        headers=_headers(access_token),
        timeout=15,
    )
    if resp.status_code == 403:
        body = resp.text[:500]
        if "not been used" in body or "is disabled" in body or "ACCESS_TOKEN_SCOPE_INSUFFICIENT" in body or "PERMISSION_DENIED" in body:
            raise GBPAccessError(body)
        logger.warning("GBP accounts 403: %s", body)
        return []
    if resp.status_code != 200:
        logger.warning("GBP accounts list failed (%s): %s", resp.status_code, resp.text[:300])
        return []
    data = resp.json()
    return data.get("accounts", [])


def get_locations(access_token, account_name):
    """List locations for a GBP account. account_name looks like 'accounts/123'."""
    resp = requests.get(
        f"{GBP_BASE}/{account_name}/locations",
        headers=_headers(access_token),
        params={"readMask": "name,title,storefrontAddress,websiteUri,phoneNumbers,metadata"},
        timeout=15,
    )
    if resp.status_code != 200:
        logger.warning("GBP locations list failed (%s): %s", resp.status_code, resp.text[:300])
        return []
    data = resp.json()
    return data.get("locations", [])


def get_location_detail(access_token, location_name):
    """Get full details for a single location. location_name like 'locations/123'."""
    resp = requests.get(
        f"{GBP_BASE}/{location_name}",
        headers=_headers(access_token),
        params={"readMask": "name,title,storefrontAddress,websiteUri,phoneNumbers,metadata,profile,regularHours,categories"},
        timeout=15,
    )
    if resp.status_code != 200:
        logger.warning("GBP location detail failed (%s): %s", resp.status_code, resp.text[:300])
        return None
    return resp.json()


def get_verification_state(access_token, location_name):
    """Check verification status for a location."""
    resp = requests.get(
        f"{VERIFY_BASE}/{location_name}/verifications",
        headers=_headers(access_token),
        timeout=15,
    )
    if resp.status_code != 200:
        logger.warning("GBP verification check failed (%s): %s", resp.status_code, resp.text[:300])
        return {"status": "UNKNOWN", "verifications": []}
    data = resp.json()
    verifications = data.get("verifications", [])
    if verifications:
        latest = verifications[0]
        return {
            "status": latest.get("state", "UNKNOWN"),
            "method": latest.get("method", ""),
            "create_time": latest.get("createTime", ""),
            "verifications": verifications,
        }
    return {"status": "UNVERIFIED", "verifications": []}


def get_available_verification_options(access_token, location_name):
    """Fetch which verification methods are available for this location."""
    resp = requests.post(
        f"{VERIFY_BASE}/{location_name}:fetchVerificationOptions",
        headers=_headers(access_token),
        json={"languageCode": "en"},
        timeout=15,
    )
    if resp.status_code != 200:
        logger.warning("GBP verification options failed (%s): %s", resp.status_code, resp.text[:300])
        return []
    data = resp.json()
    return data.get("options", [])


def score_profile_completeness(location):
    """Score profile completeness 0-100 based on key fields being populated."""
    checks = {
        "Business name": bool(location.get("title")),
        "Address": bool(location.get("storefrontAddress")),
        "Phone number": bool(location.get("phoneNumbers", {}).get("primaryPhone")),
        "Website": bool(location.get("websiteUri")),
        "Categories": bool(location.get("categories", {}).get("primaryCategory")),
        "Business hours": bool(location.get("regularHours")),
        "Description": bool(location.get("profile", {}).get("description")),
    }
    filled = sum(1 for v in checks.values() if v)
    return {
        "score": round(filled / len(checks) * 100),
        "total": len(checks),
        "filled": filled,
        "details": checks,
    }


def build_gmb_context(db, brand_id):
    """
    Gather all GMB data for a brand: accounts, locations, verification, completeness.
    Returns a dict ready for the template, or None if no Google connection.
    """
    from webapp.google_drive import get_valid_access_token

    token = get_valid_access_token(db, brand_id)
    if not token:
        return None

    result = {
        "connected": True,
        "accounts": [],
        "locations": [],
        "selected_location": None,
        "verification": None,
        "completeness": None,
        "verification_options": [],
        "error": None,
    }

    try:
        accounts = get_accounts(token)
        result["accounts"] = accounts

        if not accounts:
            result["error"] = "No Google Business Profile accounts found for this Google account."
            return result

    except GBPAccessError:
        result["error"] = "API_NOT_ENABLED"
        return result

    try:

        # Gather all locations across all accounts
        all_locations = []
        for acct in accounts:
            locs = get_locations(token, acct["name"])
            for loc in locs:
                loc["_account_name"] = acct.get("accountName", acct["name"])
            all_locations.extend(locs)

        result["locations"] = all_locations

        if not all_locations:
            result["error"] = "Google account connected but no business locations found."
            return result

        # Use the first location (or match by google_place_id if stored)
        brand = db.get_brand(brand_id)
        selected = all_locations[0]
        stored_place_id = (brand.get("google_place_id") or "").strip()
        if stored_place_id:
            for loc in all_locations:
                meta = loc.get("metadata", {})
                if meta.get("placeId") == stored_place_id:
                    selected = loc
                    break

        # Get full details
        loc_name = selected["name"]
        detail = get_location_detail(token, loc_name)
        if detail:
            selected = detail
        result["selected_location"] = selected

        # Verification status
        result["verification"] = get_verification_state(token, loc_name)

        # Completeness score
        result["completeness"] = score_profile_completeness(selected)

        # If not verified, get available methods
        v_status = result["verification"].get("status", "")
        if v_status not in ("COMPLETED", "VERIFIED"):
            result["verification_options"] = get_available_verification_options(token, loc_name)

    except Exception as e:
        logger.exception("Error building GMB context for brand %s", brand_id)
        result["error"] = f"Error loading business profile data: {str(e)}"

    return result


# Verification guidance text keyed by method
VERIFICATION_GUIDANCE = {
    "PHONE_CALL": {
        "title": "Phone Verification",
        "icon": "bi-telephone",
        "steps": [
            "Google will call the business phone number on file.",
            "Answer the call and listen for the verification code.",
            "Enter the code in your Google Business Profile dashboard.",
            "Verification is usually instant once the code is entered.",
        ],
    },
    "SMS": {
        "title": "Text Message (SMS)",
        "icon": "bi-chat-dots",
        "steps": [
            "Google will send an SMS to the business phone number.",
            "Check your text messages for a verification code.",
            "Enter the code in your Google Business Profile dashboard.",
            "Make sure the phone number on your profile is correct and can receive texts.",
        ],
    },
    "EMAIL": {
        "title": "Email Verification",
        "icon": "bi-envelope",
        "steps": [
            "Google will send an email to the address associated with your domain.",
            "Check your inbox (and spam folder) for the verification email.",
            "Click the verification link or enter the code provided.",
            "This option requires a matching domain email (e.g. you@yourbusiness.com).",
        ],
    },
    "ADDRESS": {
        "title": "Postcard Verification",
        "icon": "bi-mailbox",
        "steps": [
            "Google will mail a postcard to your business address with a verification code.",
            "The postcard typically arrives within 5-14 business days.",
            "Do NOT change your business name or address while waiting.",
            "Once received, enter the code in your Google Business Profile.",
        ],
    },
    "VIDEO": {
        "title": "Video Verification",
        "icon": "bi-camera-video",
        "steps": [
            "Record a video showing the exterior of your business (signage visible).",
            "Show the interior and any equipment/tools related to your services.",
            "Show proof of your location (street signs, nearby landmarks).",
            "Upload through the Google Business Profile verification flow.",
            "Review typically takes a few business days.",
        ],
    },
}
