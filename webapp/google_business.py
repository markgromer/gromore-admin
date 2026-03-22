"""
Google Business Profile Tool

Uses the Google Places API (New) to fetch business details, reviews,
and profile completeness using the brand's Place ID and Maps API key.
No restricted GBP API access or special OAuth scopes needed.
"""
import logging
import requests

logger = logging.getLogger(__name__)

PLACES_BASE = "https://places.googleapis.com/v1"


def get_place_details(api_key, place_id):
    """Fetch place details from the Places API (New)."""
    fields = [
        "id", "displayName", "formattedAddress", "nationalPhoneNumber",
        "internationalPhoneNumber", "websiteUri", "googleMapsUri",
        "businessStatus", "primaryType", "primaryTypeDisplayName",
        "regularOpeningHours", "rating", "userRatingCount",
        "editorialSummary", "reviews", "photos",
        "currentOpeningHours", "shortFormattedAddress",
    ]
    resp = requests.get(
        f"{PLACES_BASE}/places/{place_id}",
        headers={
            "X-Goog-Api-Key": api_key,
            "X-Goog-FieldMask": ",".join(fields),
        },
        timeout=15,
    )
    if resp.status_code != 200:
        logger.warning(
            "Places API failed (%s): %s", resp.status_code, resp.text[:300]
        )
        return None
    return resp.json()


def score_profile_completeness(place):
    """Score profile completeness 0-100 based on key fields."""
    checks = {
        "Business name": bool(place.get("displayName", {}).get("text")),
        "Address": bool(place.get("formattedAddress")),
        "Phone number": bool(place.get("nationalPhoneNumber")),
        "Website": bool(place.get("websiteUri")),
        "Business category": bool(place.get("primaryTypeDisplayName")),
        "Business hours": bool(place.get("regularOpeningHours")),
        "Photos": bool(place.get("photos")),
        "Reviews (5+)": (place.get("userRatingCount") or 0) >= 5,
    }
    filled = sum(1 for v in checks.values() if v)
    return {
        "score": round(filled / len(checks) * 100),
        "total": len(checks),
        "filled": filled,
        "details": checks,
    }


def _extract_reviews(place):
    """Pull review data into a simple list."""
    raw = place.get("reviews") or []
    reviews = []
    for r in raw[:5]:
        reviews.append({
            "author": r.get("authorAttribution", {}).get("displayName", ""),
            "rating": r.get("rating", 0),
            "text": r.get("text", {}).get("text", ""),
            "time": r.get("relativePublishTimeDescription", ""),
        })
    return reviews


def build_gbp_context(db, brand_id):
    """
    Gather Google Business Profile data for a brand using the Places API.
    Returns a dict for the template, or None if place_id/api_key are missing.
    """
    brand = db.get_brand(brand_id)
    if not brand:
        return None

    place_id = (brand.get("google_place_id") or "").strip()
    api_key = (brand.get("google_maps_api_key") or "").strip()

    if not place_id or not api_key:
        return {"error": "MISSING_CONFIG", "place": None, "completeness": None, "reviews": []}

    place = get_place_details(api_key, place_id)
    if not place:
        return {"error": "API_FAILED", "place": None, "completeness": None, "reviews": []}

    # Determine verification-like status from businessStatus
    biz_status = place.get("businessStatus", "")
    # OPERATIONAL = live on Google, CLOSED_TEMPORARILY, CLOSED_PERMANENTLY
    # If it appears in Places API at all with OPERATIONAL, it's verified + live
    if biz_status == "OPERATIONAL":
        status = "VERIFIED"
    elif biz_status in ("CLOSED_TEMPORARILY", "CLOSED_PERMANENTLY"):
        status = biz_status
    else:
        status = "UNKNOWN"

    return {
        "error": None,
        "place": place,
        "status": status,
        "business_name": place.get("displayName", {}).get("text", ""),
        "address": place.get("formattedAddress", ""),
        "short_address": place.get("shortFormattedAddress", ""),
        "phone": place.get("nationalPhoneNumber", ""),
        "website": place.get("websiteUri", ""),
        "maps_url": place.get("googleMapsUri", ""),
        "category": place.get("primaryTypeDisplayName", {}).get("text", "")
                    if isinstance(place.get("primaryTypeDisplayName"), dict)
                    else place.get("primaryTypeDisplayName", ""),
        "rating": place.get("rating", 0),
        "review_count": place.get("userRatingCount", 0),
        "hours": place.get("regularOpeningHours", {}),
        "description": place.get("editorialSummary", {}).get("text", "")
                       if isinstance(place.get("editorialSummary"), dict)
                       else "",
        "completeness": score_profile_completeness(place),
        "reviews": _extract_reviews(place),
        "photo_count": len(place.get("photos") or []),
    }


# Verification guidance for common states
VERIFICATION_GUIDANCE = {
    "not_claimed": {
        "title": "Claim Your Business",
        "icon": "bi-flag",
        "steps": [
            "Go to business.google.com and search for your business name.",
            "Select your business from the results.",
            "Click 'Claim this business' or 'Own this business?'",
            "Follow Google's verification steps (phone, email, postcard, or video).",
            "Once verified, come back here and your status will update.",
        ],
    },
    "improve_profile": {
        "title": "Improve Your Profile",
        "icon": "bi-pencil-square",
        "steps": [
            "Log in at business.google.com with the account that owns this listing.",
            "Fill in every missing field (description, hours, phone, website, photos).",
            "Add at least 5-10 high-quality photos of your work, team, and location.",
            "Write a compelling business description with your key services.",
            "Ask satisfied customers to leave Google reviews.",
        ],
    },
    "get_reviews": {
        "title": "Build Your Reviews",
        "icon": "bi-star",
        "steps": [
            "Share your Google review link with happy customers after completing a job.",
            "Add the review link to your email signature and invoices.",
            "Respond to every review (positive and negative) professionally.",
            "Aim for at least 20+ reviews with a 4.5+ average to dominate local search.",
        ],
    },
}
