"""
Google Business Profile Tool

Uses the Google Places API (New) to fetch business details, reviews,
and profile completeness using the brand's Place ID and Maps API key.
No restricted GBP API access or special OAuth scopes needed.
"""
import json
import logging
import requests

import openai

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


# ---------------------------------------------------------------------------
# GBP Approval & Optimization Audit
# ---------------------------------------------------------------------------

# Scoring weights (out of 100 total)
_AUDIT_WEIGHTS = {
    "business_name":   5,
    "address":         10,
    "phone":           8,
    "website":         8,
    "category":        8,
    "hours":           8,
    "photos":          12,
    "description":     10,
    "reviews":         15,
    "rating":          8,
    "verification":    8,
}


def _score_field(field, gbp_ctx):
    """Return (score 0-100, grade, detail_text) for one audit dimension."""
    if field == "business_name":
        name = gbp_ctx.get("business_name", "")
        if not name:
            return 0, "fail", "No business name found."
        if len(name) > 80:
            return 60, "warn", "Name is very long (%d chars). Keep it under 80 characters to avoid truncation." % len(name)
        return 100, "pass", "Business name is set: %s" % name

    if field == "address":
        addr = gbp_ctx.get("address", "")
        if not addr:
            return 0, "fail", "No address on file. Google requires a valid street address or service area for verification."
        return 100, "pass", "Address is set."

    if field == "phone":
        phone = gbp_ctx.get("phone", "")
        if not phone:
            return 0, "fail", "No phone number. A local phone number (not toll-free) helps verification and local ranking."
        return 100, "pass", "Phone number is set."

    if field == "website":
        url = gbp_ctx.get("website", "")
        if not url:
            return 0, "fail", "No website link. A live website matching your business name and address strengthens verification."
        return 100, "pass", "Website URL is linked."

    if field == "category":
        cat = gbp_ctx.get("category", "")
        if not cat:
            return 0, "fail", "No business category selected. Choose the category that best describes your primary service."
        return 100, "pass", "Primary category: %s" % cat

    if field == "hours":
        hours = gbp_ctx.get("hours") or {}
        periods = hours.get("periods") or []
        if not periods:
            return 0, "fail", "No business hours set. Set hours so customers know when you are open."
        weekdays_covered = set()
        for p in periods:
            od = p.get("open", {}).get("day")
            if od is not None:
                weekdays_covered.add(od)
        if len(weekdays_covered) < 5:
            return 60, "warn", "Only %d days have hours. Most businesses should show hours for at least 5 days." % len(weekdays_covered)
        return 100, "pass", "Hours are set for %d days." % len(weekdays_covered)

    if field == "photos":
        count = gbp_ctx.get("photo_count", 0)
        if count == 0:
            return 0, "fail", "No photos. Listings with photos get 42%% more direction requests and 35%% more website clicks."
        if count < 5:
            return 40, "warn", "%d photo(s). Google recommends at least 5-10 high-quality photos (interior, exterior, team, work examples)." % count
        if count < 10:
            return 70, "warn", "%d photos - good start. Top-performing listings average 10-25 photos. Add more variety." % count
        return 100, "pass", "%d photos uploaded." % count

    if field == "description":
        desc = gbp_ctx.get("description", "")
        if not desc:
            return 0, "fail", "No business description. Write 250-750 characters describing your services, area, and what sets you apart."
        length = len(desc)
        if length < 100:
            return 40, "warn", "Description is very short (%d chars). Aim for 250-750 characters with your services and service area." % length
        if length < 250:
            return 70, "warn", "Description could be longer (%d chars). Google allows up to 750 characters - use them." % length
        return 100, "pass", "Description is %d characters." % length

    if field == "reviews":
        count = gbp_ctx.get("review_count", 0)
        if count == 0:
            return 0, "fail", "No reviews yet. Reviews are the #1 factor in local pack ranking. Start asking every customer."
        if count < 5:
            return 30, "warn", "%d review(s). You need at least 5 to display a star rating in search results." % count
        if count < 20:
            return 60, "warn", "%d reviews. Competitive businesses typically have 20+. Keep building." % count
        if count < 50:
            return 80, "pass", "%d reviews - solid. The top local businesses in most markets have 50+." % count
        return 100, "pass", "%d reviews - strong review profile." % count

    if field == "rating":
        rating = gbp_ctx.get("rating", 0)
        count = gbp_ctx.get("review_count", 0)
        if count == 0:
            return 0, "fail", "No rating yet (no reviews)."
        if rating < 3.5:
            return 20, "fail", "%.1f star rating. Below 3.5 stars hurts click-through significantly. Focus on service quality and responding to negative reviews." % rating
        if rating < 4.0:
            return 50, "warn", "%.1f stars. Good but not competitive. Most customers filter for 4.0+ stars." % rating
        if rating < 4.5:
            return 75, "pass", "%.1f stars. Solid rating." % rating
        return 100, "pass", "%.1f stars - excellent." % rating

    if field == "verification":
        status = gbp_ctx.get("status", "UNKNOWN")
        if status == "VERIFIED":
            return 100, "pass", "Profile is live and verified on Google."
        if "CLOSED" in (status or ""):
            return 30, "fail", "Profile is marked as %s. Update status at business.google.com." % status.replace("_", " ").title()
        return 0, "fail", "Profile verification status is unknown. The listing may not be claimed or verified yet."

    return 0, "fail", "Unknown field."


def run_gbp_audit(gbp_ctx):
    """
    Run a full GBP audit scoring each dimension.
    Returns {overall_score, overall_grade, sections[], action_items[]}.
    """
    sections = []
    action_items = []
    weighted_total = 0

    for field, weight in _AUDIT_WEIGHTS.items():
        score, grade, detail = _score_field(field, gbp_ctx)
        label = field.replace("_", " ").title()
        sections.append({
            "field": field,
            "label": label,
            "score": score,
            "grade": grade,
            "weight": weight,
            "detail": detail,
        })
        weighted_total += score * weight / 100

        if grade != "pass":
            priority = "high" if grade == "fail" else "medium"
            action_items.append({
                "field": field,
                "label": label,
                "priority": priority,
                "detail": detail,
            })

    overall = round(weighted_total)
    if overall >= 85:
        overall_grade = "A"
    elif overall >= 70:
        overall_grade = "B"
    elif overall >= 50:
        overall_grade = "C"
    elif overall >= 30:
        overall_grade = "D"
    else:
        overall_grade = "F"

    # Sort action items: high priority first
    action_items.sort(key=lambda a: 0 if a["priority"] == "high" else 1)

    return {
        "overall_score": overall,
        "overall_grade": overall_grade,
        "sections": sections,
        "action_items": action_items,
    }


# ---------------------------------------------------------------------------
# AI-powered audit recommendations
# ---------------------------------------------------------------------------

_AUDIT_SYSTEM_PROMPT = """\
You are a Google Business Profile optimization expert. A business owner needs \
help getting their GBP approved, verified, and ranking well in local search.

You will receive their current profile data and audit scores. Provide:
1. A plain-English summary of where they stand (2-3 sentences, no fluff).
2. An optimized business description they can copy-paste (250-600 chars). \
   Include their services, service area, and a differentiator. No keyword stuffing.
3. A prioritized action plan - the 3-5 most impactful things to do next, in order. \
   Each item should be one concrete step, not vague advice.
4. If they have verification issues, provide specific troubleshooting steps \
   for the most common rejection reasons (address mismatch, category issues, \
   duplicate listings, suspended profiles).
5. A short review request message they can text/email to customers (under 160 chars).

Write for a non-technical business owner. Keep it direct and practical.
Do NOT use em dashes. Use regular dashes, commas, or periods instead.
Return valid JSON with these keys: summary, optimized_description, action_plan (array of strings), \
verification_help (array of strings, empty if verified), review_request_template.\
"""


def run_ai_audit(gbp_ctx, audit_result, brand, api_key, model):
    """
    Call the LLM to generate personalized audit recommendations.
    Returns dict with summary, optimized_description, action_plan, etc.
    Falls back to None on error.
    """
    if not api_key:
        return None

    profile_data = {
        "business_name": gbp_ctx.get("business_name", ""),
        "address": gbp_ctx.get("address", ""),
        "phone": gbp_ctx.get("phone", ""),
        "website": gbp_ctx.get("website", ""),
        "category": gbp_ctx.get("category", ""),
        "description": gbp_ctx.get("description", ""),
        "rating": gbp_ctx.get("rating", 0),
        "review_count": gbp_ctx.get("review_count", 0),
        "photo_count": gbp_ctx.get("photo_count", 0),
        "status": gbp_ctx.get("status", "UNKNOWN"),
        "has_hours": bool(gbp_ctx.get("hours")),
        "overall_score": audit_result["overall_score"],
        "overall_grade": audit_result["overall_grade"],
        "failing_items": [a["detail"] for a in audit_result["action_items"]],
    }

    brand_info = ""
    industry = (brand.get("industry") or "").strip()
    services = (brand.get("primary_services") or "").strip()
    area = (brand.get("service_area") or "").strip()
    voice = (brand.get("brand_voice") or "").strip()
    if industry:
        brand_info += "Industry: %s\n" % industry
    if services:
        brand_info += "Services: %s\n" % services
    if area:
        brand_info += "Service area: %s\n" % area
    if voice:
        brand_info += "Brand voice: %s\n" % voice

    user_prompt = "Profile data:\n%s\n\n" % json.dumps(profile_data, indent=2)
    if brand_info:
        user_prompt += "Additional business info:\n%s\n" % brand_info

    try:
        client = openai.OpenAI(api_key=api_key)
        resp = client.chat.completions.create(
            model=model or "gpt-4o-mini",
            messages=[
                {"role": "system", "content": _AUDIT_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.4,
            max_tokens=1500,
        )
        text = resp.choices[0].message.content.strip()
        # Strip markdown code fences if present
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3]
        if text.startswith("json"):
            text = text[4:]
        return json.loads(text.strip())
    except Exception as exc:
        logger.warning("AI audit failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Verification troubleshooter - common issues and fixes
# ---------------------------------------------------------------------------

VERIFICATION_ISSUES = [
    {
        "issue": "I never received my verification postcard",
        "icon": "bi-mailbox",
        "steps": [
            "Wait the full 14 business days before requesting a new one.",
            "Check that your address is entered exactly as it appears on official mail (USPS format).",
            "Request a new postcard from business.google.com if it has been more than 14 days.",
            "If postcards keep failing, try phone or email verification if those options appear.",
            "Some categories and locations qualify for video verification - check if that option is available.",
        ],
    },
    {
        "issue": "My profile was suspended",
        "icon": "bi-exclamation-octagon",
        "steps": [
            "Check your email for a suspension notice explaining the reason.",
            "Common causes: using a virtual office address, keyword stuffing in business name, or fake reviews.",
            "Remove any keywords from your business name - it must be your real legal business name only.",
            "If you use a virtual office or PO box, switch to your real physical or home address.",
            "Submit a reinstatement request at business.google.com/appeal.",
            "After reinstating, re-verify using the method Google provides.",
        ],
    },
    {
        "issue": "Duplicate listing blocking my verification",
        "icon": "bi-files",
        "steps": [
            "Search Google Maps for your business name and address to find duplicate listings.",
            "If you own both listings, merge them at business.google.com by marking one as duplicate.",
            "If someone else owns the duplicate, use 'Suggest an edit' on Maps to mark it as 'Place has closed or doesn't exist'.",
            "Contact Google Business Profile support through the Help Community if self-service options fail.",
            "Once duplicates are resolved, restart the verification process.",
        ],
    },
    {
        "issue": "Address verification keeps failing",
        "icon": "bi-geo-alt-fill",
        "steps": [
            "Make sure your address is formatted exactly as the postal service lists it (check USPS.com).",
            "Remove suite/unit numbers if they are causing issues, then add them back after verification.",
            "If you are a service-area business (you go to customers), switch to 'service area' and hide your address.",
            "Ensure your address matches what appears on your website, social media, and other directory listings.",
            "Try requesting video verification as an alternative - record a clear video showing your address and signage.",
        ],
    },
    {
        "issue": "I can't find my business to claim it",
        "icon": "bi-search",
        "steps": [
            "Go to business.google.com/create and enter your business name and address.",
            "If the business does not appear, choose 'Add your business to Google'.",
            "Fill out every required field: name, category, address, phone, website.",
            "If it shows as already claimed by someone else, click 'Request access' and Google will contact the current owner.",
            "For new businesses, verification typically takes 5-14 days after submitting your request.",
        ],
    },
    {
        "issue": "My business category isn't available",
        "icon": "bi-tag",
        "steps": [
            "Type your service in the category search - Google has over 4,000 categories and they may use different wording.",
            "Choose the closest match available. You can add more secondary categories after verification.",
            "Do NOT pick a popular but wrong category just for visibility - Google penalizes miscategorization.",
            "If your exact category doesn't exist, pick the broadest accurate option (e.g., 'Contractor' instead of 'Custom Deck Builder').",
            "Submit a category suggestion to Google through the Business Profile Help Community.",
        ],
    },
]
