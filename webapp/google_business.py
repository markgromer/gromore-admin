"""
Google Business Profile Tool

Uses the Google Places API (New) to fetch business details, reviews,
and profile completeness using the brand's Place ID and Maps API key.
No restricted GBP API access or special OAuth scopes needed.
"""
import json
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

    raw_editorial_summary = place.get("editorialSummary")
    if isinstance(raw_editorial_summary, dict):
        description = (raw_editorial_summary.get("text") or "").strip()
    elif isinstance(raw_editorial_summary, str):
        description = raw_editorial_summary.strip()
    else:
        description = ""

    if description:
        description_status = "present"
    elif "editorialSummary" in place:
        description_status = "missing"
    else:
        # Places API often omits the owner-written GBP description entirely,
        # so absence here is not reliable proof that the profile has no description.
        description_status = "unverified"

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
        "description": description,
        "description_status": description_status,
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
# GBP Approval & Optimization Audit  -  Quest / Micro-Win System
# ---------------------------------------------------------------------------

# Each quest: xp it's worth, icon, what the user is learning, and micro-steps
_QUEST_META = {
    "verification": {
        "xp": 200,
        "icon": "bi-shield-check",
        "quest_name": "Get on the Map",
        "skill": "Google Verification",
        "learning": "You're proving to Google that your business is real. "
                    "This is the #1 thing that decides whether customers can find you.",
    },
    "address": {
        "xp": 150,
        "icon": "bi-geo-alt-fill",
        "quest_name": "Pin Your Location",
        "skill": "Local SEO Basics",
        "learning": "Google matches your address against postal records. "
                    "An exact match helps verification and shows you in local searches for your area.",
    },
    "phone": {
        "xp": 100,
        "icon": "bi-telephone-fill",
        "quest_name": "Open the Phone Line",
        "skill": "Trust Signals",
        "learning": "A local phone number tells Google (and customers) you're a real, "
                    "reachable business, not a fly-by-night operation.",
    },
    "website": {
        "xp": 100,
        "icon": "bi-globe",
        "quest_name": "Link Your Website",
        "skill": "Online Presence",
        "learning": "Your website is your digital storefront. Linking it connects "
                    "your Google profile to your brand and gives customers a next step.",
    },
    "business_name": {
        "xp": 80,
        "icon": "bi-fonts",
        "quest_name": "Claim Your Name",
        "skill": "Brand Identity",
        "learning": "Your business name on Google should match your legal name exactly. "
                    "Adding keywords or location to it can get you suspended.",
    },
    "category": {
        "xp": 120,
        "icon": "bi-tag-fill",
        "quest_name": "Pick Your Category",
        "skill": "Search Relevance",
        "learning": "Your primary category is how Google decides which searches to show you for. "
                    "Picking the right one is more important than most people realize.",
    },
    "hours": {
        "xp": 100,
        "icon": "bi-clock-fill",
        "quest_name": "Set Your Hours",
        "skill": "Customer Experience",
        "learning": "Customers check hours before they call or drive over. "
                    "Missing hours = missed visits. Google also favors profiles with hours set.",
    },
    "description": {
        "xp": 120,
        "icon": "bi-pencil-fill",
        "quest_name": "Tell Your Story",
        "skill": "Copywriting",
        "learning": "Your description is your 750-character pitch. It helps Google "
                    "understand what you do and helps customers choose you over competitors.",
    },
    "photos": {
        "xp": 150,
        "icon": "bi-camera-fill",
        "quest_name": "Show Your Work",
        "skill": "Visual Marketing",
        "learning": "Listings with photos get 42% more direction requests. "
                    "Real photos of your work, team, and space build trust faster than any ad.",
    },
    "reviews": {
        "xp": 200,
        "icon": "bi-chat-quote-fill",
        "quest_name": "Collect Social Proof",
        "skill": "Reputation Building",
        "learning": "Reviews are the #1 factor in local search ranking. "
                    "Every new review is another customer vouching for you publicly.",
    },
    "rating": {
        "xp": 100,
        "icon": "bi-star-fill",
        "quest_name": "Earn Your Stars",
        "skill": "Service Quality",
        "learning": "Most people filter for 4+ stars when searching. "
                    "Your rating is the first thing customers see, and it directly affects clicks.",
    },
}

# XP level thresholds and titles
_LEVELS = [
    (0,    1, "Newcomer",         "Just getting started"),
    (200,  2, "Starter",          "You're on the board"),
    (400,  3, "Contender",        "Building momentum"),
    (600,  4, "Competitor",       "You're in the game"),
    (800,  5, "Rising Star",      "Customers are noticing"),
    (1000, 6, "Local Favorite",   "Standing out in your area"),
    (1200, 7, "Neighborhood Pro",  "The go-to in your neighborhood"),
    (1400, 8, "Local Legend",      "Dominating local search"),
]


def _score_field(field, gbp_ctx):
    """Return (score 0-100, grade, detail, micro_steps[]) for one audit dimension."""
    micro = []

    if field == "business_name":
        name = gbp_ctx.get("business_name", "")
        if not name:
            micro = [
                {"step": "Go to business.google.com and sign in.", "done": False},
                {"step": "Enter your exact legal business name.", "done": False},
            ]
            return 0, "fail", "No business name found.", micro
        if len(name) > 80:
            micro = [{"step": "Shorten your name to under 80 characters (currently %d)." % len(name), "done": False}]
            return 60, "warn", "Name is long (%d chars)." % len(name), micro
        return 100, "pass", name, micro

    if field == "address":
        addr = gbp_ctx.get("address", "")
        if not addr:
            micro = [
                {"step": "Open business.google.com and go to your profile info.", "done": False},
                {"step": "Enter your street address exactly as it appears on mail.", "done": False},
                {"step": "If you go to customers (no storefront), choose 'service area' instead.", "done": False},
            ]
            return 0, "fail", "No address on file.", micro
        return 100, "pass", "Address is set.", micro

    if field == "phone":
        phone = gbp_ctx.get("phone", "")
        if not phone:
            micro = [
                {"step": "Add a local phone number (not toll-free) to your profile.", "done": False},
                {"step": "Make sure the number matches what's on your website.", "done": False},
            ]
            return 0, "fail", "No phone number.", micro
        return 100, "pass", "Phone is set.", micro

    if field == "website":
        url = gbp_ctx.get("website", "")
        if not url:
            micro = [
                {"step": "Add your website URL to your Google Business Profile.", "done": False},
                {"step": "Make sure the site loads and shows your business name and address.", "done": False},
            ]
            return 0, "fail", "No website link.", micro
        return 100, "pass", "Website linked.", micro

    if field == "category":
        cat = gbp_ctx.get("category", "")
        if not cat:
            micro = [
                {"step": "Go to your profile at business.google.com.", "done": False},
                {"step": "Click 'Edit profile' and find the category field.", "done": False},
                {"step": "Search for the category that best describes your main service.", "done": False},
            ]
            return 0, "fail", "No category selected.", micro
        return 100, "pass", cat, micro

    if field == "hours":
        hours = gbp_ctx.get("hours") or {}
        periods = hours.get("periods") or []
        if not periods:
            micro = [
                {"step": "Open your profile at business.google.com.", "done": False},
                {"step": "Click 'Edit profile', then 'Hours'.", "done": False},
                {"step": "Add your hours for each day you're available.", "done": False},
            ]
            return 0, "fail", "No business hours set.", micro
        weekdays_covered = set()
        for p in periods:
            od = p.get("open", {}).get("day")
            if od is not None:
                weekdays_covered.add(od)
        if len(weekdays_covered) < 5:
            micro = [{"step": "Add hours for the remaining days (%d of 7 covered)." % len(weekdays_covered), "done": False}]
            return 60, "warn", "%d days covered." % len(weekdays_covered), micro
        return 100, "pass", "%d days covered." % len(weekdays_covered), micro

    if field == "photos":
        count = gbp_ctx.get("photo_count", 0)
        if count == 0:
            micro = [
                {"step": "Take 3 photos of your best recent work.", "done": False},
                {"step": "Take 1 photo of your team or yourself.", "done": False},
                {"step": "Take 1 photo of your shop/office/van (whatever represents you).", "done": False},
                {"step": "Upload all 5 at business.google.com under 'Photos'.", "done": False},
            ]
            return 0, "fail", "No photos.", micro
        if count < 5:
            needed = 5 - count
            micro = [{"step": "Upload %d more photo(s) to reach 5 total." % needed, "done": False}]
            return 40, "warn", "%d photo(s) uploaded." % count, micro
        if count < 10:
            micro = [{"step": "Add variety: interior shots, team photos, before/after of your work.", "done": False}]
            return 70, "warn", "%d photos." % count, micro
        return 100, "pass", "%d photos uploaded." % count, micro

    if field == "description":
        desc = gbp_ctx.get("description", "")
        desc_status = (gbp_ctx.get("description_status") or "present").lower()
        if desc_status == "unverified":
            return 100, "pass", "Google did not expose description status through the Places API, so this check is skipped.", micro
        if not desc:
            micro = [
                {"step": "Open 'Edit profile' at business.google.com.", "done": False},
                {"step": "Write 2-3 sentences: what you do, where you serve, what makes you different.", "done": False},
                {"step": "Aim for 250-750 characters. Our AI wrote one for you below - feel free to use it.", "done": False},
            ]
            return 0, "fail", "No description.", micro
        length = len(desc)
        if length < 100:
            micro = [{"step": "Expand to at least 250 characters. You're at %d." % length, "done": False}]
            return 40, "warn", "%d characters (short)." % length, micro
        if length < 250:
            micro = [{"step": "Add a bit more detail to reach 250+ characters (%d now)." % length, "done": False}]
            return 70, "warn", "%d characters." % length, micro
        return 100, "pass", "%d characters." % length, micro

    if field == "reviews":
        count = gbp_ctx.get("review_count", 0)
        if count == 0:
            micro = [
                {"step": "Copy your review link from this page (below).", "done": False},
                {"step": "Text it to your 3 most recent happy customers.", "done": False},
                {"step": "Ask in person at the end of your next job.", "done": False},
            ]
            return 0, "fail", "No reviews yet.", micro
        if count < 5:
            micro = [{"step": "Send your review link to %d more customers. You need 5 total to show stars." % (5 - count), "done": False}]
            return 30, "warn", "%d review(s)." % count, micro
        if count < 20:
            micro = [{"step": "Keep asking. Add your review link to invoices and email signatures." , "done": False}]
            return 60, "warn", "%d reviews." % count, micro
        if count < 50:
            return 80, "pass", "%d reviews." % count, micro
        return 100, "pass", "%d reviews." % count, micro

    if field == "rating":
        rating = gbp_ctx.get("rating", 0)
        count = gbp_ctx.get("review_count", 0)
        if count == 0:
            micro = [{"step": "Get your first review. Your rating starts once you have reviews.", "done": False}]
            return 0, "fail", "No reviews yet.", micro
        if rating < 3.5:
            micro = [
                {"step": "Reply to every negative review professionally and offer to fix the issue.", "done": False},
                {"step": "Ask your happiest customers to leave reviews to balance the score.", "done": False},
            ]
            return 20, "fail", "%.1f stars." % rating, micro
        if rating < 4.0:
            micro = [{"step": "Respond to all reviews (even positive ones). Keep asking happy customers.", "done": False}]
            return 50, "warn", "%.1f stars." % rating, micro
        if rating < 4.5:
            return 75, "pass", "%.1f stars." % rating, micro
        return 100, "pass", "%.1f stars." % rating, micro

    if field == "verification":
        status = gbp_ctx.get("status", "UNKNOWN")
        if status == "VERIFIED":
            return 100, "pass", "Verified and live on Google.", micro
        if "CLOSED" in (status or ""):
            micro = [
                {"step": "Log in at business.google.com.", "done": False},
                {"step": "Mark your business as 'Open' to reactivate it.", "done": False},
            ]
            return 30, "fail", "Marked as %s." % status.replace("_", " ").lower(), micro
        micro = [
            {"step": "Go to business.google.com and search for your business.", "done": False},
            {"step": "Click 'Claim this business' or 'Own this business?'.", "done": False},
            {"step": "Follow the verification steps Google gives you.", "done": False},
            {"step": "Check the troubleshooter on this page if you hit a wall.", "done": False},
        ]
        return 0, "fail", "Not verified.", micro

    return 0, "fail", "Unknown.", micro


def run_gbp_audit(gbp_ctx):
    """
    Run a gamified GBP audit with quests, XP, levels, and achievements.
    """
    quests = []
    achievements = []
    total_xp = 0
    max_xp = 0

    for field, meta in _QUEST_META.items():
        score, grade, detail, micro_steps = _score_field(field, gbp_ctx)
        xp_earned = round(meta["xp"] * score / 100)
        total_xp += xp_earned
        max_xp += meta["xp"]

        quest = {
            "field": field,
            "quest_name": meta["quest_name"],
            "icon": meta["icon"],
            "skill": meta["skill"],
            "learning": meta["learning"],
            "xp_possible": meta["xp"],
            "xp_earned": xp_earned,
            "score": score,
            "grade": grade,
            "detail": detail,
            "micro_steps": micro_steps,
            "complete": grade == "pass" and score >= 75,
        }
        quests.append(quest)

        if quest["complete"]:
            achievements.append({
                "quest_name": meta["quest_name"],
                "icon": meta["icon"],
                "skill": meta["skill"],
                "xp": xp_earned,
            })

    # Determine level
    level_num = 1
    level_name = "Newcomer"
    level_desc = "Just getting started"
    next_level_xp = _LEVELS[1][0] if len(_LEVELS) > 1 else max_xp
    for threshold, num, name, desc in _LEVELS:
        if total_xp >= threshold:
            level_num = num
            level_name = name
            level_desc = desc
    # Find next level threshold
    for threshold, num, name, desc in _LEVELS:
        if threshold > total_xp:
            next_level_xp = threshold
            break
    else:
        next_level_xp = max_xp

    # Sort: incomplete quests first (highest XP potential first), then completed
    incomplete = [q for q in quests if not q["complete"]]
    complete = [q for q in quests if q["complete"]]
    incomplete.sort(key=lambda q: -q["xp_possible"])
    quests_sorted = incomplete + complete

    # Overall score for backward compat
    overall_pct = round(total_xp / max_xp * 100) if max_xp else 0

    return {
        "total_xp": total_xp,
        "max_xp": max_xp,
        "overall_score": overall_pct,
        "level_num": level_num,
        "level_name": level_name,
        "level_desc": level_desc,
        "next_level_xp": next_level_xp,
        "quests": quests_sorted,
        "achievements": achievements,
        "quests_complete": len(complete),
        "quests_total": len(quests),
    }


# ---------------------------------------------------------------------------
# AI-powered audit recommendations
# ---------------------------------------------------------------------------

_AUDIT_SYSTEM_PROMPT = """\
You are a friendly Google Business Profile coach helping a business owner \
level up their Google presence. They've just run an audit and you can see \
their scores. Talk to them like a supportive mentor, not a textbook.

You will receive their profile data and quest scores. Provide:
1. A 2-3 sentence pep talk: acknowledge what they've done well, then point \
   to their biggest opportunity. Be specific to their business, not generic.
2. An optimized business description they can copy-paste (250-600 chars). \
   Include their services, service area, and something that makes them stand \
   out. Write it in a natural tone. No keyword stuffing.
3. Exactly 3 "quick wins" - things they can do in the next 10 minutes each. \
   Each must be a single concrete action, not vague advice. Format each as: \
   {"action": "what to do", "time": "X min", "why": "one sentence on why it matters"}.
4. If they have verification issues, provide 2-3 specific troubleshooting steps \
   (not generic - based on their actual situation).
5. A casual review request message they can text to a customer (under 140 chars, \
   conversational tone, not corporate).

Write for someone who might be intimidated by tech. Keep it warm and direct.
Do NOT use em dashes. Use regular dashes, commas, or periods instead.
Return valid JSON with these keys: pep_talk, optimized_description, \
quick_wins (array of {action, time, why}), verification_help (array of strings, \
empty array if verified), review_request_template.\
"""


def run_ai_audit(gbp_ctx, audit_result, brand, api_key, model):
    """
    Call the LLM to generate personalized, gamified audit recommendations.
    Returns dict with pep_talk, quick_wins, etc.  Falls back to None on error.
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
        "total_xp": audit_result["total_xp"],
        "max_xp": audit_result["max_xp"],
        "level": audit_result["level_name"],
        "quests_complete": audit_result["quests_complete"],
        "quests_total": audit_result["quests_total"],
        "incomplete_quests": [
            q["quest_name"] for q in audit_result["quests"]
            if not q["complete"]
        ],
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
        import openai
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
