"""Vertical intelligence helpers for WARREN client guidance.

This module keeps service-business adaptation centralized so dashboard,
missions, chat, ads, commercial, and creative workflows can consume the same
business lens without hard-coding one niche into every feature.
"""

import re


_VERTICALS = {
    "pet_services": {
        "label": "Pet Services",
        "keywords": ("pet", "dog", "cat", "waste", "poop", "scoop", "groom", "kennel", "boarding"),
        "buyer_paths": ("residential recurring", "commercial property", "community management"),
        "chatbot_questions": (
            "service address or community name",
            "number of pets or stations",
            "service frequency",
            "gate or access notes",
        ),
        "ad_angles": (
            "clean yard without thinking about it",
            "recurring route reliability",
            "HOA or property cleanliness proof",
        ),
        "commercial_targets": ("apartments", "HOAs", "dog parks", "property managers", "pet-friendly offices"),
        "mission_guardrails": (
            "Separate one-time residential requests from recurring service opportunities.",
            "Commercial missions should capture property size, stations, frequency, and decision maker.",
        ),
    },
    "home_services": {
        "label": "Home Services",
        "keywords": (
            "plumb", "hvac", "roof", "electric", "garage", "landscap", "lawn", "pest",
            "pressure wash", "clean", "junk", "remodel", "paint", "floor", "pool", "handyman",
        ),
        "buyer_paths": ("urgent residential", "scheduled residential", "commercial maintenance"),
        "chatbot_questions": (
            "service address",
            "problem or requested service",
            "urgency",
            "photos if useful",
            "preferred appointment window",
        ),
        "ad_angles": (
            "fast local response",
            "before and after proof",
            "licensed, insured, and reviewed",
            "seasonal maintenance",
        ),
        "commercial_targets": ("property managers", "facility managers", "HOAs", "retail centers", "restaurants"),
        "mission_guardrails": (
            "Treat emergency demand differently from scheduled maintenance.",
            "Commercial missions should avoid consumer-only language and focus on reliability, reporting, and coverage.",
        ),
    },
    "health_wellness": {
        "label": "Health and Wellness",
        "keywords": ("med spa", "spa", "dental", "chiro", "therapy", "clinic", "wellness", "fitness", "gym"),
        "buyer_paths": ("consultation", "appointment booking", "repeat program"),
        "chatbot_questions": (
            "desired service",
            "main concern or goal",
            "new or returning client",
            "preferred appointment time",
        ),
        "ad_angles": (
            "specific transformation outcome",
            "consultation offer",
            "trust and credentials",
            "membership or treatment plan",
        ),
        "commercial_targets": ("employers", "local partners", "event organizers", "property wellness programs"),
        "mission_guardrails": (
            "Avoid medical claims without substantiation.",
            "Lead nurture should reduce anxiety and move toward a consultation.",
        ),
    },
    "professional_services": {
        "label": "Professional Services",
        "keywords": ("law", "legal", "account", "bookkeep", "insurance", "real estate", "financial", "consult"),
        "buyer_paths": ("consultation", "case intake", "retainer or quote"),
        "chatbot_questions": (
            "service needed",
            "timeline",
            "location or jurisdiction",
            "best callback time",
            "brief situation summary",
        ),
        "ad_angles": (
            "risk reduction",
            "clear next step",
            "authority and trust",
            "case or situation fit",
        ),
        "commercial_targets": ("business owners", "operations managers", "real estate teams", "local partners"),
        "mission_guardrails": (
            "Qualify fit before pushing for a sale.",
            "Avoid promising outcomes. Push toward a consultation or review.",
        ),
    },
    "auto_services": {
        "label": "Auto Services",
        "keywords": ("auto", "detail", "mechanic", "tire", "body shop", "windshield", "car wash", "mobile detailing"),
        "buyer_paths": ("single appointment", "maintenance", "fleet or commercial account"),
        "chatbot_questions": (
            "vehicle year/make/model",
            "service needed",
            "vehicle location",
            "urgency",
            "photos if useful",
        ),
        "ad_angles": (
            "convenience",
            "visible result proof",
            "fleet reliability",
            "fast quote",
        ),
        "commercial_targets": ("fleets", "dealerships", "rental operators", "property managers", "local businesses"),
        "mission_guardrails": (
            "Separate retail jobs from fleet/account opportunities.",
            "Commercial missions should capture vehicle count, cadence, and site access.",
        ),
    },
    "food_hospitality": {
        "label": "Food and Hospitality",
        "keywords": ("restaurant", "bar", "cafe", "coffee", "catering", "food truck", "hotel", "venue"),
        "buyer_paths": ("reservation or visit", "event inquiry", "catering order"),
        "chatbot_questions": (
            "party size or event size",
            "date and time",
            "location",
            "budget or menu needs",
        ),
        "ad_angles": (
            "local craving",
            "social proof",
            "limited-time offer",
            "event/catering convenience",
        ),
        "commercial_targets": ("offices", "event planners", "venues", "schools", "local organizations"),
        "mission_guardrails": (
            "Use timely offers and proof, not generic brand awareness.",
            "Commercial missions should distinguish catering from ordinary foot traffic.",
        ),
    },
}


_DEFAULT_VERTICAL = {
    "key": "local_services",
    "label": "Local Services",
    "buyer_paths": ("lead capture", "quote request", "follow-up", "repeat or referral"),
    "chatbot_questions": (
        "service needed",
        "service location",
        "urgency",
        "best contact method",
    ),
    "ad_angles": (
        "clear local offer",
        "trust proof",
        "fast response",
        "easy quote request",
    ),
    "commercial_targets": ("property managers", "business owners", "office managers", "local partners"),
    "mission_guardrails": (
        "Do not assume the buyer is residential when commercial signals are present.",
        "Tie missions to the service, buyer type, and next conversion step.",
    ),
}


def _tokens(value):
    text = str(value or "").lower()
    return set(re.findall(r"[a-z0-9]+", text))


def build_vertical_profile(brand=None):
    """Infer a reusable service vertical profile from existing brand fields."""
    brand = brand or {}
    raw_context = " ".join(
        str(brand.get(key) or "")
        for key in (
            "industry",
            "primary_services",
            "sales_bot_service_menu",
            "target_audience",
            "commercial_goal",
            "business_description",
        )
    )
    context = raw_context.lower()
    context_tokens = _tokens(context)

    scored = []
    for key, profile in _VERTICALS.items():
        score = 0
        for phrase in profile["keywords"]:
            phrase_l = phrase.lower()
            if " " in phrase_l:
                if phrase_l in context:
                    score += 3
            elif phrase_l in context_tokens or any(tok.startswith(phrase_l) for tok in context_tokens):
                score += 2
        scored.append((score, key, profile))

    score, key, matched = max(scored, key=lambda item: item[0]) if scored else (0, "", None)
    if score <= 0:
        profile = dict(_DEFAULT_VERTICAL)
        profile["confidence"] = "low"
    else:
        profile = {"key": key, **matched}
        profile["confidence"] = "high" if score >= 4 else "medium"

    services = str(brand.get("primary_services") or "").strip()
    service_area = str(brand.get("service_area") or "").strip()
    industry = str(brand.get("industry") or "").strip()
    profile["industry"] = industry
    profile["primary_services"] = services
    profile["service_area"] = service_area
    profile["owner_summary"] = _owner_summary(profile)
    profile["mission_lenses"] = _mission_lenses(profile)
    return profile


def _owner_summary(profile):
    service_label = profile.get("primary_services") or profile.get("industry") or profile.get("label")
    area = profile.get("service_area")
    if area:
        return f"{service_label} in {area}"
    return str(service_label or profile.get("label") or "local service business")


def _mission_lenses(profile):
    return [
        {
            "key": "lead_assistant",
            "label": "Chatbot",
            "focus": "Ask the qualifying questions that move the lead toward the correct next step.",
            "checks": list(profile.get("chatbot_questions") or [])[:5],
        },
        {
            "key": "ads",
            "label": "Ads",
            "focus": "Match campaigns to buyer intent, offer, proof, and service area.",
            "checks": list(profile.get("ad_angles") or [])[:5],
        },
        {
            "key": "commercial",
            "label": "Commercial",
            "focus": "Separate higher-value account opportunities from ordinary residential lead flow.",
            "checks": list(profile.get("commercial_targets") or [])[:5],
        },
        {
            "key": "content",
            "label": "Content",
            "focus": "Use proof, local relevance, and service-specific trust instead of generic posting.",
            "checks": list(profile.get("buyer_paths") or [])[:5],
        },
    ]
