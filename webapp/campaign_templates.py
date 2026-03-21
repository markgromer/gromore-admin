"""
Campaign Strategy Templates

Predefined campaign architectures for Google Ads and Meta Ads.
Each strategy defines the optimal structure, targeting approach, and
budget allocation, then generates AI prompts that produce launch-ready
campaign plans compatible with the existing API launch functions.
"""

# ═══════════════════════════════════════════════════════════════════
#  STRATEGY DEFINITIONS
# ═══════════════════════════════════════════════════════════════════

CAMPAIGN_STRATEGIES = {

    # ── Meta / Facebook + Instagram ──────────────────────────────

    "meta_omnipresent": {
        "name": "Omnipresent Content",
        "platform": "meta",
        "icon": "bi-eye-fill",
        "color": "#8b5cf6",
        "tagline": "Be everywhere your audience looks",
        "description": (
            "Run low-cost ads across every Meta placement so your brand stays "
            "top-of-mind. Three audience tiers (warm retarget, lookalike, cold "
            "interest-based) each get their own ad set with different creative "
            "angles. Small daily spend per audience, maximum visibility."
        ),
        "best_for": "Brand awareness, staying top-of-mind, nurturing "
                     "prospects who aren't ready to buy yet",
        "recommended_min": 300,
        "objective": "OUTCOME_AWARENESS",
    },

    "meta_lead_gen": {
        "name": "Lead Generation",
        "platform": "meta",
        "icon": "bi-person-plus-fill",
        "color": "#10b981",
        "tagline": "Turn scrollers into leads",
        "description": (
            "Conversion-optimized campaign focused on getting form "
            "submissions, phone calls, and quote requests. Multiple ad sets "
            "test different audience segments and copy angles to find your "
            "best cost-per-lead."
        ),
        "best_for": "Phone calls, form fills, quote requests, "
                     "booked appointments",
        "recommended_min": 300,
        "objective": "OUTCOME_LEADS",
    },

    "meta_hyper_local": {
        "name": "Hyper-Local Blitz",
        "platform": "meta",
        "icon": "bi-geo-alt-fill",
        "color": "#f59e0b",
        "tagline": "Own your neighborhood",
        "description": (
            "Tight radius targeting puts your ads in front of everyone in "
            "your immediate service area. Two zones: a hot zone (5-mile "
            "radius) with aggressive spend and a wider service area (15-mile) "
            "for broader reach. Multiple creative angles per zone."
        ),
        "best_for": "Local service businesses, restaurants, retail, "
                     "anyone targeting a specific area",
        "recommended_min": 200,
        "objective": "OUTCOME_LEADS",
    },

    "meta_retargeting": {
        "name": "Retargeting Funnel",
        "platform": "meta",
        "icon": "bi-arrow-repeat",
        "color": "#ef4444",
        "tagline": "Close the loop on warm leads",
        "description": (
            "Re-engage people who already know you: website visitors, page "
            "engagers, and video viewers. Sequential messaging with "
            "urgency-driven offers pushes warm prospects to convert."
        ),
        "best_for": "Converting warm audiences, reducing cost per lead, "
                     "second-chance conversions",
        "recommended_min": 150,
        "objective": "OUTCOME_LEADS",
    },

    # ── Google Ads ───────────────────────────────────────────────

    "google_lead_gen": {
        "name": "Lead Generation Search",
        "platform": "google",
        "icon": "bi-search",
        "color": "#3b82f6",
        "tagline": "Capture active searchers",
        "description": (
            "High-intent keyword campaign targeting people actively searching "
            "for your services right now. 2-3 themed ad groups with RSAs, "
            "strong negative keyword lists, and conversion-focused copy."
        ),
        "best_for": "Immediate lead flow from people searching for "
                     "your services right now",
        "recommended_min": 300,
    },

    "google_local_domination": {
        "name": "Local Search Domination",
        "platform": "google",
        "icon": "bi-geo-alt-fill",
        "color": "#f59e0b",
        "tagline": "Own local search results",
        "description": (
            "City-specific keywords, 'near me' queries, and location-heavy "
            "ad copy. Three ad groups cover near-me searches, city-name "
            "queries, and emergency/urgent keywords. Perfect for service "
            "area businesses."
        ),
        "best_for": "Local businesses competing for 'near me' and "
                     "city-specific searches",
        "recommended_min": 200,
    },

    "google_competitor_conquest": {
        "name": "Competitor Conquest",
        "platform": "google",
        "icon": "bi-trophy-fill",
        "color": "#dc2626",
        "tagline": "Show up when they search competitors",
        "description": (
            "Bid on competitor brand names and 'alternative to' searches. "
            "Differentiation-focused ads highlight your advantages: better "
            "pricing, more reviews, stronger guarantees, faster response."
        ),
        "best_for": "Stealing market share from known competitors "
                     "in your area",
        "recommended_min": 300,
    },
}


# ═══════════════════════════════════════════════════════════════════
#  PUBLIC API
# ═══════════════════════════════════════════════════════════════════

def get_strategies_for_platform(platform):
    """Return {key: strategy_dict} for the given platform."""
    strategies = get_active_strategies()
    return {k: v for k, v in strategies.items()
            if v["platform"] == platform}


def get_active_strategies():
    """Load strategies from DB if available, fall back to hardcoded defaults."""
    try:
        from flask import current_app
        db = getattr(current_app, "db", None)
        if db:
            rows = db.get_all_campaign_strategies(active_only=True)
            if rows:
                return {r["strategy_key"]: r for r in rows}
    except Exception:
        pass
    return CAMPAIGN_STRATEGIES


def build_strategy_prompt(strategy_key, brand, service, location,
                          monthly_budget, notes=""):
    """
    Build (system_prompt, user_prompt) for a campaign strategy.
    Returns None if strategy_key is invalid.
    """
    strategies = get_active_strategies()
    strategy = strategies.get(strategy_key)
    if not strategy:
        return None

    platform = strategy["platform"]
    daily_budget = round(float(monthly_budget) / 30, 2)
    industry = brand.get("industry", "home services")
    brand_name = brand.get("display_name", brand.get("name", ""))

    # ── Load knowledge context from the ad intelligence engine ──
    knowledge = ""
    try:
        from webapp.ad_knowledge import build_ad_knowledge_context
        from flask import current_app
        db = getattr(current_app, "db", None)
        if db:
            knowledge = build_ad_knowledge_context(db, platform, "campaign", industry=industry)
    except Exception:
        pass

    system_parts = []
    if knowledge:
        system_parts.append(knowledge)
    system_parts.append(
        "You are a senior digital advertising strategist who builds "
        "campaign plans that are practical, conversion-focused, and ready "
        "to launch through the advertising API. The client's brand voice, "
        "competitors, offers, and audience are provided in the prompt. "
        "Ad copy MUST reflect their brand voice and positioning. "
        "Return ONLY valid JSON, no markdown fences or commentary."
    )
    system_prompt = "\n\n".join(system_parts)

    # ── Common header injected into every user prompt ──
    header = (
        f"Business: {brand_name} ({industry})\n"
        f"Service to promote: {service}\n"
        f"Target location: {location}\n"
        f"Monthly budget: ${monthly_budget}\n"
        f"Daily budget: ${daily_budget}\n"
    )
    if notes:
        header += f"Additional notes: {notes}\n"

    # Brand identity context
    voice = (brand.get("brand_voice") or "").strip()
    if voice:
        header += f"Brand voice / tone: {voice}\n"
    audience = (brand.get("target_audience") or "").strip()
    if audience:
        header += f"Target audience: {audience}\n"
    offers = (brand.get("active_offers") or "").strip()
    if offers:
        header += f"Active offers / promotions: {offers}\n"
    services = (brand.get("primary_services") or "").strip()
    if services:
        header += f"Primary services: {services}\n"
    competitors = (brand.get("competitors") or "").strip()
    if competitors:
        header += f"Known competitors: {competitors}\n"

    # ── Strategy-specific prompt ──
    builder = _PROMPT_BUILDERS.get(strategy_key)
    if not builder:
        return None

    user_prompt = builder(header, strategy, daily_budget, location)
    return system_prompt, user_prompt


# ═══════════════════════════════════════════════════════════════════
#  META PROMPT BUILDERS
# ═══════════════════════════════════════════════════════════════════

_META_JSON_FORMAT = """\
Return a JSON object with this exact structure:
{{
    "campaign_name": "descriptive campaign name",
    "objective": "{objective}",
    "daily_budget": {daily_budget},
    "ad_sets": [
        {{
            "name": "ad set name",
            "targeting_description": "who this targets and why",
            "age_min": 25,
            "age_max": 65,
            "radius_miles": 25,
            "ad_copy": [
                {{
                    "headline": "short punchy headline (<40 chars)",
                    "primary_text": "compelling 2-4 sentence body copy",
                    "description": "short supporting line (<30 chars)",
                    "call_to_action": "GET_QUOTE"
                }}
            ]
        }}
    ],
    "location_targeting": "{location}",
    "rationale": "2-3 sentences explaining the strategy and expected outcomes"
}}"""


def _prompt_meta_omnipresent(header, strategy, daily_budget, location):
    fmt = _META_JSON_FORMAT.format(
        objective="OUTCOME_AWARENESS",
        daily_budget=daily_budget,
        location=location,
    )
    return f"""{header}

STRATEGY: Omnipresent Content Campaign
Goal: Maximum brand visibility across all Meta placements. Stay in front
of every potential customer so when they need this service, you're the
first name they think of.

Create a campaign with exactly 3 ad sets:

1. "Warm Audience - Retargeting" (radius_miles: 15)
   - Targets people who've visited the website or engaged with social posts
   - Copy angle: social proof and trust (reviews, years in business)
   - 2 ad variations

2. "Lookalike - Similar Customers" (radius_miles: 25)
   - Targets people similar to existing customers
   - Copy angle: authority and expertise (before/after, credentials)
   - 2 ad variations

3. "Cold Audience - Interest Based" (radius_miles: 25)
   - Targets homeowners or relevant interest groups
   - Copy angle: value and education (tips, common problems, free info)
   - 2 ad variations

Split the daily budget roughly evenly: allocate 35% to warm, 35% to
lookalike, 30% to cold. Reflect this in a budget_note field per ad set.

Ad copy requirements:
- Headlines under 40 characters, benefit-focused
- Primary text: 2-4 sentences, conversational tone, no hard sell
- Different creative angle per ad set (do NOT repeat the same message)
- CTAs: LEARN_MORE for cold, CONTACT_US for warm, GET_QUOTE for lookalike

{fmt}"""


def _prompt_meta_lead_gen(header, strategy, daily_budget, location):
    fmt = _META_JSON_FORMAT.format(
        objective="OUTCOME_LEADS",
        daily_budget=daily_budget,
        location=location,
    )
    return f"""{header}

STRATEGY: Lead Generation Campaign
Goal: Maximize form submissions, phone calls, and quote requests at the
lowest cost-per-lead. Every element should drive action.

Create a campaign with exactly 3 ad sets:

1. "High Intent - Service Seekers" (radius_miles: 15)
   - Targets people actively looking for this service (home improvement
     interests, relevant life events)
   - Copy angle: urgency + offer (limited-time, free estimate, same-day)
   - 3 ad variations with different hooks
   - age_min: 25, age_max: 65

2. "Local Homeowners" (radius_miles: 20)
   - Targets homeowners in the service area
   - Copy angle: problem-agitate-solve (describe pain point, amplify,
     present solution)
   - 2 ad variations
   - age_min: 30, age_max: 65

3. "Broad Discovery" (radius_miles: 25)
   - Broader audience, let Meta's algorithm find converters
   - Copy angle: social proof (reviews, testimonials, before/after)
   - 2 ad variations
   - age_min: 25, age_max: 65

Ad copy requirements:
- Headlines: short, punchy, under 40 chars, include a benefit or number
- Primary text: 2-4 sentences, address a specific pain point, end with
  clear call to action
- Use specific numbers (e.g., "Serving 2,400+ homes" not "many homes")
- CTAs: GET_QUOTE or CALL_NOW for high-intent, LEARN_MORE for broad

{fmt}"""


def _prompt_meta_hyper_local(header, strategy, daily_budget, location):
    fmt = _META_JSON_FORMAT.format(
        objective="OUTCOME_LEADS",
        daily_budget=daily_budget,
        location=location,
    )
    return f"""{header}

STRATEGY: Hyper-Local Blitz Campaign
Goal: Dominate a tight geographic area. Everyone within a few miles should
see your brand repeatedly, building local authority and generating leads.

Create a campaign with exactly 2 ad sets:

1. "Hot Zone - Inner Ring" (radius_miles: 5)
   - Tight 5-mile radius around the core service area
   - Gets 60% of the daily budget
   - Copy angle: hyper-local identity ("Your [neighborhood/city] neighbors
     trust us", mention specific local landmarks or areas)
   - 3 ad variations
   - age_min: 25, age_max: 65

2. "Service Area - Outer Ring" (radius_miles: 15)
   - Wider 15-mile radius for broader coverage
   - Gets 40% of the daily budget
   - Copy angle: reliability and availability ("We come to you",
     "Serving the greater [area]", response time promises)
   - 2 ad variations
   - age_min: 25, age_max: 65

Ad copy requirements:
- Mention the specific city or area name in at least one headline per ad set
- Primary text should reference local details (neighborhoods, landmarks, etc.)
- Use proximity language ("right around the corner", "minutes away")
- Headlines under 40 chars, primary text 2-4 sentences
- CTAs: CALL_NOW for hot zone, GET_QUOTE for outer ring

{fmt}"""


def _prompt_meta_retargeting(header, strategy, daily_budget, location):
    fmt = _META_JSON_FORMAT.format(
        objective="OUTCOME_LEADS",
        daily_budget=daily_budget,
        location=location,
    )
    return f"""{header}

STRATEGY: Retargeting Funnel Campaign
Goal: Convert people who already know you. Website visitors, social
engagers, and video viewers get hit with urgency-driven, offer-focused
ads to push them over the line.

Create a campaign with exactly 2 ad sets:

1. "Website Visitors" (radius_miles: 30)
   - Targets people who visited the website but didn't convert
   - Gets 55% of the daily budget
   - Copy angle: urgency + incentive ("Still thinking about it?",
     limited-time offer, bonus for booking now)
   - 2 ad variations
   - age_min: 25, age_max: 65

2. "Social Engagers" (radius_miles: 30)
   - Targets people who engaged with Facebook/Instagram posts or page
   - Gets 45% of the daily budget
   - Copy angle: social proof + easy next step ("Join 500+ happy
     customers", "See why neighbors choose us", "Takes 60 seconds to
     get a quote")
   - 2 ad variations
   - age_min: 25, age_max: 65

Ad copy requirements:
- Acknowledge the prospect already knows you (don't re-introduce)
- Include a specific offer or incentive where natural
- Create urgency without being spammy (seasonal, limited spots, etc.)
- Headlines under 40 chars, primary text 2-3 sentences (shorter is better)
- CTAs: GET_QUOTE or CALL_NOW (direct action)

{fmt}"""


# ═══════════════════════════════════════════════════════════════════
#  GOOGLE PROMPT BUILDERS
# ═══════════════════════════════════════════════════════════════════

_GOOGLE_JSON_FORMAT = """\
Return a JSON object with this exact structure:
{{
    "campaign_name": "descriptive campaign name",
    "daily_budget": {daily_budget},
    "ad_groups": [
        {{
            "name": "ad group name",
            "keywords": ["keyword 1", "keyword 2"],
            "negative_keywords": ["negative 1"],
            "headlines": ["headline (max 30 chars)", "headline 2"],
            "descriptions": ["description (max 90 chars)", "description 2"]
        }}
    ],
    "campaign_negative_keywords": ["free", "diy", "how to"],
    "location_targeting": "{location}",
    "rationale": "2-3 sentences explaining the strategy"
}}"""


def _prompt_google_lead_gen(header, strategy, daily_budget, location):
    fmt = _GOOGLE_JSON_FORMAT.format(
        daily_budget=daily_budget,
        location=location,
    )
    return f"""{header}

STRATEGY: Lead Generation Search Campaign
Goal: Capture people actively searching for this service and turn those
clicks into phone calls and form submissions.

Create a campaign with exactly 3 ad groups:

1. Core Service Keywords
   - 10-12 high-intent keywords focused on the primary service
   - Mix of exact [brackets] and "phrase match" notation
   - Examples: [service + city], "service near me", [emergency service]
   - Headlines should lead with the core benefit or offer

2. Problem-Based Keywords
   - 8-10 keywords people search when they have the problem you solve
   - Focus on symptoms/problems, not just service names
   - Examples: "leaking faucet repair", [ac not cooling], "broken pipe fix"
   - Headlines should acknowledge the problem and promise a solution

3. Comparison / Research Keywords
   - 6-8 keywords for people comparing options
   - Focus on cost, reviews, best-in-class terms
   - Examples: "best [service] in [city]", [service] cost, "[service] reviews"
   - Headlines should highlight differentiators (ratings, years, guarantees)

Each ad group needs:
- 10-15 keywords (use [exact] and "phrase" notation)
- 5-8 negative keywords per group
- 15 headlines (under 30 characters each) - vary the angles
- 4 descriptions (under 90 characters each) - benefit-focused

Campaign-level negative keywords: free, diy, how to, jobs, hiring,
salary, youtube, video, reddit, training, course, school, classes

{fmt}"""


def _prompt_google_local_domination(header, strategy, daily_budget, location):
    fmt = _GOOGLE_JSON_FORMAT.format(
        daily_budget=daily_budget,
        location=location,
    )
    return f"""{header}

STRATEGY: Local Search Domination Campaign
Goal: Own the search results for anyone looking for this service in
the local area. Location-heavy keywords and ad copy that screams "local."

Create a campaign with exactly 3 ad groups:

1. "Near Me" Searches
   - 8-10 keywords with "near me" and "close by" variations
   - Examples: [service near me], "service close to me", "service nearby"
   - Headlines must include the city name and proximity language
   - Descriptions: emphasize fast response and local presence

2. City / Area Specific
   - 10-12 keywords combining the service with city, neighborhood, or
     zip code names
   - Examples: "[service] [city]", "[city] [service]", "[service] in [area]"
   - Headlines: "[City] [Service]", "Trusted [City] [Service Provider]"
   - Descriptions: mention years serving the community, local knowledge

3. Urgent / Emergency
   - 6-8 keywords for urgent or emergency searches
   - Examples: "emergency [service] [city]", [24 hour service],
     "same day [service]"
   - Headlines: lead with immediacy ("Same Day", "24/7", "Call Now")
   - Descriptions: response time guarantees, available now messaging

Each ad group needs:
- 8-12 keywords with [exact] and "phrase" notation
- 5-8 negative keywords per group
- 15 headlines (under 30 chars) - use city name in at least 5 headlines
- 4 descriptions (under 90 chars) - mention the specific area served

Campaign-level negative keywords: free, diy, how to, jobs, hiring,
salary, youtube, training, course, other cities not in service area

{fmt}"""


def _prompt_google_competitor_conquest(header, strategy, daily_budget, location):
    fmt = _GOOGLE_JSON_FORMAT.format(
        daily_budget=daily_budget,
        location=location,
    )
    return f"""{header}

STRATEGY: Competitor Conquest Campaign
Goal: Show up when people search for competitors. Win their clicks by
highlighting what makes this business the better choice.

IMPORTANT: Do NOT use competitor brand names in headlines or descriptions
(Google policy). Only use them as keywords. Ad copy must focus on what
makes this business better without naming competitors.

Create a campaign with exactly 2 ad groups:

1. Competitor Brand Names
   - 6-8 keywords using common competitor names in this industry/area
   - Use realistic-sounding local competitor names if you don't know real ones
   - Include "vs" and "alternative to" variations
   - Examples: "[competitor name]", "[competitor] reviews", "[competitor]
     vs", "alternative to [competitor]"
   - Headlines: "Looking For a Better Option?", "Switch & Save",
     "Compare Before You Choose"
   - Descriptions: focus on unique advantages (warranty, speed, price,
     rating) without mentioning the competitor name

2. Alternative / Comparison Searches
   - 6-8 keywords for people comparing or looking for alternatives
   - Examples: "best [service] [city]", "[service] companies near me",
     "top rated [service]", "which [service] is best"
   - Headlines: "Top Rated", "5-Star Reviews", "[Number]+ Reviews"
   - Descriptions: highlight reviews, guarantees, what sets you apart

Each ad group needs:
- 6-8 keywords with [exact] and "phrase" notation
- 5-8 negative keywords
- 15 headlines (under 30 chars) - differentiation-focused
- 4 descriptions (under 90 chars) - competitive advantages

Campaign-level negative keywords: free, diy, jobs, hiring, salary,
[competitor] jobs, [competitor] careers

{fmt}"""


# ── Prompt builder registry ──

_PROMPT_BUILDERS = {
    "meta_omnipresent": _prompt_meta_omnipresent,
    "meta_lead_gen": _prompt_meta_lead_gen,
    "meta_hyper_local": _prompt_meta_hyper_local,
    "meta_retargeting": _prompt_meta_retargeting,
    "google_lead_gen": _prompt_google_lead_gen,
    "google_local_domination": _prompt_google_local_domination,
    "google_competitor_conquest": _prompt_google_competitor_conquest,
}
