"""
Site Builder - AI website content generation engine.

Generates SEO-rich page content, JSON-LD schema markup, and structured
metadata for WordPress websites. Designed for local service businesses.
"""

import json
import logging
import re
from datetime import datetime

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Page type registry
# ---------------------------------------------------------------------------

PAGE_TYPES = {
    "home": {
        "label": "Home",
        "slug": "",
        "wp_type": "page",
        "schema_types": ["LocalBusiness", "WebSite", "WebPage"],
        "priority": 1,
    },
    "about": {
        "label": "About",
        "slug": "about",
        "wp_type": "page",
        "schema_types": ["Organization", "WebPage"],
        "priority": 2,
    },
    "services": {
        "label": "Services",
        "slug": "services",
        "wp_type": "page",
        "schema_types": ["Service", "WebPage", "BreadcrumbList"],
        "priority": 3,
    },
    "service_detail": {
        "label": "Service Detail",
        "slug": "services/{service_slug}",
        "wp_type": "page",
        "schema_types": ["Service", "WebPage", "FAQPage", "BreadcrumbList"],
        "priority": 4,
    },
    "service_area": {
        "label": "Service Area",
        "slug": "{area_slug}",
        "wp_type": "page",
        "schema_types": ["LocalBusiness", "WebPage", "BreadcrumbList"],
        "priority": 5,
    },
    "contact": {
        "label": "Contact",
        "slug": "contact",
        "wp_type": "page",
        "schema_types": ["LocalBusiness", "ContactPage", "WebPage"],
        "priority": 6,
    },
    "faq": {
        "label": "FAQ",
        "slug": "faq",
        "wp_type": "page",
        "schema_types": ["FAQPage", "WebPage", "BreadcrumbList"],
        "priority": 7,
    },
    "testimonials": {
        "label": "Testimonials",
        "slug": "testimonials",
        "wp_type": "page",
        "schema_types": ["WebPage", "BreadcrumbList"],
        "priority": 8,
    },
    "landing_page": {
        "label": "Landing Page",
        "slug": "lp/{lp_slug}",
        "wp_type": "page",
        "schema_types": ["WebPage", "LocalBusiness"],
        "priority": 9,
    },
}


# ---------------------------------------------------------------------------
# Brand context builder
# ---------------------------------------------------------------------------

def build_brand_context(brand, intake=None):
    """Extract all brand fields relevant to content generation.
    
    intake: optional dict from the site builder intake form with extra
    fields like unique_selling_points, competitors, content_goals,
    lead_form_type, seo_data, warren_brief, etc.
    """
    intake = intake or {}
    ctx = {
        "business_name": (brand.get("display_name") or "").strip(),
        "industry": (brand.get("industry") or "").strip(),
        "website": (brand.get("website") or "").strip(),
        "service_area": (brand.get("service_area") or "").strip(),
        "primary_services": (brand.get("primary_services") or "").strip(),
        "brand_voice": (brand.get("brand_voice") or "").strip(),
        "target_audience": (brand.get("target_audience") or "").strip(),
        "active_offers": (brand.get("active_offers") or "").strip(),
        "phone": (brand.get("phone") or brand.get("business_phone") or "").strip(),
        "email": (brand.get("business_email") or brand.get("email") or "").strip(),
        "address": (brand.get("address") or brand.get("business_address") or "").strip(),
        "hours": (brand.get("business_hours") or "").strip(),
        "tagline": (brand.get("tagline") or "").strip(),
        "year_founded": (brand.get("year_founded") or "").strip(),
        "license_info": (brand.get("license_info") or "").strip(),
        "certifications": (brand.get("certifications") or "").strip(),
        # Intake overrides and extras
        "unique_selling_points": (intake.get("unique_selling_points") or "").strip(),
        "competitors": (intake.get("competitors") or "").strip(),
        "content_goals": (intake.get("content_goals") or "").strip(),
        "lead_form_type": (intake.get("lead_form_type") or "").strip(),
        "lead_form_shortcode": (intake.get("lead_form_shortcode") or "").strip(),
        "plugins": (intake.get("plugins") or "").strip(),
        "cta_text": (intake.get("cta_text") or "").strip(),
        "cta_phone": (intake.get("cta_phone") or "").strip(),
    }
    # Intake can override brand-level fields
    for key in ("brand_voice", "target_audience", "active_offers", "tagline"):
        if intake.get(key):
            ctx[key] = intake[key].strip()
    # SEO intelligence
    ctx["seo_data"] = intake.get("seo_data") or {}
    ctx["warren_brief"] = (intake.get("warren_brief") or "").strip()
    return ctx


# ---------------------------------------------------------------------------
# Blueprint builder
# ---------------------------------------------------------------------------

def build_site_blueprint(brand_ctx, services=None, areas=None, landing_pages=None, page_selection=None):
    """
    Build a complete site blueprint from brand context.

    Args:
        brand_ctx: dict from build_brand_context
        services: CSV string of services (overrides brand profile)
        areas: CSV string of service areas (overrides brand profile)
        landing_pages: list of dicts with {name, keyword, offer} for standalone landing pages
        page_selection: list of page types to include (e.g. ['home','about','services','contact'])
                        If None, includes all standard pages.

    Returns a list of page specs, each with:
        page_type, label, slug, schema_types, context (page-specific data)
    """
    pages = []

    # Determine which standard pages to include
    standard_types = ("home", "about", "services", "contact", "faq", "testimonials")
    if page_selection:
        selected = set(page_selection)
    else:
        selected = set(standard_types)

    # Static pages
    for ptype in standard_types:
        if ptype not in selected:
            continue
        meta = PAGE_TYPES[ptype]
        pages.append({
            "page_type": ptype,
            "label": meta["label"],
            "slug": meta["slug"],
            "wp_type": meta["wp_type"],
            "schema_types": meta["schema_types"],
            "priority": meta["priority"],
            "context": {},
        })

    # Service detail pages
    service_list = _parse_csv(services or brand_ctx.get("primary_services") or "")
    for svc in service_list:
        slug = _slugify(svc)
        pages.append({
            "page_type": "service_detail",
            "label": svc,
            "slug": f"services/{slug}",
            "wp_type": "page",
            "schema_types": PAGE_TYPES["service_detail"]["schema_types"],
            "priority": PAGE_TYPES["service_detail"]["priority"],
            "context": {"service_name": svc, "service_slug": slug},
        })

    # Service area pages
    area_list = _parse_csv(areas or brand_ctx.get("service_area") or "")
    for area in area_list:
        if "service_area" not in selected and page_selection:
            break
        slug = _slugify(area)
        pages.append({
            "page_type": "service_area",
            "label": area,
            "slug": slug,
            "wp_type": "page",
            "schema_types": PAGE_TYPES["service_area"]["schema_types"],
            "priority": PAGE_TYPES["service_area"]["priority"],
            "context": {"area_name": area, "area_slug": slug},
        })

    # Landing pages (standalone conversion pages)
    for lp in (landing_pages or []):
        name = (lp.get("name") or "").strip()
        if not name:
            continue
        slug = _slugify(name)
        pages.append({
            "page_type": "landing_page",
            "label": name,
            "slug": f"lp/{slug}",
            "wp_type": "page",
            "schema_types": PAGE_TYPES["landing_page"]["schema_types"],
            "priority": PAGE_TYPES["landing_page"]["priority"],
            "context": {
                "lp_name": name,
                "lp_slug": slug,
                "lp_keyword": (lp.get("keyword") or "").strip(),
                "lp_offer": (lp.get("offer") or "").strip(),
                "lp_audience": (lp.get("audience") or "").strip(),
            },
        })

    pages.sort(key=lambda p: (p["priority"], p["label"]))
    return pages


# ---------------------------------------------------------------------------
# Prompt builders - one per page type
# ---------------------------------------------------------------------------

_GLOBAL_RULES = (
    "VOICE RULES (apply to every page):\n"
    "- Write at an 8th grade reading level. Short sentences. Active voice.\n"
    "- Do NOT use em dashes. Use commas, periods, colons, or regular dashes.\n"
    "- NEVER use these AI-tell words: harness, elevate, empower, streamline, cutting-edge, "
    "holistic, synergy, leverage (as a verb), robust, seamless, revolutionize, game-changing, "
    "next-level, unleash, supercharge, skyrocket, turbocharge.\n"
    "- NEVER use these filler openers: 'In today's world', 'In today's competitive landscape', "
    "'Are you tired of', 'Look no further', 'What if I told you', 'Welcome to our website', "
    "'Are you looking for'.\n"
    "- Use 'you' more than 'we'. The reader is the main character.\n"
    "- Numbers beat adjectives. '$2,400 in wasted ad spend' beats 'significant savings'.\n"
    "- One idea per paragraph. White space matters.\n"
    "- Do not hedge. 'We will' not 'We might be able to help you'. 'You get' not 'You may benefit from'.\n"
    "- Contractions are fine when natural, but don't force every sentence into one.\n"
    "\n"
    "CONVERSION RULES:\n"
    "- Every page has ONE primary CTA. Everything points toward it.\n"
    "- Headlines do the heavy lifting. If the reader only sees headlines and CTA, "
    "they should still understand the pitch.\n"
    "- Subheads must be scannable and benefit-driven. Not clever. Clear.\n"
    "- Proof comes BEFORE the ask. Social proof, specifics, results, then the CTA.\n"
    "- Remove anything that does not build desire or remove an objection.\n"
    "- The first screen must answer: What is this? Who is it for? Why should I care? "
    "What do I do next?\n"
    "\n"
    "SEO RULES:\n"
    "- Naturally incorporate the primary keyword in the first 100 words, in at least one H2, "
    "and in the meta description.\n"
    "- This page must target a UNIQUE primary keyword. Do not target the same keyword "
    "as other pages on the site. Each page owns a distinct keyword cluster.\n"
    "- Include internal link placeholders like [LINK:/services] or [LINK:/contact] where relevant.\n"
    "- Use proper HTML: h2, h3, p, ul/ol, strong, blockquote. No h1 (theme handles that).\n"
    "- No two pages on the site should have the same H1 or same H2s. Your headings must be "
    "unique to this page's specific topic.\n"
    "\n"
    "CONTENT QUALITY RULES:\n"
    "- Each heading and paragraph must add real information. No padding.\n"
    "- If a section could appear on any competitor's website by swapping the business name, "
    "it is too generic. Make it specific to this business.\n"
    "- Do not repeat the same proof points, trust signals, or messaging that would appear "
    "on other pages of this site. Each page earns its space with unique value.\n"
    "- All content must be factually defensible. No invented statistics or fake awards.\n"
    "- Never use placeholder stats like '1000+ customers served' unless that data is in the context.\n"
    "\n"
    "EMOTIONAL ARC:\n"
    "- Every page should follow this arc: Hook (earn 3 seconds) > Tension (name the problem) "
    "> Credibility (prove you solve it) > Vision (show life after) > Release (clear next step).\n"
    "- Do not dump all information at the same energy level. Build toward the CTA.\n"
    "- After making a claim, provide proof immediately. Do not save all proof for a separate section.\n"
    "- Vary section structure. Do not repeat the same layout (heading, paragraph, bullets) "
    "for every section. Use questions, short punchy lines, scenarios, and pattern interrupts.\n"
    "\n"
    "ANTI-PATTERNS (never do these):\n"
    "- Do not include 'Who This Is For' sections that list obvious audiences.\n"
    "- Do not include stats or numbers you cannot verify from the provided context.\n"
    "- Do not write feature lists without benefits. Every feature needs a 'which means...' follow-up.\n"
    "- Do not use the same sentence structure for every bullet point.\n"
    "- Do not make every paragraph the same length. Vary rhythm.\n"
)

_OUTPUT_FORMAT = (
    "\nReturn ONLY valid JSON with these exact keys:\n"
    '- "title": the page H1 title (under 70 chars)\n'
    '- "content": full HTML body content (no doctype/head/body wrapper, no h1)\n'
    '- "excerpt": 1-2 sentence page summary (under 160 chars)\n'
    '- "seo_title": SEO title tag (under 70 chars, include location if local)\n'
    '- "seo_description": meta description (under 160 chars, include CTA)\n'
    '- "primary_keyword": the main keyword this page targets\n'
    '- "secondary_keywords": comma-separated secondary keywords\n'
    '- "faq_items": array of {"question": "...", "answer": "..."} (3-6 items, '
    "relevant to the page topic). Empty array if not applicable.\n"
    '- "schema_hints": object with extra structured data fields the schema '
    "generator should use (e.g. priceRange, openingHours). Empty object if none.\n"
)


def _brand_block(ctx):
    """Format brand context for injection into prompts."""
    lines = []
    if ctx.get("business_name"):
        lines.append(f"Business Name: {ctx['business_name']}")
    if ctx.get("industry"):
        lines.append(f"Industry: {ctx['industry']}")
    if ctx.get("service_area"):
        lines.append(f"Service Area: {ctx['service_area']}")
    if ctx.get("primary_services"):
        lines.append(f"Services Offered: {ctx['primary_services']}")
    if ctx.get("target_audience"):
        lines.append(f"Target Audience: {ctx['target_audience']}")
    if ctx.get("brand_voice"):
        lines.append(f"Brand Voice: {ctx['brand_voice']}")
    if ctx.get("phone"):
        lines.append(f"Phone: {ctx['phone']}")
    if ctx.get("email"):
        lines.append(f"Email: {ctx['email']}")
    if ctx.get("address"):
        lines.append(f"Address: {ctx['address']}")
    if ctx.get("hours"):
        lines.append(f"Hours: {ctx['hours']}")
    if ctx.get("tagline"):
        lines.append(f"Tagline: {ctx['tagline']}")
    if ctx.get("year_founded"):
        lines.append(f"Founded: {ctx['year_founded']}")
    if ctx.get("license_info"):
        lines.append(f"Licensing: {ctx['license_info']}")
    if ctx.get("certifications"):
        lines.append(f"Certifications: {ctx['certifications']}")
    if ctx.get("active_offers"):
        lines.append(f"Current Offers: {ctx['active_offers']}")
    if ctx.get("unique_selling_points"):
        lines.append(f"Unique Selling Points: {ctx['unique_selling_points']}")
    if ctx.get("competitors"):
        lines.append(f"Competitors to Differentiate From: {ctx['competitors']}")
    if ctx.get("content_goals"):
        lines.append(f"Content Goals: {ctx['content_goals']}")
    if ctx.get("cta_text"):
        lines.append(f"Primary CTA: {ctx['cta_text']}")
    if ctx.get("cta_phone"):
        lines.append(f"CTA Phone: {ctx['cta_phone']}")
    return "\n".join(lines)


def _seo_intel_block(ctx):
    """Format Search Console data and Warren's SEO brief for prompt injection."""
    parts = []
    seo_data = ctx.get("seo_data") or {}

    if seo_data.get("top_queries"):
        top = seo_data["top_queries"][:15]
        query_lines = []
        for q in top:
            query_lines.append(
                f"  - \"{q['query']}\" (clicks: {q['clicks']}, impressions: {q['impressions']}, "
                f"pos: {q['position']}, ctr: {q['ctr']}%)"
            )
        parts.append(
            "SEARCH CONSOLE - TOP PERFORMING KEYWORDS (use these as your SEO foundation):\n"
            + "\n".join(query_lines)
        )

    if seo_data.get("opportunity_queries"):
        opps = seo_data["opportunity_queries"][:10]
        opp_lines = []
        for q in opps:
            opp_lines.append(
                f"  - \"{q['query']}\" (pos: {q['position']}, impressions: {q['impressions']})"
            )
        parts.append(
            "SEARCH CONSOLE - OPPORTUNITY KEYWORDS (position 4-20, high impressions - target these):\n"
            + "\n".join(opp_lines)
        )

    if seo_data.get("top_pages"):
        pg_lines = []
        for p in seo_data["top_pages"][:5]:
            pg_lines.append(f"  - {p['page']} (clicks: {p['clicks']}, pos: {p['position']})")
        parts.append(
            "SEARCH CONSOLE - TOP PAGES:\n" + "\n".join(pg_lines)
        )

    if seo_data.get("totals"):
        t = seo_data["totals"]
        parts.append(
            f"SEARCH CONSOLE - TOTALS: {t.get('clicks',0)} clicks, "
            f"{t.get('impressions',0)} impressions, "
            f"{t.get('ctr',0)}% CTR, avg position {t.get('avg_position',0)}"
        )

    warren = ctx.get("warren_brief") or ""
    if warren:
        parts.append(f"WARREN'S SEO STRATEGY BRIEF:\n{warren}")

    if not parts:
        return ""
    return "\nSEO INTELLIGENCE (from real Search Console data - use this to inform your keyword targeting):\n" + "\n\n".join(parts) + "\n"


def _lead_form_block(ctx):
    """Format lead form / plugin integration instructions."""
    form_type = ctx.get("lead_form_type") or ""
    shortcode = ctx.get("lead_form_shortcode") or ""
    plugins = ctx.get("plugins") or ""

    if not form_type and not shortcode and not plugins:
        return ""

    parts = ["LEAD FORM & PLUGIN INTEGRATION:"]
    if form_type == "wpforms":
        sc = shortcode or "[wpforms id=\"FORM_ID\"]"
        parts.append(
            f"- Include a WPForms lead capture form on this page. "
            f"Place the shortcode {sc} in the content where the form should appear. "
            f"Wrap it in a clear CTA section with a compelling headline above the form."
        )
    elif form_type == "cf7":
        sc = shortcode or "[contact-form-7 id=\"FORM_ID\" title=\"Contact\"]"
        parts.append(
            f"- Include a Contact Form 7 form. Place {sc} in the content "
            f"where the lead form should appear."
        )
    elif form_type == "gravity":
        sc = shortcode or "[gravityform id=\"FORM_ID\" title=\"true\" description=\"true\"]"
        parts.append(
            f"- Include a Gravity Forms form. Place {sc} in the content."
        )
    elif form_type == "custom" and shortcode:
        parts.append(
            f"- Include this custom form embed: {shortcode}"
        )
    elif form_type:
        parts.append(
            f"- Include a [LEAD_FORM_PLACEHOLDER] where the lead capture form should appear. "
            "Wrap it in a CTA section with a compelling, benefit-driven headline."
        )

    if plugins:
        parts.append(f"- Additional plugins/integrations to reference: {plugins}")

    return "\n".join(parts) + "\n"


def build_page_prompt(page_spec, brand_ctx):
    """
    Build a complete prompt for generating a single page's content.

    Args:
        page_spec: dict from build_site_blueprint
        brand_ctx: dict from build_brand_context

    Returns:
        (system_message, user_message) tuple
    """
    ptype = page_spec["page_type"]
    builder = _PROMPT_BUILDERS.get(ptype, _prompt_generic)
    return builder(page_spec, brand_ctx)


def _system_msg():
    return (
        "You are a direct-response copywriter and SEO strategist who writes website pages "
        "for local service businesses: plumbers, HVAC techs, roofers, painters, cleaners, "
        "electricians, landscapers, and similar trades.\n\n"
        "You think like a business owner, not a marketer. You know what the plumber is thinking "
        "when he reads a landing page. You know these people are busy, skeptical of marketing, "
        "have been burned by agencies, want results not promises, and respect directness.\n\n"
        "You write copy that converts. Not copy that sounds good. Every sentence either builds "
        "desire, removes an objection, or moves the reader toward the next action. If a sentence "
        "does none of those, delete it.\n\n"
        "You produce SEO-rich content with proper heading hierarchy, FAQ content for rich snippets, "
        "and natural keyword placement. You never use em dashes. You never use AI-tell words like "
        "harness, elevate, empower, streamline, robust, or seamless. You return only valid JSON.\n\n"
        "PAIN POINTS YOU KNOW COLD - service business owners feel:\n"
        "- Burned by agencies that took their money and delivered nothing measurable\n"
        "- Frustrated they don't understand their own marketing numbers\n"
        "- Afraid of wasting money on marketing that might not work\n"
        "- Tired of being talked down to by marketers who don't understand their trade\n"
        "- Desperate for leads but unsure which channel actually works\n\n"
        "OBJECTION HANDLING - every page should preemptively address the top objections:\n"
        "- 'Too expensive' - show the cost of NOT doing it, anchor against what they waste\n"
        "- 'Been burned before' - acknowledge directly, no-risk messaging\n"
        "- 'I don't have time' - show how simple the next step is\n"
        "- 'How is this different' - concrete differentiators they can verify"
    )


def _prompt_home(page_spec, ctx):
    brand = _brand_block(ctx)
    seo_intel = _seo_intel_block(ctx)
    lead_form = _lead_form_block(ctx)
    user_msg = (
        f"Write the HOME PAGE for this local service business.\n\n"
        f"BUSINESS CONTEXT:\n{brand}\n\n"
        f"{seo_intel}"
        f"{lead_form}"
        f"{_GLOBAL_RULES}\n"
        "CONVERSION FRAMEWORK: AIDA (Attention-Interest-Desire-Action)\n"
        "Build through a full arc from stranger to 'I should call these people.'\n\n"
        "PAGE-SPECIFIC REQUIREMENTS:\n"
        "- HERO: Open with a value proposition that names who you serve and what they get. "
        "Not 'Welcome to...'. Not a generic tagline. A specific promise. "
        "Example pattern: '[Outcome] for [audience] in [area]'.\n"
        "- SERVICES OVERVIEW: Brief descriptions of 3-6 core services. Each service gets "
        "a benefit-first sentence, not a feature description. Link to detail pages with "
        "[LINK:/services/slug].\n"
        "- TRUST SECTION: Concrete differentiators only. Years in business, licensing, "
        "guarantees, response time. Only include what is in the context. Do not invent "
        "credentials. If the brand has licensing info, lead with it.\n"
        "- SOCIAL PROOF: Include [TESTIMONIAL_PLACEHOLDER] markers where real reviews "
        "should be inserted. Frame the section around results, not generic praise.\n"
        "- CTA SECTION: Phone number and contact link. Make the next step feel easy and "
        "low-risk: 'free estimate', 'no obligation', 'call now and we pick up'.\n"
        "- LOCAL SEO: Mention the primary service area in the hero, in at least one H2, "
        "and naturally 2-3 more times throughout. Do not stuff.\n"
        "- PATTERN INTERRUPT: After the services section, include a short, punchy line or "
        "question that breaks the rhythm. Something specific to the industry that makes "
        "the reader think 'they get it'.\n"
        "- Target word count: 800-1200 words.\n"
        "- Primary keyword: '[industry] in [primary area]' (e.g. 'plumber in Springfield').\n"
        "- This page is the hub. It links to services, about, and contact. Make those "
        "connections explicit with [LINK:] placeholders.\n"
        f"{_OUTPUT_FORMAT}"
    )
    return _system_msg(), user_msg


def _prompt_about(page_spec, ctx):
    brand = _brand_block(ctx)
    seo_intel = _seo_intel_block(ctx)
    user_msg = (
        f"Write the ABOUT PAGE for this local service business.\n\n"
        f"BUSINESS CONTEXT:\n{brand}\n\n"
        f"{seo_intel}"
        f"{_GLOBAL_RULES}\n"
        "CONVERSION FRAMEWORK: Star-Story-Solution\n"
        "The star is the founder/team. The story is why this business exists. "
        "The solution is how that origin drives how they serve customers today.\n\n"
        "PAGE-SPECIFIC REQUIREMENTS:\n"
        "- COMPANY STORY: Tell it like a real person talking. Not corporate history. "
        "Use founding year if available. Why did this person start this business? "
        "What problem were they solving? Keep it to 2-3 paragraphs max.\n"
        "- CREDENTIALS: Licenses, certifications, insurance, BBB rating. Only mention "
        "what is provided in the context. Present these as proof of commitment, not a "
        "brag list. 'Licensed and insured since 2008' carries more weight than a bullet list.\n"
        "- VALUES/TEAM: What the company stands for in practice, not in theory. "
        "'We show up on time' beats 'We value punctuality'. Tie values to customer outcomes.\n"
        "- LOCAL ROOTS: If a service area is provided, connect the company to the community. "
        "They live and work here. They are your neighbor, not a faceless company.\n"
        "- CTA: Close with a warm push to contact or request a quote. The reader just learned "
        "about these people. Now make it easy to reach them.\n"
        "- DEDUP NOTE: This page's unique value is the HUMAN story behind the business. "
        "Do not repeat service descriptions (those belong on /services). Do not repeat "
        "pricing details. Do not list services with descriptions.\n"
        "- Target word count: 600-900 words.\n"
        "- Primary keyword: '[business name] [city/area]' pattern.\n"
        f"{_OUTPUT_FORMAT}"
    )
    return _system_msg(), user_msg


def _prompt_services(page_spec, ctx):
    brand = _brand_block(ctx)
    seo_intel = _seo_intel_block(ctx)
    lead_form = _lead_form_block(ctx)
    user_msg = (
        f"Write the SERVICES OVERVIEW PAGE for this local service business.\n\n"
        f"BUSINESS CONTEXT:\n{brand}\n\n"
        f"{seo_intel}"
        f"{lead_form}"
        f"{_GLOBAL_RULES}\n"
        "CONVERSION FRAMEWORK: One-to-One Conversation\n"
        "Write like you are sitting across the table from a homeowner explaining what you do.\n\n"
        "PAGE-SPECIFIC REQUIREMENTS:\n"
        "- INTRO: 1-2 paragraphs about the company's service range. Frame it around "
        "customer problems solved, not a company capability statement.\n"
        "- SERVICE SECTIONS: One H2 per service. For each:\n"
        "  * Lead with the customer problem ('Your water heater is leaking at 2am')\n"
        "  * Explain the solution in 2-3 sentences\n"
        "  * End with an internal link to the detail page: [LINK:/services/service-slug]\n"
        "  * Each service section must read differently. Vary the structure. Do not repeat "
        "the same pattern of [problem sentence, solution sentence, link] for every one.\n"
        "- CTA: End with a 'not sure what service you need?' section. Phone number, "
        "'call us and describe the problem, we will figure out the right fix'.\n"
        "- DEDUP NOTE: This page is the HUB. Keep service descriptions to 2-3 sentences each. "
        "The detail pages go deep. This page gives the bird's-eye view and routes people "
        "to the right detail page. Do NOT write 400 words per service here.\n"
        "- Target word count: 600-1000 words.\n"
        "- Primary keyword: '[industry] services in [area]' pattern.\n"
        f"{_OUTPUT_FORMAT}"
    )
    return _system_msg(), user_msg


def _prompt_service_detail(page_spec, ctx):
    brand = _brand_block(ctx)
    seo_intel = _seo_intel_block(ctx)
    lead_form = _lead_form_block(ctx)
    svc_name = page_spec.get("context", {}).get("service_name", "")
    user_msg = (
        f"Write a SERVICE DETAIL PAGE for: {svc_name}\n\n"
        f"BUSINESS CONTEXT:\n{brand}\n\n"
        f"{seo_intel}"
        f"{lead_form}"
        f"{_GLOBAL_RULES}\n"
        "CONVERSION FRAMEWORK: PAS (Problem-Agitate-Solve)\n"
        "Name the problem this service solves. Make it sting (what happens if they wait). "
        "Present the fix with confidence.\n\n"
        "PAGE-SPECIFIC REQUIREMENTS:\n"
        f"- This page is entirely about '{svc_name}'. Go deep on this one service.\n"
        "- PROBLEM SECTION: What situation leads someone to search for this service? "
        "Be specific. A homeowner wakes up to a flooded basement. A business owner "
        "notices the AC unit is louder than usual. Make the reader see themselves.\n"
        "- AGITATE: What happens if they ignore it? Higher water bill. Mold risk. "
        "Emergency call at 3x the price. Name real consequences in the industry.\n"
        "- WHAT WE DO: Explain the actual process. What happens when they call? "
        "Who shows up? What does the work involve? How long does it take? "
        "Real details build trust. Vague descriptions build suspicion.\n"
        "- SIGNS YOU NEED THIS: A scannable list of warning signs. These become "
        "long-tail keyword targets and match how real people search.\n"
        "- PRICING TRANSPARENCY: If the brand has active offers, mention them. "
        "If not, use 'free estimates' or 'upfront pricing, no surprises' framing. "
        "Address the 'too expensive' objection before the reader thinks it.\n"
        "- FAQ: 3-6 questions specific to THIS service. Not generic company FAQs. "
        "'How long does a drain cleaning take?' not 'What areas do you serve?'\n"
        "- INTERNAL LINKS: Link to related services and to the contact page.\n"
        "- DEDUP NOTE: This page goes DEEP on one service. Do not repeat the company "
        "story (that's /about). Do not repeat general trust signals that appear on the "
        "home page. This page's unique value is the specific expertise in {svc_name}.\n"
        "- Target word count: 900-1400 words.\n"
        f"- Primary keyword: '{svc_name} [area]' pattern.\n"
        "- schema_hints should include 'serviceType' and 'areaServed'.\n"
        f"{_OUTPUT_FORMAT}"
    )
    return _system_msg(), user_msg


def _prompt_service_area(page_spec, ctx):
    brand = _brand_block(ctx)
    seo_intel = _seo_intel_block(ctx)
    lead_form = _lead_form_block(ctx)
    area = page_spec.get("context", {}).get("area_name", "")
    user_msg = (
        f"Write a SERVICE AREA PAGE for: {area}\n\n"
        f"BUSINESS CONTEXT:\n{brand}\n\n"
        f"{seo_intel}"
        f"{lead_form}"
        f"{_GLOBAL_RULES}\n"
        "CONVERSION FRAMEWORK: BAB (Before-After-Bridge)\n"
        f"Before: the homeowner in {area} has a problem and doesn't know who to call locally. "
        f"After: they find a trusted, nearby company that serves {area} specifically. "
        "Bridge: this page connects them.\n\n"
        "PAGE-SPECIFIC REQUIREMENTS:\n"
        f"- This page targets customers specifically in {area}.\n"
        f"- LOCAL AUTHORITY: Mention {area} naturally 3-5 times. Not stuffed, woven in. "
        "Reference neighborhoods, landmarks, or local context where it adds authenticity. "
        f"The goal is for someone searching '{ctx.get('industry', 'services')} in {area}' "
        "to immediately see this page is about THEIR area.\n"
        "- SERVICES IN THIS AREA: List the services available. Keep descriptions to one "
        "sentence each with links to detail pages. Example: 'Need drain cleaning in "
        f"{area}? [LINK:/services/drain-cleaning]'\n"
        "- RESPONSE TIME: Include a 'local team, fast response' angle. Service businesses "
        "win on proximity and speed. 'We are based near [area]. When you call, we are on "
        "our way, not driving 45 minutes from across the metro.'\n"
        "- CTA with phone number and [LINK:/contact].\n"
        f"- DEDUP NOTE: This page's unique value is the LOCAL angle for {area}. Do not "
        "repeat the detailed service descriptions from /services/[slug]. Do not repeat "
        "the company story from /about. Mention services briefly and link out.\n"
        "- Target word count: 600-900 words.\n"
        f"- Primary keyword: '{ctx.get('industry', 'services')} in {area}' pattern.\n"
        f"- Secondary keywords: specific service + {area} combos.\n"
        "- schema_hints should include 'areaServed' with the area name.\n"
        f"{_OUTPUT_FORMAT}"
    )
    return _system_msg(), user_msg


def _prompt_contact(page_spec, ctx):
    brand = _brand_block(ctx)
    lead_form = _lead_form_block(ctx)
    user_msg = (
        f"Write the CONTACT PAGE for this local service business.\n\n"
        f"BUSINESS CONTEXT:\n{brand}\n\n"
        f"{lead_form}"
        f"{_GLOBAL_RULES}\n"
        "CONVERSION FRAMEWORK: One-to-One Conversation\n"
        "The reader already decided they want to reach out. Do not re-sell. "
        "Make the process feel easy and human.\n\n"
        "PAGE-SPECIFIC REQUIREMENTS:\n"
        "- OPENING: Lead with a clear, inviting line. Not 'Contact us today!' "
        "Something that reduces friction: 'Got a question? A leaking pipe? A project "
        "you need a quote on? Here is how to reach us.'\n"
        "- CONTACT METHODS: List all methods provided: phone, email, address. "
        "Format phone as a clickable link. Make each method scannable.\n"
        "- BUSINESS HOURS: Include if available. For emergency trades (plumbing, "
        "HVAC, electrical, roofing), add a note about after-hours/emergency availability.\n"
        "- WHAT HAPPENS NEXT: Describe the process after they reach out. 'You call. "
        "We answer (or call back within 30 minutes). We schedule a time that works. "
        "We show up on time.' This reduces the anxiety of the unknown.\n"
        "- SERVICE AREA: Mention it for local SEO. 'We serve [area] and surrounding communities.'\n"
        "- CTA: Even on a contact page, give a clear nudge. 'Call now for a free estimate' "
        "or 'Fill out the form and we will get back to you today.'\n"
        "- DEDUP NOTE: This page is purely about making contact easy. Do not retell the "
        "company story. Do not list services with descriptions. A brief mention of what "
        "you do is fine, but link to /services for details.\n"
        "- Target word count: 300-500 words.\n"
        "- Primary keyword: 'contact [business name]' or '[industry] near me'.\n"
        "- schema_hints should include 'telephone', 'email', and 'address' from the context.\n"
        f"{_OUTPUT_FORMAT}"
    )
    return _system_msg(), user_msg


def _prompt_faq(page_spec, ctx):
    brand = _brand_block(ctx)
    seo_intel = _seo_intel_block(ctx)
    user_msg = (
        f"Write the FAQ PAGE for this local service business.\n\n"
        f"BUSINESS CONTEXT:\n{brand}\n\n"
        f"{seo_intel}"
        f"{_GLOBAL_RULES}\n"
        "CONVERSION FRAMEWORK: FAQ_Objection_Block\n"
        "Every FAQ answer is an objection removed. The reader who finishes this page "
        "should have zero reasons left not to call.\n\n"
        "PAGE-SPECIFIC REQUIREMENTS:\n"
        "- Generate 8-12 real questions a customer would ask BEFORE HIRING. "
        "Not questions about the website. Questions about the actual service.\n"
        "- GROUP BY THEME: Pricing, Process, Qualifications, Emergency, Guarantees.\n"
        "- ANSWER QUALITY: Each answer should be 2-4 sentences. Specific, not vague. "
        "'Yes, we offer free estimates for any job' beats 'We offer competitive pricing'. "
        "Reference the company's actual credentials and service area in answers.\n"
        "- OBJECTION-FIRST QUESTIONS: Include at least:\n"
        "  * A pricing/cost question ('How much does [service] cost?')\n"
        "  * A trust/qualification question ('Are you licensed and insured?')\n"
        "  * An emergency question ('Do you handle emergencies/after-hours calls?')\n"
        "  * A process question ('What should I expect when I call?')\n"
        "- LICENSING: If license_info or certifications are provided in context, "
        "include a specific question about it and answer with the actual credentials.\n"
        "- CTA: End with a 'Still have questions?' section. Phone number. "
        "'We would rather answer your questions in person. Call us.'\n"
        "- DEDUP NOTE: This page's unique value is the Q&A format optimized for "
        "FAQPage schema rich results in Google. Do not repeat paragraphs of content "
        "from other pages. The answers should be concise and direct.\n"
        "- Target word count: 800-1200 words.\n"
        "- Primary keyword: '[industry] FAQ' or '[industry] questions [area]'.\n"
        "- faq_items MUST contain all Q&A pairs from the content (needed for FAQPage schema).\n"
        f"{_OUTPUT_FORMAT}"
    )
    return _system_msg(), user_msg


def _prompt_testimonials(page_spec, ctx):
    brand = _brand_block(ctx)
    seo_intel = _seo_intel_block(ctx)
    user_msg = (
        f"Write the TESTIMONIALS / REVIEWS PAGE for this local service business.\n\n"
        f"BUSINESS CONTEXT:\n{brand}\n\n"
        f"{seo_intel}"
        f"{_GLOBAL_RULES}\n"
        "CONVERSION FRAMEWORK: Proof_Wall\n"
        "Social proof is the single strongest conversion lever for local service businesses. "
        "This page exists so every other page can link here when they need to back up a claim.\n\n"
        "PAGE-SPECIFIC REQUIREMENTS:\n"
        "ABSOLUTE RULE: Do NOT invent fake testimonials. Real businesses live and die by "
        "authentic reviews. Fake ones destroy trust faster than no reviews at all.\n\n"
        "- OPENING SECTION: Skip the 'we value our customers' filler. Start with a bold, "
        "specific claim: the number of 5-star reviews, years of repeat customers, or a "
        "stat about referral rate. If the data isn't available, frame it as: 'Don't take "
        "our word for it. Here's what our customers say.'\n"
        "- TESTIMONIAL SLOTS: Create 6-8 placeholder blocks marked with [TESTIMONIAL_PLACEHOLDER]. "
        "Each placeholder should include structure for: customer first name, service type, "
        "star rating, and the review text. Format them as cards/blocks that look designed, "
        "not like an afterthought list.\n"
        "- VARIETY: Mix the placeholder types. Some short one-liners, some 2-3 sentence stories. "
        "Include a suggested video testimonial placeholder. Variety signals authenticity.\n"
        "- TRUST SIGNALS AROUND THE REVIEWS: Google rating badge placeholder, BBB or "
        "industry certification badges, 'Verified on Google' callout. These frame the "
        "reviews as real and vetted.\n"
        "- RESPONSE SECTION: Include a 'What happens when something goes wrong?' section. "
        "This is counterintuitive but powerful. Address it head-on: the business responds "
        "to every review, stands behind its work, and makes things right. This builds more "
        "trust than 100 five-star reviews.\n"
        "- LEAVE A REVIEW CTA: A clear section inviting happy customers to share their "
        "experience. Include placeholder links for Google Reviews and Facebook Reviews. "
        "Make it dead simple, one click.\n"
        "- BOTTOM CTA: 'Ready to become our next success story?' or something in that vein. "
        "Link to contact page. Do not be corny about it.\n"
        "- DEDUP NOTE: This page is ONLY about social proof and reviews. Do not retell the "
        "company story (that's /about). Do not list services (that's /services). This page's "
        "unique value is concentrated proof that real people trust this business.\n"
        "- Target word count: 500-800 words (excluding placeholder content).\n"
        "- Primary keyword: '[business name] reviews' or '[industry] reviews [area]'.\n"
        f"{_OUTPUT_FORMAT}"
    )
    return _system_msg(), user_msg


def _prompt_generic(page_spec, ctx):
    brand = _brand_block(ctx)
    seo_intel = _seo_intel_block(ctx)
    lead_form = _lead_form_block(ctx)
    label = page_spec.get("label", "Page")
    user_msg = (
        f"Write a website page titled '{label}' for this local service business.\n\n"
        f"BUSINESS CONTEXT:\n{brand}\n\n"
        f"{seo_intel}"
        f"{lead_form}"
        f"{_GLOBAL_RULES}\n"
        "- Target word count: 500-800 words.\n"
        f"{_OUTPUT_FORMAT}"
    )
    return _system_msg(), user_msg


def _prompt_landing_page(page_spec, ctx):
    brand = _brand_block(ctx)
    seo_intel = _seo_intel_block(ctx)
    lead_form = _lead_form_block(ctx)
    lp_ctx = page_spec.get("context", {})
    lp_name = lp_ctx.get("lp_name", "")
    lp_keyword = lp_ctx.get("lp_keyword", "")
    lp_offer = lp_ctx.get("lp_offer", "")
    lp_audience = lp_ctx.get("lp_audience", ctx.get("target_audience", ""))

    user_msg = (
        f"Write a HIGH-CONVERTING LANDING PAGE for: {lp_name}\n\n"
        f"BUSINESS CONTEXT:\n{brand}\n\n"
        f"{seo_intel}"
        f"{lead_form}"
        f"{_GLOBAL_RULES}\n"
        "CONVERSION FRAMEWORK: AIDA with urgency layer\n"
        "This is a LANDING PAGE, not a standard website page. It has ONE job: convert the visitor "
        "into a lead or customer. Every word either moves them toward the CTA or gets cut.\n\n"
        "PAGE-SPECIFIC REQUIREMENTS:\n"
        f"- TARGET KEYWORD: '{lp_keyword}' (weave naturally, first 100 words, H2, meta desc).\n"
        f"- TARGET AUDIENCE: {lp_audience}\n"
    )

    if lp_offer:
        user_msg += f"- OFFER: {lp_offer}. Lead with this. Make it the hero.\n"

    user_msg += (
        "- HERO SECTION: Big, bold value proposition. What they get, who it's for, and one clear CTA button. "
        "No navigation distractions. No 'welcome to'. Straight to the point.\n"
        "- PROBLEM SECTION: Name the exact pain. Make them feel seen. Be specific to the industry.\n"
        "- SOLUTION SECTION: Show how you solve it. Process steps, not vague promises. 3-4 steps max.\n"
        "- PROOF SECTION: Include [TESTIMONIAL_PLACEHOLDER] markers. Social proof, stats, trust badges.\n"
        "- OBJECTION HANDLING: Address 2-3 top objections inline. 'What if it doesn't work?' "
        "'Is it worth the cost?' 'How is this different?'\n"
        "- CTA SECTION: Repeat the offer with urgency. Phone number prominent. Form placement "
        "marked with [LEAD_FORM_PLACEHOLDER] if no specific form shortcode provided.\n"
        "- NO NAVIGATION: This page should not link to other site pages except via the CTA. "
        "No sidebar. No footer links. Single-purpose page.\n"
        "- MOBILE FIRST: Short paragraphs. Big buttons. Scannable headings.\n"
        "- Target word count: 600-1000 words.\n"
        f"- Primary keyword: '{lp_keyword}' or '{ctx.get('industry', '')} {ctx.get('service_area', '').split(',')[0].strip()}'.\n"
        f"{_OUTPUT_FORMAT}"
    )
    return _system_msg(), user_msg


_PROMPT_BUILDERS = {
    "home": _prompt_home,
    "about": _prompt_about,
    "services": _prompt_services,
    "service_detail": _prompt_service_detail,
    "service_area": _prompt_service_area,
    "contact": _prompt_contact,
    "faq": _prompt_faq,
    "testimonials": _prompt_testimonials,
    "landing_page": _prompt_landing_page,
}


# ---------------------------------------------------------------------------
# Schema markup generators
# ---------------------------------------------------------------------------

def build_schema_markup(page_spec, page_content, brand_ctx):
    """
    Build JSON-LD schema markup for a page.

    Returns a list of schema objects (each becomes a <script type="application/ld+json"> block).
    """
    schemas = []
    schema_types = page_spec.get("schema_types") or []
    hints = {}
    if isinstance(page_content, dict):
        hints = page_content.get("schema_hints") or {}

    site_url = brand_ctx.get("website") or ""
    page_slug = page_spec.get("slug") or ""
    page_url = f"{site_url.rstrip('/')}/{page_slug}".rstrip("/") if site_url else ""

    for schema_type in schema_types:
        builder = _SCHEMA_BUILDERS.get(schema_type)
        if builder:
            schema = builder(page_spec, page_content, brand_ctx, hints, page_url)
            if schema:
                schemas.append(schema)

    return schemas


def _schema_local_business(page_spec, content, ctx, hints, page_url):
    biz = {
        "@context": "https://schema.org",
        "@type": "LocalBusiness",
        "name": ctx.get("business_name") or "",
        "url": ctx.get("website") or page_url,
    }
    if ctx.get("phone"):
        biz["telephone"] = ctx["phone"]
    if ctx.get("email"):
        biz["email"] = ctx["email"]
    if ctx.get("address"):
        biz["address"] = {
            "@type": "PostalAddress",
            "streetAddress": ctx["address"],
        }
    if ctx.get("service_area"):
        areas = _parse_csv(ctx["service_area"])
        if areas:
            biz["areaServed"] = [{"@type": "City", "name": a} for a in areas]
    if ctx.get("hours"):
        biz["openingHours"] = ctx["hours"]
    if hints.get("priceRange"):
        biz["priceRange"] = hints["priceRange"]
    if ctx.get("industry"):
        biz["description"] = (
            f"{ctx['business_name']} provides {ctx['industry']} services"
            + (f" in {ctx['service_area']}" if ctx.get("service_area") else "")
            + "."
        )
    return biz


def _schema_website(page_spec, content, ctx, hints, page_url):
    site_url = ctx.get("website") or page_url
    if not site_url:
        return None
    schema = {
        "@context": "https://schema.org",
        "@type": "WebSite",
        "name": ctx.get("business_name") or "",
        "url": site_url,
    }
    # Add SearchAction for site search if on home page
    if page_spec.get("page_type") == "home":
        schema["potentialAction"] = {
            "@type": "SearchAction",
            "target": f"{site_url.rstrip('/')}/?s={{search_term_string}}",
            "query-input": "required name=search_term_string",
        }
    return schema


def _schema_webpage(page_spec, content, ctx, hints, page_url):
    title = ""
    description = ""
    if isinstance(content, dict):
        title = content.get("seo_title") or content.get("title") or ""
        description = content.get("seo_description") or content.get("excerpt") or ""
    schema = {
        "@context": "https://schema.org",
        "@type": "WebPage",
        "name": title,
        "url": page_url or ctx.get("website") or "",
    }
    if description:
        schema["description"] = description
    if ctx.get("business_name"):
        schema["isPartOf"] = {
            "@type": "WebSite",
            "name": ctx["business_name"],
            "url": ctx.get("website") or "",
        }
    return schema


def _schema_organization(page_spec, content, ctx, hints, page_url):
    org = {
        "@context": "https://schema.org",
        "@type": "Organization",
        "name": ctx.get("business_name") or "",
        "url": ctx.get("website") or "",
    }
    if ctx.get("phone"):
        org["telephone"] = ctx["phone"]
    if ctx.get("email"):
        org["email"] = ctx["email"]
    if ctx.get("year_founded"):
        org["foundingDate"] = ctx["year_founded"]
    if ctx.get("address"):
        org["address"] = {
            "@type": "PostalAddress",
            "streetAddress": ctx["address"],
        }
    return org


def _schema_service(page_spec, content, ctx, hints, page_url):
    svc_name = page_spec.get("context", {}).get("service_name") or ""
    if not svc_name and isinstance(content, dict):
        svc_name = content.get("title") or ""

    schema = {
        "@context": "https://schema.org",
        "@type": "Service",
        "name": svc_name,
        "url": page_url,
        "provider": {
            "@type": "LocalBusiness",
            "name": ctx.get("business_name") or "",
            "url": ctx.get("website") or "",
        },
    }
    if hints.get("serviceType"):
        schema["serviceType"] = hints["serviceType"]
    if hints.get("areaServed") or ctx.get("service_area"):
        area = hints.get("areaServed") or ctx.get("service_area") or ""
        areas = _parse_csv(area) if isinstance(area, str) else [area]
        schema["areaServed"] = [{"@type": "City", "name": a} for a in areas]
    if isinstance(content, dict) and content.get("seo_description"):
        schema["description"] = content["seo_description"]
    return schema


def _schema_faq_page(page_spec, content, ctx, hints, page_url):
    faq_items = []
    if isinstance(content, dict):
        faq_items = content.get("faq_items") or []
    if not faq_items:
        return None

    entities = []
    for item in faq_items:
        q = (item.get("question") or "").strip()
        a = (item.get("answer") or "").strip()
        if q and a:
            entities.append({
                "@type": "Question",
                "name": q,
                "acceptedAnswer": {
                    "@type": "Answer",
                    "text": a,
                },
            })
    if not entities:
        return None

    return {
        "@context": "https://schema.org",
        "@type": "FAQPage",
        "mainEntity": entities,
    }


def _schema_contact_page(page_spec, content, ctx, hints, page_url):
    return {
        "@context": "https://schema.org",
        "@type": "ContactPage",
        "name": "Contact",
        "url": page_url or "",
        "mainEntity": {
            "@type": "LocalBusiness",
            "name": ctx.get("business_name") or "",
            "telephone": ctx.get("phone") or "",
            "email": ctx.get("email") or "",
        },
    }


def _schema_breadcrumb(page_spec, content, ctx, hints, page_url):
    site_url = (ctx.get("website") or "").rstrip("/")
    if not site_url:
        return None

    slug = page_spec.get("slug") or ""
    parts = [p for p in slug.split("/") if p]
    if not parts:
        return None

    items = [{
        "@type": "ListItem",
        "position": 1,
        "name": "Home",
        "item": site_url,
    }]

    running = site_url
    for i, part in enumerate(parts, start=2):
        running = f"{running}/{part}"
        name = part.replace("-", " ").title()
        items.append({
            "@type": "ListItem",
            "position": i,
            "name": name,
            "item": running,
        })

    return {
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": items,
    }


_SCHEMA_BUILDERS = {
    "LocalBusiness": _schema_local_business,
    "WebSite": _schema_website,
    "WebPage": _schema_webpage,
    "Organization": _schema_organization,
    "Service": _schema_service,
    "FAQPage": _schema_faq_page,
    "ContactPage": _schema_contact_page,
    "BreadcrumbList": _schema_breadcrumb,
}


# ---------------------------------------------------------------------------
# Content generation pipeline
# ---------------------------------------------------------------------------

def generate_page_content(page_spec, brand_ctx, api_key, model="gpt-4o-mini"):
    """
    Generate content for a single page via OpenAI.

    Returns dict with: title, content, excerpt, seo_title, seo_description,
    primary_keyword, secondary_keywords, faq_items, schema_hints
    """
    import openai

    system_msg, user_msg = build_page_prompt(page_spec, brand_ctx)

    client = openai.OpenAI(api_key=api_key)
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": user_msg},
        ],
        temperature=0.5,
        response_format={"type": "json_object"},
    )
    raw = (response.choices[0].message.content or "{}").strip()
    result = _extract_json(raw)
    return result


def assemble_page(page_spec, brand_ctx, content):
    """
    Assemble final page output: combine generated content with schema markup.

    Returns:
        {
            "page_spec": {...},
            "content": {...},  (AI-generated fields)
            "schemas": [...],  (JSON-LD objects)
            "schema_html": "...",  (ready-to-inject script tags)
            "full_html": "...",  (content + schema scripts appended)
        }
    """
    schemas = build_schema_markup(page_spec, content, brand_ctx)
    schema_html = "\n".join(
        f'<script type="application/ld+json">\n{json.dumps(s, indent=2)}\n</script>'
        for s in schemas
    )

    body_html = content.get("content") or ""
    full_html = f"{body_html}\n\n<!-- Schema Markup -->\n{schema_html}" if schema_html else body_html

    return {
        "page_spec": page_spec,
        "content": content,
        "schemas": schemas,
        "schema_html": schema_html,
        "full_html": full_html,
    }


def generate_site(brand_ctx, blueprint, api_key, model="gpt-4o-mini", progress_cb=None):
    """
    Generate all pages for a site build.

    Args:
        brand_ctx: from build_brand_context
        blueprint: from build_site_blueprint
        api_key: OpenAI API key
        model: model name
        progress_cb: optional callback(page_index, total, page_spec) for progress

    Returns list of assembled page dicts.
    """
    pages = []
    total = len(blueprint)
    for i, page_spec in enumerate(blueprint):
        if progress_cb:
            progress_cb(i, total, page_spec)

        content = generate_page_content(page_spec, brand_ctx, api_key, model)
        assembled = assemble_page(page_spec, brand_ctx, content)
        pages.append(assembled)

    return pages


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _parse_csv(text):
    """Parse comma-separated values, stripping whitespace and empties."""
    if not text:
        return []
    return [item.strip() for item in text.split(",") if item.strip()]


def _slugify(text):
    """Convert text to URL-safe slug."""
    slug = text.lower().strip()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"[\s_]+", "-", slug)
    slug = re.sub(r"-+", "-", slug)
    return slug.strip("-")


def _extract_json(raw):
    """Extract JSON from a response that may have markdown fences."""
    text = raw.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = lines[1:]  # remove opening fence
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        logger.warning("Failed to parse AI response as JSON, returning empty dict")
        return {}


# ---------------------------------------------------------------------------
# Warren SEO brief generator
# ---------------------------------------------------------------------------

def generate_warren_seo_brief(brand_ctx, seo_data, api_key, model="gpt-4o-mini"):
    """
    Have Warren analyze Search Console data and produce an SEO strategy brief
    for the site builder. Returns a plain-text brief string.
    """
    import openai

    brand = _brand_block(brand_ctx)

    # Format SC data into a readable block
    sc_lines = []
    if seo_data.get("totals"):
        t = seo_data["totals"]
        sc_lines.append(
            f"Overall: {t.get('clicks',0)} clicks, {t.get('impressions',0)} impressions, "
            f"{t.get('ctr',0)}% CTR, avg position {t.get('avg_position',0)}"
        )

    if seo_data.get("top_queries"):
        sc_lines.append("\nTop performing queries:")
        for q in seo_data["top_queries"][:20]:
            sc_lines.append(
                f"  - \"{q['query']}\" - clicks: {q['clicks']}, imp: {q['impressions']}, "
                f"pos: {q['position']}, ctr: {q['ctr']}%"
            )

    if seo_data.get("opportunity_queries"):
        sc_lines.append("\nOpportunity queries (high impressions, position 4-20):")
        for q in seo_data["opportunity_queries"][:15]:
            sc_lines.append(
                f"  - \"{q['query']}\" - imp: {q['impressions']}, pos: {q['position']}"
            )

    if seo_data.get("top_pages"):
        sc_lines.append("\nTop pages by clicks:")
        for p in seo_data["top_pages"][:10]:
            sc_lines.append(f"  - {p['page']} - clicks: {p['clicks']}, pos: {p['position']}")

    sc_block = "\n".join(sc_lines)

    system = (
        "You are Warren, an SEO strategist for a marketing platform that builds websites "
        "for local service businesses. You have access to real Google Search Console data. "
        "Your job is to analyze this data and produce a concise, actionable SEO strategy brief "
        "that will guide the AI content generator when building website pages.\n\n"
        "Be specific. Reference actual queries from the data. Identify patterns, gaps, and opportunities. "
        "Do NOT use em dashes. Do NOT use AI-tell words like harness, leverage, elevate, robust, seamless."
    )

    user_msg = (
        f"Analyze this business's Search Console data and write an SEO strategy brief.\n\n"
        f"BUSINESS:\n{brand}\n\n"
        f"SEARCH CONSOLE DATA:\n{sc_block}\n\n"
        "Produce a brief (300-500 words) covering:\n"
        "1. KEYWORD CLUSTERS: Group the queries into 3-6 topic clusters. For each cluster, "
        "identify which page type should target it (home, service detail, service area, FAQ, landing page).\n"
        "2. QUICK WINS: Queries in position 4-15 with high impressions that could move to page 1 "
        "with better on-page content. Name the specific queries.\n"
        "3. CONTENT GAPS: Topics the business SHOULD rank for based on their services but currently "
        "has no visibility on. Suggest specific page types or landing pages to create.\n"
        "4. KEYWORD MAPPING: For each page that will be generated, suggest the primary keyword "
        "and 2-3 secondary keywords based on the actual search data.\n"
        "5. INTERNAL LINKING STRATEGY: How pages should link to each other to build topic authority.\n\n"
        "Write in plain text, not JSON. Be direct and specific. Reference actual query data."
    )

    try:
        client = openai.OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.4,
            max_tokens=1500,
        )
        return (response.choices[0].message.content or "").strip()
    except Exception as exc:
        logger.warning("Warren SEO brief generation failed: %s", exc)
        return ""
