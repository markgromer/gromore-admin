"""
Ad Knowledge Engine

Central intelligence system for advertising best practices, examples, and news digests.
- Stores and retrieves curated ad examples (good and bad, with analysis)
- Maintains platform-specific best practices
- Runs weekly news digest agent (Google/Meta ad updates)
- Builds master prompts that sub-account ad builders reference
"""
import json
import logging
import os
from datetime import datetime, timedelta

import requests as _requests

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
#  Seed Data: Ad Examples (Good + Bad, with analysis)
# ─────────────────────────────────────────────────────────────

SEED_AD_EXAMPLES = [
    # ── Google Search RSA: Good examples ──
    {
        "platform": "google", "format": "search_rsa", "industry": "plumbing",
        "headline": "24/7 Emergency Plumber",
        "description": "Licensed plumbers available now. Same-day service, upfront pricing. Call for a free estimate today.",
        "quality": "good", "score": 9,
        "analysis": "Strong because: (1) Urgency in headline with '24/7 Emergency' - matches high-intent searches. "
                    "(2) Trust signals: 'Licensed', 'upfront pricing'. (3) Clear CTA: 'Call for a free estimate'. "
                    "(4) Under character limits. (5) Addresses the searcher's pain - they need help NOW.",
        "principles": ["urgency", "trust_signals", "clear_cta", "pain_point_match"],
    },
    {
        "platform": "google", "format": "search_rsa", "industry": "plumbing",
        "headline": "Best Plumbing Services",
        "description": "We offer the best plumbing services in the area. Contact us today for more information about what we can do.",
        "quality": "bad", "score": 2,
        "analysis": "Weak because: (1) 'Best' is a meaningless superlative - Google may flag it, and users ignore it. "
                    "(2) No specifics: what services? what area? (3) 'Contact us for more information' is a weak CTA - "
                    "no reason to act now. (4) 'What we can do' is vague. (5) No trust signals, no urgency, no differentiator.",
        "principles": ["vague_claims", "weak_cta", "no_specifics", "no_urgency"],
    },
    {
        "platform": "google", "format": "search_rsa", "industry": "hvac",
        "headline": "AC Repair - Same Day",
        "description": "Your AC broke? We fix it today. $50 off first repair. 4.9 stars, 500+ reviews. Book online now.",
        "quality": "good", "score": 9,
        "analysis": "Excellent because: (1) Problem-solution in 5 words. (2) Same-day promise. (3) Concrete offer: '$50 off'. "
                    "(4) Social proof with specific numbers: '4.9 stars, 500+ reviews'. (5) Multiple conversion paths: 'Book online now'. "
                    "(6) Conversational tone ('Your AC broke?') matches how people think when searching.",
        "principles": ["problem_solution", "concrete_offer", "social_proof_numbers", "conversational_tone", "same_day"],
    },
    {
        "platform": "google", "format": "search_rsa", "industry": "dental",
        "headline": "New Patient Special $99",
        "description": "Exam, X-rays & cleaning for $99. Accepting new patients now. Evening & weekend hours available.",
        "quality": "good", "score": 8,
        "analysis": "Works well because: (1) Price in headline grabs attention and pre-qualifies. (2) Specifics on what's included. "
                    "(3) 'Accepting new patients' removes the 'are they taking people?' barrier. "
                    "(4) 'Evening & weekend hours' addresses a real objection for working adults.",
        "principles": ["price_in_headline", "specifics", "barrier_removal", "objection_handling"],
    },
    {
        "platform": "google", "format": "search_rsa", "industry": "legal",
        "headline": "Injured? Free Consult",
        "description": "No fee unless we win. 30+ years experience. Over $100M recovered for clients. Call 24/7.",
        "quality": "good", "score": 9,
        "analysis": "Powerful because: (1) One-word pain point question 'Injured?' - instant relevance. "
                    "(2) Zero-risk offer: 'No fee unless we win'. (3) Authority: '30+ years', '$100M recovered'. "
                    "(4) Available 24/7. Each element removes a different objection.",
        "principles": ["pain_question", "zero_risk", "authority_numbers", "always_available"],
    },

    # ── Google Display: Good example ──
    {
        "platform": "google", "format": "display", "industry": "home_services",
        "headline": "Your Neighbors Trust Us",
        "description": "Over 2,000 homes serviced in [City]. See why your neighbors chose us for their home repairs.",
        "quality": "good", "score": 8,
        "analysis": "Display ads need to interrupt and intrigue. This works because: (1) Social proof via proximity - 'your neighbors'. "
                    "(2) Specific number (2,000) adds credibility. (3) Curiosity gap: 'See why'. "
                    "(4) Local relevance with city name. Display is about awareness, and this creates familiarity.",
        "principles": ["social_proof_proximity", "curiosity_gap", "local_relevance", "specific_numbers"],
    },

    # ── Meta Ads: Good examples ──
    {
        "platform": "meta", "format": "feed", "industry": "plumbing",
        "headline": "Before You Call a Plumber",
        "description": "3 things every homeowner should check before calling a plumber (number 2 saves most people $200+). "
                       "Free guide - no email required.",
        "quality": "good", "score": 9,
        "analysis": "Meta feed gold because: (1) Pattern interrupt - not selling, educating. (2) Numbered list creates curiosity. "
                    "(3) Specific savings amount ($200+). (4) Low friction: 'no email required'. "
                    "(5) Positions the business as helpful expert, not desperate seller. (6) Thumb-stopping because it's not an ad, it's advice.",
        "principles": ["education_first", "numbered_list", "specific_savings", "low_friction", "expert_positioning"],
    },
    {
        "platform": "meta", "format": "feed", "industry": "restaurant",
        "headline": "This Sold Out Last Week",
        "description": "Our smoked brisket platter sold out by 2pm Saturday. This week we doubled the batch. "
                       "Pre-order yours before it's gone again.",
        "quality": "good", "score": 9,
        "analysis": "Brilliant because: (1) Scarcity is REAL (it actually sold out), not manufactured. "
                    "(2) Story format - it reads like a post, not an ad. (3) FOMO: 'before it's gone again'. "
                    "(4) Social proof implied: so popular it sold out. (5) Clear action: pre-order.",
        "principles": ["real_scarcity", "story_format", "fomo", "implied_social_proof", "clear_action"],
    },
    {
        "platform": "meta", "format": "feed", "industry": "fitness",
        "headline": "Join Our Gym! Best Prices!",
        "description": "We have the best gym in town with great equipment and amazing trainers. "
                       "Sign up today and get fit! Don't miss out on our amazing deals!",
        "quality": "bad", "score": 2,
        "analysis": "Everything wrong: (1) 'Best Prices' and 'Best gym' - empty superlatives. (2) 'Amazing' used twice. "
                    "(3) Reads like an ad from 2005 - no personality. (4) 'Don't miss out' is generic FOMO that means nothing. "
                    "(5) No specifics: what equipment? what are the prices? what deals? "
                    "(6) On Meta, this gets scrolled past instantly because it looks and reads like an ad.",
        "principles": ["empty_superlatives", "no_specifics", "generic_fomo", "looks_like_an_ad", "no_personality"],
    },
    {
        "platform": "meta", "format": "feed", "industry": "ecommerce",
        "headline": "Still thinking about it?",
        "description": "You looked at the [Product] 3 days ago. 47 people bought it since then. "
                       "Your size is still in stock - for now. Free returns, always.",
        "quality": "good", "score": 9,
        "analysis": "Retargeting masterclass: (1) Acknowledges the browse without being creepy. (2) Social proof with real-time number. "
                    "(3) Scarcity on their specific attribute (size). (4) Risk removal: 'Free returns, always'. "
                    "(5) Conversational, not pushy. Feels like a friend nudging, not a corporation demanding.",
        "principles": ["retargeting_acknowledgment", "real_time_social_proof", "specific_scarcity", "risk_removal", "conversational"],
    },

    # ── Video ads: Good example ──
    {
        "platform": "google", "format": "video", "industry": "home_services",
        "headline": "Watch: $12K Kitchen in 3 Days",
        "description": "See the full transformation of a dated kitchen to modern dream in 72 hours. Real job, real client, real budget.",
        "quality": "good", "score": 8,
        "analysis": "Video ads need a hook in the first 3 seconds. This works because: (1) Specific price and timeframe in headline. "
                    "(2) Before/after transformation is inherently watchable. (3) 'Real job, real client, real budget' - authenticity. "
                    "(4) Aspirational but achievable.",
        "principles": ["specific_price_time", "transformation", "authenticity", "aspirational_achievable"],
    },

    # ── PMax: Good example ──
    {
        "platform": "google", "format": "performance_max", "industry": "legal",
        "headline": "Free Case Review Today",
        "description": "Injured at work? Talk to a workers comp attorney in 15 minutes. No upfront cost. We handle everything.",
        "quality": "good", "score": 8,
        "analysis": "PMax needs to work across Search, Display, YouTube, Gmail, and Maps. This is strong because: "
                    "(1) Lead with the offer, not the firm name. (2) Specific time: '15 minutes' - fast. "
                    "(3) 'We handle everything' reduces overwhelm. (4) Works as text or overlay on any format.",
        "principles": ["offer_first", "specific_time", "reduce_overwhelm", "format_flexible"],
    },
]


# ─────────────────────────────────────────────────────────────
#  Seed Data: Best Practices (Research-Backed)
# ─────────────────────────────────────────────────────────────

SEED_BEST_PRACTICES = [
    # ── Universal Principles ──
    {
        "platform": "all", "format": "", "category": "psychology",
        "title": "Loss aversion outperforms gain framing 2:1",
        "content": "Research consistently shows people are more motivated to avoid losing something than gaining something equivalent. "
                   "'Don't lose your spot' outperforms 'Reserve your spot'. 'Stop wasting $X/month' outperforms 'Save $X/month'. "
                   "Apply this to headlines and CTAs. Frame the cost of inaction, not just the benefit of action.",
        "priority": 10,
        "source": "Kahneman & Tversky - Prospect Theory; Google Ads internal studies",
    },
    {
        "platform": "all", "format": "", "category": "psychology",
        "title": "Specific numbers beat round numbers by 20-30%",
        "content": "Ads with specific numbers ($197 vs $200, 4.8 stars vs '5-star', 847 reviews vs 'hundreds of reviews') "
                   "consistently outperform round numbers. Specifics signal authenticity. Round numbers signal estimates or lies. "
                   "Use exact figures from real data: actual review count, exact price, real completion time.",
        "priority": 10,
        "source": "Multiple A/B testing platforms; Unbounce conversion research",
    },
    {
        "platform": "all", "format": "", "category": "psychology",
        "title": "Questions in headlines increase CTR 15-25%",
        "content": "Headlines phrased as questions the searcher is already asking themselves get higher engagement. "
                   "'Need a plumber fast?' > 'Fast plumbing services'. 'Tired of back pain?' > 'Back pain treatment'. "
                   "The question creates an internal 'yes' that primes the click. But only use questions that match the search intent.",
        "priority": 9,
        "source": "Wordstream headline analysis; AdEspresso split test database",
    },
    {
        "platform": "all", "format": "", "category": "structure",
        "title": "One message per ad, one CTA per ad",
        "content": "Ads that try to say everything say nothing. Each ad should have one clear message and one clear action. "
                   "Not: 'We do plumbing, HVAC, and electrical - call, email, or visit our website!' "
                   "Instead: 'Emergency plumber - call now' or 'AC repair today - book online'. "
                   "Use different ad variations for different messages, not one ad for all messages.",
        "priority": 10,
        "source": "Google Ads best practices guide; Ogilvy on Advertising",
    },
    {
        "platform": "all", "format": "", "category": "structure",
        "title": "Front-load the value proposition",
        "content": "The first 3-5 words determine if someone reads the rest. Lead with the benefit or offer, not the business name. "
                   "Wrong: 'ABC Plumbing offers 24/7 emergency service'. Right: '24/7 Emergency Plumber - ABC Plumbing'. "
                   "On mobile especially, only the first 1-2 headlines may be visible. Make them count.",
        "priority": 9,
        "source": "Nielsen Norman Group eye-tracking studies; Google mobile truncation data",
    },

    # ── Google Ads Specific ──
    {
        "platform": "google", "format": "search_rsa", "category": "technical",
        "title": "RSA headline pinning strategy",
        "content": "Pin your strongest, most relevant headline to position 1. Pin your brand or trust signal to position 2. "
                   "Leave position 3 unpinned for Google to optimize. For descriptions, pin your strongest CTA to position 1. "
                   "Never pin all positions - it defeats RSA's machine learning. Aim for 2-3 pins maximum.",
        "priority": 9,
        "source": "Google Ads Help Center; Search Engine Journal RSA optimization guide",
    },
    {
        "platform": "google", "format": "search_rsa", "category": "technical",
        "title": "Headlines: aim for 3 distinct themes across 15",
        "content": "Google recommends 15 headlines but they need thematic diversity. Structure them as: "
                   "5 headlines about the service/offer, 5 about trust/proof (reviews, years, guarantees), "
                   "5 about urgency/CTA (call now, book today, limited time). "
                   "This ensures every combination Google serves has a compelling mix.",
        "priority": 8,
        "source": "Google Ads optimization playbook 2024",
    },
    {
        "platform": "google", "format": "search_rsa", "category": "copywriting",
        "title": "Mirror the search query in headline 1",
        "content": "When someone searches 'emergency plumber near me', seeing 'Emergency Plumber Near You' in headline 1 "
                   "creates instant relevance. Google bolds matching words. This improves both Quality Score and CTR. "
                   "Use Dynamic Keyword Insertion cautiously, but manually matching high-volume queries is more reliable.",
        "priority": 9,
        "source": "Google Quality Score documentation; internal testing across 200+ accounts",
    },
    {
        "platform": "google", "format": "performance_max", "category": "technical",
        "title": "PMax asset quality matters more than quantity",
        "content": "Performance Max uses machine learning to combine your assets. Bad assets pollute every combination. "
                   "Check the Asset Detail report weekly. Remove any asset rated 'Low' and replace it. "
                   "Quality signals: headlines with the service area convert higher for local businesses. "
                   "Image assets should include at least one lifestyle shot and one results/proof shot.",
        "priority": 8,
        "source": "Google Performance Max best practices 2024; Think With Google",
    },

    # ── Meta/Facebook Specific ──
    {
        "platform": "meta", "format": "", "category": "creative",
        "title": "UGC-style creative outperforms polished ads 2-3x on Meta",
        "content": "User-generated content style (phone-shot video, casual tone, real people) consistently outperforms "
                   "studio-quality ads on Meta platforms. The algorithm favors content that looks native to the feed. "
                   "Best performers: customer testimonial videos, behind-the-scenes, before/after transformations, "
                   "owner-to-camera talking head. Avoid stock photos and corporate graphics.",
        "priority": 10,
        "source": "Meta Creative Best Practices 2024; Apptopia ad intelligence data",
    },
    {
        "platform": "meta", "format": "", "category": "creative",
        "title": "Hook in first 3 seconds or lose them forever",
        "content": "63% of Meta video views are under 3 seconds. Your hook must work in silent autoplay. "
                   "Winning hooks: bold text overlay with the problem statement, dramatic before/after reveal, "
                   "surprising statistic, counter-intuitive claim. Never start with your logo or business name.",
        "priority": 10,
        "source": "Meta Business Insights; Vidyard video engagement research",
    },
    {
        "platform": "meta", "format": "", "category": "copywriting",
        "title": "Primary text: problem-agitate-solve in 3 lines",
        "content": "Line 1: Name the problem they're experiencing (empathy). "
                   "Line 2: Agitate - make them feel the cost of not solving it. "
                   "Line 3: Present your solution with a specific, low-friction next step. "
                   "Keep it under the 'See More' fold (125 chars for mobile). If they need to click to read your pitch, most won't.",
        "priority": 9,
        "source": "Meta ad copywriting playbook; tested across 50+ local business accounts",
    },
    {
        "platform": "meta", "format": "", "category": "targeting",
        "title": "Broad targeting + strong creative > narrow targeting + weak creative",
        "content": "Meta's algorithm in 2024-2025 favors broader audiences with Advantage+ optimization. "
                   "Instead of 47 interest layers, go broader and let the creative do the qualifying. "
                   "Your ad copy and imagery should naturally repel the wrong audience and attract the right one. "
                   "Exception: retargeting custom audiences should still be specific.",
        "priority": 9,
        "source": "Meta Advantage+ documentation; Jon Loomer performance testing",
    },
    {
        "platform": "meta", "format": "", "category": "technical",
        "title": "Advantage+ Shopping campaigns for e-commerce",
        "content": "For e-commerce: Advantage+ Shopping Campaigns (ASC) automate targeting, placements, and creative. "
                   "Feed them 10+ creative variants and let the algorithm find winners. "
                   "Set a CPA cap to control costs. Review creative-level reporting weekly to understand what themes win. "
                   "Not recommended for lead gen or local services.",
        "priority": 7,
        "source": "Meta Advantage+ Shopping documentation",
    },

    # ── Landing Page (affects ad quality) ──
    {
        "platform": "all", "format": "", "category": "landing_page",
        "title": "Ad-to-landing page message match is non-negotiable",
        "content": "If your ad says '$99 AC tune-up', the landing page must say '$99 AC tune-up' above the fold. "
                   "Message mismatch is the #1 reason campaigns with great ads still fail. "
                   "Google measures this for Quality Score. Meta measures it for relevance score. "
                   "Users measure it by bouncing in 3 seconds. One ad = one dedicated landing page (or section).",
        "priority": 10,
        "source": "Unbounce State of Landing Pages report; Google Quality Score factors",
    },
    {
        "platform": "all", "format": "", "category": "landing_page",
        "title": "Mobile load speed: every second costs 7% conversions",
        "content": "53% of mobile visitors abandon pages that take >3 seconds to load. "
                   "Test with Google PageSpeed Insights. Target: <2.5s LCP. "
                   "Quick wins: compress images, lazy-load below-fold content, use a CDN. "
                   "The best ad in the world is worthless if the landing page takes 5 seconds to load on 4G.",
        "priority": 9,
        "source": "Google/SOASTA research; Think With Google mobile benchmarks",
    },
]


# ─────────────────────────────────────────────────────────────
#  Seed the database
# ─────────────────────────────────────────────────────────────

def seed_ad_knowledge(db):
    """Populate the ad_examples and ad_best_practices tables with initial data.
    Skips if data already exists."""
    existing_examples = db.get_ad_examples(limit=1)
    if not existing_examples:
        log.info("Seeding %d ad examples...", len(SEED_AD_EXAMPLES))
        for ex in SEED_AD_EXAMPLES:
            db.add_ad_example(
                platform=ex["platform"],
                fmt=ex["format"],
                industry=ex.get("industry", ""),
                headline=ex.get("headline", ""),
                description=ex.get("description", ""),
                full_ad_json=json.dumps(ex),
                quality=ex["quality"],
                score=ex["score"],
                analysis=ex["analysis"],
                principles=json.dumps(ex["principles"]),
                source=ex.get("source", "seed"),
            )

    existing_bp = db.get_ad_best_practices()
    if not existing_bp:
        log.info("Seeding %d ad best practices...", len(SEED_BEST_PRACTICES))
        for bp in SEED_BEST_PRACTICES:
            db.add_ad_best_practice(
                platform=bp["platform"],
                fmt=bp.get("format", ""),
                category=bp["category"],
                title=bp["title"],
                content=bp["content"],
                priority=bp.get("priority", 0),
                source=bp.get("source", "seed"),
            )


# ─────────────────────────────────────────────────────────────
#  Build context for ad generation (examples + best practices)
# ─────────────────────────────────────────────────────────────

def build_ad_knowledge_context(db, platform, fmt):
    """Return a knowledge block that gets injected into the ad builder's system prompt.
    Contains relevant examples (good + bad) and best practices for the given platform/format."""

    # Get relevant good examples
    good_examples = db.get_ad_examples(platform=platform, fmt=fmt, quality="good", limit=5)
    if len(good_examples) < 3:
        good_examples += db.get_ad_examples(platform=platform, quality="good", limit=5 - len(good_examples))

    # Get relevant bad examples
    bad_examples = db.get_ad_examples(platform=platform, fmt=fmt, quality="bad", limit=3)
    if not bad_examples:
        bad_examples = db.get_ad_examples(platform=platform, quality="bad", limit=2)

    # Get best practices
    practices = db.get_ad_best_practices(platform=platform, fmt=fmt)

    # Get the active master prompt override if one exists
    master = db.get_active_master_prompt("ad_builder", platform, fmt)
    if not master:
        master = db.get_active_master_prompt("ad_builder", "all", "")

    parts = []

    if master:
        parts.append(
            "MASTER ADVERTISING INTELLIGENCE (updated by the agency's research agent):\n"
            + master["content"]
        )

    if good_examples:
        lines = ["REFERENCE: HIGH-PERFORMING AD EXAMPLES (study these patterns):"]
        for i, ex in enumerate(good_examples, 1):
            lines.append(
                f"\nExample {i} ({ex.get('industry', 'general')} - {ex['platform']}/{ex['format']} - Score {ex['score']}/10):\n"
                f"  Headline: {ex['headline']}\n"
                f"  Description: {ex['description']}\n"
                f"  Why it works: {ex['analysis']}"
            )
        parts.append("\n".join(lines))

    if bad_examples:
        lines = ["REFERENCE: WEAK AD EXAMPLES (avoid these patterns):"]
        for i, ex in enumerate(bad_examples, 1):
            lines.append(
                f"\nAnti-Example {i} ({ex.get('industry', 'general')} - Score {ex['score']}/10):\n"
                f"  Headline: {ex['headline']}\n"
                f"  Description: {ex['description']}\n"
                f"  Why it fails: {ex['analysis']}"
            )
        parts.append("\n".join(lines))

    if practices:
        lines = ["BEST PRACTICES (research-backed, apply these):"]
        for bp in practices:
            lines.append(f"\n[{bp['category'].upper()}] {bp['title']}:\n  {bp['content']}")
        parts.append("\n".join(lines))

    return "\n\n".join(parts) if parts else ""


# ─────────────────────────────────────────────────────────────
#  News Digest Agent
# ─────────────────────────────────────────────────────────────

NEWS_SEARCH_QUERIES = [
    "Google Ads new features updates this week",
    "Meta Facebook Ads platform changes updates this week",
    "Google Ads policy changes 2025 2026",
    "Meta Advantage+ updates changes",
    "Performance Max Google Ads updates",
    "digital advertising industry changes this week",
]

NEWS_DIGEST_SYSTEM_PROMPT = """You are an advertising intelligence analyst for a digital marketing agency.
You have been given search results about recent Google Ads and Meta/Facebook Ads platform updates.

Your job is to:
1. Identify the ACTIONABLE changes - things that actually affect how we build and manage ad campaigns
2. Ignore fluff, opinion pieces, and general marketing advice that isn't tied to a specific platform change
3. For each real change, explain: what changed, when, and exactly how it affects ad campaign management
4. Provide specific action items the agency should take in response to each change
5. Suggest any updates to our ad generation system prompts based on these changes

Return ONLY valid JSON with this structure:
{
    "digest_date": "YYYY-MM-DD",
    "findings": [
        {
            "platform": "google|meta|both",
            "title": "Short title of the change",
            "summary": "2-3 sentence explanation of what changed and why it matters",
            "impact": "high|medium|low",
            "action_items": ["Specific action 1", "Specific action 2"],
            "source_snippet": "Key quote or detail from the search results"
        }
    ],
    "prompt_update_suggestions": "If any findings require updating the ad generation prompts, describe what should change. Be specific about which rules or guidelines need modification. If nothing needs to change, say 'No prompt updates needed this week.'",
    "summary": "2-3 paragraph executive summary of the most important changes this week and their combined impact on campaign management."
}

Rules:
- Only include findings that are REAL platform changes, not speculation or rumors
- 'high' impact = changes how we build campaigns or manage budgets immediately
- 'medium' impact = new feature or option to test, or upcoming deprecation to prepare for
- 'low' impact = nice to know, minor UI change, or very niche feature
- Action items must be concrete: 'Check all Performance Max campaigns for the new asset report' not 'Stay updated'
- If search results are sparse or repetitive, say so honestly. Don't pad the report."""


def run_news_digest(db, api_key=None):
    """Run the weekly advertising news digest agent.
    Uses web search to find recent platform updates, then AI to analyze them."""
    if not api_key:
        try:
            from flask import current_app
            api_key = (current_app.config.get("OPENAI_API_KEY", "") or "").strip()
        except RuntimeError:
            api_key = os.environ.get("OPENAI_API_KEY", "").strip()

    if not api_key:
        return {"error": "No OpenAI API key configured"}

    today = datetime.now().strftime("%Y-%m-%d")

    # Step 1: Run web searches for recent ad news
    all_results = []
    for query in NEWS_SEARCH_QUERIES:
        try:
            results = _web_search_for_news(query)
            if results:
                all_results.append({"query": query, "results": results})
        except Exception as e:
            log.warning("News search failed for '%s': %s", query, e)

    if not all_results:
        return {"error": "All news searches failed. Check your API configuration."}

    # Step 2: Feed search results to AI for analysis
    search_context = json.dumps(all_results, ensure_ascii=False)

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    try:
        resp = _requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json={
                "model": "gpt-4o",
                "temperature": 0.3,
                "messages": [
                    {"role": "system", "content": NEWS_DIGEST_SYSTEM_PROMPT},
                    {"role": "user", "content": f"Today's date: {today}\n\nSearch results:\n{search_context}"},
                ],
                "response_format": {"type": "json_object"},
            },
            timeout=90,
        )

        if resp.status_code != 200:
            return {"error": f"AI analysis failed ({resp.status_code}): {resp.text[:200]}"}

        data = resp.json()
        content = (data.get("choices") or [{}])[0].get("message", {}).get("content", "")
        digest = json.loads(content)

    except Exception as e:
        return {"error": f"Digest generation failed: {e}"}

    # Step 3: Save to database
    db.add_ad_news_digest(
        digest_date=today,
        platform="all",
        raw_findings=json.dumps(digest.get("findings", []), ensure_ascii=False),
        summary=digest.get("summary", ""),
        action_items=json.dumps(digest.get("findings", []), ensure_ascii=False),
        prompt_updates=digest.get("prompt_update_suggestions", ""),
        status="draft",
    )

    return digest


def _web_search_for_news(query):
    """Search the web for advertising news. Uses the same search mechanism as Warren."""
    try:
        from webapp.ai_assistant import _execute_web_search
        result = _execute_web_search(query)
        return result if result else None
    except Exception as e:
        log.warning("Web search for news failed: %s", e)
        return None


# ─────────────────────────────────────────────────────────────
#  Master Prompt Builder
# ─────────────────────────────────────────────────────────────

MASTER_PROMPT_BUILDER_SYSTEM = """You are building a master advertising system prompt for an AI ad copy generator.
This prompt will be injected into every ad generation request across all client accounts.

You have been given:
1. A database of proven ad examples (good and bad) with analysis
2. Research-backed best practices
3. Recent platform news and changes (if available)
4. The current active master prompt (if any)

Your job: Synthesize all of this into a single, comprehensive set of instructions that will make the AI ad generator produce better ads.

The output should be a clear, well-organized set of rules and guidelines that covers:
- What makes ads convert (based on the examples and research)
- What to avoid (based on the bad examples)
- Platform-specific rules based on the latest changes
- Industry-specific patterns if enough examples exist
- Concrete, actionable guidance (not vague advice)

Keep it under 2000 words. Every sentence should be actionable. No filler.
Write it as direct instructions to the AI ad copy engine.
Do not use em dashes. Use commas, periods, colons, or regular dashes instead."""


def rebuild_master_prompt(db, api_key=None, platform="all", fmt=""):
    """Rebuild the master advertising prompt from examples, practices, and news digests."""
    if not api_key:
        try:
            from flask import current_app
            api_key = (current_app.config.get("OPENAI_API_KEY", "") or "").strip()
        except RuntimeError:
            api_key = os.environ.get("OPENAI_API_KEY", "").strip()

    if not api_key:
        return {"error": "No OpenAI API key configured"}

    # Gather all knowledge
    examples = db.get_ad_examples(limit=100)
    practices = db.get_ad_best_practices()
    digests = db.get_ad_news_digests(limit=4)
    current_master = db.get_active_master_prompt("ad_builder", platform, fmt)

    # Build the context
    context_parts = []

    if examples:
        good = [e for e in examples if e["quality"] == "good"]
        bad = [e for e in examples if e["quality"] == "bad"]
        context_parts.append(f"GOOD AD EXAMPLES ({len(good)} total):")
        for ex in good[:20]:
            context_parts.append(
                f"  [{ex['platform']}/{ex['format']}] Score {ex['score']}/10 - {ex['headline']}\n"
                f"    Analysis: {ex['analysis']}"
            )
        if bad:
            context_parts.append(f"\nBAD AD EXAMPLES ({len(bad)} total):")
            for ex in bad[:10]:
                context_parts.append(
                    f"  [{ex['platform']}/{ex['format']}] Score {ex['score']}/10 - {ex['headline']}\n"
                    f"    Analysis: {ex['analysis']}"
                )

    if practices:
        context_parts.append(f"\nBEST PRACTICES ({len(practices)} total):")
        for bp in practices:
            context_parts.append(f"  [{bp['category']}] {bp['title']}: {bp['content'][:300]}")

    if digests:
        context_parts.append("\nRECENT NEWS DIGESTS:")
        for d in digests[:4]:
            context_parts.append(f"  [{d['digest_date']}] {d['summary'][:500]}")
            if d.get("prompt_updates"):
                context_parts.append(f"    Prompt updates suggested: {d['prompt_updates'][:300]}")

    if current_master:
        context_parts.append(f"\nCURRENT ACTIVE MASTER PROMPT (v{current_master['version']}):\n{current_master['content'][:2000]}")

    context = "\n".join(context_parts)

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    try:
        resp = _requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json={
                "model": "gpt-4o",
                "temperature": 0.4,
                "messages": [
                    {"role": "system", "content": MASTER_PROMPT_BUILDER_SYSTEM},
                    {"role": "user", "content": f"Build the master prompt from this knowledge base:\n\n{context}"},
                ],
            },
            timeout=90,
        )

        if resp.status_code != 200:
            return {"error": f"Master prompt build failed ({resp.status_code}): {resp.text[:200]}"}

        data = resp.json()
        content = (data.get("choices") or [{}])[0].get("message", {}).get("content", "").strip()

        if not content:
            return {"error": "AI returned empty master prompt"}

        # Save as new version
        db.save_master_prompt("ad_builder", platform, fmt, content)

        return {"content": content, "platform": platform, "format": fmt}

    except Exception as e:
        return {"error": f"Master prompt build error: {e}"}
