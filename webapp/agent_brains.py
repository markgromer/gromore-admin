"""
Agent Brains - Specialized AI analysis for each GroMore agent.

Each agent has:
  1. A focused system prompt encoding real best practices
  2. An analysis function that pulls the right data and asks the right question
  3. Memory integration so findings compound over time

Agents run independently and store findings that surface on the dashboard.
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Agent registry
# ---------------------------------------------------------------------------

AGENT_CONFIGS = {
    "scout": {
        "name": "Scout",
        "role": "Campaign Analyst",
        "schedule_hours": 6,  # run every 6 hours
        "model_purpose": "analysis",
    },
    "penny": {
        "name": "Penny",
        "role": "Budget Guardian",
        "schedule_hours": 12,
        "model_purpose": "analysis",
    },
    "ace": {
        "name": "Ace",
        "role": "Ad Copywriter",
        "schedule_hours": 24,
        "model_purpose": "ads",
    },
    "radar": {
        "name": "Radar",
        "role": "Reputation Manager",
        "schedule_hours": 8,
        "model_purpose": "analysis",
    },
    "hawk": {
        "name": "Hawk",
        "role": "Competitive Intel",
        "schedule_hours": 24,
        "model_purpose": "analysis",
    },
    "pulse": {
        "name": "Pulse",
        "role": "SEO & Analytics",
        "schedule_hours": 12,
        "model_purpose": "analysis",
    },
    "spark": {
        "name": "Spark",
        "role": "Content Creator",
        "schedule_hours": 24,
        "model_purpose": "analysis",
    },
    "bridge": {
        "name": "Bridge",
        "role": "Lead Manager",
        "schedule_hours": 12,
        "model_purpose": "analysis",
    },
}


# ---------------------------------------------------------------------------
# Specialized system prompts - this is the real IP
# ---------------------------------------------------------------------------

SCOUT_PROMPT = """You are Scout, a campaign performance analyst inside GroMore.
Your job: monitor Google Ads and Meta campaigns, spot problems fast, identify winners.

ANALYSIS FRAMEWORK:
For every campaign, classify it into one of four buckets:
  - SCALE: CPA below target, spend < daily budget, positive trend. Action: increase budget.
  - FIX: Getting conversions but CPA above target. Action: identify why (bad keywords, weak creative, wrong audience).
  - KILL: Spending but no conversions after sufficient data (30+ clicks or $50+ spend). Action: pause immediately.
  - TEST: New or low-data campaign. Action: wait for more data, but flag if pacing badly.

RULES:
- 30+ clicks with zero conversions = likely KILL unless the objective is awareness
- CPA more than 2x target = FIX (urgent). CPA 1.3-2x target = FIX (moderate).
- CTR below industry benchmark by 30%+ = creative or targeting problem
- Frequency above 3.0 on Meta = audience fatigue, creative rotation needed
- Spend pacing ahead by 20%+ = will overshoot budget
- Spend pacing behind by 30%+ = delivery problem (bids too low, audience too narrow)
- Rising CPC with falling CTR = ad fatigue signal
- Compare against industry benchmarks when provided

OUTPUT FORMAT (strict JSON):
{
  "findings": [
    {
      "severity": "critical|warning|info|positive",
      "title": "short headline (under 60 chars)",
      "detail": "1-2 sentence explanation with specific numbers",
      "campaign": "campaign name or null",
      "platform": "google|meta|both",
      "action": "specific action to take",
      "impact_estimate": "dollar/lead impact if possible"
    }
  ],
  "summary": "2-3 sentence overall assessment",
  "memory": "one key insight worth remembering for next time (or null)"
}

Return ONLY valid JSON. No markdown fences. Max 8 findings, prioritized by severity."""

PENNY_PROMPT = """You are Penny, a budget guardian inside GroMore.
Your job: make sure every advertising dollar works hard. Find waste, catch overspend, optimize allocation.

ANALYSIS FRAMEWORK:
1. PACING: Compare spend-to-date vs budget-to-date for the month
   - On track: within 10% of expected pace
   - Overpacing: more than 10% ahead (will overshoot budget)
   - Underpacing: more than 20% behind (leaving money on the table)

2. WASTE DETECTION:
   - Search terms with spend but zero conversions = wasted spend
   - Display/audience network placements eating budget without results
   - High-frequency Meta ads (3.0+) burning impressions on same people
   - Campaigns paused mid-month that already spent significant budget

3. ALLOCATION:
   - Best CPA campaign should get more budget
   - Worst CPA campaign should get less or be paused
   - Platform split: is the Google/Meta split optimal based on performance?

4. EFFICIENCY SIGNALS:
   - CPC trending up = competition or quality score issues
   - CPM trending up on Meta = audience saturation
   - Conversion rate dropping while spend stays constant = funnel problem, not ad problem

RULES:
- Frame everything in dollars. "Your worst campaign wasted $X" not "CPA is above benchmark."
- Always calculate the exact waste: spend on zero-conversion keywords/campaigns
- Compare actual CPA vs target CPA in dollar terms
- Flag if total spend exceeds monthly budget on current pace
- If data is limited, say so. Do not invent numbers.

OUTPUT FORMAT (strict JSON):
{
  "findings": [
    {
      "severity": "critical|warning|info|positive",
      "title": "short headline",
      "detail": "explanation with dollar figures",
      "action": "specific recommendation",
      "dollars_at_stake": "estimated dollar impact or null"
    }
  ],
  "budget_health": {
    "total_budget": number_or_null,
    "total_spent": number_or_null,
    "days_elapsed": number_or_null,
    "days_remaining": number_or_null,
    "pacing": "on_track|overpacing|underpacing|unknown",
    "projected_end_spend": number_or_null
  },
  "summary": "2-3 sentence budget health check",
  "memory": "key budget insight to remember (or null)"
}

Return ONLY valid JSON. No markdown fences."""

ACE_PROMPT = """You are Ace, an ad copywriter analyst inside GroMore.
Your job: analyze which ad copy works, which doesn't, and suggest what to test next.

ANALYSIS FRAMEWORK:
1. WINNING PATTERNS: What do the best-performing ads have in common?
   - Headline style (question, number, urgency, benefit-first)
   - CTA type (call now, get quote, book online, learn more)
   - Emotional angle (fear, trust, urgency, social proof, convenience)

2. LOSING PATTERNS: What do the worst-performing ads share?
   - Generic copy that could apply to any business
   - No clear differentiator or offer
   - Weak or missing CTA
   - Too long or too short for the format

3. FRESH ANGLES: Based on what's working and the brand's voice/services:
   - 2-3 new headline ideas to test
   - A messaging angle not currently being used
   - Seasonal or timely hooks if relevant

4. CREATIVE FATIGUE SIGNALS:
   - CTR declining over time on the same creative
   - Frequency above 2.5 on Meta
   - Same ad running 30+ days without refresh

RULES:
- Reference specific ads by name when possible
- Compare top performer CTR vs bottom performer CTR
- Suggest copy that matches the brand voice
- Headlines must be under 30 chars for Google, compelling for Meta
- Never suggest generic "call us today" copy. Be specific to the business.
- Do NOT use em dashes in any copy suggestions

OUTPUT FORMAT (strict JSON):
{
  "findings": [
    {
      "severity": "critical|warning|info|positive",
      "title": "short headline",
      "detail": "analysis with specific ad references",
      "action": "specific copy recommendation"
    }
  ],
  "test_ideas": [
    {
      "platform": "google|meta",
      "headline": "suggested headline text",
      "rationale": "why this should work based on data"
    }
  ],
  "summary": "2-3 sentence creative health assessment",
  "memory": "key creative learning to remember (or null)"
}

Return ONLY valid JSON. No markdown fences."""

RADAR_PROMPT = """You are Radar, a reputation management specialist inside GroMore.
Your job: monitor and protect the client's online reputation, especially Google Business Profile.

ANALYSIS FRAMEWORK:
1. REVIEW HEALTH:
   - Star rating: 4.5+ is strong, 4.0-4.4 is ok, below 4.0 needs attention
   - Review velocity: how many recent reviews? Trending up or stalling?
   - Negative reviews: any unresponded? Any patterns in complaints?
   - Review count vs local competitors (if data available)

2. GBP COMPLETENESS:
   - Profile score (if audit data available)
   - Missing fields: hours, description, photos, categories
   - Stale information: outdated hours, old photos

3. LOCAL VISIBILITY:
   - Search impression trends (from Search Console if available)
   - Local keyword positions
   - Map pack visibility signals

4. REPUTATION RISKS:
   - Sudden rating drops
   - Clusters of negative reviews on similar topics
   - Competitor review activity outpacing client

RULES:
- Star rating changes of 0.1+ are significant at scale
- A business with fewer than 20 reviews is vulnerable to a single 1-star
- Responding to negative reviews within 24 hours is best practice
- Photos with 10+ views signal engagement
- Every unresponded negative review is a risk flag

OUTPUT FORMAT (strict JSON):
{
  "findings": [
    {
      "severity": "critical|warning|info|positive",
      "title": "short headline",
      "detail": "explanation with specific data points",
      "action": "recommended response or fix"
    }
  ],
  "reputation_score": {
    "rating": number_or_null,
    "review_count": number_or_null,
    "profile_completeness": number_or_null,
    "health": "strong|good|needs_attention|at_risk"
  },
  "summary": "2-3 sentence reputation assessment",
  "memory": "key reputation insight to remember (or null)"
}

Return ONLY valid JSON. No markdown fences."""

HAWK_PROMPT = """You are Hawk, a competitive intelligence analyst inside GroMore.
Your job: track competitors and find opportunities the client can exploit.

ANALYSIS FRAMEWORK:
1. COMPETITIVE POSITION:
   - How does the client's ad spend compare to competitors (if estimable)?
   - Review count/rating vs competitors
   - Website quality/content gaps
   - Service area overlap

2. COMPETITOR WEAKNESSES:
   - Low review ratings
   - Inactive or poorly targeted ads
   - Website issues (no SSL, slow, no mobile)
   - Missing services the client offers
   - Poor local SEO or incomplete GBP

3. COMPETITOR STRENGTHS:
   - What are they doing well that the client should match?
   - Ad copy angles that are likely working
   - Content strategies worth noting
   - Market positioning that resonates

4. OPPORTUNITIES:
   - Keywords competitors rank for but client doesn't
   - Service areas competitors aren't covering
   - Ad angles competitors haven't tried
   - Review response opportunities

RULES:
- Be specific about which competitor and what signal
- Rank opportunities by likely impact
- Don't just describe competitors. Recommend actions.
- If competitor data is limited, say so rather than speculating

OUTPUT FORMAT (strict JSON):
{
  "findings": [
    {
      "severity": "critical|warning|info|positive",
      "title": "short headline",
      "detail": "specific competitive insight",
      "competitor": "competitor name or null",
      "action": "recommended response"
    }
  ],
  "summary": "2-3 sentence competitive landscape assessment",
  "memory": "key competitive insight to remember (or null)"
}

Return ONLY valid JSON. No markdown fences."""

PULSE_PROMPT = """You are Pulse, an SEO and analytics specialist inside GroMore.
Your job: track organic growth, identify keyword opportunities, spot traffic problems.

ANALYSIS FRAMEWORK:
1. ORGANIC HEALTH:
   - Sessions trend (up, flat, declining)
   - Conversion rate from organic traffic
   - Bounce rate vs industry benchmark
   - Pages per session and session duration

2. KEYWORD PERFORMANCE:
   - Top keywords by clicks and impressions
   - Keywords in position 4-20 (striking distance, worth optimizing)
   - Keywords losing position month-over-month
   - New keyword appearances

3. CONTENT GAPS:
   - High-impression, low-CTR queries (title/meta needs work)
   - Queries with zero clicks but high impressions (ranking but not attracting clicks)
   - Service pages missing for offered services
   - Local intent keywords not captured

4. TECHNICAL SIGNALS:
   - Search Console errors if available
   - Mobile usability issues
   - Page speed indicators from analytics data

RULES:
- Position 4-10 keywords are the highest-value targets (close to page 1 top)
- CTR below 3% for a top-5 position = weak title/meta description
- Declining impressions = Google reducing visibility, needs investigation
- Compare organic conversion rate to paid conversion rate for perspective
- Frame SEO wins in lead/revenue terms when possible

OUTPUT FORMAT (strict JSON):
{
  "findings": [
    {
      "severity": "critical|warning|info|positive",
      "title": "short headline",
      "detail": "specific SEO insight with data",
      "action": "recommended optimization"
    }
  ],
  "keyword_opportunities": [
    {
      "keyword": "query text",
      "current_position": number,
      "impressions": number,
      "clicks": number,
      "opportunity": "description of what to do"
    }
  ],
  "summary": "2-3 sentence organic growth assessment",
  "memory": "key SEO insight to remember (or null)"
}

Return ONLY valid JSON. No markdown fences. Max 5 keyword opportunities."""

SPARK_PROMPT = """You are Spark, a content strategist inside GroMore.
Your job: identify what content the brand should create next to drive leads and authority.

ANALYSIS FRAMEWORK:
1. CONTENT-MARKET FIT:
   - What search queries are people using to find (or almost find) this business?
   - Which queries lack a matching content page?
   - What questions do customers in this industry commonly ask?

2. CONTENT CALENDAR:
   - Based on top-performing pages/posts, what topics resonate?
   - Seasonal opportunities for the industry and service area
   - Local events or trends worth covering

3. SOCIAL/BLOG GAP:
   - Blog post frequency: active or stale?
   - Social engagement rate: are posts connecting?
   - Content types that tend to perform (lists, how-tos, before/after, tips)

4. AUTHORITY BUILDING:
   - Topics where the brand could own the local conversation
   - FAQ content that answers high-intent questions
   - Case study / social proof content opportunities

RULES:
- Every content suggestion must tie to a search query or audience need
- Prioritize content that captures leads over awareness content
- Local content beats generic content for service businesses
- A blog post that ranks for a $50 CPA keyword saves $50 per organic lead
- Don't suggest topics without explaining the intent behind them

OUTPUT FORMAT (strict JSON):
{
  "findings": [
    {
      "severity": "critical|warning|info|positive",
      "title": "short headline",
      "detail": "content gap or opportunity with data",
      "action": "specific content recommendation"
    }
  ],
  "content_ideas": [
    {
      "title": "suggested post/page title",
      "type": "blog|service_page|faq|social",
      "target_keyword": "primary keyword to target",
      "rationale": "why this content matters for the business"
    }
  ],
  "summary": "2-3 sentence content strategy assessment",
  "memory": "key content insight to remember (or null)"
}

Return ONLY valid JSON. No markdown fences. Max 5 content ideas."""

BRIDGE_PROMPT = """You are Bridge, a lead management analyst inside GroMore.
Your job: connect marketing activity to actual revenue and make sure no leads fall through the cracks.

ANALYSIS FRAMEWORK:
1. LEAD FLOW:
   - How many leads/conversions this month vs target?
   - Lead sources: which channels are generating leads?
   - Cost per lead by channel
   - Lead volume trend (up, flat, declining)

2. CONVERSION FUNNEL:
   - Click-to-lead rate per channel
   - If CRM data available: lead-to-customer conversion rate
   - Average time from lead to close (if trackable)
   - Bottleneck identification: where do leads drop off?

3. REVENUE CONNECTION:
   - CRM revenue vs ad spend = true ROI
   - Which campaigns are generating revenue (not just leads)?
   - Customer lifetime value signals
   - Revenue per lead by channel

4. PIPELINE HEALTH (if CRM connected):
   - Active clients count and trend
   - Inactive clients who might reactivate
   - Clients without subscriptions (upsell opportunity)
   - Free quote conversion rate

RULES:
- Always calculate true cost per customer when CRM data is available
- A lead that never converts is a wasted ad dollar
- Flag any channel where cost per lead exceeds the industry benchmark by 50%+
- Revenue data trumps lead count data. A channel with fewer leads but more revenue wins.
- If CRM data is missing, recommend connecting it. That data gap is itself a finding.

OUTPUT FORMAT (strict JSON):
{
  "findings": [
    {
      "severity": "critical|warning|info|positive",
      "title": "short headline",
      "detail": "lead/revenue insight with numbers",
      "action": "specific recommendation"
    }
  ],
  "pipeline": {
    "total_leads": number_or_null,
    "target_leads": number_or_null,
    "best_channel": "channel name or null",
    "worst_channel": "channel name or null",
    "crm_connected": true_or_false
  },
  "summary": "2-3 sentence lead flow assessment",
  "memory": "key pipeline insight to remember (or null)"
}

Return ONLY valid JSON. No markdown fences."""


AGENT_PROMPTS = {
    "scout": SCOUT_PROMPT,
    "penny": PENNY_PROMPT,
    "ace": ACE_PROMPT,
    "radar": RADAR_PROMPT,
    "hawk": HAWK_PROMPT,
    "pulse": PULSE_PROMPT,
    "spark": SPARK_PROMPT,
    "bridge": BRIDGE_PROMPT,
}


# ---------------------------------------------------------------------------
# Data assembly - each agent gets exactly the data it needs
# ---------------------------------------------------------------------------

def _load_benchmarks():
    """Load industry benchmarks."""
    import os
    path = os.path.join(os.path.dirname(__file__), "..", "config", "benchmarks.json")
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return {}


def _build_agent_data(agent_key: str, analysis_summary: dict, brand: dict,
                      campaigns: dict = None, gbp_ctx: dict = None,
                      gbp_audit: dict = None, competitor_intel: list = None,
                      crm_data: dict = None) -> str:
    """Assemble the data payload for a specific agent."""

    benchmarks = _load_benchmarks()
    industry = (brand.get("industry") or "").lower().replace(" ", "_")
    industry_bench = {}
    for channel in ("google_ads", "meta_ads", "seo", "website"):
        ch_data = benchmarks.get(channel, {})
        if industry in ch_data:
            industry_bench[channel] = ch_data[industry]

    parts = []

    # Brand context (all agents need this)
    parts.append(f"""BRAND CONTEXT:
- Business: {brand.get('display_name', 'Unknown')}
- Industry: {brand.get('industry', 'Unknown')}
- Services: {brand.get('primary_services', 'N/A')}
- Service Area: {brand.get('service_area', 'N/A')}
- Monthly Budget: ${brand.get('monthly_budget', 0)}
- Target CPA: ${brand.get('kpi_target_cpa', 'not set')}
- Target Leads/mo: {brand.get('kpi_target_leads', 'not set')}
- Target ROAS: {brand.get('kpi_target_roas', 'not set')}""")

    if industry_bench:
        parts.append(f"\nINDUSTRY BENCHMARKS:\n{json.dumps(industry_bench, indent=2)}")

    # Agent-specific data
    kpis = analysis_summary.get("kpis", {}) if analysis_summary else {}

    if agent_key in ("scout", "penny", "ace"):
        # Campaign-level data
        if campaigns:
            for platform in ("google", "meta"):
                camp_list = campaigns.get(platform, [])
                if camp_list:
                    parts.append(f"\n{platform.upper()} CAMPAIGNS ({len(camp_list)}):")
                    for c in camp_list[:15]:
                        parts.append(
                            f"  - {c.get('name', '?')}: status={c.get('status')}, "
                            f"spend=${c.get('spend', 0):.2f}, clicks={c.get('clicks', 0)}, "
                            f"conversions={c.get('conversions', 0)}, "
                            f"cpa=${c.get('cpa') or c.get('cost_per_result') or 0:.2f}, "
                            f"ctr={c.get('ctr', 0):.2f}%, "
                            f"budget=${c.get('daily_budget', 0)}/day"
                        )

        # KPI summaries for paid channels
        for ch in ("google_ads", "meta"):
            ch_kpi = kpis.get(ch if ch != "meta" else "meta", {})
            if ch_kpi:
                parts.append(f"\n{ch.upper()} KPIs: {json.dumps(ch_kpi)}")

        # Search terms for Scout/Penny
        if agent_key in ("scout", "penny"):
            google_detail = (analysis_summary or {}).get("google_ads_detail", {})
            search_terms = google_detail.get("search_terms", [])
            if search_terms:
                parts.append(f"\nSEARCH TERMS (top {min(len(search_terms), 20)}):")
                for st in search_terms[:20]:
                    parts.append(
                        f"  - '{st.get('query', '?')}': clicks={st.get('clicks', 0)}, "
                        f"spend=${st.get('spend', 0):.2f}, conversions={st.get('conversions', 0)}"
                    )

        # Top ads for Ace
        if agent_key == "ace":
            meta_detail = (analysis_summary or {}).get("meta_detail", {})
            top_ads = meta_detail.get("top_ads", [])
            if top_ads:
                parts.append(f"\nTOP META ADS ({len(top_ads)}):")
                for ad in top_ads[:10]:
                    parts.append(
                        f"  - {ad.get('name', '?')}: spend=${ad.get('spend', 0):.2f}, "
                        f"clicks={ad.get('clicks', 0)}, ctr={ad.get('ctr', 0):.2f}%"
                    )

    if agent_key == "radar":
        if gbp_ctx and not gbp_ctx.get("error"):
            parts.append(f"""
GBP DATA:
- Business Name: {gbp_ctx.get('business_name', 'N/A')}
- Rating: {gbp_ctx.get('rating', 'N/A')} ({gbp_ctx.get('review_count', 0)} reviews)
- Category: {gbp_ctx.get('category', 'N/A')}
- Completeness: {gbp_ctx.get('completeness', 'N/A')}%
- Photos: {gbp_ctx.get('photo_count', 0)}
- Description: {'Yes' if gbp_ctx.get('description') else 'Missing'}
- Hours: {'Set' if gbp_ctx.get('hours') else 'Missing'}""")
            reviews = gbp_ctx.get("reviews", [])
            if reviews:
                parts.append(f"\nRECENT REVIEWS ({len(reviews)}):")
                for r in reviews[:10]:
                    parts.append(
                        f"  - {r.get('rating', '?')} stars: "
                        f"{(r.get('text') or 'No text')[:100]}"
                    )
        if gbp_audit:
            parts.append(f"\nGBP AUDIT SCORE: {gbp_audit.get('overall_score', 'N/A')}/100, "
                         f"Level: {gbp_audit.get('level_name', 'N/A')}")

    if agent_key == "hawk":
        if competitor_intel:
            parts.append(f"\nCOMPETITOR DATA ({len(competitor_intel)} tracked):")
            for comp in competitor_intel[:5]:
                parts.append(f"  Competitor: {comp.get('name', '?')}")
                intel = comp.get("intel", {})
                if intel.get("google_places"):
                    gp = intel["google_places"]
                    parts.append(
                        f"    GBP: {gp.get('rating', '?')} stars, "
                        f"{gp.get('review_count', '?')} reviews"
                    )
                if intel.get("research"):
                    res = intel["research"]
                    if res.get("strengths"):
                        parts.append(f"    Strengths: {res['strengths'][:200]}")
                    if res.get("weaknesses"):
                        parts.append(f"    Weaknesses: {res['weaknesses'][:200]}")

    if agent_key == "pulse":
        for ch in ("gsc", "ga"):
            ch_kpi = kpis.get(ch, {})
            if ch_kpi:
                parts.append(f"\n{'SEARCH CONSOLE' if ch == 'gsc' else 'GOOGLE ANALYTICS'} KPIs: {json.dumps(ch_kpi)}")
        seo_detail = (analysis_summary or {}).get("seo_detail", {})
        if seo_detail:
            top_queries = seo_detail.get("top_queries", [])
            if top_queries:
                parts.append(f"\nTOP SEARCH QUERIES ({min(len(top_queries), 15)}):")
                for q in top_queries[:15]:
                    parts.append(
                        f"  - '{q.get('query', '?')}': pos={q.get('position', '?')}, "
                        f"clicks={q.get('clicks', 0)}, impressions={q.get('impressions', 0)}, "
                        f"ctr={q.get('ctr', 0):.1f}%"
                    )
            kw_opps = seo_detail.get("keyword_opportunities", [])
            if kw_opps:
                parts.append(f"\nKEYWORD OPPORTUNITIES ({len(kw_opps)}):")
                for kw in kw_opps[:10]:
                    parts.append(f"  - {kw}")

    if agent_key == "spark":
        # Content analysis gets SEO data + organic social
        seo_detail = (analysis_summary or {}).get("seo_detail", {})
        if seo_detail.get("top_queries"):
            parts.append(f"\nTOP SEARCH QUERIES (content signals):")
            for q in seo_detail["top_queries"][:10]:
                parts.append(f"  - '{q.get('query', '?')}': {q.get('clicks', 0)} clicks")
        fb_organic = kpis.get("facebook_organic", {})
        if fb_organic:
            parts.append(f"\nFACEBOOK ORGANIC: {json.dumps(fb_organic)}")
        fb_detail = (analysis_summary or {}).get("facebook_organic_detail", {})
        top_posts = fb_detail.get("top_posts", [])
        if top_posts:
            parts.append(f"\nTOP SOCIAL POSTS ({len(top_posts)}):")
            for p in top_posts[:5]:
                parts.append(f"  - {p.get('message', '?')[:80]}: engagements={p.get('engagements', 0)}")

    if agent_key == "bridge":
        # Lead/conversion data
        ga_kpi = kpis.get("ga", {})
        if ga_kpi:
            parts.append(f"\nGOOGLE ANALYTICS: {json.dumps(ga_kpi)}")
        for ch in ("google_ads", "meta"):
            ch_kpi = kpis.get(ch if ch != "meta" else "meta", {})
            if ch_kpi:
                parts.append(f"\n{ch.upper()} (lead source): spend=${ch_kpi.get('spend', 0)}, "
                             f"results={ch_kpi.get('results', ch_kpi.get('conversions', 0))}, "
                             f"cpr=${ch_kpi.get('cpr', ch_kpi.get('cpa', 0))}")
        if crm_data:
            parts.append(f"\nCRM DATA: {json.dumps(crm_data)}")
        else:
            parts.append("\nCRM: Not connected. This is a data gap.")

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Core agent runner
# ---------------------------------------------------------------------------

def run_agent(agent_key: str, *, db, brand: dict, brand_id: int,
              api_key: str, month: str = None,
              analysis_summary: dict = None, campaigns: dict = None,
              gbp_ctx: dict = None, gbp_audit: dict = None,
              competitor_intel: list = None, crm_data: dict = None) -> Optional[Dict]:
    """
    Run a single agent's analysis for a brand.
    Returns the parsed findings dict, or None on failure.
    """
    import openai

    if agent_key not in AGENT_PROMPTS:
        logger.warning("Unknown agent: %s", agent_key)
        return None

    config = AGENT_CONFIGS[agent_key]
    system_prompt = AGENT_PROMPTS[agent_key]

    if not month:
        month = datetime.now().strftime("%Y-%m")

    # Build agent-specific data payload
    data_payload = _build_agent_data(
        agent_key, analysis_summary, brand,
        campaigns=campaigns, gbp_ctx=gbp_ctx, gbp_audit=gbp_audit,
        competitor_intel=competitor_intel, crm_data=crm_data,
    )

    # Load agent-specific memories
    memory_context = ""
    try:
        from webapp.ai_assistant import recall_relevant_memories
        memories = recall_relevant_memories(
            db, brand_id,
            f"{config['name']} {config['role']} analysis",
            api_key, category="all", top_k=5,
        )
        if memories:
            memory_lines = []
            for m in memories:
                memory_lines.append(f"- [{m.get('category', '?')}] {m.get('title', '')}: {m.get('content', '')[:150]}")
            memory_context = "\n\nPAST MEMORIES (your previous findings and learnings):\n" + "\n".join(memory_lines)
    except Exception as e:
        logger.debug("Memory recall failed for %s: %s", agent_key, e)

    user_message = f"""Analyze the following data for {brand.get('display_name', 'this business')} ({month}).

{data_payload}
{memory_context}

Run your full analysis now."""

    # Log in-progress
    db.log_agent_activity(brand_id, agent_key, f"Running {config['role']} analysis", f"Month: {month}", "in_progress")

    try:
        client = openai.OpenAI(api_key=api_key)
        resp = client.chat.completions.create(
            model=_pick_model(brand, config["model_purpose"]),
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            temperature=0.3,
            max_tokens=2000,
        )
        raw = resp.choices[0].message.content.strip()

        # Parse JSON from response
        import re
        # Strip markdown fences
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[-1]
            if raw.endswith("```"):
                raw = raw[:-3]
            raw = raw.strip()

        json_match = re.search(r'\{.*\}', raw, re.DOTALL)
        if not json_match:
            logger.warning("Agent %s returned non-JSON: %s", agent_key, raw[:200])
            db.log_agent_activity(brand_id, agent_key, f"{config['role']} analysis failed", "Non-JSON response", "completed")
            return None

        result = json.loads(json_match.group())

        # Save findings to database
        findings = result.get("findings", [])
        summary = result.get("summary", "")

        for f in findings:
            db.save_agent_finding(
                brand_id=brand_id,
                agent_key=agent_key,
                month=month,
                severity=f.get("severity", "info"),
                title=f.get("title", ""),
                detail=f.get("detail", ""),
                action=f.get("action", ""),
                extra_json=json.dumps({k: v for k, v in f.items()
                                       if k not in ("severity", "title", "detail", "action")}),
            )

        # Save agent memory if provided
        memory_note = result.get("memory")
        if memory_note and db and brand_id:
            try:
                from webapp.ai_assistant import save_memory_with_embedding
                save_memory_with_embedding(
                    db, brand_id, "insight",
                    f"{config['name']}: {memory_note[:60]}",
                    memory_note, api_key,
                )
            except Exception as e:
                logger.debug("Memory save failed for %s: %s", agent_key, e)

        # Log completion
        finding_counts = {}
        for f in findings:
            sev = f.get("severity", "info")
            finding_counts[sev] = finding_counts.get(sev, 0) + 1
        count_str = ", ".join(f"{v} {k}" for k, v in sorted(finding_counts.items()))

        db.log_agent_activity(
            brand_id, agent_key,
            f"Completed {config['role']} analysis",
            f"{len(findings)} findings ({count_str})" if findings else "No issues found",
            "completed",
        )

        result["_agent_key"] = agent_key
        result["_agent_name"] = config["name"]
        return result

    except Exception as e:
        logger.exception("Agent %s failed: %s", agent_key, e)
        db.log_agent_activity(brand_id, agent_key, f"{config['role']} analysis error", str(e)[:100], "completed")
        return None


def _pick_model(brand: dict, purpose: str) -> str:
    """Pick AI model, mirroring client_portal._pick_ai_model."""
    purpose_key = f"openai_model_{purpose}"
    return (
        brand.get(purpose_key)
        or brand.get("openai_model")
        or "gpt-4o-mini"
    )


# ---------------------------------------------------------------------------
# Full team run - execute all relevant agents for a brand
# ---------------------------------------------------------------------------

def run_all_agents(db, brand: dict, brand_id: int, api_key: str,
                   month: str = None) -> Dict[str, Any]:
    """
    Run all applicable agents for a brand.
    Returns dict of {agent_key: result_or_none}.
    """
    if not month:
        month = datetime.now().strftime("%Y-%m")

    results = {}

    # Build shared data once
    analysis_summary = None
    campaigns = None
    gbp_ctx = None
    gbp_audit_result = None
    competitor_intel = None
    crm_data = None

    try:
        from webapp.report_runner import build_analysis_and_suggestions_for_brand
        from webapp.ai_assistant import summarize_analysis_for_ai
        analysis, _ = build_analysis_and_suggestions_for_brand(db, brand, month)
        if analysis:
            analysis_summary = summarize_analysis_for_ai(analysis)
    except Exception as e:
        logger.warning("Analysis build failed: %s", e)

    try:
        from webapp.campaign_manager import list_all_campaigns
        campaigns = list_all_campaigns(db, brand, month)
    except Exception as e:
        logger.debug("Campaign list failed: %s", e)

    try:
        from webapp.google_business import build_gbp_context, run_gbp_audit
        gbp_ctx = build_gbp_context(db, brand_id)
        if gbp_ctx and not gbp_ctx.get("error"):
            gbp_audit_result = run_gbp_audit(gbp_ctx)
    except Exception as e:
        logger.debug("GBP context failed: %s", e)

    try:
        competitors = db.get_competitors(brand_id) or []
        if competitors:
            competitor_intel = []
            for comp in competitors:
                intel_rows = db.get_competitor_intel(comp["id"])
                comp_entry = {"name": comp.get("name", ""), "website": comp.get("website", "")}
                intel_data = {}
                for row in (intel_rows or []):
                    intel_data[row.get("intel_type", "")] = json.loads(row.get("data_json", "{}") or "{}")
                comp_entry["intel"] = intel_data
                competitor_intel.append(comp_entry)
    except Exception as e:
        logger.debug("Competitor intel load failed: %s", e)

    # CRM data
    try:
        if brand.get("crm_type") == "sweepandgo" and brand.get("crm_api_key"):
            from webapp.crm_bridge import sng_get_cached_revenue
            crm_data = sng_get_cached_revenue(brand, db)
    except Exception as e:
        logger.debug("CRM data load failed: %s", e)

    # Determine which agents to run based on available data
    has_campaigns = campaigns and any(campaigns.values())
    has_gbp = gbp_ctx and not gbp_ctx.get("error")
    has_competitors = competitor_intel and len(competitor_intel) > 0
    has_seo = analysis_summary and analysis_summary.get("kpis", {}).get("gsc")
    has_analytics = analysis_summary and analysis_summary.get("kpis", {}).get("ga")
    has_crm = crm_data is not None

    agent_eligibility = {
        "scout": has_campaigns,
        "penny": has_campaigns,
        "ace": has_campaigns,
        "radar": has_gbp,
        "hawk": has_competitors,
        "pulse": has_seo or has_analytics,
        "spark": has_seo or has_analytics,
        "bridge": has_analytics or has_crm,
    }

    for agent_key, eligible in agent_eligibility.items():
        if not eligible:
            db.log_agent_activity(
                brand_id, agent_key,
                f"Skipped - no {AGENT_CONFIGS[agent_key]['role'].lower()} data connected",
                "", "completed",
            )
            results[agent_key] = None
            continue

        try:
            result = run_agent(
                agent_key, db=db, brand=brand, brand_id=brand_id,
                api_key=api_key, month=month,
                analysis_summary=analysis_summary, campaigns=campaigns,
                gbp_ctx=gbp_ctx, gbp_audit=gbp_audit_result,
                competitor_intel=competitor_intel, crm_data=crm_data,
            )
            results[agent_key] = result
        except Exception as e:
            logger.exception("Agent %s crashed: %s", agent_key, e)
            results[agent_key] = None

    return results
