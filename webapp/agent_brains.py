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
    "chief": {
        "name": "Chief",
        "role": "Quality Control",
        "schedule_hours": 0,   # only runs as part of team run
        "model_purpose": "analysis",
    },
}


# ---------------------------------------------------------------------------
# Agent quality specifications - Chief's reference for grading each agent
# ---------------------------------------------------------------------------

AGENT_QUALITY_SPECS = {
    "scout": {
        "required_fields": ["severity", "title", "detail", "action", "campaign", "platform"],
        "max_findings": 8,
        "min_findings": 1,
        "severity_max": {"critical": 3},
        "detail_needs_numbers": True,
        "must_reference_data": True,  # must cite actual campaign names/metrics from input
        "banned_phrases": [
            "monitor closely", "consider adjusting", "keep an eye on",
            "optimize your campaigns", "could be improved", "may want to",
            "leverage", "harness", "elevate", "supercharge", "unlock",
            "in today's competitive landscape", "don't miss out",
        ],
        "specificity": "Must reference actual campaign names and include specific metrics (spend, CPA, CTR, conversions).",
    },
    "penny": {
        "required_fields": ["severity", "title", "detail", "action"],
        "max_findings": 6,
        "min_findings": 1,
        "severity_max": {"critical": 2},
        "detail_needs_numbers": True,
        "must_reference_data": True,
        "banned_phrases": [
            "consider increasing", "consider decreasing", "keep an eye on",
            "optimize your budget", "could be improved", "may want to",
            "leverage", "harness", "elevate", "supercharge", "unlock",
        ],
        "budget_constraint": True,  # recommendations must respect monthly_budget
        "specificity": "Must include specific dollar amounts. Tell them exactly how much to move and where.",
    },
    "ace": {
        "required_fields": ["severity", "title", "detail", "action"],
        "max_findings": 6,
        "min_findings": 1,
        "severity_max": {"critical": 2},
        "detail_needs_numbers": False,  # creative agent, less number-dependent
        "must_reference_data": False,
        "brand_specificity": True,  # copy MUST mention the actual business, services, or area
        "banned_phrases": [
            "call today for a free quote", "professional and reliable",
            "your trusted local", "quality service at affordable prices",
            "don't miss out", "act now", "limited time offer",
            "leverage", "harness", "elevate", "supercharge", "unlock",
            "in today's competitive landscape", "take your business to the next level",
        ],
        "specificity": "Ad copy must name the actual business, specific services, and service area. Generic CTAs are auto-rejected.",
    },
    "radar": {
        "required_fields": ["severity", "title", "detail", "action"],
        "max_findings": 6,
        "min_findings": 1,
        "severity_max": {"critical": 2},
        "detail_needs_numbers": True,  # should cite rating, review count
        "must_reference_data": True,
        "banned_phrases": [
            "monitor your reviews", "respond to all reviews",
            "leverage", "harness", "elevate", "supercharge", "unlock",
            "in today's competitive landscape",
        ],
        "specificity": "Must reference actual review data, ratings, or GBP profile details. Response templates must be business-specific.",
    },
    "hawk": {
        "required_fields": ["severity", "title", "detail"],
        "max_findings": 6,
        "min_findings": 1,
        "severity_max": {"critical": 2},
        "detail_needs_numbers": True,
        "must_reference_data": True,
        "banned_phrases": [
            "stay ahead of the competition", "competitive advantage",
            "leverage", "harness", "elevate", "supercharge", "unlock",
            "in today's competitive landscape",
        ],
        "specificity": "Must reference actual competitor names and specific differences. No generic competitive advice.",
    },
    "pulse": {
        "required_fields": ["severity", "title", "detail", "action"],
        "max_findings": 8,
        "min_findings": 1,
        "severity_max": {"critical": 3},
        "detail_needs_numbers": True,
        "must_reference_data": True,
        "banned_phrases": [
            "optimize for seo", "improve your rankings", "focus on keywords",
            "leverage", "harness", "elevate", "supercharge", "unlock",
            "in today's competitive landscape",
        ],
        "specificity": "Must reference actual keywords, positions, or traffic numbers from the data.",
    },
    "spark": {
        "required_fields": ["severity", "title", "detail"],
        "max_findings": 6,
        "min_findings": 1,
        "severity_max": {"critical": 1},
        "detail_needs_numbers": False,
        "must_reference_data": False,
        "brand_specificity": True,
        "banned_phrases": [
            "create engaging content", "post consistently",
            "leverage", "harness", "elevate", "supercharge", "unlock",
            "in today's competitive landscape", "take your business to the next level",
        ],
        "specificity": "Content ideas must be tied to actual search data or social performance. Must be specific to the business/industry.",
    },
    "bridge": {
        "required_fields": ["severity", "title", "detail", "action"],
        "max_findings": 6,
        "min_findings": 1,
        "severity_max": {"critical": 2},
        "detail_needs_numbers": True,
        "must_reference_data": True,
        "banned_phrases": [
            "improve your conversion rate", "nurture your leads",
            "leverage", "harness", "elevate", "supercharge", "unlock",
            "in today's competitive landscape",
        ],
        "specificity": "Must reference actual conversion/lead data and connect ad spend to outcomes with specific numbers.",
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


# ---------------------------------------------------------------------------
# QA Agent - reviews all other agents' output before it ships
# ---------------------------------------------------------------------------

CHIEF_PROMPT = """You are Chief, the quality control officer inside GroMore.
Your job: review every finding from the other AI agents and catch anything lazy, generic, or wrong.

Automated tests already catch structural and rule-based problems (banned phrases, missing numbers,
severity flooding, budget violations). Those results are included below as PRE-TEST RESULTS.
Your job is the NUANCED review that code can't do:

FOCUS AREAS (these are YOUR domain - the automated tests don't catch these):

1. CONTRADICTIONS across agents:
   - Scout says "scale this campaign" while Penny says "cut its budget" = FLAG both
   - One agent says performance is great while another says it's failing = FLAG
   - Review the full set of findings for internal consistency

2. UNSUPPORTED CLAIMS - Numbers or claims not backed by the data:
   - Agent claims "CPA is too high" but no CPA data was provided = FLAG
   - Agent claims "competitors are outspending you" with zero competitor spend data = FLAG
   - Any dollar figure or percentage that can't be traced to the input data = FLAG

3. CREATIVE QUALITY (especially Ace's ad copy):
   - Is the copy actually compelling? Would you click on it?
   - Does it differentiate this business from competitors?
   - Is the tone right for the industry?

4. ACTIONABILITY - Is the "action" actually something the client can do?
   - "Improve your landing page" is vague. "Add a phone number above the fold on your service page" is specific.
   - Actions must be concrete, not directional

5. SEVERITY ACCURACY - Is the severity level justified?
   - Critical = immediate revenue impact, needs action today
   - Warning = should address this week
   - Info = worth knowing, low urgency
   - Positive = good news worth celebrating

6. MISSING CONTEXT - Agent ignoring critical brand context:
   - Suggesting services the brand doesn't offer = FLAG
   - Copy that ignores the brand's industry or voice = FLAG

For each finding, assign one of:
- PASS: Good finding. Ship it.
- FLAG: Has an issue Chief caught. Include your note.
- REJECT: Fails quality bar entirely. Remove it.
- DOWNGRADE: Wrong severity. Include the correct level.

OUTPUT FORMAT (strict JSON):
{
  "reviews": [
    {
      "agent_key": "agent who produced this",
      "finding_index": 0,
      "verdict": "pass|flag|reject|downgrade",
      "reason": "why (only needed for flag/reject/downgrade)",
      "corrected_severity": "only for downgrade verdicts",
      "corrected_text": "only for flag - your suggested fix"
    }
  ],
  "contradictions": ["description of any cross-agent contradictions found"],
  "team_notes": "2-3 sentence summary of team output quality",
  "worst_offender": "agent_key of whichever agent had the most issues (or null)",
  "memory": "pattern to watch for next time (or null)"
}

Return ONLY valid JSON. No markdown fences.
Be honest and harsh. Three great findings beat eight mediocre ones."""

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
# QA Tests - structural and rule-based (no LLM cost)
# ---------------------------------------------------------------------------

import re as _re


def _run_structural_tests(agent_results: Dict[str, Any]) -> List[Dict]:
    """
    Validate agent output structure against AGENT_QUALITY_SPECS.
    Returns a list of issues found.
    """
    issues = []

    for agent_key, result in agent_results.items():
        if not result or agent_key.startswith("_"):
            continue
        spec = AGENT_QUALITY_SPECS.get(agent_key)
        if not spec:
            continue

        findings = result.get("findings", [])

        # Check finding count
        if len(findings) > spec.get("max_findings", 99):
            issues.append({
                "agent_key": agent_key,
                "finding_index": -1,
                "test": "too_many_findings",
                "detail": f"{len(findings)} findings exceeds max of {spec['max_findings']}",
                "auto_action": "flag",
            })

        # Check severity distribution
        sev_counts = {}
        for f in findings:
            sev = f.get("severity", "info")
            sev_counts[sev] = sev_counts.get(sev, 0) + 1

        for sev, max_count in spec.get("severity_max", {}).items():
            if sev_counts.get(sev, 0) > max_count:
                issues.append({
                    "agent_key": agent_key,
                    "finding_index": -1,
                    "test": "severity_flood",
                    "detail": f"{sev_counts[sev]} {sev} findings exceeds cap of {max_count}",
                    "auto_action": "downgrade_weakest",
                })

        # Check required fields per finding
        required = spec.get("required_fields", [])
        for i, f in enumerate(findings):
            missing = [fld for fld in required if not f.get(fld)]
            if missing:
                issues.append({
                    "agent_key": agent_key,
                    "finding_index": i,
                    "test": "missing_fields",
                    "detail": f"Missing: {', '.join(missing)}",
                    "auto_action": "flag",
                })

            # Valid severity check
            if f.get("severity") not in ("critical", "warning", "info", "positive"):
                issues.append({
                    "agent_key": agent_key,
                    "finding_index": i,
                    "test": "invalid_severity",
                    "detail": f"Invalid severity: {f.get('severity')}",
                    "auto_action": "flag",
                })

            # Title length
            title = f.get("title", "")
            if len(title) > 80:
                issues.append({
                    "agent_key": agent_key,
                    "finding_index": i,
                    "test": "title_too_long",
                    "detail": f"Title is {len(title)} chars (max 80)",
                    "auto_action": "flag",
                })

    return issues


def _run_rule_tests(agent_results: Dict[str, Any], brand: dict) -> List[Dict]:
    """
    Content-quality checks using rules and regex (no LLM).
    Returns a list of issues found.
    """
    issues = []
    brand_name = (brand.get("display_name") or "").lower()
    services = (brand.get("primary_services") or "").lower()
    area = (brand.get("service_area") or "").lower()
    monthly_budget = brand.get("monthly_budget") or 0

    for agent_key, result in agent_results.items():
        if not result or agent_key.startswith("_"):
            continue
        spec = AGENT_QUALITY_SPECS.get(agent_key)
        if not spec:
            continue

        findings = result.get("findings", [])
        banned = [p.lower() for p in spec.get("banned_phrases", [])]

        for i, f in enumerate(findings):
            text_fields = " ".join([
                f.get("title", ""), f.get("detail", ""), f.get("action", ""),
            ]).lower()

            # Banned phrase scan
            for phrase in banned:
                if phrase in text_fields:
                    issues.append({
                        "agent_key": agent_key,
                        "finding_index": i,
                        "test": "banned_phrase",
                        "detail": f"Contains banned phrase: '{phrase}'",
                        "auto_action": "reject",
                    })
                    break  # one banned phrase is enough to reject

            # AI writing tells - em dashes
            raw_text = f.get("title", "") + " " + f.get("detail", "") + " " + f.get("action", "")
            if "\u2014" in raw_text or "\u2013" in raw_text:
                issues.append({
                    "agent_key": agent_key,
                    "finding_index": i,
                    "test": "ai_tell_emdash",
                    "detail": "Contains em dash or en dash - obvious AI tell",
                    "auto_action": "flag",
                })

            # Numbers check - detail should contain actual data points
            if spec.get("detail_needs_numbers"):
                detail = f.get("detail", "")
                has_number = bool(_re.search(r'\d+\.?\d*', detail))
                if not has_number:
                    issues.append({
                        "agent_key": agent_key,
                        "finding_index": i,
                        "test": "no_numbers",
                        "detail": "Detail has no metrics or numbers - likely vague",
                        "auto_action": "flag",
                    })

            # Brand specificity check (for creative agents)
            if spec.get("brand_specificity"):
                has_brand_ref = False
                if brand_name and brand_name in text_fields:
                    has_brand_ref = True
                elif services:
                    for svc in services.split(","):
                        if svc.strip() and svc.strip() in text_fields:
                            has_brand_ref = True
                            break
                elif area and area in text_fields:
                    has_brand_ref = True
                if not has_brand_ref and brand_name:
                    issues.append({
                        "agent_key": agent_key,
                        "finding_index": i,
                        "test": "not_brand_specific",
                        "detail": "Copy doesn't reference the business name, services, or area",
                        "auto_action": "flag",
                    })

            # Budget constraint check
            if spec.get("budget_constraint") and monthly_budget > 0:
                numbers = _re.findall(r'\$[\d,]+(?:\.\d+)?', f.get("action", "") + " " + f.get("detail", ""))
                for num_str in numbers:
                    try:
                        val = float(num_str.replace("$", "").replace(",", ""))
                        if val > monthly_budget * 1.5:
                            issues.append({
                                "agent_key": agent_key,
                                "finding_index": i,
                                "test": "exceeds_budget",
                                "detail": f"Recommends {num_str} but monthly budget is ${monthly_budget}",
                                "auto_action": "reject",
                            })
                            break
                    except ValueError:
                        pass

        # Check for repetitive sentence structure (AI tell)
        if len(findings) >= 3:
            first_words = []
            for f in findings:
                action = (f.get("action") or "").strip()
                if action:
                    first_words.append(action.split()[0].lower() if action.split() else "")
            if first_words and len(set(first_words)) == 1 and len(first_words) >= 3:
                issues.append({
                    "agent_key": agent_key,
                    "finding_index": -1,
                    "test": "repetitive_structure",
                    "detail": f"All {len(first_words)} actions start with '{first_words[0]}' - AI writing pattern",
                    "auto_action": "flag",
                })

    return issues


# ---------------------------------------------------------------------------
# Warren orchestration prompt - he's the boss, he decides what ships
# ---------------------------------------------------------------------------

WARREN_ORCHESTRATION_PROMPT = """You are W.A.R.R.E.N. (Weighted Analysis for Revenue, Reach, Engagement & Navigation).
You are the BOSS. You are responsible for ALL output that reaches the client. Period.

Chief (your QA officer) has reviewed the team's findings and run quality tests.
Now YOU decide the final fate of every finding.

YOUR DECISION RULES:
1. You own every finding that ships. If bad work reaches the client, it's YOUR failure.
2. Revenue impact comes first. Findings that save or make the client money get priority.
3. Three great findings beat eight mediocre ones. Cut aggressively.
4. Structural/rule test failures are objective. If a finding has a banned phrase or no data, that's a real problem.
5. Chief's LLM review catches nuance - contradictions, vague advice, creative quality. Trust it but verify.
6. REWORK is expensive (another API call per agent). Only use it when the finding has real potential but bad execution.
7. KILL anything generic, vague, or not worth the client's attention.
8. SHIP findings that passed QA or have only cosmetic issues.

DECISIONS (one per finding):
- SHIP: goes to client as-is
- KILL: removed entirely
- REWORK: send back to the agent with your specific feedback

For REWORK, you MUST include a feedback_note telling the agent exactly what to fix.
Group rework decisions by agent - if 3 of an agent's 5 findings need rework, rework the whole agent.

OUTPUT FORMAT (strict JSON):
{
  "decisions": [
    {
      "agent_key": "scout",
      "finding_index": 0,
      "decision": "ship|kill|rework",
      "reason": "brief reason",
      "feedback_note": "only for rework - specific instructions for the agent"
    }
  ],
  "agents_to_retry": ["ace"],
  "overall_grade": "A|B|C|D|F",
  "overall_notes": "1-2 sentence summary of team output quality"
}

Return ONLY valid JSON. No markdown fences.
Be decisive. Every finding you ship has your name on it."""


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


def _safe_float(val, default=0.0):
    """Safely convert a value to float for formatting."""
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


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
    # Calculate actual spend from analysis if available
    actual_spend_parts = []
    if analysis_summary:
        for ch_key, ch_label in [("meta", "Meta"), ("google_ads", "Google Ads")]:
            ch_spend = (analysis_summary.get("kpis", {}).get(ch_key, {}) or {}).get("spend")
            if ch_spend:
                actual_spend_parts.append(f"{ch_label}: ${ch_spend:,.2f}")
    actual_spend_line = f"\n- Actual Spend This Month: {', '.join(actual_spend_parts)}" if actual_spend_parts else ""

    parts.append(f"""BRAND CONTEXT:
- Business: {brand.get('display_name', 'Unknown')}
- Industry: {brand.get('industry', 'Unknown')}
- Services: {brand.get('primary_services', 'N/A')}
- Service Area: {brand.get('service_area', 'N/A')}
- Monthly Budget Target: ${brand.get('monthly_budget', 0)}{actual_spend_line}
- Target CPA: ${brand.get('kpi_target_cpa', 'not set')}
- Target Leads/mo: {brand.get('kpi_target_leads', 'not set')}
- Target ROAS: {brand.get('kpi_target_roas', 'not set')}""")

    # Inject per-agent custom context from brand settings
    agent_context = {}
    try:
        agent_context = json.loads(brand.get("agent_context") or "{}")
    except (json.JSONDecodeError, TypeError):
        pass
    custom_ctx = (agent_context.get(agent_key) or "").strip()
    if custom_ctx:
        parts.append(f"\nOWNER INSTRUCTIONS (from the business owner, follow these):\n{custom_ctx}")

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
                            f"spend=${_safe_float(c.get('spend')):.2f}, clicks={c.get('clicks', 0)}, "
                            f"conversions={c.get('conversions', 0)}, "
                            f"cpa=${_safe_float(c.get('cpa') or c.get('cost_per_result')):.2f}, "
                            f"ctr={_safe_float(c.get('ctr')):.2f}%, "
                            f"budget=${_safe_float(c.get('daily_budget')):.2f}/day"
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
                        f"spend=${_safe_float(st.get('spend')):.2f}, conversions={st.get('conversions', 0)}"
                    )

        # Top ads for Ace
        if agent_key == "ace":
            meta_detail = (analysis_summary or {}).get("meta_detail", {})
            top_ads = meta_detail.get("top_ads", [])
            if top_ads:
                parts.append(f"\nTOP META ADS ({len(top_ads)}):")
                for ad in top_ads[:10]:
                    parts.append(
                        f"  - {ad.get('name', '?')}: spend=${_safe_float(ad.get('spend')):.2f}, "
                        f"clicks={ad.get('clicks', 0)}, ctr={_safe_float(ad.get('ctr')):.2f}%"
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
        # Give Hawk the brand's own GBP data for comparison
        if gbp_ctx and not gbp_ctx.get("error"):
            parts.append(f"""\nYOUR BUSINESS GBP PROFILE:
- Rating: {gbp_ctx.get('rating', 'N/A')} ({gbp_ctx.get('review_count', 0)} reviews)
- Category: {gbp_ctx.get('category', 'N/A')}
- Photos: {gbp_ctx.get('photo_count', 0)}""")
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
                        parts.append(f"    Strengths: {str(res['strengths'])[:200]}")
                    if res.get("weaknesses"):
                        parts.append(f"    Weaknesses: {str(res['weaknesses'])[:200]}")

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
                        f"ctr={_safe_float(q.get('ctr')):.1f}%"
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
                parts.append(f"  - {str(p.get('message', '?'))[:80]}: engagements={p.get('engagements', 0)}")

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
              competitor_intel: list = None, crm_data: dict = None,
              feedback: str = None) -> Optional[Dict]:
    """
    Run a single agent's analysis for a brand.
    If feedback is provided, this is a retry - the agent gets QA notes to fix.
    Returns the parsed findings dict, or None on failure.
    """
    import openai
    import re
    import time as _time

    if agent_key not in AGENT_PROMPTS:
        logger.warning("Unknown agent: %s", agent_key)
        return None

    config = AGENT_CONFIGS[agent_key]
    system_prompt = AGENT_PROMPTS[agent_key]

    if not month:
        month = datetime.now().strftime("%Y-%m")

    # Build agent-specific data payload
    try:
        data_payload = _build_agent_data(
            agent_key, analysis_summary, brand,
            campaigns=campaigns, gbp_ctx=gbp_ctx, gbp_audit=gbp_audit,
            competitor_intel=competitor_intel, crm_data=crm_data,
        )
    except Exception as e:
        logger.exception("Data build failed for %s: %s", agent_key, e)
        db.log_agent_activity(brand_id, agent_key, f"{config['role']} analysis error", f"Data build: {str(e)[:80]}", "completed")
        return None

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

    # If this is a retry, inject QA feedback
    if feedback:
        user_message += f"""

IMPORTANT - THIS IS A RETRY. Your previous output was reviewed and sent back.
W.A.R.R.E.N. (the boss) flagged these issues with your last attempt:

{feedback}

Fix ALL of these issues. Be more specific. Use actual numbers from the data.
Reference the actual business name, services, and area. No generic output."""

    # Log in-progress
    is_retry = " (retry)" if feedback else ""
    db.log_agent_activity(brand_id, agent_key, f"Running {config['role']} analysis{is_retry}", f"Month: {month}", "in_progress")

    model = _pick_model(brand, config["model_purpose"])

    # Retry with backoff for transient errors (rate limits, server errors)
    max_attempts = 2
    for attempt in range(max_attempts):
        try:
            client = openai.OpenAI(api_key=api_key)
            resp = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_message},
                ],
                temperature=0.3,
                **_completion_kwargs(model),
            )
            raw = resp.choices[0].message.content.strip()
            break  # success
        except (openai.RateLimitError, openai.APITimeoutError, openai.APIConnectionError) as e:
            if attempt < max_attempts - 1:
                wait = 3 * (attempt + 1)
                logger.warning("Agent %s transient error (attempt %d), retrying in %ds: %s", agent_key, attempt + 1, wait, e)
                _time.sleep(wait)
                continue
            logger.exception("Agent %s failed after %d attempts: %s", agent_key, max_attempts, e)
            db.log_agent_activity(brand_id, agent_key, f"{config['role']} analysis error", f"API: {str(e)[:80]}", "completed")
            return None
        except Exception as e:
            logger.exception("Agent %s OpenAI call failed: %s", agent_key, e)
            db.log_agent_activity(brand_id, agent_key, f"{config['role']} analysis error", f"API: {str(e)[:80]}", "completed")
            return None

    try:
        # Parse JSON from response
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
            try:
                extra = {k: v for k, v in f.items() if k not in ("severity", "title", "detail", "action")}
                db.save_agent_finding(
                    brand_id=brand_id,
                    agent_key=agent_key,
                    month=month,
                    severity=f.get("severity", "info"),
                    title=str(f.get("title", ""))[:200],
                    detail=str(f.get("detail", "")),
                    action=str(f.get("action", "")),
                    extra_json=json.dumps(extra, default=str),
                )
            except Exception as save_err:
                logger.warning("Failed to save finding for %s: %s", agent_key, save_err)

        # Save agent memory if provided
        memory_note = result.get("memory")
        if memory_note and db and brand_id:
            try:
                from webapp.ai_assistant import save_memory_with_embedding
                save_memory_with_embedding(
                    db, brand_id, "insight",
                    f"{config['name']}: {str(memory_note)[:60]}",
                    str(memory_note), api_key,
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
        logger.exception("Agent %s post-processing failed: %s", agent_key, e)
        db.log_agent_activity(brand_id, agent_key, f"{config['role']} analysis error", f"Parse: {str(e)[:80]}", "completed")
        return None


_VALID_MODELS = {
    "gpt-4o-mini", "gpt-4o", "gpt-4-turbo", "gpt-4.1-mini", "gpt-4.1", "o3-mini", "o4-mini",
}

# Quality tier → model mapping
_TIER_MODELS = {
    "efficient": {"analysis": "gpt-4o-mini", "ads": "gpt-4o-mini", "chat": "gpt-4o-mini", "images": "gpt-4o-mini"},
    "balanced":  {"analysis": "gpt-4o-mini", "ads": "gpt-4o",      "chat": "gpt-4o-mini", "images": "gpt-4o"},
    "premium":   {"analysis": "gpt-4.1",     "ads": "gpt-4.1",     "chat": "gpt-4.1",     "images": "gpt-4o"},
}


def _pick_model(brand: dict, purpose: str) -> str:
    """Pick AI model. Checks quality tier first, then per-purpose field, then default."""
    # Check quality tier
    tier = (brand.get("ai_quality_tier") or "").strip().lower()
    if tier in _TIER_MODELS:
        return _TIER_MODELS[tier].get(purpose, "gpt-4o-mini")

    # Fall back to per-purpose brand setting
    purpose_key = f"openai_model_{purpose}"
    model = (brand.get(purpose_key) or "").strip()
    if model and model in _VALID_MODELS:
        return model

    model = (brand.get("openai_model") or "").strip()
    if model and model in _VALID_MODELS:
        return model

    return "gpt-4o-mini"


def _completion_kwargs(model: str, limit: int = 2000) -> dict:
    """Return the correct token-limit kwarg for the model.

    Reasoning models (o-series) and gpt-4.1+ require max_completion_tokens;
    older chat models use max_tokens.
    """
    if model.startswith("o") or "4.1" in model:
        return {"max_completion_tokens": limit}
    return {"max_tokens": limit}


# ---------------------------------------------------------------------------
# Multi-stage QA pipeline: structural tests -> rule tests -> Chief LLM -> Warren
# ---------------------------------------------------------------------------

def run_qa_review(db, brand: dict, brand_id: int, api_key: str,
                  agent_results: Dict[str, Any], month: str = None) -> Dict[str, Any]:
    """
    Multi-stage QA review:
      1. Structural tests (code) - validate output schema
      2. Rule-based tests (code) - banned phrases, numbers, specificity
      3. Chief LLM review - nuanced quality (contradictions, creative quality, actionability)
    Returns compiled QA report for Warren to review.
    """
    import openai

    if not month:
        month = datetime.now().strftime("%Y-%m")

    # Stage 1: Structural tests
    structural_issues = _run_structural_tests(agent_results)
    logger.info("QA Stage 1 (structural): %d issues found", len(structural_issues))

    # Stage 2: Rule-based tests
    rule_issues = _run_rule_tests(agent_results, brand)
    logger.info("QA Stage 2 (rules): %d issues found", len(rule_issues))

    # Combine pre-test results
    all_pre_issues = structural_issues + rule_issues

    # Build the review payload for Chief
    review_items = []
    for agent_key, result in agent_results.items():
        if not result or not result.get("findings") or agent_key.startswith("_"):
            continue
        for i, finding in enumerate(result["findings"]):
            review_items.append({
                "agent_key": agent_key,
                "agent_name": AGENT_CONFIGS.get(agent_key, {}).get("name", agent_key),
                "finding_index": i,
                "severity": finding.get("severity", "info"),
                "title": finding.get("title", ""),
                "detail": finding.get("detail", ""),
                "action": finding.get("action", ""),
            })

    if not review_items:
        logger.info("QA: No findings to review for brand %s", brand_id)
        db.log_agent_activity(brand_id, "chief", "QA review skipped", "No findings from team", "completed")
        return {
            "pre_test_issues": all_pre_issues,
            "chief_reviews": [],
            "team_notes": "No findings to review.",
            "worst_offender": None,
        }

    # Stage 3: Chief LLM review
    brand_context = (
        f"Business: {brand.get('display_name', 'Unknown')}, "
        f"Industry: {brand.get('industry', 'Unknown')}, "
        f"Services: {brand.get('primary_services', 'N/A')}, "
        f"Area: {brand.get('service_area', 'N/A')}, "
        f"Budget: ${brand.get('monthly_budget', 0)}/mo"
    )

    # Format pre-test results for Chief
    pre_test_summary = "None - all findings passed structural and rule tests."
    if all_pre_issues:
        pre_test_lines = []
        for issue in all_pre_issues:
            idx_str = f"finding #{issue['finding_index']}" if issue['finding_index'] >= 0 else "overall"
            pre_test_lines.append(
                f"  - {issue['agent_key']} {idx_str}: [{issue['test']}] {issue['detail']} (auto: {issue['auto_action']})"
            )
        pre_test_summary = "\n".join(pre_test_lines)

    user_message = f"""Review the following {len(review_items)} findings from our agent team for {brand.get('display_name', 'this business')}.

BRAND CONTEXT: {brand_context}

PRE-TEST RESULTS (automated structural and rule checks):
{pre_test_summary}

FINDINGS TO REVIEW:
{json.dumps(review_items, indent=2)}

Focus on nuance that automated tests miss: contradictions, unsupported claims, creative quality, actionability.
Don't re-flag things the pre-tests already caught unless you have additional context."""

    db.log_agent_activity(
        brand_id, "chief", "Running multi-stage QA",
        f"{len(review_items)} findings, {len(all_pre_issues)} pre-test issues",
        "in_progress",
    )

    chief_reviews = []
    chief_notes = ""
    worst_offender = None
    chief_memory = None

    try:
        client = openai.OpenAI(api_key=api_key)
        chief_model = _pick_model(brand, "analysis")
        resp = client.chat.completions.create(
            model=chief_model,
            messages=[
                {"role": "system", "content": CHIEF_PROMPT},
                {"role": "user", "content": user_message},
            ],
            temperature=0.2,
            **_completion_kwargs(chief_model),
        )
        raw = resp.choices[0].message.content.strip()

        # Parse JSON
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[-1]
            if raw.endswith("```"):
                raw = raw[:-3]
            raw = raw.strip()

        json_match = _re.search(r'\{.*\}', raw, _re.DOTALL)
        if json_match:
            qa_result = json.loads(json_match.group())
            chief_reviews = qa_result.get("reviews", [])
            chief_notes = qa_result.get("team_notes", "")
            worst_offender = qa_result.get("worst_offender")
            chief_memory = qa_result.get("memory")
        else:
            logger.warning("Chief returned non-JSON: %s", raw[:200])

    except Exception as e:
        logger.exception("Chief LLM review failed: %s", e)

    db.log_agent_activity(
        brand_id, "chief",
        f"QA complete: {len(all_pre_issues)} pre-test issues, {len(chief_reviews)} LLM reviews",
        chief_notes[:100] if chief_notes else "Review done",
        "completed",
    )

    # Save Chief's memory
    if chief_memory:
        try:
            from webapp.ai_assistant import save_memory_with_embedding
            save_memory_with_embedding(
                db, brand_id, "insight",
                f"Chief QA: {chief_memory[:60]}",
                chief_memory, api_key,
            )
        except Exception:
            pass

    return {
        "pre_test_issues": all_pre_issues,
        "chief_reviews": chief_reviews,
        "team_notes": chief_notes,
        "worst_offender": worst_offender,
    }


# ---------------------------------------------------------------------------
# Warren orchestration - the boss reviews Chief's report and decides
# ---------------------------------------------------------------------------

def warren_orchestrate(db, brand: dict, brand_id: int, api_key: str,
                       agent_results: Dict[str, Any], qa_report: Dict[str, Any],
                       month: str = None) -> Dict[str, Any]:
    """
    Warren reviews Chief's QA report and makes final decisions on every finding.
    Returns dict with decisions, agents_to_retry, and the results of applying those decisions.
    """
    import openai

    if not month:
        month = datetime.now().strftime("%Y-%m")

    pre_issues = qa_report.get("pre_test_issues", [])
    chief_reviews = qa_report.get("chief_reviews", [])

    # Build the briefing for Warren
    finding_list = []
    for agent_key, result in agent_results.items():
        if not result or not result.get("findings") or agent_key.startswith("_"):
            continue
        for i, finding in enumerate(result["findings"]):
            entry = {
                "agent_key": agent_key,
                "finding_index": i,
                "severity": finding.get("severity", "info"),
                "title": finding.get("title", ""),
                "detail": finding.get("detail", "")[:200],
                "action": finding.get("action", "")[:200],
            }

            # Attach pre-test issues for this finding
            related_pre = [
                iss for iss in pre_issues
                if iss["agent_key"] == agent_key and iss["finding_index"] in (i, -1)
            ]
            if related_pre:
                entry["pre_test_flags"] = [
                    f"[{iss['test']}] {iss['detail']} (auto: {iss['auto_action']})"
                    for iss in related_pre
                ]

            # Attach Chief's review for this finding
            related_chief = [
                r for r in chief_reviews
                if r.get("agent_key") == agent_key and r.get("finding_index") == i
            ]
            if related_chief:
                cr = related_chief[0]
                entry["chief_verdict"] = cr.get("verdict", "pass")
                if cr.get("reason"):
                    entry["chief_reason"] = cr["reason"]

            finding_list.append(entry)

    if not finding_list:
        return {
            "decisions": [],
            "agents_to_retry": [],
            "overall_grade": "N/A",
            "overall_notes": "No findings to review.",
            "applied": {"shipped": 0, "killed": 0, "rework": 0},
        }

    brand_context = (
        f"Business: {brand.get('display_name', 'Unknown')}, "
        f"Industry: {brand.get('industry', 'Unknown')}, "
        f"Budget: ${brand.get('monthly_budget', 0)}/mo"
    )

    user_message = f"""You have {len(finding_list)} findings from the team for {brand.get('display_name', 'this business')}.

BRAND: {brand_context}

Chief's overall notes: {qa_report.get('team_notes', 'None')}
Pre-test issues: {len(pre_issues)} found
Chief worst offender: {qa_report.get('worst_offender', 'None')}

FINDINGS WITH QA ANNOTATIONS:
{json.dumps(finding_list, indent=2)}

Review each finding. Make the call: SHIP, KILL, or REWORK.
Remember - you own everything that ships. Be decisive."""

    db.log_agent_activity(
        brand_id, "warren", "Reviewing team output",
        f"{len(finding_list)} findings to judge",
        "in_progress",
    )

    try:
        client = openai.OpenAI(api_key=api_key)
        warren_model = _pick_model(brand, "analysis")
        resp = client.chat.completions.create(
            model=warren_model,
            messages=[
                {"role": "system", "content": WARREN_ORCHESTRATION_PROMPT},
                {"role": "user", "content": user_message},
            ],
            temperature=0.2,
            **_completion_kwargs(warren_model),
        )
        raw = resp.choices[0].message.content.strip()

        if raw.startswith("```"):
            raw = raw.split("\n", 1)[-1]
            if raw.endswith("```"):
                raw = raw[:-3]
            raw = raw.strip()

        json_match = _re.search(r'\{.*\}', raw, _re.DOTALL)
        if not json_match:
            logger.warning("Warren returned non-JSON: %s", raw[:200])
            db.log_agent_activity(brand_id, "warren", "Review failed", "Non-JSON response", "completed")
            # Fallback: ship everything that Chief passed, kill what Chief rejected
            return _fallback_decisions(db, brand_id, agent_results, qa_report, month)

        warren_result = json.loads(json_match.group())

    except Exception as e:
        logger.exception("Warren orchestration failed: %s", e)
        db.log_agent_activity(brand_id, "warren", "Review error", str(e)[:100], "completed")
        return _fallback_decisions(db, brand_id, agent_results, qa_report, month)

    # Apply Warren's decisions to the database
    decisions = warren_result.get("decisions", [])
    agents_to_retry = warren_result.get("agents_to_retry", [])
    overall_grade = warren_result.get("overall_grade", "?")
    overall_notes = warren_result.get("overall_notes", "")

    shipped = 0
    killed = 0
    rework_count = 0
    rework_feedback = {}  # agent_key -> list of feedback notes

    db_findings = db.get_agent_findings(brand_id, month=month, limit=200)

    for dec in decisions:
        d_agent = dec.get("agent_key", "")
        d_index = dec.get("finding_index", -1)
        decision = dec.get("decision", "ship")

        agent_findings = [f for f in db_findings if f.get("agent_key") == d_agent]
        if d_index < 0 or d_index >= len(agent_findings):
            continue
        db_finding = agent_findings[d_index]
        finding_id = db_finding.get("id")
        if not finding_id:
            continue

        if decision == "kill":
            db.dismiss_agent_finding(finding_id, brand_id)
            killed += 1

        elif decision == "rework":
            # Dismiss the current finding - it will be replaced on retry
            db.dismiss_agent_finding(finding_id, brand_id)
            rework_count += 1
            note = dec.get("feedback_note", dec.get("reason", "Improve quality and specificity"))
            rework_feedback.setdefault(d_agent, []).append(note)

        else:  # ship
            # Add Warren's approval stamp
            reason = dec.get("reason", "")
            if reason:
                try:
                    conn = db._conn()
                    conn.execute(
                        """UPDATE agent_findings
                           SET extra_json = json_set(
                               COALESCE(extra_json, '{}'), '$.warren_note', ?
                           )
                           WHERE id = ? AND brand_id = ?""",
                        (f"Approved: {reason}", finding_id, brand_id),
                    )
                    conn.commit()
                except Exception:
                    pass
            shipped += 1

    summary = f"Grade: {overall_grade} | {shipped} shipped, {killed} killed, {rework_count} rework"
    db.log_agent_activity(
        brand_id, "warren",
        f"Team review complete - {overall_grade}",
        summary,
        "completed",
    )

    return {
        "decisions": decisions,
        "agents_to_retry": agents_to_retry,
        "rework_feedback": rework_feedback,
        "overall_grade": overall_grade,
        "overall_notes": overall_notes,
        "applied": {"shipped": shipped, "killed": killed, "rework": rework_count},
    }


def _fallback_decisions(db, brand_id: int, agent_results: Dict, qa_report: Dict,
                        month: str) -> Dict[str, Any]:
    """
    Fallback if Warren's LLM call fails: use Chief's verdicts and pre-test auto-actions.
    Ships passes, kills rejects and banned-phrase hits, flags everything else.
    """
    pre_issues = qa_report.get("pre_test_issues", [])
    chief_reviews = qa_report.get("chief_reviews", [])

    db_findings = db.get_agent_findings(brand_id, month=month, limit=200)
    shipped = 0
    killed = 0

    # Build a lookup of issues by (agent_key, finding_index)
    issue_map = {}
    for iss in pre_issues:
        key = (iss["agent_key"], iss["finding_index"])
        issue_map.setdefault(key, []).append(iss)

    chief_map = {}
    for r in chief_reviews:
        key = (r.get("agent_key", ""), r.get("finding_index", -1))
        chief_map[key] = r

    for agent_key, result in agent_results.items():
        if not result or not result.get("findings") or agent_key.startswith("_"):
            continue
        agent_db = [f for f in db_findings if f.get("agent_key") == agent_key]

        for i, finding in enumerate(result["findings"]):
            if i >= len(agent_db):
                break
            finding_id = agent_db[i].get("id")
            if not finding_id:
                continue

            # Check for auto-reject from pre-tests
            related = issue_map.get((agent_key, i), []) + issue_map.get((agent_key, -1), [])
            auto_reject = any(iss["auto_action"] == "reject" for iss in related)

            # Check Chief's verdict
            chief_r = chief_map.get((agent_key, i), {})
            chief_reject = chief_r.get("verdict") == "reject"

            if auto_reject or chief_reject:
                db.dismiss_agent_finding(finding_id, brand_id)
                killed += 1
            else:
                shipped += 1

    return {
        "decisions": [],
        "agents_to_retry": [],
        "rework_feedback": {},
        "overall_grade": "?",
        "overall_notes": "Warren review failed - used fallback (Chief + pre-test auto-actions).",
        "applied": {"shipped": shipped, "killed": killed, "rework": 0},
    }


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
        else:
            logger.warning("Analysis build returned empty for brand %s month %s", brand_id, month)
    except Exception as e:
        logger.warning("Analysis build failed for brand %s: %s", brand_id, e)
        db.log_agent_activity(brand_id, "system", f"Analysis build error: {e}", "", "completed")

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

    # ── QA Pipeline: Chief multi-test → Warren orchestration → retry loop ──
    agents_with_findings = {k: v for k, v in results.items() if v and v.get("findings")}
    if agents_with_findings:
        try:
            # Step 1: Chief runs multi-stage QA (structural + rules + LLM)
            qa_report = run_qa_review(
                db, brand, brand_id, api_key,
                agents_with_findings, month=month,
            )

            # Step 2: Warren reviews Chief's report and makes final decisions
            warren_result = warren_orchestrate(
                db, brand, brand_id, api_key,
                agents_with_findings, qa_report, month=month,
            )

            # Step 3: Retry loop - re-run agents Warren flagged for rework (max 1 retry)
            rework_feedback = warren_result.get("rework_feedback", {})
            retry_results = {}

            for retry_agent, feedback_notes in rework_feedback.items():
                if retry_agent not in agent_eligibility or not agent_eligibility.get(retry_agent):
                    continue
                combined_feedback = "\n".join(f"- {note}" for note in feedback_notes)
                logger.info("Retrying agent %s with Warren's feedback", retry_agent)

                # Clear the old (now-dismissed) findings and run again with feedback
                db.clear_agent_findings(brand_id, month, agent_key=retry_agent)
                try:
                    retry_result = run_agent(
                        retry_agent, db=db, brand=brand, brand_id=brand_id,
                        api_key=api_key, month=month,
                        analysis_summary=analysis_summary, campaigns=campaigns,
                        gbp_ctx=gbp_ctx, gbp_audit=gbp_audit_result,
                        competitor_intel=competitor_intel, crm_data=crm_data,
                        feedback=combined_feedback,
                    )
                    if retry_result and retry_result.get("findings"):
                        retry_results[retry_agent] = retry_result
                        results[retry_agent] = retry_result  # update main results
                except Exception as e:
                    logger.exception("Retry of %s failed: %s", retry_agent, e)

            # Step 4: Quick QA pass on retried output (structural + rules only, no LLM)
            if retry_results:
                retry_structural = _run_structural_tests(retry_results)
                retry_rules = _run_rule_tests(retry_results, brand)
                retry_issues = retry_structural + retry_rules

                # Auto-reject findings that still fail after retry
                db_findings_post = db.get_agent_findings(brand_id, month=month, limit=200)
                for issue in retry_issues:
                    if issue["auto_action"] == "reject":
                        agent_db = [f for f in db_findings_post if f.get("agent_key") == issue["agent_key"]]
                        idx = issue["finding_index"]
                        if 0 <= idx < len(agent_db):
                            fid = agent_db[idx].get("id")
                            if fid:
                                db.dismiss_agent_finding(fid, brand_id)

                logger.info("Post-retry QA: %d issues on retried output", len(retry_issues))

            results["_qa"] = {
                "qa_report": qa_report,
                "warren": warren_result,
                "retried_agents": list(rework_feedback.keys()),
            }

        except Exception as e:
            logger.exception("QA pipeline crashed: %s", e)
            results["_qa"] = {
                "overall_grade": "?",
                "overall_notes": f"QA pipeline failed: {str(e)[:80]}",
            }

    return results
