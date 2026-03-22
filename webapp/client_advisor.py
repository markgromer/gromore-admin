"""
Client Action Advisor

Takes analysis + suggestions from the analytics pipeline and produces:
1. Plain-English metric explanations ("Your click-through rate is 3.2% - this means...")
2. AI-generated action deliverables based on real account data
3. Priority-ranked action cards (max 2 per priority level)

Action cards are powered by AI that reads the actual campaign data, keyword
performance, competitor signals, and account metrics to produce specific,
ready-to-implement deliverables rather than generic how-to instructions.
"""
import json
import logging
import os

import requests as _requests

log = logging.getLogger(__name__)

# ── Mission metadata for gamified action plan ──

_CATEGORY_META = {
    "paid_advertising": {"icon": "bi-megaphone-fill", "color": "#6366f1", "skill": "Ad Optimization",
                         "platform_url": "https://ads.google.com", "platform_label": "Open Google Ads"},
    "seo":              {"icon": "bi-search",         "color": "#059669", "skill": "Search Visibility",
                         "platform_url": "https://search.google.com/search-console", "platform_label": "Open Search Console"},
    "website":          {"icon": "bi-globe2",         "color": "#2563eb", "skill": "Website Performance",
                         "platform_url": "https://analytics.google.com", "platform_label": "Open Google Analytics"},
    "strategy":         {"icon": "bi-compass-fill",   "color": "#7c3aed", "skill": "Growth Strategy",
                         "platform_url": "", "platform_label": ""},
    "creative":         {"icon": "bi-palette-fill",   "color": "#db2777", "skill": "Creative Impact",
                         "platform_url": "https://business.facebook.com/adsmanager", "platform_label": "Open Ads Manager"},
    "budget":           {"icon": "bi-piggy-bank-fill","color": "#d97706", "skill": "Budget Strategy",
                         "platform_url": "https://ads.google.com", "platform_label": "Open Google Ads"},
    "organic_social":   {"icon": "bi-people-fill",    "color": "#0891b2", "skill": "Social Engagement",
                         "platform_url": "https://business.facebook.com", "platform_label": "Open Meta Business Suite"},
}

MONTH_LEVELS = [
    (0,    1, "Rookie",          "Just getting started"),
    (200,  2, "Apprentice",      "Finding your stride"),
    (400,  3, "Strategist",      "Thinking like a marketer"),
    (700,  4, "Optimizer",       "Squeezing more from every dollar"),
    (1000, 5, "Growth Hacker",   "Your competitors should worry"),
    (1400, 6, "Marketing Pro",   "Running a tight ship"),
    (1800, 7, "Marketing Legend", "Nothing gets past you"),
]


def _parse_difficulty(time_str):
    """Return 1-3 star difficulty from a time estimate string."""
    if not time_str:
        return 2
    t = time_str.lower()
    if any(w in t for w in ("1 hour", "2 hour", "1-2", "3 hour")):
        return 3
    if any(w in t for w in ("30", "45")):
        return 2
    return 1


def build_client_dashboard(analysis, suggestions, brand, ai_model=None, include_deep_analysis=False):
    """
    Build the full client dashboard payload from raw analysis + suggestions.

    Returns dict with:
        - health: overall grade + score
        - channels: per-channel metric cards with explanations
        - actions: prioritized action cards with step-by-step instructions
        - kpi_status: target vs actual KPIs
    """
    channels = {}

    ga = analysis.get("google_analytics")
    if ga:
        channels["website"] = _explain_website(ga)

    meta = analysis.get("meta_business")
    if meta:
        channels["facebook_ads"] = _explain_meta(meta)

    fb_organic = analysis.get("facebook_organic")
    if fb_organic:
        channels["facebook_organic"] = _explain_facebook_organic(fb_organic)

    google_ads = analysis.get("google_ads")
    if google_ads:
        channels["google_ads"] = _explain_google_ads(google_ads)

    gsc = analysis.get("search_console")
    if gsc:
        channels["seo"] = _explain_seo(gsc)

    actions = _build_action_cards(analysis, suggestions, brand, ai_model=ai_model)
    kpi_status = _explain_kpis(analysis)

    overall_score = analysis.get("overall_score")
    overall_grade = analysis.get("overall_grade", "N/A")

    result = {
        "health": {
            "grade": overall_grade,
            "score": overall_score,
            "label": _grade_label(overall_grade),
        },
        "channels": channels,
        "actions": actions,
        "kpi_status": kpi_status,
        "highlights": analysis.get("highlights", []),
        "concerns": analysis.get("concerns", []),
    }

    if include_deep_analysis:
        result["ai_analysis"] = _generate_ai_analysis_brief(
            analysis,
            suggestions,
            brand,
            ai_model=ai_model,
        )

    return result


# ── Metric Explanations ──

def _explain_website(ga):
    metrics = ga.get("metrics", {})
    scores = ga.get("scores", {})
    mom = ga.get("month_over_month", {})

    cards = []

    sessions = metrics.get("sessions", 0)
    sessions_mom = mom.get("sessions", {})
    trend = _trend_text(sessions_mom)
    cards.append({
        "metric": "Website Visitors",
        "value": f"{sessions:,}",
        "status": _score_to_status(scores.get("sessions", "no_data")),
        "explanation": (
            f"Your website had {sessions:,} visits this month{trend}. "
            "This counts every time someone landed on your site from any source: "
            "Google searches, ads, social media, or typing your URL directly."
        ),
    })

    bounce = metrics.get("bounce_rate", 0)
    cards.append({
        "metric": "Bounce Rate",
        "value": f"{bounce}%",
        "status": _score_to_status(scores.get("bounce_rate", "no_data")),
        "explanation": (
            f"{bounce}% of visitors left your site without clicking anything else. "
            + ("This is higher than it should be. Your landing pages may be loading slowly, "
               "or visitors aren't finding what they need right away."
               if bounce > 50
               else "This is within a healthy range - most visitors are exploring your site.")
        ),
    })

    duration = metrics.get("avg_session_duration", 0)
    cards.append({
        "metric": "Time on Site",
        "value": f"{int(duration)}s",
        "status": _score_to_status(scores.get("avg_session_duration", "no_data")),
        "explanation": (
            f"Visitors spend an average of {int(duration)} seconds on your site. "
            + ("That's less than 2 minutes, which usually means people aren't finding "
               "enough reason to stick around. Adding photos of your work, reviews, "
               "and detailed service info helps keep visitors engaged."
               if duration < 120
               else "That's a solid amount of time, meaning visitors are reading your "
                    "content and exploring your services.")
        ),
    })

    conv_rate = metrics.get("conversion_rate", 0)
    conversions = metrics.get("conversions", 0)
    if conv_rate > 0 or conversions > 0:
        cards.append({
            "metric": "Website Conversions",
            "value": f"{conversions:,} ({conv_rate}%)",
            "status": _score_to_status(scores.get("conversion_rate", "no_data")),
            "explanation": (
                f"Out of every 100 visitors, about {conv_rate} filled out a form, "
                f"called, or took action. You got {conversions:,} total conversions this month. "
                + ("This rate could be higher. Make sure your phone number is clickable, "
                   "your contact form is short (name, phone, service needed), and you have "
                   "clear 'Get a Quote' buttons on every page."
                   if conv_rate < 5
                   else "This is a strong conversion rate. Your site is doing a good job "
                        "turning visitors into leads.")
            ),
        })

    return {"title": "Your Website", "icon": "bi-globe", "cards": cards}


def _explain_meta(meta):
    metrics = meta.get("metrics", {})
    scores = meta.get("scores", {})

    cards = []

    spend = metrics.get("spend", 0)
    results = metrics.get("results", 0)
    cpr = round(spend / results, 2) if results > 0 else 0

    cards.append({
        "metric": "Ad Spend",
        "value": f"${spend:,.2f}",
        "status": "neutral",
        "explanation": (
            f"You spent ${spend:,.2f} on Facebook/Instagram ads this month"
            + (f" and got {results:,} leads, costing ${cpr:.2f} each."
               if results > 0
               else ". No leads were tracked yet - make sure your conversion tracking is set up.")
        ),
    })

    ctr = metrics.get("ctr", 0)
    cards.append({
        "metric": "Click Rate",
        "value": f"{ctr}%",
        "status": _score_to_status(scores.get("ctr", "no_data")),
        "explanation": (
            f"Out of everyone who saw your ads, {ctr}% clicked on them. "
            + ("This is lower than ideal. Your ad images or text may not be grabbing attention. "
               "Try using real photos of your work instead of stock images, and make your "
               "headline about what the customer gets, not what you do."
               if ctr < 1.0
               else "This is a healthy click rate - your ads are resonating with your audience.")
        ),
    })

    cpc = metrics.get("cpc", 0)
    cards.append({
        "metric": "Cost Per Click",
        "value": f"${cpc:.2f}",
        "status": _score_to_status(scores.get("cpc", "no_data")),
        "explanation": (
            f"Each click on your ad costs ${cpc:.2f}. "
            + ("This is on the higher side. You can bring it down by improving your ad quality "
               "score (better images, more relevant text) or adjusting your audience targeting."
               if cpc > 2.5
               else "This is a reasonable cost per click for your industry.")
        ),
    })

    frequency = metrics.get("frequency", 0)
    if frequency > 0:
        cards.append({
            "metric": "Ad Frequency",
            "value": f"{frequency:.1f}x",
            "status": "warning" if frequency > 3.5 else "good",
            "explanation": (
                f"On average, each person in your audience saw your ads {frequency:.1f} times. "
                + ("This is very high - people are seeing the same ads too many times and "
                   "starting to ignore them. You need fresh ad creative (new images, new text) "
                   "or a larger audience."
                   if frequency > 4
                   else "This is within a healthy range."
                   if frequency <= 3
                   else "Getting close to ad fatigue territory. Consider refreshing your creative soon.")
            ),
        })

    return {"title": "Facebook & Instagram Ads", "icon": "bi-meta", "cards": cards}


def _explain_facebook_organic(fb_organic):
    metrics = fb_organic.get("metrics") or {}
    top_posts = fb_organic.get("top_posts") or []
    post_count = fb_organic.get("post_count", 0)

    cards = []

    followers = metrics.get("followers") or 0
    fans = metrics.get("fans") or 0
    new_fans = metrics.get("new_fans") or 0
    net_fans = metrics.get("net_fans") or 0

    cards.append({
        "metric": "Page Followers",
        "value": f"{followers:,}",
        "status": "good" if net_fans > 0 else ("warning" if net_fans < 0 else "neutral"),
        "explanation": (
            f"Your Facebook page has {followers:,} followers."
            + (f" You gained {net_fans:,} net new followers this month - your audience is growing!"
               if net_fans > 0
               else f" You lost {abs(net_fans):,} followers this month. Review your content mix to keep people engaged."
               if net_fans < 0
               else "")
        ),
    })

    organic_impressions = metrics.get("organic_impressions") or 0
    if organic_impressions > 0 or followers > 0:
        reach_pct = round((organic_impressions / followers) * 100, 1) if followers > 0 else 0
        cards.append({
            "metric": "Organic Reach",
            "value": f"{organic_impressions:,}",
            "status": "good" if reach_pct > 30 else ("warning" if reach_pct < 15 else "neutral"),
            "explanation": (
                f"Your posts were seen {organic_impressions:,} times organically (without paying). "
                + (f"That's {reach_pct}% of your followers."
                   if followers > 0 else "")
                + (" Great reach - your content is getting shared and picked up by the algorithm."
                   if reach_pct > 30
                   else " Try posting more engaging content (questions, before/after photos, videos) to boost this."
                   if reach_pct < 15 and followers > 0
                   else "")
            ),
        })

    post_engagements = metrics.get("post_engagements") or 0
    engagement_rate = metrics.get("engagement_rate") or 0
    cards.append({
        "metric": "Engagement",
        "value": f"{post_engagements:,}",
        "status": "good" if engagement_rate > 2 else ("warning" if engagement_rate < 1 else "neutral"),
        "explanation": (
            f"Your posts received {post_engagements:,} total engagements (likes, comments, shares). "
            + (f"That's a {engagement_rate:.1f}% engagement rate. "
               if engagement_rate > 0 else "")
            + ("This is strong for a local business page."
               if engagement_rate > 2
               else "This is below average. Posting more consistently and using photos/videos from your actual work can help."
               if engagement_rate < 1
               else "Solid engagement - keep the content coming.")
        ),
    })

    cards.append({
        "metric": "Posts This Month",
        "value": str(post_count),
        "status": "good" if post_count >= 12 else ("warning" if post_count < 8 else "neutral"),
        "explanation": (
            f"You published {post_count} posts this month. "
            + ("Great consistency! Regular posting keeps your page active in the algorithm."
               if post_count >= 12
               else "Aim for at least 3 posts per week (12+/month). Consistency matters more than perfection."
               if post_count < 8
               else "Decent posting pace. A few more posts per week could help grow your reach.")
        ),
    })

    # Top post highlight
    if top_posts:
        best = top_posts[0]
        best_eng = best.get("engagement_rate", 0)
        best_type = best.get("type", "post")
        best_msg = (best.get("message") or "")[:80]
        if best_eng > 0:
            cards.append({
                "metric": "Top Post",
                "value": f"{best_eng:.1f}% engagement",
                "status": "good" if best_eng > 3 else "neutral",
                "explanation": (
                    f"Your best-performing post was a {best_type}"
                    + (f': "{best_msg}..."' if best_msg else "")
                    + f" with {best_eng:.1f}% engagement. "
                    "Look at what made this one work and create more content like it."
                ),
            })

    page_views = metrics.get("page_views") or 0
    if page_views > 0:
        cards.append({
            "metric": "Page Views",
            "value": f"{page_views:,}",
            "status": "neutral",
            "explanation": (
                f"Your Facebook page was viewed {page_views:,} times. "
                "These are people actively looking at your business page, "
                "so make sure your page info, services, and contact details are up to date."
            ),
        })

    return {"title": "Facebook Organic", "icon": "bi-facebook", "cards": cards}


def _explain_google_ads(google_ads):
    metrics = google_ads.get("metrics", {})
    scores = google_ads.get("scores", {})

    cards = []

    spend = metrics.get("spend", 0)
    conversions = metrics.get("conversions", 0)
    cpa = metrics.get("cpa", 0)

    cards.append({
        "metric": "Ad Spend",
        "value": f"${spend:,.2f}",
        "status": "neutral",
        "explanation": (
            f"You spent ${spend:,.2f} on Google Ads this month"
            + (f" and got {conversions:,} leads at ${cpa:.2f} each."
               if conversions > 0
               else ". No conversions tracked yet - check that your conversion tracking is working.")
        ),
    })

    ctr = metrics.get("ctr", 0)
    cards.append({
        "metric": "Click Rate",
        "value": f"{ctr}%",
        "status": _score_to_status(scores.get("ctr", "no_data")),
        "explanation": (
            f"{ctr}% of people who saw your Google ad clicked on it. "
            + ("This is below average. Your ad copy may not match what people are searching for. "
               "Make sure your headlines include the exact service + city people are looking for."
               if ctr < 4
               else "Good click rate - your ads are relevant to what people are searching.")
        ),
    })

    cpc = metrics.get("cpc", 0)
    cards.append({
        "metric": "Cost Per Click",
        "value": f"${cpc:.2f}",
        "status": _score_to_status(scores.get("cpc", "no_data")),
        "explanation": (
            f"Each click costs ${cpc:.2f}. Google Ads tend to be more expensive per click "
            "than Facebook because people searching on Google have higher intent - they're "
            "actively looking for your service right now."
        ),
    })

    if cpa > 0:
        cards.append({
            "metric": "Cost Per Lead",
            "value": f"${cpa:.2f}",
            "status": _score_to_status(scores.get("cpa", "no_data")),
            "explanation": (
                f"Each lead from Google Ads costs ${cpa:.2f}. "
                + ("This is higher than ideal. Check which keywords are eating budget "
                   "without converting, and pause or adjust them."
                   if cpa > 50
                   else "This is a solid cost per lead for paid search.")
            ),
        })

    campaign_analysis = google_ads.get("campaign_analysis", [])
    underperforming = [c for c in campaign_analysis if c.get("status") == "underperforming"]
    if underperforming:
        names = ", ".join(c.get("name", "Unknown") for c in underperforming[:3])
        cards.append({
            "metric": "Underperforming Campaigns",
            "value": f"{len(underperforming)}",
            "status": "bad",
            "explanation": (
                f"These campaigns are below benchmark: {names}. "
                "They're spending money but not getting enough leads. "
                "Check the action steps below for what to fix."
            ),
        })

    return {"title": "Google Ads", "icon": "bi-google", "cards": cards}


def _explain_seo(gsc):
    metrics = gsc.get("metrics", {})
    scores = gsc.get("scores", {})

    cards = []

    clicks = metrics.get("clicks", 0)
    impressions = metrics.get("impressions", 0)
    avg_pos = metrics.get("avg_position", 0)

    cards.append({
        "metric": "Google Searches Showing You",
        "value": f"{impressions:,}",
        "status": "neutral",
        "explanation": (
            f"Your website appeared in Google search results {impressions:,} times this month. "
            "This is free visibility - the more people see you in search results, "
            "the more potential customers can find you."
        ),
    })

    cards.append({
        "metric": "Clicks from Google",
        "value": f"{clicks:,}",
        "status": _score_to_status(scores.get("clicks", "no_data")),
        "explanation": (
            f"{clicks:,} people clicked through from Google search to your website. "
            + (f"That's a {round(clicks/impressions*100, 1)}% click rate from searches. "
               if impressions > 0 else "")
            + ("More clicks means more free leads without paying for ads."
               if clicks > 0
               else "Focus on improving your Google rankings to start getting free traffic.")
        ),
    })

    if avg_pos > 0:
        page = "page 1" if avg_pos <= 10 else f"page {int((avg_pos - 1) // 10) + 1}"
        cards.append({
            "metric": "Average Search Position",
            "value": f"#{avg_pos:.1f}",
            "status": _score_to_status(scores.get("avg_position", "no_data")),
            "explanation": (
                f"For your top search terms, you rank around position {avg_pos:.1f} ({page}). "
                + ("Most clicks go to the top 3 results. Being on page 2+ means most people "
                   "never see your listing."
                   if avg_pos > 10
                   else "You're showing up on page 1 for your most important searches, which is where you want to be."
                   if avg_pos <= 10
                   else "")
            ),
        })

    opportunities = gsc.get("keyword_opportunities", [])
    if opportunities:
        top = opportunities[:3]
        kw_list = ", ".join(f'"{o["query"]}"' for o in top)
        cards.append({
            "metric": "SEO Opportunities",
            "value": f"{len(opportunities)}",
            "status": "info",
            "explanation": (
                f"Found {len(opportunities)} keywords where you rank close to page 1. "
                f"Top opportunities: {kw_list}. With some targeted work on these pages, "
                "you could start showing up higher and getting more free clicks."
            ),
        })

    return {"title": "SEO (Free Google Traffic)", "icon": "bi-search", "cards": cards}


# ── Action Cards with AI-Generated Deliverables ──

def _build_action_cards(analysis, suggestions, brand, ai_model=None):
    """Convert top suggestions into action cards with AI-generated deliverables.

    Generate up to 10 action items per load (high priority first, then medium,
    then low). The monthly cap of 20 is enforced at the route level by tracking
    completed items in the database, so we generate a healthy pool here.
    """
    high_cards = []
    medium_cards = []
    low_cards = []

    for s in suggestions:
        if s["priority"] == "high" and len(high_cards) < 5:
            high_cards.append(s)
        elif s["priority"] == "medium" and len(medium_cards) < 5:
            medium_cards.append(s)
        elif s["priority"] == "low" and len(low_cards) < 3:
            low_cards.append(s)

    selected = high_cards + medium_cards + low_cards
    if not selected:
        return []

    # Build basic card structure first
    actions = []
    for s in selected:
        cat_key = s.get("category", "")
        cat_meta = _CATEGORY_META.get(
            cat_key,
            {"icon": "bi-star-fill", "color": "#6b7280", "skill": "Marketing",
             "platform_url": "", "platform_label": ""},
        )
        xp = 150 if s["priority"] == "high" else (100 if s["priority"] == "medium" else 75)

        # Detect platform from title when category is ambiguous
        title_lower = s["title"].lower()
        platform_url = cat_meta.get("platform_url", "")
        platform_label = cat_meta.get("platform_label", "")
        if cat_key in ("paid_advertising", "budget", "creative"):
            if any(w in title_lower for w in ("facebook", "meta", "instagram")):
                platform_url = "https://business.facebook.com/adsmanager"
                platform_label = "Open Ads Manager"
            elif any(w in title_lower for w in ("google", "search", "cpc", "ppc")):
                platform_url = "https://ads.google.com"
                platform_label = "Open Google Ads"

        card = {
            "title": _client_friendly_title(s["title"]),
            "priority": "Do This Now" if s["priority"] == "high" else "Worth Doing Soon",
            "priority_class": "danger" if s["priority"] == "high" else "warning",
            "category": _client_friendly_category(s["category"]),
            "what": _plain_english_what(s),
            "steps": [],
            "impact": "",
            "time": "",
            "data_point": s.get("data_point", ""),
            # Mission metadata
            "mission_name": "",
            "why": "",
            "reward": "",
            "icon": cat_meta["icon"],
            "icon_color": cat_meta["color"],
            "skill": cat_meta["skill"],
            "platform_url": platform_url,
            "platform_label": platform_label,
            "xp": xp,
            "difficulty": 0,
        }
        actions.append(card)

    # Generate AI deliverables using actual account data
    ai_actions = _generate_ai_actions(selected, analysis, brand, ai_model=ai_model)
    if ai_actions:
        for i, card in enumerate(actions):
            if i < len(ai_actions):
                ai = ai_actions[i]
                card["steps"] = ai.get("steps", [])
                card["impact"] = ai.get("impact", "")
                card["time"] = ai.get("time", "")
                card["mission_name"] = ai.get("mission_name", "")
                card["why"] = ai.get("why", "")
                card["reward"] = ai.get("reward", "")

    # Fallback: if AI didn't return steps, generate basic ones from the suggestion data
    for i, card in enumerate(actions):
        if not card["steps"] and i < len(selected):
            s = selected[i]
            card["steps"] = _fallback_steps(s)
            if not card["time"]:
                card["time"] = "15-30 minutes"
        # Fallback mission name
        if not card["mission_name"]:
            card["mission_name"] = card["title"]
        # Calculate difficulty from time
        card["difficulty"] = _parse_difficulty(card["time"])

    return actions


def _fallback_steps(suggestion):
    """Build actionable steps from the suggestion when AI generation fails.

    Instead of just splitting the detail into sentences, produce concrete
    steps with platform links so the user knows WHERE to go.
    """
    steps = []
    detail = suggestion.get("detail", "")
    title = suggestion.get("title", "")
    category = suggestion.get("category", "")
    data_point = suggestion.get("data_point", "")

    _platform_urls = {
        "paid_advertising": ("ads.google.com", "Google Ads"),
        "budget": ("ads.google.com", "Google Ads"),
        "creative": ("business.facebook.com/adsmanager", "Ads Manager"),
        "seo": ("search.google.com/search-console", "Google Search Console"),
        "website": ("analytics.google.com", "Google Analytics"),
        "organic_social": ("business.facebook.com", "Meta Business Suite"),
    }

    title_lower = title.lower()

    # Detect correct platform from title keywords
    if any(w in title_lower for w in ("facebook", "meta", "instagram")):
        url, label = "business.facebook.com/adsmanager", "Ads Manager"
    elif any(w in title_lower for w in ("google ads", "cpc", "cost per click", "ppc")):
        url, label = "ads.google.com", "Google Ads"
    elif any(w in title_lower for w in ("seo", "ranking", "search console")):
        url, label = "search.google.com/search-console", "Google Search Console"
    elif "analytic" in title_lower:
        url, label = "analytics.google.com", "Google Analytics"
    else:
        url, label = _platform_urls.get(category, ("", ""))

    if url:
        steps.append(f"Go to {url} and log in to your {label} account.")

    if data_point:
        steps.append(f"Your current number: {data_point}. Look for this in your dashboard to confirm.")

    # Extract actionable info from detail
    if detail:
        sentences = [s.strip() for s in detail.replace(". ", ".\n").split("\n") if s.strip()]
        # Take first sentence that looks actionable (contains a verb-like word)
        for s in sentences[:3]:
            if not s.endswith("."):
                s += "."
            steps.append(s)

    if len(steps) < 2:
        steps.append(f"Take action on: {title}.")

    return steps[:5]


def _generate_ai_actions(suggestions, analysis, brand, ai_model=None):
    """Call AI to generate specific deliverables for each action card.

    Instead of 'go to Google Ads and click...', this produces the actual work:
    real ad headlines to test, real keywords to pause, real negative keywords,
    specific audience changes, actual content recommendations tied to data.

    Returns a list of step-lists (one per suggestion), or empty list on failure.
    """
    api_key = (brand.get("openai_api_key") or "").strip()
    try:
        from flask import current_app
        if not api_key:
            api_key = (current_app.config.get("OPENAI_API_KEY", "") or "").strip()
    except RuntimeError:
        if not api_key:
            api_key = os.environ.get("OPENAI_API_KEY", "").strip()

    if not api_key:
        return []

    model = (
        (ai_model or "").strip()
        or (brand.get("openai_model_analysis") or "").strip()
        or (brand.get("openai_model") or "").strip()
        or "gpt-4o-mini"
    )

    from webapp.ai_assistant import _summarize_analysis_for_ai
    analysis_summary = _summarize_analysis_for_ai(analysis)

    # ── Build per-action-item context with relevant data attached ──
    # Instead of sending raw data in a separate blob, attach the relevant
    # slice of detailed data directly to each action item so the AI can't
    # ignore it.

    _cat_paid = {"paid_advertising", "budget", "creative"}
    _cat_seo = {"seo"}
    _cat_web = {"website"}
    _cat_organic = {"organic_social"}

    google_ads_detail = analysis_summary.get("google_ads_detail", {})
    meta_detail = analysis_summary.get("meta_detail", {})
    seo_detail = analysis_summary.get("seo_detail", {})
    fb_organic_detail = analysis_summary.get("facebook_organic_detail", {})
    kpis = analysis_summary.get("kpis", {})

    action_items = []
    for s in suggestions:
        cat = s.get("category", "")
        item = {
            "title": s["title"],
            "detail": s["detail"],
            "category": cat,
            "data_point": s.get("data_point", ""),
            "relevant_data": {},
        }

        # Attach channel-specific data the AI must reference
        if cat in _cat_paid or "google" in s["title"].lower() or "cpc" in s["title"].lower():
            campaigns = google_ads_detail.get("campaigns") or []
            item["relevant_data"]["google_ads_campaigns"] = campaigns[:10]
            item["relevant_data"]["google_ads_search_terms"] = (
                google_ads_detail.get("search_terms") or []
            )[:30]
            item["relevant_data"]["google_ads_kpis"] = kpis.get("google_ads", {})

        if cat in _cat_paid or "meta" in s["title"].lower() or "facebook" in s["title"].lower():
            item["relevant_data"]["meta_campaigns"] = (
                meta_detail.get("campaigns") or []
            )[:10]
            item["relevant_data"]["meta_top_ads"] = (
                meta_detail.get("top_ads") or []
            )[:10]
            item["relevant_data"]["meta_kpis"] = kpis.get("meta", {})

        if cat in _cat_seo or "seo" in cat or "keyword" in s["title"].lower():
            item["relevant_data"]["seo_top_queries"] = (
                seo_detail.get("top_queries") or []
            )[:15]
            item["relevant_data"]["seo_keyword_opportunities"] = (
                seo_detail.get("keyword_opportunities") or []
            )[:15]
            item["relevant_data"]["seo_top_pages"] = (
                seo_detail.get("top_pages") or []
            )[:10]
            item["relevant_data"]["seo_kpis"] = kpis.get("gsc", {})

        if cat in _cat_web:
            item["relevant_data"]["website_kpis"] = kpis.get("ga", {})

        if cat in _cat_organic or "organic" in s["title"].lower():
            item["relevant_data"]["fb_organic_top_posts"] = (
                fb_organic_detail.get("top_posts") or []
            )[:10]
            item["relevant_data"]["fb_organic_kpis"] = kpis.get("facebook_organic", {})

        # Competitor data is relevant across all categories
        comp = analysis_summary.get("competitor_watch") or {}
        if comp:
            item["relevant_data"]["competitors"] = comp

        # Strip empty relevant_data keys
        item["relevant_data"] = {k: v for k, v in item["relevant_data"].items() if v}

        action_items.append(item)

    client_info = analysis_summary.get("client", {})

    prompt_data = {
        "client": {
            "name": client_info.get("name"),
            "industry": client_info.get("industry"),
            "service_area": client_info.get("service_area"),
            "services": client_info.get("primary_services"),
            "budget": client_info.get("monthly_budget"),
            "goals": client_info.get("goals"),
            "competitors": client_info.get("competitors"),
            "target_audience": client_info.get("target_audience"),
            "active_offers": client_info.get("active_offers"),
        },
        "highlights": analysis_summary.get("highlights", []),
        "concerns": analysis_summary.get("concerns", []),
        "action_items": action_items,
    }

    system = (
        "You are the senior paid-media and SEO strategist inside GroMore. "
        "You have completed a deep-dive analysis of this account. "
        "Now produce MISSIONS the business owner can execute right now.\n\n"

        "These are for a BUSINESS OWNER, not a marketer. They do not know industry terms. "
        "Every step must be like a GPS direction: go here, click this, type this, done.\n\n"

        "OUTPUT FORMAT (JSON only):\n"
        "{\"actions\": [\n"
        "  {\n"
        "    \"mission_name\": \"3-6 word punchy verb phrase\",\n"
        "    \"micro_steps\": [\"step 1\", \"step 2\", ...],\n"
        "    \"why\": \"one sentence: how this is costing them money or losing them leads\",\n"
        "    \"reward\": \"one sentence: what improves when they finish\",\n"
        "    \"impact\": \"one sentence with specific projected numbers\",\n"
        "    \"time\": \"15 minutes\"\n"
        "  }\n"
        "]}\n"
        "One object per action_item, same order as input.\n\n"

        "MISSION NAME RULES:\n"
        "- 3-6 words, starts with a verb. Punchy and specific.\n"
        "- GOOD: \"Kill the $340 Money Drain\", \"Stop Paying for Junk Clicks\", "
        "\"Fix Your Broken Landing Page\", \"Launch a High-Converting Ad\"\n"
        "- BAD: \"Optimize Campaign Performance\", \"Improve Your SEO\", \"Tune Underperforming Campaigns\"\n\n"

        "MICRO-STEP FORMAT - CRITICAL:\n"
        "Each step MUST follow this exact pattern:\n"
        "  [Action verb] at [platform URL or path]. [Exactly what to do]. [One data point why.]\n\n"
        "3-5 steps per mission. Each step = ONE click-level action done in 2-3 minutes.\n\n"

        "STEP LINK RULES (MANDATORY):\n"
        "- Google Ads steps: start with 'Go to ads.google.com >'\n"
        "- Facebook/Meta steps: start with 'Go to business.facebook.com/adsmanager >'\n"
        "- Google Analytics steps: start with 'Go to analytics.google.com >'\n"
        "- Search Console steps: start with 'Go to search.google.com/search-console >'\n"
        "- Google Business Profile steps: start with 'Go to business.google.com >'\n"
        "- Website edits: tell them exactly which page URL to edit and what to change\n"
        "- NEVER write a step without telling them WHERE to go first\n\n"

        "WHAT MAKES A BAD STEP (NEVER do this):\n"
        "- 'Review your campaigns and pause underperformers' (WHICH campaigns?)\n"
        "- 'Add negative keywords to reduce waste' (WHICH keywords?)\n"
        "- 'Reallocate spend toward top converters' (move how much from where to where?)\n"
        "- 'Optimize your landing pages' (WHICH page? change WHAT?)\n"
        "- 'Tighten targeting' (change what setting to what value?)\n"
        "- 'These campaigns are under target' (that's a FACT, not a STEP)\n"
        "- 'Consider testing new ad copy' (WRITE the actual copy for them)\n"
        "- Any sentence that DESCRIBES a problem instead of SOLVING it\n\n"

        "WHAT MAKES A GOOD STEP (ALWAYS do this):\n"
        "- 'Go to ads.google.com > Campaigns > \"SDL Search Campaign\". Click budget. Change daily budget from $50 to $35. This campaign is spending $7.84/click with no leads.'\n"
        "- 'Go to ads.google.com > Tools > Negative Keywords. Add these words: \"DIY\", \"how to\", \"free\", \"salary\". They appeared 89 times and wasted about $120.'\n"
        "- 'Go to business.facebook.com/adsmanager. Click Create. Choose Lead Generation. Set budget to $10/day. Use this headline: \"Same-Day AC Repair - Free Estimates | Licensed & Insured\".'\n"
        "- 'Go to search.google.com/search-console > Performance. Find \"water heater installation Phoenix\". You have 1,200 impressions at position 18. Create a new page on your site targeting that exact phrase.'\n\n"

        "WHY field: One sentence using a specific dollar amount or lead count. "
        "Example: \"You burned $340 last month on clicks that never turned into a phone call.\"\n\n"

        "REWARD field: Concrete result, not vague improvement. "
        "Example: \"$340/month gets redirected to keywords that actually generate calls.\"\n\n"

        "IMPACT: Specific projected numbers. "
        "Example: 'Could save $340/month and generate about 5 more leads at $38 each.'\n\n"

        "TIME: '5 minutes', '10 minutes', '15 minutes', '30 minutes'. Not 'varies'.\n\n"

        "If relevant_data for an action item is empty, still give concrete steps using the detail and data_point, "
        "always including the platform URL. Never use filler like 'consider', 'you might want to', or 'look into'."
    )

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    try:
        resp = _requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json={
                "model": model,
                "temperature": 0.4,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": json.dumps(prompt_data)},
                ],
                "response_format": {"type": "json_object"},
            },
            timeout=60,
        )

        if resp.status_code != 200:
            log.warning("AI action generation failed (%s): %s", resp.status_code, resp.text[:200])
            return []

        data = resp.json()
        content = (
            (data.get("choices") or [{}])[0]
            .get("message", {})
            .get("content", "")
        )

        parsed = json.loads(content)

        # Extract the actions array from the response
        actions_list = parsed
        if isinstance(parsed, dict):
            actions_list = parsed.get("actions", [])
            if not actions_list:
                for v in parsed.values():
                    if isinstance(v, list):
                        actions_list = v
                        break

        if not isinstance(actions_list, list):
            return []

        # Each item should be {"mission_name": "...", "micro_steps": [...], "why": "...", "reward": "...", "impact": "...", "time": "..."}
        result = []
        for item in actions_list:
            if isinstance(item, dict):
                # Accept both "micro_steps" (new) and "steps" (old) keys
                steps = item.get("micro_steps") or item.get("steps") or []
                result.append({
                    "steps": [str(s) for s in steps if s],
                    "impact": str(item.get("impact", "")),
                    "time": str(item.get("time", "")),
                    "mission_name": str(item.get("mission_name", "")),
                    "why": str(item.get("why", "")),
                    "reward": str(item.get("reward", "")),
                })
            elif isinstance(item, list):
                # Fallback: plain list of strings (old format)
                result.append({
                    "steps": [str(s) for s in item if s],
                    "impact": "",
                    "time": "",
                })
            else:
                result.append({"steps": [], "impact": "", "time": ""})

        return result

    except Exception as e:
        log.warning("AI action generation error: %s", e)
        return []


def _generate_ai_analysis_brief(analysis, suggestions, brand, ai_model=None):
    """Generate a deeper cross-source analysis brief for the Action Plan page."""
    api_key = (brand.get("openai_api_key") or "").strip()
    try:
        from flask import current_app
        if not api_key:
            api_key = (current_app.config.get("OPENAI_API_KEY", "") or "").strip()
    except RuntimeError:
        if not api_key:
            api_key = os.environ.get("OPENAI_API_KEY", "").strip()

    if not api_key:
        return ""

    model = (
        (ai_model or "").strip()
        or (brand.get("openai_model_analysis") or "").strip()
        or (brand.get("openai_model") or "").strip()
        or "gpt-4o-mini"
    )

    from webapp.ai_assistant import _summarize_analysis_for_ai
    analysis_summary = _summarize_analysis_for_ai(analysis)

    payload = {
        "analysis": analysis_summary,
        "suggestions": suggestions,
    }

    system = (
        "You are a principal growth operator writing a short executive analysis for a business owner. "
        "Use only data in context. No generic filler, no speculation, no platform blame without proof. "
        "Return concise markdown with sections: Top Risks, Best Opportunities, 30-Day Action Plan, What To Watch Weekly. "
        "Each bullet must include at least one concrete data point (metric, campaign, keyword, query, spend, CPA, CTR, or position)."
    )

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    try:
        resp = _requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json={
                "model": model,
                "temperature": 0.3,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": json.dumps(payload)},
                ],
            },
            timeout=45,
        )
        if resp.status_code != 200:
            log.warning("AI analysis brief failed (%s): %s", resp.status_code, resp.text[:200])
            return ""

        data = resp.json()
        content = (
            (data.get("choices") or [{}])[0]
            .get("message", {})
            .get("content", "")
        )
        return (content or "").strip()
    except Exception as e:
        log.warning("AI analysis brief error: %s", e)
        return ""


def _explain_kpis(analysis):
    """Build plain-English KPI status cards."""
    kpi = analysis.get("kpi_status", {})
    targets = kpi.get("targets", {})
    actual = kpi.get("actual", {})
    evaluation = kpi.get("evaluation", {})

    cards = []

    if targets.get("cpa") and actual.get("blended_cpa"):
        cpa_eval = evaluation.get("cpa", {})
        on_track = cpa_eval.get("on_track", False)
        cards.append({
            "label": "Cost Per Lead",
            "target": f"${targets['cpa']:.2f}",
            "actual": f"${actual['blended_cpa']:.2f}",
            "on_track": on_track,
            "explanation": (
                f"Your target is ${targets['cpa']:.2f} per lead and you're at ${actual['blended_cpa']:.2f}. "
                + ("You're beating your target. Nice work."
                   if on_track
                   else "You're above target. Check which campaigns are driving up costs "
                        "and see the action steps below.")
            ),
        })

    if targets.get("leads") and actual.get("paid_leads"):
        leads_eval = evaluation.get("leads", {})
        on_track = leads_eval.get("on_track", False)
        cards.append({
            "label": "Total Leads",
            "target": f"{int(targets['leads'])}",
            "actual": f"{int(actual['paid_leads'])}",
            "on_track": on_track,
            "explanation": (
                f"Target is {int(targets['leads'])} leads and you got {int(actual['paid_leads'])}. "
                + ("You're hitting your lead target."
                   if on_track
                   else "You're below your lead target. Consider increasing budget on "
                        "your best-performing campaigns or launching new ad variations.")
            ),
        })

    if targets.get("roas") and actual.get("blended_roas"):
        roas_eval = evaluation.get("roas", {})
        on_track = roas_eval.get("on_track", False)
        cards.append({
            "label": "Return on Ad Spend",
            "target": f"{targets['roas']:.1f}x",
            "actual": f"{actual['blended_roas']:.1f}x",
            "on_track": on_track,
            "explanation": (
                f"For every $1 you spend on ads, you're making ${actual['blended_roas']:.2f} back. "
                f"Your target is ${targets['roas']:.1f}x. "
                + ("You're above target - your ads are profitable."
                   if on_track
                   else "Below target. Focus on reducing cost per lead or improving close rate.")
            ),
        })

    return cards


# ── Helpers ──

def _score_to_status(score):
    return {
        "excellent": "great",
        "good": "good",
        "average": "ok",
        "below_average": "warning",
        "poor": "bad",
        "no_data": "neutral",
    }.get(score, "neutral")


def _grade_label(grade):
    return {
        "A": "Excellent - your marketing is performing well across the board",
        "B": "Good - solid performance with some room to improve",
        "C": "Average - several areas need attention to get better results",
        "D": "Below Average - significant improvements needed to hit your goals",
        "F": "Needs Work - major changes required across multiple channels",
        "N/A": "Not enough data to grade yet",
    }.get(grade, "")


def _trend_text(mom_data):
    change = mom_data.get("change_pct")
    if change is None:
        return ""
    direction = "up" if change > 0 else "down"
    return f" ({direction} {abs(change):.0f}% from last month)"


def _client_friendly_title(title):
    """Strip jargon from suggestion titles."""
    replacements = {
        "CTR": "Click Rate",
        "CPC": "Cost Per Click",
        "CPA": "Cost Per Lead",
        "CPM": "Ad View Cost",
        "Meta Ad": "Facebook Ad",
        "Meta ": "Facebook ",
        "MoM": "Month-over-Month",
        "ROAS": "Return on Ad Spend",
        "GSC": "Google Search",
    }
    result = title
    for old, new in replacements.items():
        result = result.replace(old, new)
    return result


def _client_friendly_category(category):
    return {
        "paid_advertising": "Paid Ads",
        "seo": "SEO (Free Traffic)",
        "website": "Your Website",
        "strategy": "Strategy",
        "creative": "Ad Creative",
        "budget": "Budget",
    }.get(category, category.replace("_", " ").title())


def _plain_english_what(suggestion):
    """Create a 1-2 sentence plain-English summary of what needs to happen."""
    detail = suggestion["detail"]
    # Dejargon
    detail = detail.replace("CTR", "click rate")
    detail = detail.replace("CPC", "cost per click")
    detail = detail.replace("CPA", "cost per lead")
    detail = detail.replace("CPM", "cost per thousand views")
    detail = detail.replace("MoM", "compared to last month")
    detail = detail.replace("ROAS", "return on ad spend")
    detail = detail.replace("RSA", "responsive search ad")

    # Take first 2 sentences max
    sentences = detail.split(". ")
    if len(sentences) > 2:
        return ". ".join(sentences[:2]) + "."
    return detail
