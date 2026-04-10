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


_MISSION_SKILL_PROFILES = {
    "beginner": {
        "skill_level": "beginner",
        "label": "Starter Track",
        "summary": "Fewer missions, simpler language, and obvious first steps so new owners can build confidence fast.",
        "tagline": "Start small. Stack wins. Keep momentum.",
        "max_active": 3,
        "preview_steps": 3,
        "max_steps_hint": 4,
        "queue_title": "More Missions",
    },
    "intermediate": {
        "skill_level": "intermediate",
        "label": "Builder Track",
        "summary": "A compact queue with enough detail to move quickly without burying the user in operator noise.",
        "tagline": "Balanced pace, clearer priorities, stronger execution.",
        "max_active": 4,
        "preview_steps": 3,
        "max_steps_hint": 5,
        "queue_title": "Mission Queue",
    },
    "advanced": {
        "skill_level": "advanced",
        "label": "Operator Track",
        "summary": "A denser stack for owners who already know the tools and want tighter, faster operator-level missions.",
        "tagline": "Less hand-holding, more leverage.",
        "max_active": 6,
        "preview_steps": 4,
        "max_steps_hint": 5,
        "queue_title": "Extended Queue",
    },
}


def infer_mission_profile(completed_count=0, requested_level="auto"):
    requested = (requested_level or "auto").strip().lower()
    if requested in _MISSION_SKILL_PROFILES:
        profile = dict(_MISSION_SKILL_PROFILES[requested])
        profile["source"] = "manual"
        profile["requested_level"] = requested
        return profile

    if completed_count >= 8:
        resolved = "advanced"
    elif completed_count >= 3:
        resolved = "intermediate"
    else:
        resolved = "beginner"

    profile = dict(_MISSION_SKILL_PROFILES[resolved])
    profile["source"] = "auto"
    profile["requested_level"] = requested
    return profile


def _trim_copy(value, limit=160):
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


def _normalize_text_list(value):
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if not isinstance(value, (list, tuple)):
        return []
    items = []
    for raw in value:
        text = str(raw or "").strip()
        if text:
            items.append(text)
    return items


def _build_action_items(suggestions, analysis_summary):
    paid_categories = {"paid_advertising", "budget", "creative"}
    seo_categories = {"seo"}
    web_categories = {"website"}
    organic_categories = {"organic_social"}

    google_ads_detail = analysis_summary.get("google_ads_detail", {})
    meta_detail = analysis_summary.get("meta_detail", {})
    seo_detail = analysis_summary.get("seo_detail", {})
    website_detail = analysis_summary.get("website_detail", {})
    fb_organic_detail = analysis_summary.get("facebook_organic_detail", {})
    kpis = analysis_summary.get("kpis", {})

    action_items = []
    for suggestion in suggestions:
        category = suggestion.get("category", "")
        item = {
            "title": suggestion["title"],
            "detail": suggestion["detail"],
            "category": category,
            "data_point": suggestion.get("data_point", ""),
            "relevant_data": {},
        }

        if category in paid_categories or "google" in suggestion["title"].lower() or "cpc" in suggestion["title"].lower():
            item["relevant_data"]["google_ads_campaigns"] = (google_ads_detail.get("campaigns") or [])[:10]
            item["relevant_data"]["google_ads_search_terms"] = (google_ads_detail.get("search_terms") or [])[:30]
            item["relevant_data"]["google_ads_kpis"] = kpis.get("google_ads", {})

        if category in paid_categories or "meta" in suggestion["title"].lower() or "facebook" in suggestion["title"].lower():
            item["relevant_data"]["meta_campaigns"] = (meta_detail.get("campaigns") or [])[:10]
            item["relevant_data"]["meta_top_ads"] = (meta_detail.get("top_ads") or [])[:10]
            item["relevant_data"]["meta_kpis"] = kpis.get("meta", {})

        if category in seo_categories or "seo" in category or "keyword" in suggestion["title"].lower():
            item["relevant_data"]["seo_top_queries"] = (seo_detail.get("top_queries") or [])[:15]
            item["relevant_data"]["seo_keyword_opportunities"] = (seo_detail.get("keyword_opportunities") or [])[:15]
            item["relevant_data"]["seo_top_pages"] = (seo_detail.get("top_pages") or [])[:10]
            item["relevant_data"]["seo_kpis"] = kpis.get("gsc", {})

        if category in web_categories:
            item["relevant_data"]["website_kpis"] = kpis.get("ga", {})
            item["relevant_data"]["website_landing_pages"] = (website_detail.get("top_landing_pages") or [])[:10]
            item["relevant_data"]["seo_top_pages"] = (seo_detail.get("top_pages") or [])[:10]
            item["relevant_data"]["seo_keyword_opportunities"] = (seo_detail.get("keyword_opportunities") or [])[:10]

        if category in organic_categories or "organic" in suggestion["title"].lower():
            item["relevant_data"]["fb_organic_top_posts"] = (fb_organic_detail.get("top_posts") or [])[:10]
            item["relevant_data"]["fb_organic_kpis"] = kpis.get("facebook_organic", {})

        competitors = analysis_summary.get("competitor_watch") or {}
        if competitors:
            item["relevant_data"]["competitors"] = competitors

        item["relevant_data"] = {key: value for key, value in item["relevant_data"].items() if value}
        action_items.append(item)

    return action_items


def _format_exact_targets(category, relevant_data):
    targets = []

    if category == "website":
        for page in (relevant_data.get("website_landing_pages") or [])[:3]:
            path = page.get("page") or page.get("path") or page.get("url") or "this landing page"
            sessions = page.get("sessions")
            conversions = page.get("conversions")
            bounce = page.get("bounce_rate")
            parts = [str(path)]
            if sessions is not None:
                parts.append(f"{int(float(sessions))} sessions")
            if conversions is not None:
                parts.append(f"{int(float(conversions))} conversions")
            if bounce is not None:
                parts.append(f"{round(float(bounce), 1)}% bounce")
            targets.append(" - ".join(parts))

    if category in {"seo", "website"}:
        for row in (relevant_data.get("seo_keyword_opportunities") or [])[:3]:
            query = row.get("query") or row.get("keyword")
            if not query:
                continue
            page = row.get("page") or row.get("url") or "no clear page yet"
            impressions = row.get("impressions")
            position = row.get("position")
            parts = [f'"{query}"']
            if page:
                parts.append(str(page))
            if impressions is not None:
                parts.append(f"{int(float(impressions))} impressions")
            if position is not None:
                parts.append(f"position {round(float(position), 1)}")
            targets.append(" - ".join(parts))

    deduped = []
    for target in targets:
        cleaned = _trim_copy(target, 180)
        if cleaned and cleaned not in deduped:
            deduped.append(cleaned)
    return deduped[:4]


def _build_delegate_plan(title, category, relevant_data):
    title_lower = str(title or "").lower()
    exact_targets = _format_exact_targets(category, relevant_data)
    needs_delegate = category in {"website", "seo", "strategy", "creative"} or any(
        word in title_lower for word in ("page", "website", "landing", "seo", "headline", "design", "copy", "competitor")
    )
    if not needs_delegate:
        return {
            "execution_mode": "direct",
            "delegate_to": "",
            "delegate_message": "",
            "exact_targets": exact_targets,
        }

    if category == "creative" or any(word in title_lower for word in ("design", "image", "creative", "headline")):
        delegate_to = "designer"
        intro = "Please update these creative assets based on GroMore's latest findings:"
        close = "Goal: improve click-through rate without changing the offer."
    else:
        delegate_to = "developer"
        intro = "Please handle these website and SEO updates from GroMore:"
        close = "Goal: remove conversion leaks and improve organic visibility on the pages already getting traffic."

    bullets = exact_targets or [_trim_copy(title or "Mission update", 140)]
    lines = [intro]
    for bullet in bullets:
        lines.append(f"- {bullet}")

    if category == "website":
        lines.append("- Add a clear call-to-action above the fold, tighten the headline to match the service intent, and reduce friction in the contact form.")
    elif category == "seo":
        lines.append("- Rewrite the title tag and on-page copy to match the target query, then strengthen internal links and the primary CTA.")
    elif category == "creative":
        lines.append("- Keep the offer the same, but refresh the visual and headline so the winner is obvious in one glance.")
    else:
        lines.append("- Reply with the ETA and any blockers before making the update.")

    lines.append(close)
    return {
        "execution_mode": "delegate",
        "delegate_to": delegate_to,
        "delegate_message": "\n".join(lines),
        "exact_targets": exact_targets,
    }


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


def build_client_dashboard(analysis, suggestions, brand, ai_model=None, include_deep_analysis=False, mission_profile=None):
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

    actions = _build_action_cards(analysis, suggestions, brand, ai_model=ai_model, mission_profile=mission_profile)
    kpi_status = _explain_kpis(analysis)

    overall_score = analysis.get("overall_score")
    overall_grade = analysis.get("overall_grade", "N/A")

    result = {
        "health": {
            "grade": overall_grade,
            "score": overall_score,
            "label": _grade_label(overall_grade),
        },
        "health_summary": _build_health_summary(analysis, actions, overall_grade, overall_score),
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

    bounce = round(float(metrics.get("bounce_rate", 0)), 1)
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

    conv_rate = round(float(metrics.get("conversion_rate", 0)), 1)
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

    ctr = round(float(metrics.get("ctr", 0)), 1)
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
    debug = metrics.get("_debug") or {}

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
    total_reach = metrics.get("reach") or organic_impressions
    display_reach = organic_impressions if organic_impressions > 0 else total_reach
    if display_reach > 0 or followers > 0:
        reach_pct = round((display_reach / followers) * 100, 1) if followers > 0 else 0
        reach_label = "organically" if organic_impressions > 0 else "in total"
        cards.append({
            "metric": "Organic Reach",
            "value": f"{display_reach:,}",
            "status": "good" if reach_pct > 30 else ("warning" if reach_pct < 15 else "neutral"),
            "explanation": (
                f"Your posts were seen {display_reach:,} times {reach_label} (without paying). "
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

    # Diagnostic card: if we have followers but all insights are zero, likely a permissions issue
    organic_impressions = metrics.get("organic_impressions") or 0
    post_engagements = metrics.get("post_engagements") or 0
    insights_found = debug.get("insights_metrics_found", [])
    insights_status = debug.get("insights_status", "unknown")
    if followers > 0 and organic_impressions == 0 and post_engagements == 0 and post_count == 0:
        if insights_status in ("empty_response", "not_attempted") or insights_status.startswith("http_"):
            hint = (
                "We can see your page info but could not pull engagement data. "
                "This usually means Facebook permissions need updating. "
                "Go to Connections, disconnect Meta, then reconnect and make sure you approve ALL permissions "
                "(pages_read_engagement, read_insights, pages_show_list, pages_manage_metadata, pages_messaging, leads_retrieval). "
                "Also confirm your Meta app has Advanced Access for these permissions in the App Dashboard."
            )
        else:
            hint = (
                "Your page had no organic reach, engagement, or posts this period. "
                "If you have been posting, check that the correct Facebook Page is linked in your brand settings."
            )
        if insights_found:
            hint += f" (Metrics returned by API: {', '.join(insights_found)})"
        elif insights_status and insights_status != "unknown":
            hint += f" (Insights API status: {insights_status})"
        cards.append({
            "metric": "Data Status",
            "value": "Limited",
            "status": "bad",
            "explanation": hint,
        })

    return {"title": "Facebook Organic", "icon": "bi-facebook", "cards": cards}


def _explain_google_ads(google_ads):
    metrics = google_ads.get("metrics", {})
    scores = google_ads.get("scores", {})

    cards = []

    spend = metrics.get("spend", 0)
    conversions = metrics.get("results", 0)
    cpa = metrics.get("cost_per_result", 0)

    cards.append({
        "metric": "Ad Spend",
        "value": f"${spend:,.2f}",
        "status": "neutral",
        "explanation": (
            f"You spent ${spend:,.2f} on Google Ads this month"
            + (f" and got {int(conversions):,} leads at ${cpa:.2f} each."
               if conversions > 0
               else ". No conversions tracked yet - check that your conversion tracking is working.")
        ),
    })

    ctr = round(float(metrics.get("ctr", 0)), 1)
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
            "status": _score_to_status(scores.get("cost_per_result", "no_data")),
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

def _build_action_cards(analysis, suggestions, brand, ai_model=None, mission_profile=None):
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

    from webapp.ai_assistant import _summarize_analysis_for_ai

    analysis_summary = _summarize_analysis_for_ai(analysis)
    action_items = _build_action_items(selected, analysis_summary)

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
            "execution_mode": "direct",
            "delegate_to": "",
            "delegate_message": "",
            "exact_targets": [],
        }
        actions.append(card)

    # Generate AI deliverables using actual account data
    ai_actions = _generate_ai_actions(
        selected,
        analysis,
        brand,
        ai_model=ai_model,
        mission_profile=mission_profile,
        action_items=action_items,
    )
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
                card["execution_mode"] = str(ai.get("execution_mode") or card["execution_mode"] or "direct")
                card["delegate_to"] = str(ai.get("delegate_to") or card["delegate_to"] or "")
                card["delegate_message"] = str(ai.get("delegate_message") or card["delegate_message"] or "")
                card["exact_targets"] = _normalize_text_list(ai.get("exact_targets") or card["exact_targets"])

    # Fallback: if AI didn't return steps, generate basic ones from the suggestion data
    for i, card in enumerate(actions):
        action_item = action_items[i] if i < len(action_items) else {"relevant_data": {}}
        delegate_plan = _build_delegate_plan(
            card["title"],
            selected[i].get("category", "") if i < len(selected) else "",
            action_item.get("relevant_data") or {},
        )
        if not card.get("exact_targets"):
            card["exact_targets"] = delegate_plan["exact_targets"]
        if not card.get("delegate_message"):
            card["delegate_message"] = delegate_plan["delegate_message"]
        if not card.get("delegate_to"):
            card["delegate_to"] = delegate_plan["delegate_to"]
        if not card.get("execution_mode") or card.get("execution_mode") == "direct":
            card["execution_mode"] = delegate_plan["execution_mode"]
        if not card["steps"] and i < len(selected):
            s = selected[i]
            card["steps"] = _fallback_steps(s, action_item=action_item, delegate_plan=delegate_plan)
            if not card["time"]:
                card["time"] = "15-30 minutes"
        # Fallback mission name
        if not card["mission_name"]:
            card["mission_name"] = card["title"]
        if card.get("execution_mode") == "delegate" and selected[i].get("category") in {"website", "seo", "strategy", "creative"}:
            card["platform_url"] = ""
            card["platform_label"] = ""
        # Calculate difficulty from time
        card["difficulty"] = _parse_difficulty(card["time"])

    return actions


def _fallback_steps(suggestion, action_item=None, delegate_plan=None):
    """Build detailed, click-by-click steps a fifth grader could follow.

    These are used when the AI prompt fails or no API key is set. Each step
    tells the user exactly where to click, what to look for, and what to do.
    """
    title = suggestion.get("title", "")
    category = suggestion.get("category", "")
    data_point = suggestion.get("data_point", "")
    detail = suggestion.get("detail", "")
    title_lower = title.lower()
    relevant_data = (action_item or {}).get("relevant_data") or {}
    delegate_plan = delegate_plan or {"execution_mode": "direct", "delegate_message": "", "exact_targets": []}

    dp = data_point  # short alias

    if delegate_plan.get("execution_mode") == "delegate":
        steps = ["Copy the developer brief below and send it today."]
        for target in delegate_plan.get("exact_targets") or []:
            steps.append(f"Have them update this exact target: {target}.")
        steps.append("Once the update is live, mark the mission complete so GroMore can re-check performance on the next refresh.")
        return steps[:5]

    # --- Google Ads: CPC / Cost related ---
    if any(w in title_lower for w in ("cost per click", "cpc", "lower cost")):
        return [
            f"Go to ads.google.com. Click \"Campaigns\" in the left sidebar. Click \"Keywords\" then \"Search terms\" at the top.{f' Your CPC right now is {dp}.' if dp else ''}",
            "Sort the list by \"Cost\" (highest first). Find search terms that have spent money but show 0 conversions. Check the box next to each one.",
            "Click the blue \"Add as negative keyword\" button at the top. Choose \"Account level\" so they're blocked everywhere.",
            "Now click \"Keywords\" (not search terms). Sort by \"Cost/conv.\" highest first. Any keyword over 2x your target CPA, click the green dot and change it to \"Paused.\"",
            "Click on your highest-spending campaign. Click \"Settings.\" Lower the daily budget by 10-15% and move that money to your best-converting campaign instead.",
        ]

    # --- Google Ads: general campaign optimization ---
    if category == "paid_advertising" and any(w in title_lower for w in ("google", "campaign", "search ad")):
        return [
            f"Go to ads.google.com. Click \"Campaigns\" on the left side.{f' Data point: {dp}.' if dp else ''} Sort by \"Cost\" to see which campaign spends the most.",
            "Click the campaign name that's spending the most. Click \"Ad groups\" to see all ad groups inside it. Look for any with a high cost but 0 conversions.",
            "For ad groups with 0 conversions: click the green dot next to it and choose \"Paused.\" This stops wasting money on ads that don't work.",
            "Go back to the campaign. Click \"Keywords\" then \"Search terms.\" Add anything irrelevant as a negative keyword (check the box, then click \"Add as negative keyword\").",
            "Click \"Ads & assets.\" If any ad has a CTR below 2%, click the pencil icon and rewrite the headline to include your main service + city name.",
        ]

    # --- Facebook / Meta Ads ---
    if any(w in title_lower for w in ("facebook", "meta", "instagram", "roas")):
        return [
            f"Go to business.facebook.com/adsmanager. Click \"Campaigns\" at the top.{f' Current metric: {dp}.' if dp else ''} Sort by \"Cost per result\" (click the column header).",
            "Find any campaign where the cost per result is more than double your goal. Click the toggle switch on the left side to turn it OFF.",
            "For campaigns that ARE working: click the campaign name, then click into the ad set level. Click \"Edit.\" Under \"Budget,\" increase the daily budget by $5-10.",
            "Still in the ad set, scroll down to \"Placements.\" Switch to \"Manual placements\" and uncheck anything except Facebook Feed, Instagram Feed, and Instagram Stories.",
            "Click \"Ads\" at the top. Look at each ad's \"CTR (link).\" If any ad has under 1% CTR, click the pencil icon and change the image or headline.",
        ]

    # --- SEO / Search Console ---
    if category == "seo" or any(w in title_lower for w in ("seo", "ranking", "organic", "search console")):
        return [
            f"Go to search.google.com/search-console. Click \"Performance\" on the left side.{f' Current data: {dp}.' if dp else ''} Make sure \"Average position\" is checked at the top.",
            "Click the \"Pages\" tab. Sort by \"Impressions\" (highest first). Find pages with lots of impressions but very few clicks - those need better titles.",
            "Click on a page with high impressions but low clicks. Go to your website editor and change that page's title tag to include the exact keyword people searched.",
            "Now click the \"Queries\" tab in Search Console. Look for keywords in positions 8-20 (page 1-2 of Google). These are close to ranking. Write a new section on your page about that exact topic.",
            "Go to your website. Make sure every service page has at least 500 words, your city name in the title, and a clear \"Call Now\" or \"Get a Quote\" button at the top.",
        ]

    # --- Website / Analytics ---
    if category == "website" or any(w in title_lower for w in ("website", "landing page", "conversion", "bounce", "analytics")):
        exact_targets = _format_exact_targets("website", relevant_data)
        if exact_targets:
            steps = []
            for target in exact_targets[:3]:
                steps.append(f"Update this exact page next: {target}.")
            steps.append("On each page, put the phone CTA or quote form above the fold and make the headline match the service intent.")
            steps.append("When the edits are live, compare lead volume and bounce rate after 7 days.")
            return steps[:5]
        return [
            f"Go to analytics.google.com. Click \"Reports\" on the left, then \"Pages and screens.\"{f' Current metric: {dp}.' if dp else ''} Sort by \"Views\" to see your most visited pages.",
            "Look at the \"Bounce rate\" column. Find any page with a bounce rate over 70%. That means most people leave without doing anything. Those pages need fixing first.",
            "Open your website in a new tab. Go to each high-bounce page. Ask yourself: is there a phone number or form visible without scrolling? If not, add one at the very top.",
            "Check if your pages load in under 3 seconds. Go to pagespeed.web.dev, paste each page URL, and click \"Analyze.\" Fix anything it flags as red.",
            "On every page, add a clear button that says exactly what you want them to do: \"Call Now,\" \"Get a Free Quote,\" or \"Book Online.\" Put it above the fold (visible without scrolling).",
        ]

    # --- Budget / Spend efficiency ---
    if category == "budget":
        return [
            f"Go to ads.google.com. Click \"Campaigns\" on the left.{f' Budget data: {dp}.' if dp else ''} Write down how much each campaign spent this month and how many leads it got.",
            "Divide each campaign's spend by its leads. The one with the LOWEST cost per lead is your best campaign. The one with the HIGHEST cost per lead (or 0 leads) is your worst.",
            "Click on your worst campaign (highest cost per lead). Click \"Settings.\" Lower the daily budget by 20%. Write down the dollar amount you saved.",
            "Now click on your best campaign (lowest cost per lead). Click \"Settings.\" Add the money you just saved to this campaign's daily budget.",
            "Set a calendar reminder for 7 days from now to check again. Look at the same numbers. If the change helped, keep it. If not, reverse it.",
        ]

    # --- Creative / Ad copy ---
    if category == "creative":
        return [
            f"Go to your ads platform (ads.google.com or business.facebook.com/adsmanager).{f' Creative data: {dp}.' if dp else ''} Click into your top-spending campaign, then click \"Ads\" or \"Ads & assets.\"",
            "Look at each ad's CTR (Click-Through Rate). Find the ad with the HIGHEST CTR. That's your winning style. Write down what its headline and image look like.",
            "Find ads with the LOWEST CTR. Click the pencil/edit icon. Rewrite the headline to match the style of your winning ad, but test a different angle (urgency, price, guarantee).",
            "For image ads: make sure the image shows your actual work, team, or a real before/after. Remove any ad that uses a generic stock photo. Replace with a real photo from your phone.",
            "Duplicate your best-performing ad. Change ONLY the headline (keep the image). This lets you test which words get more clicks without losing what already works.",
        ]

    # --- Organic social ---
    if category == "organic_social":
        return [
            f"Go to business.facebook.com. Click your page name, then \"Insights\" on the left side.{f' Data point: {dp}.' if dp else ''} Click \"Posts\" and sort by \"Reach\" (highest first).",
            "Look at your top 3 posts by reach. Write down what they have in common: was it a photo, video, question, or tip? That type of post is what your audience likes.",
            "Open your phone. Take a photo or short video (under 60 seconds) of your work, your team, or a customer result. Something real, NOT a stock image or graphic.",
            "Write a post using this exact format: Start with a question or bold statement. Then 2-3 short sentences. End with a call to action (\"Comment below\" or \"DM us\").",
            "Post it NOW. Don't overthink it. Then set a reminder to post again in 3 days. Consistency beats perfection.",
        ]

    # --- Catch-all with platform detection ---
    if any(w in title_lower for w in ("facebook", "meta", "instagram")):
        url = "business.facebook.com/adsmanager"
        label = "Ads Manager"
    elif any(w in title_lower for w in ("google ads", "cpc", "ppc")):
        url = "ads.google.com"
        label = "Google Ads"
    elif any(w in title_lower for w in ("seo", "ranking", "search console")):
        url = "search.google.com/search-console"
        label = "Search Console"
    elif "analytic" in title_lower:
        url = "analytics.google.com"
        label = "Google Analytics"
    else:
        url, label = {
            "paid_advertising": ("ads.google.com", "Google Ads"),
            "budget": ("ads.google.com", "Google Ads"),
            "creative": ("business.facebook.com/adsmanager", "Ads Manager"),
            "seo": ("search.google.com/search-console", "Search Console"),
            "website": ("analytics.google.com", "Google Analytics"),
            "organic_social": ("business.facebook.com", "Meta Business Suite"),
        }.get(category, ("", ""))

    steps = []
    if url:
        steps.append(f"Go to {url}. Log in and find the section related to \"{title}.\"{f' Your current number is {dp}.' if dp else ''}")

    if detail:
        sentences = [s.strip() for s in detail.replace(". ", ".\n").split("\n") if s.strip() and len(s.strip()) > 15]
        for s in sentences[:2]:
            if not s.endswith("."):
                s += "."
            steps.append(s)

    steps.append(f"Make one specific change today. Write down what you changed and what the number was before, so you can check if it helped next week.")
    if url:
        steps.append(f"Set a reminder for 7 days from now. Go back to {url} and compare the numbers to see if your change made a difference.")

    return steps[:5]


def _generate_ai_actions(suggestions, analysis, brand, ai_model=None, mission_profile=None, action_items=None):
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
        or "gpt-4o"
    )

    from webapp.ai_assistant import _summarize_analysis_for_ai
    analysis_summary = _summarize_analysis_for_ai(analysis)

    if action_items is None:
        action_items = _build_action_items(suggestions, analysis_summary)

    for item in action_items:
        category = item.get("category", "")
        exact_targets = _format_exact_targets(category, item.get("relevant_data") or {})
        if exact_targets:
            item["detail"] = _trim_copy(
                f"{item.get('detail', '')} Exact targets from the connected data: {'; '.join(exact_targets)}",
                900,
            )

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
        "mission_profile": mission_profile or infer_mission_profile(),
        "highlights": analysis_summary.get("highlights", []),
        "concerns": analysis_summary.get("concerns", []),
        "action_items": action_items,
    }

    skill_level = ((mission_profile or {}).get("skill_level") or "beginner").strip().lower()
    if skill_level == "advanced":
        audience_block = (
            "AUDIENCE: An owner or operator who already knows the major tools. "
            "Keep the missions tighter and less tutorial-heavy, but still name the exact menu, button, or field when a mistake would be costly. "
            "Prefer concise operator language over hand-holding."
        )
        step_rule = "Write 3-5 steps per mission."
    elif skill_level == "intermediate":
        audience_block = (
            "AUDIENCE: A business owner who has used these tools before, but is not a full-time operator. "
            "Be clear and specific without sounding overwhelming. Name the exact menus and buttons for important actions."
        )
        step_rule = "Write 4-5 steps per mission."
    else:
        audience_block = (
            "AUDIENCE: A business owner who has NEVER been inside Google Ads before. "
            "Write every step so a literal fifth grader could follow it. "
            "Name every button, every menu, every tab. "
            "If you say 'click', say exactly what words are on the button. "
            "If you say 'change', say the exact old value and exact new value. "
            "NEVER assume they know where anything is."
        )
        step_rule = "Write 4-6 steps per mission."

    system = (
        "You are the senior paid-media and SEO strategist inside GroMore. "
        "You have completed a deep-dive analysis of this account. "
        "Now produce MISSIONS the business owner can execute right now.\n\n"

        + audience_block + "\n\n"

        "CRITICAL EXECUTION RULES:\n"
        "- The user should NEVER have to hunt through analytics, Search Console, or ad dashboards for targets that are already in the connected data.\n"
        "- Name the exact page, query, campaign, ad, keyword, or search term from the provided data whenever one exists.\n"
        "- If the work belongs with a developer, designer, or assistant, do NOT send the owner into the tool to figure it out. Write a delegation-ready mission and include a ready-to-send handoff note.\n"
        "- Avoid verbs like 'look for', 'review', 'find', 'evaluate', or 'assess' unless the exact thing to inspect is named in the same sentence.\n\n"

        "OUTPUT FORMAT (JSON only):\n"
        "{\"actions\": [\n"
        "  {\n"
        "    \"mission_name\": \"3-6 word punchy verb phrase\",\n"
        "    \"micro_steps\": [\"step 1\", \"step 2\", ...],\n"
        "    \"exact_targets\": [\"exact page/query/campaign names from the data\"],\n"
        "    \"execution_mode\": \"direct\" or \"delegate\",\n"
        "    \"delegate_to\": \"developer\" or \"designer\" or \"assistant\" or \"\",\n"
        "    \"delegate_message\": \"ready-to-send note if the owner should hand this off\",\n"
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

        "MICRO-STEP RULES - THIS IS THE MOST IMPORTANT PART:\n"
        + step_rule + " Each step = ONE specific action.\n\n"

        "Each step MUST include ALL of these:\n"
        "1. The exact URL to go to (ads.google.com, business.facebook.com/adsmanager, etc.)\n"
        "2. The exact menu/tab/button to click, using the exact words shown on screen\n"
        "3. The exact thing to type, change, pause, or enable\n"
        "4. WHY this specific thing (reference a campaign name, keyword, dollar amount, or metric from the data)\n\n"

        "STEP EXAMPLES THAT ARE CORRECT:\n"
        "- 'Go to ads.google.com. Click \"Keywords\" in the left sidebar, then click \"Search terms\" at the top. Sort the list by \"Cost\" (click the column header). Find any search term that spent over $20 but has 0 conversions. Check the box next to it, then click \"Add as negative keyword\" and choose \"Account level.\"'\n"
        "- 'Go to ads.google.com. Click \"Campaigns\" on the left. Find \"SDL Search Campaign\" (it spent $340 and got 0 leads). Click the green dot under \"Status\" and change it to \"Paused.\"'\n"
        "- 'Go to business.facebook.com/adsmanager. Click on your active campaign. Click \"Ad sets\" at the top. Click \"Edit\" on the ad set. Scroll down to \"Budget.\" Change the daily budget from $15 to $25 because this ad set has the lowest cost per lead at $12.'\n"
        "- 'Go to search.google.com/search-console. Click \"Performance\" on the left. Click the \"Queries\" tab. Find \"plumber near me\" - you are at position 14 with 800 impressions. Go to your website and add a new page titled \"Plumber Near Me in [Your City]\" with at least 500 words about that service.'\n\n"
        "- 'Copy the developer note below and send it to your website developer. Update /emergency-plumbing - 92 sessions, 0 conversions, 81% bounce. Put the phone CTA above the fold, tighten the H1 to match emergency service intent, and shorten the quote form to name, phone, and service.'\n\n"

        "STEP EXAMPLES THAT ARE WRONG (NEVER WRITE THESE):\n"
        "- 'Go to ads.google.com and log in to your Google Ads account.' (FILLER. They know how to log in.)\n"
        "- 'Your current number: CPC: $7.84. Look for this in your dashboard to confirm.' (That's just restating data. It's not a step.)\n"
        "- 'Add negative keywords weekly, improve Quality Score, and split high-cost broad groups into tighter exact and phrase match groups.' (Three vague actions crammed into one sentence. No specifics on WHICH keywords, WHICH groups.)\n"
        "- 'Review your campaigns and pause underperformers' (WHICH campaigns? Name them.)\n"
        "- 'Reallocate spend toward top converters' (Move how much? From which campaign to which?)\n"
        "- 'Optimize your landing pages' (WHICH page? Change what text to what?)\n"
        "- 'Consider testing new ad copy' (Don't suggest it - WRITE the actual headline for them.)\n"
        "- 'Tighten targeting' (Change what setting? To what value?)\n"
        "- 'These campaigns are under target: SDL Search Campaign' (That's a fact, not a step.)\n"
        "- 'Average CPC is $7.84, above benchmark.' (That's data, not an action.)\n"
        "- Any step that starts with 'Review', 'Consider', 'Look into', 'Assess', or 'Evaluate'\n\n"

        "USE THE DATA: You have relevant_data attached to each action item. "
        "Use actual campaign names, actual keyword names, actual dollar amounts, actual search terms from the data. "
        "If data says campaign X spent $Y with Z conversions, reference those exact numbers. "
        "If search_terms data shows wasted terms, name those exact terms. "
        "NEVER write a generic step when you have specific data available.\n\n"

        "WHY field: One sentence using a specific dollar amount or lead count. "
        "Example: \"You burned $340 last month on clicks that never turned into a phone call.\"\n\n"

        "REWARD field: Concrete result, not vague improvement. "
        "Example: \"$340/month gets redirected to keywords that actually generate calls.\"\n\n"

        "IMPACT: Specific projected numbers. "
        "Example: 'Could save $340/month and generate about 5 more leads at $38 each.'\n\n"

        "TIME: '5 minutes', '10 minutes', '15 minutes', '30 minutes'. Not 'varies'.\n\n"

        "FINAL CHECK before returning: Read each step out loud. "
        "If a step does not tell the user EXACTLY which button to click and EXACTLY what to type or change, rewrite it. "
        "If a step just restates a metric or describes a problem, delete it and replace it with an action. "
        "No filler steps like 'log in to your account' or 'check your dashboard.' "
        "Every step must CHANGE something or CREATE something."
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
                    "exact_targets": _normalize_text_list(item.get("exact_targets")),
                    "execution_mode": str(item.get("execution_mode", "direct") or "direct"),
                    "delegate_to": str(item.get("delegate_to", "")),
                    "delegate_message": str(item.get("delegate_message", "")),
                    "why": str(item.get("why", "")),
                    "reward": str(item.get("reward", "")),
                })
            elif isinstance(item, list):
                # Fallback: plain list of strings (old format)
                result.append({
                    "steps": [str(s) for s in item if s],
                    "impact": "",
                    "time": "",
                    "exact_targets": [],
                    "execution_mode": "direct",
                    "delegate_to": "",
                    "delegate_message": "",
                })
            else:
                result.append({"steps": [], "impact": "", "time": "", "exact_targets": [], "execution_mode": "direct", "delegate_to": "", "delegate_message": ""})

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
        or "gpt-4o"
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

    if targets.get("leads") and actual.get("paid_leads") is not None:
        leads_eval = evaluation.get("leads", {})
        on_track = leads_eval.get("on_track", False)
        expected_to_date = leads_eval.get("expected_to_date")
        is_current_month = leads_eval.get("is_current_month", False)
        pace_label = leads_eval.get("pace_label") or ("On pace" if on_track else "Behind pace")
        elapsed_days = leads_eval.get("elapsed_days")
        cards.append({
            "label": "Total Leads",
            "target": f"{int(targets['leads'])}",
            "actual": f"{int(actual['paid_leads'])}",
            "on_track": on_track,
            "status_label": pace_label,
            "explanation": (
                (
                    f"You have {int(actual['paid_leads'])} leads against a monthly target of {int(targets['leads'])}. "
                    f"By day {int(elapsed_days or 0)}, the paced target is {expected_to_date:.1f}, so you're {pace_label.lower()}."
                    if is_current_month and expected_to_date is not None
                    else f"Target is {int(targets['leads'])} leads and you got {int(actual['paid_leads'])}. "
                )
                + (
                    " Keep spend stable and focus on lead quality before making major changes."
                    if on_track and is_current_month
                    else " You're hitting your lead target."
                    if on_track
                    else " Consider increasing budget on your best-performing campaigns or launching new ad variations."
                )
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


def _build_health_summary(analysis, actions, overall_grade, overall_score):
    kpi = analysis.get("kpi_status", {}) if isinstance(analysis, dict) else {}
    evaluation = kpi.get("evaluation", {}) if isinstance(kpi, dict) else {}
    actual = kpi.get("actual", {}) if isinstance(kpi, dict) else {}
    targets = kpi.get("targets", {}) if isinstance(kpi, dict) else {}

    leads_eval = evaluation.get("leads") if isinstance(evaluation.get("leads"), dict) else None
    cpa_eval = evaluation.get("cpa") if isinstance(evaluation.get("cpa"), dict) else None
    roas_eval = evaluation.get("roas") if isinstance(evaluation.get("roas"), dict) else None

    tone = "neutral"
    label = "Needs more data"
    summary = "Connect more data sources or wait for more activity before the dashboard can call the month clearly."
    numbers = []

    if leads_eval:
        paid_leads = int(actual.get("paid_leads") or 0)
        target_leads = int(targets.get("leads") or 0)
        expected_to_date = leads_eval.get("expected_to_date")
        pace_status = leads_eval.get("pace_status") or "full_month"

        numbers.append(f"{paid_leads} leads so far")
        if target_leads:
            numbers.append(f"{target_leads} target this month")
        if expected_to_date is not None and leads_eval.get("is_current_month"):
            numbers.append(f"{expected_to_date:.1f} paced target by today")

        if pace_status == "ahead":
            tone = "positive"
            label = "Ahead of pace"
            summary = (
                f"The numbers say lead volume is running ahead of plan. You have {paid_leads} leads so far, "
                f"which is above the paced target of {expected_to_date:.1f} for this point in the month."
            )
        elif pace_status == "on_track":
            tone = "good"
            label = "On pace"
            summary = (
                f"The numbers say lead volume is on track. You have {paid_leads} leads so far against a paced target of {expected_to_date:.1f}."
            )
        elif pace_status == "watch":
            tone = "caution"
            label = "Watch closely"
            summary = (
                f"The numbers say lead flow is a little behind pace. You have {paid_leads} leads so far against a paced target of {expected_to_date:.1f}."
            )
        elif pace_status == "at_risk":
            tone = "warning"
            label = "Needs attention"
            summary = (
                f"The numbers say lead flow is behind pace. You have {paid_leads} leads so far against a paced target of {expected_to_date:.1f}."
            )
        elif leads_eval.get("on_track"):
            tone = "good"
            label = "On target"
            summary = f"The numbers say you are hitting your lead target with {paid_leads} leads against a goal of {target_leads}."
        else:
            tone = "warning"
            label = "Off target"
            summary = f"The numbers say you are below your lead target with {paid_leads} leads against a goal of {target_leads}."
    elif cpa_eval and cpa_eval.get("on_track") is not None:
        tone = "good" if cpa_eval.get("on_track") else "warning"
        label = "Efficient" if cpa_eval.get("on_track") else "Needs attention"
        summary = (
            f"The numbers say cost efficiency is {'healthy' if cpa_eval.get('on_track') else 'slipping'}. "
            f"Your blended cost per lead is ${cpa_eval.get('actual'):.2f} against a target of ${cpa_eval.get('target'):.2f}."
        )

    action_titles = []
    for action in actions or []:
        title = (action.get("mission_name") or action.get("title") or "").strip()
        if title:
            action_titles.append(title)
        if len(action_titles) == 2:
            break

    if not action_titles:
        if tone in {"positive", "good"}:
            action_titles = ["Keep budget stable", "Watch lead quality before making changes"]
        elif tone == "caution":
            action_titles = ["Review your top-performing campaigns", "Refresh one underperforming ad or offer"]
        elif tone == "warning":
            action_titles = ["Shift spend to your best campaign", "Launch one new ad variation this week"]
        else:
            action_titles = ["Connect your data sources", "Wait for more month-to-date data"]

    meter_pct_map = {
        "positive": 88,
        "good": 74,
        "caution": 52,
        "warning": 28,
        "neutral": 40,
    }

    return {
        "tone": tone,
        "label": label,
        "summary": summary,
        "numbers": numbers,
        "actions": action_titles,
        "meter_pct": meter_pct_map.get(tone, 40),
        "grade": overall_grade,
        "score": overall_score,
        "grade_label": _grade_label(overall_grade),
        "cpa_on_track": cpa_eval.get("on_track") if cpa_eval else None,
        "roas_on_track": roas_eval.get("on_track") if roas_eval else None,
    }


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
