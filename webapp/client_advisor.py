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
                f"Your average ranking across all keywords is position {avg_pos:.1f} ({page}). "
                + ("Most clicks go to the top 3 results. Being on page 2+ means most people "
                   "never see your listing."
                   if avg_pos > 10
                   else "You're on page 1 on average, which is where you want to be."
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

    Max 2 per priority level (2 high + 2 medium = 4 max).
    The AI reads actual account data and produces specific work product,
    not generic how-to instructions.
    """
    high_cards = []
    medium_cards = []

    for s in suggestions:
        if s["priority"] == "high" and len(high_cards) < 2:
            high_cards.append(s)
        elif s["priority"] == "medium" and len(medium_cards) < 2:
            medium_cards.append(s)
        if len(high_cards) >= 2 and len(medium_cards) >= 2:
            break

    selected = high_cards + medium_cards
    if not selected:
        return []

    # Build basic card structure first
    actions = []
    for s in selected:
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

    # Fallback: if AI didn't return steps, generate basic ones from the suggestion data
    for i, card in enumerate(actions):
        if not card["steps"] and i < len(selected):
            s = selected[i]
            card["steps"] = _fallback_steps(s)
            if not card["time"]:
                card["time"] = "15-30 minutes"

    return actions


def _fallback_steps(suggestion):
    """Build basic actionable steps from the suggestion when AI generation fails."""
    steps = []
    detail = suggestion.get("detail", "")
    title = suggestion.get("title", "")
    category = suggestion.get("category", "")
    data_point = suggestion.get("data_point", "")

    if detail:
        # Split detail into sentences and use each as a step
        sentences = [s.strip() for s in detail.replace(". ", ".\n").split("\n") if s.strip()]
        for s in sentences[:4]:
            if not s.endswith("."):
                s += "."
            steps.append(s)

    if not steps:
        steps.append(f"Review your {category} performance data for this month.")
        if data_point:
            steps.append(f"Focus on the key metric: {data_point}.")
        steps.append(f"Take action on: {title}.")

    return steps


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
        # Outside app context (e.g. testing)
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

    # Build the request
    action_items = []
    for s in suggestions:
        action_items.append({
            "title": s["title"],
            "detail": s["detail"],
            "category": s["category"],
            "data_point": s.get("data_point", ""),
        })

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
        "kpis": analysis_summary.get("kpis", {}),
        "seo_detail": analysis_summary.get("seo_detail", {}),
        "google_ads_detail": analysis_summary.get("google_ads_detail", {}),
        "meta_detail": analysis_summary.get("meta_detail", {}),
        "competitor_watch": analysis_summary.get("competitor_watch", {}),
        "highlights": analysis_summary.get("highlights", []),
        "concerns": analysis_summary.get("concerns", []),
        "action_items": action_items,
    }

    system = (
        "You are the AI engine inside GroMore, a platform that replaces ad agencies for local service businesses. "
        "The business owner reading this is in the driver's seat. You are their always-on marketing director.\n\n"
        "For each action item, produce a JSON object with:\n"
        "- \"steps\": array of 3-5 specific, concrete actions the owner can take (or that the platform is doing)\n"
        "- \"impact\": one sentence estimating the expected result (e.g., 'Should save ~$180/month and generate 3-4 additional leads')\n"
        "- \"time\": estimated time to execute (e.g., '10 minutes', '30 minutes', '1 hour')\n\n"
        "Rules:\n"
        "- DO NOT give generic recommendations; every recommendation must cite concrete evidence from provided metrics, campaigns, keywords, queries, or competitors\n"
        "- If evidence is missing for a tactic, do not include that tactic\n"
        "- Never blame external platforms (Google, Meta, algorithm changes) unless a specific metric in data proves the issue\n"
        "- Reference actual campaign names, keywords, metrics, and competitors from the data\n"
        "- For ad copy: write the actual headlines and descriptions ready to paste\n"
        "- For keywords: name the specific keywords to pause, add, or adjust bids on\n"
        "- For SEO: reference the actual pages and queries from the data\n"
        "- For budget: give exact dollar amounts and percentages based on the numbers\n"
        "- For competitors: name the competitor and the specific counter-move\n"
        "- Be direct: 'Pause the emergency plumber keyword ($94 CPL)' not 'Consider pausing expensive keywords'\n"
        "- Each step should be 1-2 sentences, actionable and specific\n"
        "- If data is limited, still be concrete about the approach using what's available\n"
        "- Never use filler phrases like 'consider', 'you might want to', 'it could be beneficial'\n\n"
        "Return ONLY valid JSON: {\"actions\": [{\"steps\": [...], \"impact\": \"...\", \"time\": \"...\"}, ...]}. "
        "One object per action item, in the same order as the input."
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
                    {"role": "user", "content": json.dumps(prompt_data)},
                ],
                "response_format": {"type": "json_object"},
            },
            timeout=45,
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

        # Each item should be {"steps": [...], "impact": "...", "time": "..."}
        result = []
        for item in actions_list:
            if isinstance(item, dict):
                result.append({
                    "steps": [str(s) for s in item.get("steps", []) if s],
                    "impact": str(item.get("impact", "")),
                    "time": str(item.get("time", "")),
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
