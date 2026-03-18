"""
Client Action Advisor

Takes analysis + suggestions from the analytics pipeline and produces:
1. Plain-English metric explanations ("Your click-through rate is 3.2% - this means...")
2. Step-by-step change instructions ("In Google Ads, go to Campaigns > ...")
3. Priority-ranked action cards with clear next steps

Designed for business owners who run their own ads but need guided help.
"""


def build_client_dashboard(analysis, suggestions, brand):
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

    actions = _build_action_cards(analysis, suggestions, brand)
    kpi_status = _explain_kpis(analysis)

    overall_score = analysis.get("overall_score")
    overall_grade = analysis.get("overall_grade", "N/A")

    return {
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


# ── Action Cards with Step-by-Step Instructions ──

def _build_action_cards(analysis, suggestions, brand):
    """Convert suggestions into step-by-step action cards for clients."""
    actions = []

    for s in suggestions:
        if s["priority"] not in ("high", "medium"):
            continue

        card = {
            "title": _client_friendly_title(s["title"]),
            "priority": "Do This Now" if s["priority"] == "high" else "Worth Doing Soon",
            "priority_class": "danger" if s["priority"] == "high" else "warning",
            "category": _client_friendly_category(s["category"]),
            "what": _plain_english_what(s),
            "steps": _get_steps(s, brand),
            "data_point": s.get("data_point", ""),
        }
        actions.append(card)

        if len(actions) >= 10:
            break

    return actions


def _get_steps(suggestion, brand):
    """Generate specific step-by-step instructions based on the suggestion category and title."""
    title = suggestion["title"].lower()
    category = suggestion["category"]

    # Google Ads steps
    if "google ads" in title and "ctr" in title:
        return [
            "Log into Google Ads at ads.google.com",
            "Click 'Campaigns' in the left menu, then click into your main campaign",
            "Click 'Ad groups' - look for any with CTR below 3%",
            "Click into that ad group, then click 'Ads & assets'",
            "Click the blue + button to create a new responsive search ad",
            "Write 5 new headlines that include your service + city (e.g., 'Fast Plumber in Austin')",
            "Add 2-3 descriptions mentioning what makes you different (24/7 service, free estimates, etc.)",
            "Save the new ad and let it run for 2 weeks before comparing results",
        ]

    if "google ads" in title and ("cpc" in title or "cost per click" in title.lower()):
        return [
            "Log into Google Ads at ads.google.com",
            "Click 'Keywords' then 'Search keywords' in the left menu",
            "Sort by 'Cost' (highest first) to find your most expensive keywords",
            "For any keyword spending a lot with zero conversions, click the green dot to pause it",
            "Click 'Search terms' to see what people actually typed - add irrelevant ones as negative keywords",
            "Check your bid strategy - if using 'Maximize clicks', consider switching to 'Maximize conversions'",
        ]

    if "google ads" in title and ("cpa" in title or "cost per lead" in title.lower()):
        return [
            "Log into Google Ads at ads.google.com",
            "Go to 'Campaigns' and sort by 'Cost/conv.' (cost per lead)",
            "For campaigns with cost per lead above your target, click in to investigate",
            "Check 'Search terms' - are people searching for things you don't offer? Add those as negative keywords",
            "Check 'Locations' - are you getting clicks from areas you don't serve? Exclude those",
            "Consider adding a target CPA bid strategy set to your desired cost per lead",
        ]

    # Meta / Facebook Ads steps
    if ("meta" in title or "facebook" in title or category == "creative") and "ctr" in title.lower():
        return [
            "Go to Facebook Ads Manager (business.facebook.com)",
            "Click on your active campaign, then the ad set, then the individual ads",
            "Look at which ads have the lowest click rate (CTR column)",
            "Create a new ad: click '+ Create' at the ad level",
            "Use a real photo from a recent job (before/after works great) instead of stock photos",
            "Write a headline focused on the customer's problem, not your company name",
            "Add a clear call-to-action like 'Get Your Free Quote Today'",
            "Set budget to match your lowest-performing ad, then pause that old ad after 5 days",
        ]

    if ("meta" in title or "facebook" in title) and ("cpc" in title.lower() or "cost per click" in title.lower()):
        return [
            "Go to Facebook Ads Manager (business.facebook.com)",
            "Click your campaign, then the ad set level",
            "Check 'Audience' - narrow to homeowners in your service area, ages 30-65",
            "Under 'Placements', try 'Advantage+ placements' to let Facebook find cheaper spots",
            "Create a lookalike audience: go to Audiences > Create > Lookalike > based on your customer list",
            "Test this new audience in a separate ad set with the same ads to compare costs",
        ]

    if "frequency" in title.lower() or "fatigue" in title.lower():
        return [
            "Go to Facebook Ads Manager (business.facebook.com)",
            "Check your ad frequency in the columns (add 'Frequency' if not showing)",
            "For any ad set with frequency above 4, create new ad creative (images + text)",
            "Use different photos, different angles, different customer testimonials",
            "Consider expanding your audience size: increase the radius or broaden age range",
            "Set a frequency cap: in Campaign settings, under 'Reach and frequency', set max 3x per week",
        ]

    # Website / bounce rate steps
    if "bounce" in title.lower():
        return [
            "Open your website on your phone - does it load in under 3 seconds?",
            "Check that your phone number is large and clickable at the top of every page",
            "Make sure your main service and city are visible without scrolling",
            "Add customer reviews or star ratings near the top of your homepage",
            "Check that your 'Get a Quote' or 'Call Now' button is above the fold (visible without scrolling)",
            "Test your site speed at PageSpeed Insights (pagespeed.web.dev) and fix any red issues",
        ]

    if "conversion" in title.lower() and ("website" in category or "funnel" in title.lower()):
        return [
            "Open your website and try to fill out your own contact form - is it easy?",
            "Simplify your form: only ask for name, phone, and service needed (3 fields max)",
            "Add a click-to-call button on every page (especially on mobile)",
            "Add trust signals: Google reviews badge, 'Licensed & Insured', years in business",
            "Make sure every service page has its own contact form or call button",
            "Consider adding live chat (many free options like Tawk.to)",
        ]

    if "engagement" in title.lower() or "session duration" in title.lower():
        return [
            "Add before-and-after photos of your recent work to your service pages",
            "Create a short FAQ section on each service page (5-8 common questions)",
            "Add a video testimonial from a happy customer (even a phone recording works)",
            "Link between related services ('Need a drain cleaned? We also do water heater installs')",
            "Add pricing ranges or 'starting at' prices - visitors want to know cost before calling",
        ]

    # SEO steps
    if "seo" in title.lower() and ("opportunity" in title.lower() or "quick win" in title.lower()):
        return [
            "These are keywords you almost rank for on page 1 - small improvements can make a big difference",
            "For each opportunity keyword, find the page on your site that ranks for it",
            "Update that page's title tag to include the keyword naturally",
            "Add 200-300 more words of helpful content about that topic to the page",
            "Link to that page from 2-3 other pages on your site",
            "If you have a blog, write a related post and link to the service page",
        ]

    if "position" in title.lower() or "visibility" in title.lower():
        return [
            "Claim and fully optimize your Google Business Profile (business.google.com)",
            "Add your business to local directories: Yelp, Angi, HomeAdvisor, BBB",
            "Ask your last 5 happy customers to leave Google reviews (send them a direct link)",
            "Make sure every page on your site has your city/service area in the title tag",
            "Create a dedicated page for each service you offer (not just one big services page)",
        ]

    if "click-through" in title.lower() and "organic" in title.lower():
        return [
            "Go to Google Search Console (search.google.com/search-console)",
            "Click 'Search results' in the left menu",
            "Sort by 'Impressions' (highest first) to find your most-seen pages",
            "For pages with high impressions but low clicks, rewrite the title tag to be more compelling",
            "Add action words like 'Free Estimates', 'Same-Day Service', or 'Rated #1' to your meta descriptions",
            "Add review schema markup to show star ratings in search results",
        ]

    # Competitor watch
    if "competitor" in title.lower():
        return [
            "Review what your competitors are offering that you're not highlighting",
            "Check their Google Ads by searching your main keywords - what do their ads say?",
            "Look at their website - what trust signals or offers are they showing?",
            "Consider matching or beating their offer (free estimate, discount, warranty)",
            "Update your ad copy and website to address what makes you different from them",
        ]

    # Budget / scaling
    if "budget" in title.lower() or "scale" in title.lower():
        return [
            "Only increase budget on campaigns that are already getting leads at a good cost",
            "Increase by 15-20% at a time, not more (big jumps confuse the algorithms)",
            "Wait 5-7 days after each increase before raising again",
            "Monitor your cost per lead daily during scaling - if it jumps up, pause the increase",
            "Set a maximum cost-per-lead limit so you don't overspend during scaling",
        ]

    # Generic fallback
    return [
        "Review the data point mentioned above to understand the current situation",
        "Check your ad accounts or website for the specific issue described",
        "Make the recommended changes one at a time so you can measure impact",
        "Wait at least 1-2 weeks before judging whether a change is working",
        "If you're unsure about any step, reach out to your account manager",
    ]


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
