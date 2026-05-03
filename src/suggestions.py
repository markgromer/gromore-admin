"""
Suggestions Engine

Generates actionable, prioritized recommendations for next month based on:
- Current performance vs benchmarks
- Month-over-month trends
- Industry-specific best practices for home services
- Data from all connected platforms (GA, Meta, Google Ads, GSC)
"""
import json
from pathlib import Path


PRIORITY_HIGH = "high"
PRIORITY_MEDIUM = "medium"
PRIORITY_LOW = "low"

CATEGORY_PAID = "paid_advertising"
CATEGORY_SEO = "seo"
CATEGORY_WEBSITE = "website"
CATEGORY_STRATEGY = "strategy"
CATEGORY_CREATIVE = "creative"
CATEGORY_BUDGET = "budget"
CATEGORY_ORGANIC = "organic_social"


def make_suggestion(title, detail, priority, category, impact_area, data_point=None):
    return {
        "title": title,
        "detail": detail,
        "priority": priority,
        "category": category,
        "impact_area": impact_area,
        "data_point": data_point,
    }


def generate_suggestions(analysis):
    """
    Generate actionable suggestions based on the full analysis.

    Args:
        analysis: Output from analytics.build_full_analysis()

    Returns:
        List of suggestion dicts, sorted by priority
    """
    suggestions = []
    industry = analysis.get("industry", "plumbing")
    client_config = analysis.get("client_config", {})
    goals = client_config.get("goals", [])

    # ── Google Analytics suggestions ──
    ga = analysis.get("google_analytics")
    if ga:
        suggestions.extend(_ga_suggestions(ga, goals, analysis.get("top_landing_pages") or []))

    # ── Meta Business Suite suggestions ──
    meta = analysis.get("meta_business")
    if meta:
        suggestions.extend(_meta_suggestions(meta, industry, goals, client_config))

    # ── Google Ads suggestions ──
    google_ads = analysis.get("google_ads")
    if google_ads:
        suggestions.extend(_google_ads_suggestions(google_ads, industry, goals))

    # ── Facebook Organic suggestions ──
    fb_organic = analysis.get("facebook_organic")
    if fb_organic:
        suggestions.extend(_facebook_organic_suggestions(fb_organic, industry, goals))

    # ── Search Console suggestions ──
    gsc = analysis.get("search_console")
    if gsc:
        suggestions.extend(_gsc_suggestions(gsc, industry, client_config, analysis))

    # ── Cross-platform suggestions ──
    suggestions.extend(_cross_platform_suggestions(analysis))

    # ── Competitor watch suggestions ──
    suggestions.extend(_competitor_watch_suggestions(analysis))

    # ── KPI target-driven suggestions ──
    suggestions.extend(_target_kpi_suggestions(analysis))

    # Sort by priority
    priority_order = {PRIORITY_HIGH: 0, PRIORITY_MEDIUM: 1, PRIORITY_LOW: 2}
    suggestions.sort(key=lambda s: priority_order.get(s["priority"], 99))

    return suggestions


def _ga_suggestions(ga, goals, top_landing_pages=None):
    suggestions = []
    metrics = ga.get("metrics", {})
    scores = ga.get("scores", {})
    mom = ga.get("month_over_month", {})
    period = ga.get("period") or {}
    early_current_month = bool(period.get("is_current_month") and period.get("early_month"))
    landing_pages = top_landing_pages or []

    # ── Landing pages with high bounce / low conversion ──
    problem_pages = []
    for page in landing_pages[:10]:
        sessions = _safe_num(page.get("sessions") or 0)
        bounce = _safe_num(page.get("bounce_rate") or 0)
        conversions = _safe_num(page.get("conversions") or 0)
        path = page.get("page") or page.get("path") or page.get("url") or ""
        if sessions >= 20 and bounce > 70 and conversions == 0:
            problem_pages.append({"path": path, "sessions": sessions, "bounce": bounce, "conversions": conversions})

    if problem_pages:
        page_details = "; ".join(
            f"{p['path']} ({int(p['sessions'])} sessions, {p['bounce']:.0f}% bounce, 0 conversions)"
            for p in problem_pages[:3]
        )
        suggestions.append(make_suggestion(
            f"Fix {len(problem_pages)} Landing Pages Losing All Visitors",
            f"These pages get traffic but nobody converts or sticks around: {page_details}. "
            f"For each page: put the phone number and CTA above the fold, "
            f"tighten the headline to match the search intent, and shorten the contact form "
            f"to name + phone + service only.",
            PRIORITY_HIGH, CATEGORY_WEBSITE, "conversions",
            data_point=f"{len(problem_pages)} pages with 0 conversions"
        ))

    # ── Bounce rate with specific page context ──
    bounce_score = scores.get("bounce_rate", "no_data")
    bounce_rate = _safe_num(metrics.get("bounce_rate", 0))
    if bounce_score in ("below_average", "poor") and bounce_rate:
        worst_bounce = sorted(
            [p for p in landing_pages[:10] if _safe_num(p.get("sessions") or 0) >= 10],
            key=lambda p: _safe_num(p.get("bounce_rate") or 0), reverse=True
        )[:3]
        page_detail = ""
        if worst_bounce:
            page_detail = " Worst pages: " + ", ".join(
                f"{p.get('page') or p.get('path') or '?'} ({_safe_num(p.get('bounce_rate') or 0):.0f}% bounce)"
                for p in worst_bounce
            ) + "."
        suggestions.append(make_suggestion(
            f"Cut Bounce Rate from {bounce_rate:.0f}%",
            f"Site-wide bounce rate is {bounce_rate:.0f}%, above industry average.{page_detail} "
            f"Check that each page loads in under 3 seconds, has a clear CTA above the fold, "
            f"and shows trust signals (reviews, badges, service area) immediately.",
            PRIORITY_HIGH, CATEGORY_WEBSITE, "conversions",
            data_point=f"Bounce rate: {bounce_rate:.0f}%"
        ))

    # ── Session duration ──
    duration_score = scores.get("avg_session_duration", "no_data")
    duration = _safe_num(metrics.get("avg_session_duration", 0))
    if duration_score in ("below_average", "poor") and duration:
        suggestions.append(make_suggestion(
            f"Session Duration is Only {duration:.0f}s",
            f"Visitors spend just {duration:.0f} seconds on the site before leaving. "
            f"Add engaging content: before/after project galleries, video testimonials, "
            f"and detailed FAQ sections on service pages.",
            PRIORITY_MEDIUM, CATEGORY_WEBSITE, "engagement",
            data_point=f"Avg duration: {duration:.0f}s"
        ))

    # ── Pages per session ──
    pps_score = scores.get("pages_per_session", "no_data")
    pps = _safe_num(metrics.get("pages_per_session", 0))
    if pps_score in ("below_average", "poor") and pps:
        suggestions.append(make_suggestion(
            f"Visitors Only View {pps:.1f} Pages",
            f"Pages per session is {pps:.1f}. Add internal links between service pages, "
            f"a 'Related Services' section on each page, and make the navigation menu "
            f"clearly show all service categories.",
            PRIORITY_MEDIUM, CATEGORY_WEBSITE, "engagement",
            data_point=f"Pages/session: {pps:.1f}"
        ))

    # ── Conversion rate with landing page context ──
    conv_rate_score = scores.get("conversion_rate", "no_data")
    conv_rate = _safe_num(metrics.get("conversion_rate", 0))
    sessions = _safe_num(metrics.get("sessions", 0))
    conversions = _safe_num(metrics.get("conversions", 0))
    if conv_rate_score in ("below_average", "poor") and sessions > 0:
        # Find best and worst converting pages
        converting_pages = [p for p in landing_pages[:10] if _safe_num(p.get("conversions") or 0) > 0]
        converting_pages.sort(key=lambda p: _safe_num(p.get("conversions") or 0), reverse=True)
        page_detail = ""
        if converting_pages:
            best = converting_pages[0]
            page_detail = (f" Your best page is {best.get('page') or best.get('path') or '?'} "
                          f"with {int(_safe_num(best.get('conversions') or 0))} conversions. "
                          f"Apply what works there to your other pages.")
        suggestions.append(make_suggestion(
            f"Conversion Rate is {conv_rate:.1f}% - {int(conversions)} Leads from {int(sessions)} Visits",
            f"Only {conv_rate:.1f}% of visitors become leads ({int(conversions)} conversions "
            f"from {int(sessions)} sessions).{page_detail} "
            f"Add click-to-call buttons on every page and simplify the contact form.",
            PRIORITY_HIGH, CATEGORY_WEBSITE, "conversions",
            data_point=f"Conv rate: {conv_rate:.1f}%"
        ))

    # ── Traffic decline with specific numbers ──
    sessions_mom = mom.get("sessions", {})
    if (
        not early_current_month
        and sessions_mom.get("change_pct") is not None
        and sessions_mom["change_pct"] <= -10
    ):
        current = int(_safe_num(sessions_mom.get("current") or 0))
        previous = int(_safe_num(sessions_mom.get("previous") or 0))
        drop = abs(sessions_mom["change_pct"])
        suggestions.append(make_suggestion(
            f"Traffic Dropped {drop:.0f}% - Down to {current} Sessions",
            f"Sessions fell from {previous} to {current} ({drop:.0f}% drop). "
            f"Check if ad spend decreased, review which traffic sources declined in Analytics "
            f"under Acquisition > Traffic acquisition, and check Google Search Console "
            f"for any ranking drops.",
            PRIORITY_HIGH, CATEGORY_STRATEGY, "traffic",
            data_point=f"Sessions: {current} (was {previous})"
        ))

    # ── Device-specific conversion gaps ──
    device_rows = ga.get("device_breakdown") or []
    mobile = next(
        (row for row in device_rows if str(row.get("device") or row.get("deviceCategory") or "").lower() == "mobile"),
        None,
    )
    desktop = next(
        (row for row in device_rows if str(row.get("device") or row.get("deviceCategory") or "").lower() == "desktop"),
        None,
    )
    if mobile and desktop:
        mobile_sessions = _safe_num(mobile.get("sessions") or 0)
        desktop_sessions = _safe_num(desktop.get("sessions") or 0)
        mobile_rate = _safe_num(mobile.get("conversion_rate") or 0)
        desktop_rate = _safe_num(desktop.get("conversion_rate") or 0)
        if (
            mobile_sessions >= 25
            and mobile_sessions >= desktop_sessions * 0.5
            and desktop_rate > 0
            and mobile_rate < desktop_rate * 0.6
        ):
            suggestions.append(make_suggestion(
                "Mobile Visitors Are Converting Worse Than Desktop",
                f"Mobile has {int(mobile_sessions)} sessions at {mobile_rate:.1f}% conversion, "
                f"while desktop is at {desktop_rate:.1f}%. Test the mobile contact flow: sticky call button, "
                "shorter form, faster page load, and service-area proof above the fold.",
                PRIORITY_HIGH, CATEGORY_WEBSITE, "conversions",
                data_point=f"Mobile CVR {mobile_rate:.1f}% vs desktop {desktop_rate:.1f}%"
            ))

    # ── Conversion growth: scale what's working ──
    conversions_mom = mom.get("conversions", {})
    if conversions_mom.get("change_pct") is not None and conversions_mom["change_pct"] >= 20:
        current_conv = int(_safe_num(conversions_mom.get("current") or 0))
        pct_up = conversions_mom["change_pct"]
        suggestions.append(make_suggestion(
            f"Conversions Up {pct_up:.0f}% - Scale What's Working",
            f"Conversions jumped to {current_conv} ({pct_up:.0f}% increase). "
            f"Find which campaigns or traffic sources drove this growth and increase budget there by 15-20%.",
            PRIORITY_MEDIUM, CATEGORY_STRATEGY, "growth",
            data_point=f"Conversions: {current_conv} (+{pct_up:.0f}%)"
        ))

    return suggestions


def _meta_suggestions(meta, industry, goals, client_config):
    suggestions = []
    metrics = meta.get("metrics", {})
    scores = meta.get("scores", {})
    mom = meta.get("month_over_month", {})
    campaign_analysis = meta.get("campaign_analysis", [])
    top_ads = meta.get("top_ads") or []

    # ── Campaign-level analysis: name the worst and best ──
    underperforming = [c for c in campaign_analysis if c.get("status") == "underperforming"]
    performing_camps = []
    for c in campaign_analysis:
        c_metrics = c.get("metrics") or c
        c_spend = _safe_num(c_metrics.get("spend") or 0)
        c_results = _safe_num(c_metrics.get("results") or 0)
        c_cpr = c_spend / c_results if c_results > 0 else None
        if c_results > 0:
            performing_camps.append({"name": c.get("name", "Unknown"), "spend": c_spend,
                                      "results": c_results, "cpr": c_cpr})

    if underperforming:
        details = []
        total_under_spend = 0
        for c in underperforming[:3]:
            c_metrics = c.get("metrics") or c
            c_spend = _safe_num(c_metrics.get("spend") or 0)
            c_results = _safe_num(c_metrics.get("results") or 0)
            total_under_spend += c_spend
            parts = [f"'{c.get('name', 'Unknown')}'"]
            if c_spend:
                parts.append(f"${c_spend:.0f} spent")
            parts.append(f"{c_results:.0f} results")
            if c.get("issue"):
                parts.append(c["issue"])
            details.append(" - ".join(parts))
        campaign_list = "; ".join(details)
        suggestions.append(make_suggestion(
            f"Fix {len(underperforming)} Underperforming Facebook Campaigns",
            f"These campaigns are below benchmarks: {campaign_list}. "
            f"Total at-risk spend: ${total_under_spend:.0f}. "
            f"Check audience overlap, creative fatigue, and landing page alignment for each.",
            PRIORITY_HIGH, CATEGORY_PAID, "campaign_optimization",
            data_point=f"{len(underperforming)} underperforming, ${total_under_spend:.0f} at risk"
        ))

    # ── CTR issues with specific data ──
    ctr_score = scores.get("ctr", "no_data")
    ctr = _safe_num(metrics.get("ctr", 0))
    if ctr_score in ("below_average", "poor") and ctr:
        # Find worst CTR campaigns
        low_ctr_camps = sorted(
            [c for c in campaign_analysis if _safe_num((c.get("metrics") or c).get("ctr") or 0) > 0],
            key=lambda c: _safe_num((c.get("metrics") or c).get("ctr") or 0)
        )[:2]
        camp_detail = ""
        if low_ctr_camps:
            camp_detail = " Worst CTR: " + ", ".join(
                f"'{c.get('name', '?')}' at {_safe_num((c.get('metrics') or c).get('ctr') or 0):.2f}%"
                for c in low_ctr_camps
            ) + "."
        suggestions.append(make_suggestion(
            f"Lift Facebook Ad CTR from {ctr:.2f}%",
            f"Overall CTR is {ctr:.2f}%, below industry benchmark.{camp_detail} "
            f"For home services: use before/after photos of real jobs, feature customer testimonials in video, "
            f"add urgency ('Book this week, save 10%'), and test carousel ads showing completed projects.",
            PRIORITY_HIGH, CATEGORY_CREATIVE, "paid_traffic",
            data_point=f"CTR: {ctr}%"
        ))

    # ── CPC issues with specific data ──
    cpc_score = scores.get("cpc", "no_data")
    cpc = _safe_num(metrics.get("cpc", 0))
    if cpc_score in ("below_average", "poor") and cpc:
        suggestions.append(make_suggestion(
            f"Cut Facebook CPC from ${cpc:.2f}",
            f"Paying ${cpc:.2f} per click. Refine targeting to homeowners in your service area (age 30-65). "
            f"Test lookalike audiences from past converters. "
            f"Try Advantage+ audience to let Meta find cheaper clicks.",
            PRIORITY_HIGH, CATEGORY_PAID, "cost_efficiency",
            data_point=f"CPC: ${cpc}"
        ))

    # ── Frequency fatigue ──
    frequency = _safe_num(metrics.get("frequency", 0))
    if frequency > 3.5:
        suggestions.append(make_suggestion(
            f"Ad Fatigue Alert - Frequency at {frequency:.1f}x",
            f"Your audience is seeing ads {frequency:.1f} times on average (healthy is under 3). "
            f"Rotate in fresh creative immediately. Swap out the main image/video and headline. "
            f"If frequency keeps climbing, expand the audience radius or add new interest segments.",
            PRIORITY_HIGH, CATEGORY_CREATIVE, "ad_efficiency",
            data_point=f"Frequency: {frequency:.1f}x"
        ))

    # ── Cost per result with specific campaign context ──
    results = _safe_num(metrics.get("results", 0))
    spend = _safe_num(metrics.get("spend", 0))
    if results > 0 and spend > 0:
        cpr = spend / results
        if "reduce_cpa" in goals and cpr > 50:
            best_camp = min(performing_camps, key=lambda c: c["cpr"] or 999) if performing_camps else None
            best_detail = ""
            if best_camp and best_camp["cpr"]:
                best_detail = f" Your best campaign is '{best_camp['name']}' at ${best_camp['cpr']:.0f}/lead. "
            suggestions.append(make_suggestion(
                f"Cut Facebook Cost Per Lead from ${cpr:.0f}",
                f"Paying ${cpr:.0f} per lead across ${spend:.0f} total spend.{best_detail}"
                f"Test lead form ads to reduce friction, retarget website visitors, "
                f"and try call ads for faster conversions.",
                PRIORITY_HIGH, CATEGORY_PAID, "cost_efficiency",
                data_point=f"CPL: ${cpr:.2f}"
            ))
        elif cpr < 60 and "increase_leads" in goals:
            suggestions.append(make_suggestion(
                f"Scale Facebook Ads - CPL ${cpr:.0f} is Strong",
                f"At ${cpr:.0f} per lead ({results:.0f} leads on ${spend:.0f} spend), "
                f"there's room to grow. Increase daily budget by 20% every 3-4 days. "
                f"Gradual scaling keeps Meta's algorithm stable.",
                PRIORITY_MEDIUM, CATEGORY_BUDGET, "growth",
                data_point=f"CPL: ${cpr:.2f} on ${spend:.0f} spend"
            ))

    # ── Top performing ads: double down ──
    if top_ads:
        best_ad = top_ads[0]
        best_ad_ctr = _safe_num(best_ad.get("ctr") or 0)
        best_ad_name = best_ad.get("name") or best_ad.get("ad_name") or "Top Ad"
        if best_ad_ctr > 0:
            suggestions.append(make_suggestion(
                f"Clone Your Best Ad - '{best_ad_name}' is Winning",
                f"Ad '{best_ad_name}' has the highest CTR at {best_ad_ctr:.2f}%. "
                f"Duplicate it into a new ad set with a different audience segment. "
                f"Also create 2 variations with the same image but different headlines to A/B test.",
                PRIORITY_MEDIUM, CATEGORY_CREATIVE, "creative_optimization",
                data_point=f"Best ad CTR: {best_ad_ctr:.2f}%"
            ))

    # ── Rising CPM ──
    cpm = _safe_num(metrics.get("cpm", 0))
    cpm_mom = mom.get("cpm", {})
    if cpm_mom.get("change_pct") is not None and cpm_mom["change_pct"] >= 20:
        suggestions.append(make_suggestion(
            f"CPM Jumped {cpm_mom['change_pct']:.0f}% to ${cpm:.0f}",
            f"Cost per 1,000 views rose {cpm_mom['change_pct']:.0f}% to ${cpm:.0f}. "
            f"Could be increased competition or audience saturation. "
            f"Test new audience segments, expand geographic targeting slightly, "
            f"or shift budget to less competitive time slots.",
            PRIORITY_MEDIUM, CATEGORY_PAID, "cost_efficiency",
            data_point=f"CPM: ${cpm:.0f} (+{cpm_mom['change_pct']:.0f}%)"
        ))

    return suggestions


def _facebook_organic_suggestions(fb_organic, industry, goals):
    suggestions = []
    metrics = fb_organic.get("metrics") or {}
    top_posts = fb_organic.get("top_posts") or []
    post_count = fb_organic.get("post_count", 0)

    followers = metrics.get("followers") or 0
    organic_impressions = metrics.get("organic_impressions") or 0
    engaged_users = metrics.get("engaged_users") or 0
    post_engagements = metrics.get("post_engagements") or 0
    new_fans = metrics.get("new_fans") or 0
    lost_fans = metrics.get("lost_fans") or 0
    net_fans = metrics.get("net_fans") or 0
    page_views = metrics.get("page_views") or 0
    engagement_rate = metrics.get("engagement_rate") or 0

    # Low posting frequency
    if post_count < 8:
        suggestions.append(make_suggestion(
            "Increase Facebook Posting Frequency",
            f"Only {post_count} posts this month. For home services, aim for 12-20 posts/month. "
            "Mix content types: job completion photos (before/after), quick tips for homeowners, "
            "team spotlights, customer reviews/shoutouts, seasonal maintenance reminders, "
            "and behind-the-scenes content. Consistency builds trust with local audiences.",
            PRIORITY_HIGH, CATEGORY_ORGANIC, "organic_reach",
            data_point=f"{post_count} posts this month"
        ))

    # Low engagement rate
    if followers > 0 and engagement_rate < 1.0 and post_count > 0:
        suggestions.append(make_suggestion(
            "Boost Organic Engagement Rate",
            f"Engagement rate is {engagement_rate:.1f}%, below the 1-3% benchmark for local businesses. "
            "Try asking questions in posts ('What project are you tackling this weekend?'), "
            "run simple polls, respond to every comment within 2 hours, "
            "post at peak times (typically 9-11am and 7-9pm for local service pages), "
            "and tag customers (with permission) in job completion posts.",
            PRIORITY_HIGH, CATEGORY_ORGANIC, "engagement",
            data_point=f"Engagement rate: {engagement_rate:.1f}%"
        ))

    # Losing followers
    if net_fans < 0:
        suggestions.append(make_suggestion(
            "Address Follower Decline",
            f"Lost {abs(net_fans)} net followers this month ({new_fans} gained, {lost_fans} lost). "
            "Review recent content for anything that may be turning people off. "
            "Focus on value-driven posts: seasonal tips, free advice, community involvement. "
            "Avoid posting only promotions; aim for 80% helpful content, 20% sales.",
            PRIORITY_HIGH, CATEGORY_ORGANIC, "audience_growth",
            data_point=f"Net fans: {net_fans}"
        ))

    # Low organic reach relative to followers
    if followers > 100 and organic_impressions > 0:
        reach_pct = (organic_impressions / followers) * 100 if followers else 0
        if reach_pct < 20:
            suggestions.append(make_suggestion(
                "Improve Organic Reach",
                f"Organic impressions reached {reach_pct:.0f}% of followers. Facebook's algorithm "
                "favors engagement-heavy content. Try Reels/short videos (30-60 seconds of a job in progress), "
                "carousel posts with multiple project photos, and posts that spark conversation. "
                "Also consider going Live once a month for a quick Q&A or job walkthrough.",
                PRIORITY_MEDIUM, CATEGORY_ORGANIC, "organic_reach",
                data_point=f"Reach: {reach_pct:.0f}% of {followers} followers"
            ))

    # Best performing post analysis
    if top_posts:
        best = top_posts[0]
        best_type = best.get("type", "post")
        best_eng = best.get("engagement_rate", 0)
        if best_eng > 3:
            suggestions.append(make_suggestion(
                "Double Down on Top-Performing Content Type",
                f"Your best post this month ({best_type}) hit {best_eng:.1f}% engagement. "
                "Create more content in this style. Analyze what made it work: "
                "was it the visual, the caption, the timing, or the topic? "
                "Replicate the winning formula 2-3 times in the coming month.",
                PRIORITY_MEDIUM, CATEGORY_ORGANIC, "content_strategy",
                data_point=f"Top post engagement: {best_eng:.1f}%"
            ))

    # Low page views
    if page_views < 50 and followers > 100:
        suggestions.append(make_suggestion(
            "Drive More Facebook Page Visits",
            f"Only {page_views} page views this month. Make sure your page info is complete: "
            "business hours, service area, phone number, website link, and a clear CTA button. "
            "Pin your best review or a seasonal offer to the top of the page. "
            "Cross-link your Facebook page from your website, email signature, and Google Business Profile.",
            PRIORITY_LOW, CATEGORY_ORGANIC, "page_visibility",
            data_point=f"{page_views} page views"
        ))

    # Audience growth opportunity
    if followers > 0 and new_fans > 0 and net_fans >= 0 and "increase_leads" in goals:
        suggestions.append(make_suggestion(
            "Leverage Organic Growth for Lead Generation",
            f"Gained {new_fans} new followers this month. Convert followers into leads: "
            "post special offers exclusively for Facebook followers, use Facebook Events for "
            "seasonal promotions, and add a 'Book Now' or 'Get Quote' CTA button to your page. "
            "Share customer success stories with a clear call-to-action.",
            PRIORITY_MEDIUM, CATEGORY_ORGANIC, "lead_generation",
            data_point=f"+{new_fans} new followers"
        ))

    return suggestions


def _gsc_suggestions(gsc, industry, client_config, analysis=None):
    suggestions = []
    metrics = gsc.get("metrics", {})
    scores = gsc.get("scores", {})
    opportunities = gsc.get("keyword_opportunities", [])
    top_queries = gsc.get("top_queries", [])
    top_pages = gsc.get("top_pages") or []

    # ── Keyword opportunities: name the exact queries and positions ──
    if opportunities:
        top_opps = opportunities[:5]
        opp_details = []
        for o in top_opps:
            query = o.get("query", "?")
            pos = _safe_num(o.get("position") or 0)
            impr = int(_safe_num(o.get("impressions") or 0))
            clicks = int(_safe_num(o.get("clicks") or 0))
            page = o.get("page") or ""
            detail = f"'{query}' at position {pos:.0f} ({impr} impressions, {clicks} clicks)"
            if page:
                detail += f" on {page}"
            opp_details.append(detail)
        opp_list = "; ".join(opp_details)
        suggestions.append(make_suggestion(
            f"Push {len(top_opps)} Keywords from Page 2 to Page 1",
            f"These queries have strong search volume but rank just off page 1: {opp_list}. "
            f"For each one: check if a dedicated page exists. If not, create one. "
            f"If yes, add the query to the title tag and H1, build 2-3 internal links to it, "
            f"and add 200+ words of supporting content.",
            PRIORITY_HIGH, CATEGORY_SEO, "organic_traffic",
            data_point=f"{len(opportunities)} keyword opportunities found"
        ))

    # ── Top queries performing well: protect and expand ──
    top_performers = [q for q in top_queries[:10]
                      if _safe_num(q.get("position") or 99) <= 5 and _safe_num(q.get("clicks") or 0) > 5]
    if top_performers:
        perf_list = ", ".join(
            f"'{q.get('query', '?')}' (pos {_safe_num(q.get('position') or 0):.0f}, {int(_safe_num(q.get('clicks') or 0))} clicks)"
            for q in top_performers[:4]
        )
        suggestions.append(make_suggestion(
            f"Protect Your Top {len(top_performers)} Ranking Keywords",
            f"These queries are driving real traffic from top positions: {perf_list}. "
            f"Make sure the landing pages for these queries have strong calls-to-action, "
            f"fast load times, and fresh content. Any drop in these rankings would directly hurt leads.",
            PRIORITY_MEDIUM, CATEGORY_SEO, "organic_traffic",
            data_point=f"{len(top_performers)} keywords in top 5"
        ))

    # ── High-impression, low-click queries: CTR optimization ──
    low_ctr_queries = [q for q in top_queries[:20]
                       if _safe_num(q.get("impressions") or 0) > 100
                       and _safe_num(q.get("ctr") or 0) < 2.0
                       and _safe_num(q.get("position") or 99) <= 10]
    if low_ctr_queries:
        low_ctr_queries.sort(key=lambda q: _safe_num(q.get("impressions") or 0), reverse=True)
        detail_list = "; ".join(
            f"'{q.get('query', '?')}' ({int(_safe_num(q.get('impressions') or 0))} impressions, "
            f"{_safe_num(q.get('ctr') or 0):.1f}% CTR, pos {_safe_num(q.get('position') or 0):.0f})"
            for q in low_ctr_queries[:4]
        )
        suggestions.append(make_suggestion(
            f"Fix Low CTR on {len(low_ctr_queries)} Page-1 Keywords",
            f"These keywords rank on page 1 but aren't getting clicked: {detail_list}. "
            f"Rewrite the title tags and meta descriptions for these pages. "
            f"Add review stars, service area, and a CTA like 'Free Estimate' to the meta description.",
            PRIORITY_HIGH, CATEGORY_SEO, "organic_traffic",
            data_point=f"{len(low_ctr_queries)} keywords with low CTR"
        ))

    # ── Average position ──
    avg_pos = _safe_num(metrics.get("avg_position", 0))
    if avg_pos > 15:
        suggestions.append(make_suggestion(
            f"Overall SEO Visibility Weak - Avg Position {avg_pos:.0f}",
            f"Your average search position is {avg_pos:.0f} (page 2+). "
            f"{_seo_foundation_detail(analysis)}",
            PRIORITY_HIGH, CATEGORY_SEO, "organic_traffic",
            data_point=f"Avg position: {avg_pos:.0f}"
        ))

    # ── Top pages with issues ──
    if top_pages:
        low_ctr_pages = [p for p in top_pages[:10]
                         if _safe_num(p.get("impressions") or 0) > 200
                         and _safe_num(p.get("ctr") or 0) < 2.0]
        if low_ctr_pages:
            page_detail = "; ".join(
                f"{p.get('page') or p.get('url') or '?'} ({int(_safe_num(p.get('impressions') or 0))} impressions, "
                f"{_safe_num(p.get('ctr') or 0):.1f}% CTR)"
                for p in low_ctr_pages[:3]
            )
            suggestions.append(make_suggestion(
                f"Rewrite Title Tags on {len(low_ctr_pages)} High-Impression Pages",
                f"These pages show up in search a lot but get very few clicks: {page_detail}. "
                f"The title tag and meta description need to be rewritten to match search intent "
                f"and include a compelling reason to click (free estimates, same-day service, etc).",
                PRIORITY_MEDIUM, CATEGORY_SEO, "organic_traffic",
                data_point=f"{len(low_ctr_pages)} pages with low CTR"
            ))

    # ── Content gap: high impressions, very few clicks ──
    clicks = _safe_num(metrics.get("clicks", 0))
    impressions = _safe_num(metrics.get("impressions", 0))
    ctr = _safe_num(metrics.get("ctr", 0))
    if impressions > 1000 and clicks < 50:
        suggestions.append(make_suggestion(
            f"{int(impressions)} Search Impressions But Only {int(clicks)} Clicks",
            f"Your site is showing in search results {int(impressions)} times but only getting "
            f"{int(clicks)} clicks ({ctr:.1f}% CTR). This means people see your listing and skip it. "
            f"Audit your title tags and meta descriptions across the top 10 pages.",
            PRIORITY_HIGH, CATEGORY_SEO, "organic_traffic",
            data_point=f"{int(impressions)} impressions, {int(clicks)} clicks ({ctr:.1f}% CTR)"
        ))

    # ── Local SEO for home services ──
    services = client_config.get("primary_services", [])
    service_area = client_config.get("service_area", "")
    if services and service_area:
        has_local = any(
            service_area.lower() in q.get("query", "").lower()
            for q in top_queries[:20]
        )
        if not has_local:
            suggestions.append(make_suggestion(
                f"Create '{services[0]} in {service_area}' Pages",
                f"No local search queries with '{service_area}' are ranking. Create dedicated pages for "
                f"each service + location combo (e.g., '{services[0]} in {service_area}'). "
                f"Include Google Maps embed, nearby neighborhoods, and local schema markup.",
                PRIORITY_MEDIUM, CATEGORY_SEO, "local_visibility",
            ))

    return suggestions


def _google_ads_suggestions(google_ads, industry, goals):
    suggestions = []
    metrics = google_ads.get("metrics", {})
    scores = google_ads.get("scores", {})
    campaign_analysis = google_ads.get("campaign_analysis", [])
    search_terms = google_ads.get("search_terms") or []

    # ── Wasted search term spend (zero-conversion terms) ──
    wasted_terms = []
    for term in search_terms:
        cost = _safe_num(term.get("cost") or term.get("spend") or 0)
        conversions = _safe_num(term.get("conversions") or term.get("results") or 0)
        if cost > 5 and conversions == 0:
            wasted_terms.append({"term": term.get("search_term") or term.get("query") or term.get("term", "?"), "cost": cost})
    wasted_terms.sort(key=lambda t: t["cost"], reverse=True)

    if wasted_terms:
        total_waste = sum(t["cost"] for t in wasted_terms)
        top_waste = wasted_terms[:5]
        term_list = ", ".join(f"'{t['term']}' (${t['cost']:.0f})" for t in top_waste)
        suggestions.append(make_suggestion(
            f"Block ${total_waste:.0f} in Wasted Search Terms",
            f"These search terms cost ${total_waste:.0f} total this month with zero conversions: "
            f"{term_list}. "
            f"Add every one of these as a negative keyword at account level immediately. "
            f"This is money going straight to clicks that never turn into calls or leads.",
            PRIORITY_HIGH, CATEGORY_PAID, "cost_efficiency",
            data_point=f"${total_waste:.0f} wasted on {len(wasted_terms)} zero-conversion terms"
        ))

    # ── Campaign-level detail: name the worst and best ──
    performing = []
    underperforming = []
    for camp in campaign_analysis:
        camp_metrics = camp.get("metrics") or {}
        camp_spend = _safe_num(camp_metrics.get("spend") or camp_metrics.get("cost") or 0)
        camp_results = _safe_num(camp_metrics.get("results") or camp_metrics.get("conversions") or 0)
        camp_cpa = camp_spend / camp_results if camp_results > 0 else None
        camp_name = camp.get("name", "Unknown")
        entry = {"name": camp_name, "spend": camp_spend, "results": camp_results, "cpa": camp_cpa,
                 "ctr": _safe_num(camp_metrics.get("ctr") or 0), "cpc": _safe_num(camp_metrics.get("cpc") or 0),
                 "status": camp.get("status", "ok"), "issue": camp.get("issue", "")}
        if camp.get("status") == "underperforming":
            underperforming.append(entry)
        elif camp_results > 0:
            performing.append(entry)

    if underperforming:
        underperforming.sort(key=lambda c: c["spend"], reverse=True)
        details = []
        for c in underperforming[:3]:
            parts = [f"'{c['name']}'"]
            if c["spend"]:
                parts.append(f"${c['spend']:.0f} spent")
            if c["results"]:
                parts.append(f"{c['results']:.0f} conversions")
            else:
                parts.append("0 conversions")
            if c["issue"]:
                parts.append(c["issue"])
            details.append(" - ".join(parts))
        total_under_spend = sum(c["spend"] for c in underperforming)
        campaign_list = "; ".join(details)
        suggestions.append(make_suggestion(
            f"Fix {len(underperforming)} Underperforming Campaigns (${total_under_spend:.0f} at risk)",
            f"These campaigns are spending money below benchmark: {campaign_list}. "
            f"Pause the worst performer if it has zero conversions. For the others, check the landing page, "
            f"tighten the keyword match types, and review the ad copy relevance.",
            PRIORITY_HIGH, CATEGORY_PAID, "campaign_optimization",
            data_point=f"{len(underperforming)} campaigns, ${total_under_spend:.0f} total spend"
        ))

    if performing:
        performing.sort(key=lambda c: c["cpa"] or 999)
        best = performing[0]
        if best["cpa"]:
            suggestions.append(make_suggestion(
                f"Scale '{best['name']}' - Best CPA at ${best['cpa']:.0f}",
                f"Campaign '{best['name']}' is your best performer at ${best['cpa']:.0f} per lead "
                f"({best['results']:.0f} conversions on ${best['spend']:.0f} spend). "
                f"Increase this campaign's daily budget by 15-20% to capture more of this high-intent traffic. "
                f"Monitor CPA for 5 days after the increase to make sure it holds.",
                PRIORITY_MEDIUM, CATEGORY_BUDGET, "growth",
                data_point=f"CPA: ${best['cpa']:.2f}, {best['results']:.0f} conversions"
            ))

    # ── High spend, zero results campaigns ──
    money_pits = [c for c in campaign_analysis
                  if _safe_num((c.get("metrics") or {}).get("spend") or 0) > 50
                  and _safe_num((c.get("metrics") or {}).get("results") or (c.get("metrics") or {}).get("conversions") or 0) == 0]
    if money_pits:
        for camp in money_pits[:2]:
            camp_m = camp.get("metrics") or {}
            camp_spend = _safe_num(camp_m.get("spend") or 0)
            camp_name = camp.get("name", "Unknown")
            suggestions.append(make_suggestion(
                f"Pause '{camp_name}' - ${camp_spend:.0f} Spent, Zero Leads",
                f"Campaign '{camp_name}' burned ${camp_spend:.0f} this month and generated zero conversions. "
                f"Pause this campaign today. Before restarting it, review the landing page conversion rate, "
                f"check that the keywords match actual buyer intent (not informational/DIY searches), "
                f"and rewrite the ad copy to match the exact service the landing page offers.",
                PRIORITY_HIGH, CATEGORY_PAID, "cost_efficiency",
                data_point=f"${camp_spend:.0f} spent, 0 conversions"
            ))

    # ── Overall CPC above benchmark ──
    cpc_score = scores.get("cpc", "no_data")
    cpc = metrics.get("cpc", 0)
    spend = _safe_num(metrics.get("spend", 0))
    clicks = _safe_num(metrics.get("clicks", 0))
    if cpc_score in ("below_average", "poor") and cpc:
        high_cpc_terms = [t for t in search_terms if _safe_num(t.get("cpc") or t.get("cost_per_click") or 0) > cpc * 1.5]
        high_cpc_terms.sort(key=lambda t: _safe_num(t.get("cpc") or t.get("cost_per_click") or 0), reverse=True)
        term_detail = ""
        if high_cpc_terms[:3]:
            term_detail = " Worst offenders: " + ", ".join(
                f"'{t.get('search_term') or t.get('query') or t.get('term', '?')}' at "
                f"${_safe_num(t.get('cpc') or t.get('cost_per_click') or 0):.2f}/click"
                for t in high_cpc_terms[:3]
            ) + "."
        suggestions.append(make_suggestion(
            f"Cut CPC from ${cpc:.2f} - Above Benchmark",
            f"Average CPC is ${cpc:.2f} across ${spend:.0f} total spend.{term_detail} "
            f"Move expensive broad match keywords to phrase or exact match. "
            f"Improve Quality Score by tightening ad copy to match keyword intent.",
            PRIORITY_HIGH, CATEGORY_PAID, "cost_efficiency",
            data_point=f"CPC: ${cpc:.2f}"
        ))

    # ── Overall CTR below benchmark ──
    ctr_score = scores.get("ctr", "no_data")
    ctr = metrics.get("ctr", 0)
    if ctr_score in ("below_average", "poor") and ctr:
        suggestions.append(make_suggestion(
            f"Lift Google Ads CTR from {ctr:.1f}%",
            f"CTR is {ctr:.1f}% - below industry benchmark. Low CTR means your ads aren't compelling enough "
            f"for the searches they're showing on. Split ad groups so each group targets one tight theme. "
            f"Write headlines that mirror the exact search query. Add sitelinks, callouts, and call extensions "
            f"to take up more space on the results page.",
            PRIORITY_HIGH, CATEGORY_PAID, "paid_traffic",
            data_point=f"CTR: {ctr}%"
        ))

    # ── CPA above benchmark ──
    cpa_score = scores.get("cost_per_result", "no_data")
    cpa = metrics.get("cost_per_result", 0)
    results = _safe_num(metrics.get("results", 0))
    if cpa_score in ("below_average", "poor") and cpa:
        suggestions.append(make_suggestion(
            f"Cut Cost Per Lead from ${cpa:.0f}",
            f"Paying ${cpa:.0f} per conversion ({results:.0f} conversions on ${spend:.0f} spend). "
            f"The fastest fix: block the wasted search terms above, then check landing page load speed and "
            f"make sure the phone number and form are above the fold on mobile.",
            PRIORITY_HIGH, CATEGORY_PAID, "cost_efficiency",
            data_point=f"CPA: ${cpa:.0f}"
        ))

    # ── Scaling opportunity ──
    if results > 0 and spend > 0:
        current_cpa = spend / results
        if current_cpa < 60 and "increase_leads" in goals:
            best_camp_name = performing[0]["name"] if performing else "your best campaign"
            suggestions.append(make_suggestion(
                f"Scale Ads - CPA is ${current_cpa:.0f}, Room to Grow",
                f"At ${current_cpa:.0f} per lead, you have room to spend more. "
                f"Increase budget on '{best_camp_name}' by 15-20%. "
                f"This could generate {int(spend * 0.2 / current_cpa)} extra leads per month "
                f"at roughly the same cost per lead.",
                PRIORITY_MEDIUM, CATEGORY_BUDGET, "growth",
                data_point=f"CPA: ${current_cpa:.2f}"
            ))

    return suggestions


def _safe_num(val):
    try:
        return float(val or 0)
    except (ValueError, TypeError):
        return 0.0


def _gbp_snapshot(analysis):
    gbp = analysis.get("google_business_profile") or analysis.get("gbp") or {}
    return gbp if isinstance(gbp, dict) else {}


def _gbp_is_effectively_healthy(analysis):
    gbp = _gbp_snapshot(analysis)
    if not gbp or gbp.get("error") or not gbp.get("connected"):
        return False

    completeness = gbp.get("completeness") or {}
    audit = gbp.get("audit") or {}
    completeness_score = _safe_num(completeness.get("score") or 0)
    audit_score = _safe_num(audit.get("overall_score") or 0)
    quests = audit.get("quests") or []
    critical_fields = {"verification", "address", "phone", "website", "hours"}
    critical_gaps = [
        quest for quest in quests
        if quest.get("field") in critical_fields and not quest.get("complete")
    ]

    if completeness_score >= 100 and not critical_gaps:
        return True
    return audit_score >= 85 and completeness_score >= 88 and not critical_gaps


def _seo_foundation_detail(analysis):
    if _gbp_is_effectively_healthy(analysis):
        return (
            "Focus on local citations, service-area page coverage, and customer reviews "
            "that mention specific services and your city name."
        )
    return (
        "Focus on Google Business Profile optimization, building local citations, "
        "and generating customer reviews that mention specific services and your city name."
    )


def _organic_growth_start_detail(analysis):
    if _gbp_is_effectively_healthy(analysis):
        return "Start with service-area page coverage, local citations, and local SEO improvements."
    return "Start with Google Business Profile optimization and local SEO."


def _cross_platform_suggestions(analysis):
    """Suggestions that span multiple data sources."""
    suggestions = []
    ga = analysis.get("google_analytics")
    meta = analysis.get("meta_business")
    google_ads = analysis.get("google_ads")
    gsc = analysis.get("search_console")
    roas = analysis.get("roas", {})
    goals = analysis.get("client_config", {}).get("goals", [])
    grade = analysis.get("overall_grade", "N/A")

    # Overall performance check
    if grade in ("D", "F"):
        suggestions.append(make_suggestion(
            "Overall Performance Needs Attention",
            f"Overall grade is {grade}. Multiple metrics are below industry benchmarks. "
            "Schedule an account audit to review: targeting strategy, creative assets, "
            "landing page experience, and budget allocation across channels.",
            PRIORITY_HIGH, CATEGORY_STRATEGY, "overall_performance",
        ))

    # Cost per conversion
    if roas.get("cost_per_conversion"):
        cpc = roas["cost_per_conversion"]
        total_conv = roas["total_conversions"]
        total_spend = roas["total_spend"]
        if cpc > 100:
            suggestions.append(make_suggestion(
                "High Overall Cost Per Conversion",
                f"Across all channels, cost per conversion is ${cpc:.2f} "
                f"({total_conv} conversions on ${total_spend:.2f} total spend). "
                "Review which channels produce the cheapest conversions and shift "
                "budget accordingly. For home services, Google Search typically "
                "has higher intent and better conversion rates than social.",
                PRIORITY_HIGH, CATEGORY_BUDGET, "cost_efficiency",
                data_point=f"${cpc:.2f}/conversion"
            ))

    # Channel mix
    if ga and (meta or google_ads):
        ga_sessions = ga.get("metrics", {}).get("sessions", 0)
        ga_conversions = ga.get("metrics", {}).get("conversions", 0)
        paid_results = 0
        if meta:
            paid_results += meta.get("metrics", {}).get("results", 0)
        if google_ads:
            paid_results += google_ads.get("metrics", {}).get("results", 0)

        if ga_conversions > 0 and paid_results > 0:
            organic_conv_pct = ga_conversions / (ga_conversions + paid_results) * 100
            if organic_conv_pct > 60:
                suggestions.append(make_suggestion(
                    "Strong Organic - Reduce Paid Dependency",
                    f"Organic drives {organic_conv_pct:.0f}% of conversions. "
                    "Invest more in SEO to compound this advantage. "
                    "Consider shifting 10-15% of paid budget to content creation "
                    "and link building.",
                    PRIORITY_MEDIUM, CATEGORY_STRATEGY, "channel_mix",
                ))
            elif organic_conv_pct < 20:
                suggestions.append(make_suggestion(
                    "Over-Reliant on Paid Traffic",
                    f"Only {organic_conv_pct:.0f}% of conversions come from organic. "
                    "This creates risk if ad costs rise or budgets get cut. "
                    "Invest in SEO as a long-term lead generation channel. "
                    f"{_organic_growth_start_detail(analysis)}",
                    PRIORITY_MEDIUM, CATEGORY_STRATEGY, "channel_mix",
                ))

    # Seasonal suggestions for home services
    suggestions.extend(_seasonal_suggestions(analysis))

    return suggestions


def _seasonal_suggestions(analysis):
    """Home services seasonal recommendations."""
    suggestions = []
    month = analysis.get("month", "")
    industry = analysis.get("industry", "")

    if not month:
        return suggestions

    month_num = int(month.split("-")[1]) if "-" in month else 0

    seasonal_map = {
        "hvac": {
            (3, 4, 5): "Spring is peak AC season prep. Ramp up AC maintenance/tune-up campaigns. Target 'AC repair' and 'AC installation' keywords.",
            (6, 7, 8): "Peak AC season. Maximize budget on emergency repair and replacement campaigns. Increase ad schedule to evenings/weekends.",
            (9, 10): "Transition to heating season. Start furnace tune-up campaigns. Target 'heater not working' keywords.",
            (11, 12, 1, 2): "Peak heating season. Push furnace repair and emergency heating campaigns. Emphasize 24/7 availability.",
        },
        "plumbing": {
            (3, 4, 5): "Spring thaw can cause pipe issues. Target water heater and sump pump campaigns.",
            (6, 7, 8): "Outdoor plumbing and sprinkler system installations. Push remodel-related plumbing.",
            (11, 12, 1, 2): "Frozen pipe season. Push emergency plumbing, winterization, and pipe burst campaigns.",
        },
        "roofing": {
            (3, 4, 5): "Storm damage season starting. Ramp up roof inspection and repair campaigns.",
            (6, 7, 8): "Peak roofing season. Push replacement campaigns and financing offers.",
            (9, 10): "Pre-winter roof inspections. Target 'roof inspection before winter' keywords.",
        },
        "landscaping": {
            (2, 3): "Early spring - push spring cleanup and lawn care plan sign-ups.",
            (4, 5, 6): "Peak season. Maximize budget on design/install and maintenance campaigns.",
            (9, 10): "Fall cleanup campaigns. Push leaf removal and fall planting.",
            (11, 12): "Off-season. Reduce ad spend, focus on holiday lighting or snow removal if applicable.",
        },
        "pest_control": {
            (3, 4, 5): "Bug season starting. Push preventive treatment and ant/termite campaigns.",
            (6, 7, 8): "Peak bug and mosquito season. Maximize budget. Target specific pest types.",
            (9, 10): "Rodent season prep. Push mice/rat prevention campaigns as weather cools.",
        },
    }

    industry_seasons = seasonal_map.get(industry, {})
    for months_tuple, advice in industry_seasons.items():
        if month_num in months_tuple:
            suggestions.append(make_suggestion(
                f"Seasonal Strategy - {industry.replace('_', ' ').title()}",
                advice,
                PRIORITY_MEDIUM, CATEGORY_STRATEGY, "seasonal",
            ))
            break

    return suggestions


def _target_kpi_suggestions(analysis):
    """Suggestions tied to brand KPI targets (CPA/leads/ROAS)."""
    suggestions = []
    kpi_status = analysis.get("kpi_status", {})
    evaluation = kpi_status.get("evaluation", {}) if isinstance(kpi_status, dict) else {}

    cpa_eval = evaluation.get("cpa") if isinstance(evaluation, dict) else None
    if isinstance(cpa_eval, dict) and cpa_eval.get("on_track") is False:
        suggestions.append(make_suggestion(
            "Close CPA Gap to Target",
            f"Current blended CPA is ${cpa_eval.get('actual')} vs target ${cpa_eval.get('target')}. "
            "Focus next sprint on search term pruning, budget shifts to top-converting campaigns, "
            "and a landing page CRO pass on highest-spend traffic.",
            PRIORITY_HIGH, CATEGORY_BUDGET, "cost_efficiency",
            data_point=f"CPA gap: {cpa_eval.get('gap_pct')}%"
        ))

    leads_eval = evaluation.get("leads") if isinstance(evaluation, dict) else None
    if isinstance(leads_eval, dict) and leads_eval.get("on_track") is False:
        suggestions.append(make_suggestion(
            "Recover Lead Volume to Target",
            f"Paid leads are {leads_eval.get('actual')} vs target {leads_eval.get('target')}. "
            "Increase impression share in high-intent campaigns and launch 1-2 new offer variants "
            "to improve click-to-lead conversion.",
            PRIORITY_HIGH, CATEGORY_STRATEGY, "growth",
            data_point=f"Lead gap: {leads_eval.get('gap_pct')}%"
        ))

    roas_eval = evaluation.get("roas") if isinstance(evaluation, dict) else None
    if isinstance(roas_eval, dict) and roas_eval.get("target"):
        if roas_eval.get("actual") is None:
            suggestions.append(make_suggestion(
                "Enable Revenue Tracking for True ROAS",
                "ROAS target is configured, but revenue events are not connected. "
                "Connect CRM revenue or offline conversion values so bidding and reporting can optimize to actual return.",
                PRIORITY_MEDIUM, CATEGORY_STRATEGY, "measurement"
            ))
        elif roas_eval.get("on_track") is False:
            suggestions.append(make_suggestion(
                "Recover ROAS to Target",
                f"Blended ROAS is {roas_eval.get('actual')}x vs target {roas_eval.get('target')}x. "
                "Shift spend toward the highest-margin campaigns and tighten low-intent traffic segments.",
                PRIORITY_HIGH, CATEGORY_BUDGET, "profitability",
                data_point=f"ROAS gap: {roas_eval.get('gap_pct')}%"
            ))

    return suggestions


def _competitor_watch_suggestions(analysis):
    suggestions = []
    competitor_watch = analysis.get("competitor_watch")
    if not isinstance(competitor_watch, dict):
        return suggestions

    competitors = competitor_watch.get("competitors", [])
    signals = competitor_watch.get("signals", [])
    counter_moves = competitor_watch.get("counter_moves", [])

    for signal in signals[:2]:
        severity = (signal.get("severity") or "low").lower()
        priority = PRIORITY_HIGH if severity == "high" else PRIORITY_MEDIUM if severity == "medium" else PRIORITY_LOW
        suggestions.append(make_suggestion(
            f"Competitor Watch - {signal.get('title', 'Market signal')}",
            signal.get("detail", ""),
            priority,
            CATEGORY_STRATEGY,
            "competitive_positioning",
            data_point=f"Competitors tracked: {', '.join(competitors[:4])}" if competitors else None,
        ))

    for move in counter_moves[:3]:
        move_priority = (move.get("priority") or "medium").lower()
        priority = PRIORITY_HIGH if move_priority == "high" else PRIORITY_LOW if move_priority == "low" else PRIORITY_MEDIUM
        suggestions.append(make_suggestion(
            move.get("title", "Competitor counter-move"),
            move.get("detail", ""),
            priority,
            CATEGORY_STRATEGY,
            "competitive_positioning",
        ))

    return suggestions


def format_suggestions_for_internal(suggestions):
    """Format suggestions for the internal team report."""
    formatted = []
    for i, s in enumerate(suggestions, 1):
        formatted.append({
            "number": i,
            "title": s["title"],
            "detail": s["detail"],
            "priority": s["priority"].upper(),
            "category": s["category"].replace("_", " ").title(),
            "impact_area": s["impact_area"].replace("_", " ").title(),
            "data_point": s.get("data_point", ""),
        })
    return formatted


def format_suggestions_for_client(suggestions):
    """
    Format suggestions for client report - simplified, no jargon, action-oriented.
    Only include high and medium priority.
    """
    formatted = []
    for s in suggestions:
        if s["priority"] not in (PRIORITY_HIGH, PRIORITY_MEDIUM):
            continue

        # Simplify for client
        client_detail = s["detail"]
        # Remove overly technical terms for client-facing
        client_detail = client_detail.replace("CTR", "click-through rate")
        client_detail = client_detail.replace("CPC", "cost per click")
        client_detail = client_detail.replace("CPM", "cost per thousand views")
        client_detail = client_detail.replace("CPA", "cost per lead")
        client_detail = client_detail.replace("CPL", "cost per lead")
        client_detail = client_detail.replace("MoM", "compared to last month")
        client_detail = client_detail.replace("ROAS", "return on ad spend")

        formatted.append({
            "title": s["title"],
            "detail": client_detail,
            "priority": "Recommended" if s["priority"] == PRIORITY_HIGH else "Suggested",
        })

    return formatted[:8]  # Cap at 8 for client reports
