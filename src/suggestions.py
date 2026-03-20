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
        suggestions.extend(_ga_suggestions(ga, goals))

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
        suggestions.extend(_gsc_suggestions(gsc, industry, client_config))

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


def _ga_suggestions(ga, goals):
    suggestions = []
    metrics = ga.get("metrics", {})
    scores = ga.get("scores", {})
    mom = ga.get("month_over_month", {})

    # Bounce rate
    bounce_score = scores.get("bounce_rate", "no_data")
    bounce_rate = metrics.get("bounce_rate", 0)
    if bounce_score in ("below_average", "poor"):
        suggestions.append(make_suggestion(
            "Reduce Landing Page Bounce Rate",
            f"Bounce rate is {bounce_rate}%, which is above industry average. "
            "Review top landing pages and ensure: clear call-to-action above the fold, "
            "phone number is clickable on mobile, page loads in under 3 seconds, "
            "service area and trust signals (reviews, badges) are visible immediately. "
            "For home services, customers want to see pricing ranges and availability fast.",
            PRIORITY_HIGH, CATEGORY_WEBSITE, "conversions",
            data_point=f"Bounce rate: {bounce_rate}%"
        ))

    # Session duration
    duration_score = scores.get("avg_session_duration", "no_data")
    duration = metrics.get("avg_session_duration", 0)
    if duration_score in ("below_average", "poor"):
        suggestions.append(make_suggestion(
            "Improve Website Engagement",
            f"Average session duration is {duration}s. Add engaging content: "
            "before/after project galleries, video testimonials from real customers, "
            "detailed service pages with FAQs. For home services, customers spend more "
            "time on sites that show proof of quality work.",
            PRIORITY_MEDIUM, CATEGORY_WEBSITE, "engagement",
            data_point=f"Avg duration: {duration}s"
        ))

    # Pages per session
    pps_score = scores.get("pages_per_session", "no_data")
    pps = metrics.get("pages_per_session", 0)
    if pps_score in ("below_average", "poor"):
        suggestions.append(make_suggestion(
            "Improve Internal Linking and Site Navigation",
            f"Pages per session is {pps}. Improve internal linking between service pages. "
            "Add related services sections, link from blog posts to service pages, "
            "and ensure the navigation menu clearly shows all service categories.",
            PRIORITY_MEDIUM, CATEGORY_WEBSITE, "engagement",
            data_point=f"Pages/session: {pps}"
        ))

    # Conversion rate
    conv_rate_score = scores.get("conversion_rate", "no_data")
    conv_rate = metrics.get("conversion_rate", 0)
    if conv_rate_score in ("below_average", "poor"):
        suggestions.append(make_suggestion(
            "Optimize Conversion Funnel",
            f"Conversion rate is {conv_rate}%. Key fixes for home services sites: "
            "add click-to-call buttons on every page, simplify the contact form "
            "(name, phone, service needed - nothing more), add live chat, "
            "display reviews prominently, and add urgency elements for emergency services.",
            PRIORITY_HIGH, CATEGORY_WEBSITE, "conversions",
            data_point=f"Conv rate: {conv_rate}%"
        ))

    # Traffic decline
    sessions_mom = mom.get("sessions", {})
    if sessions_mom.get("change_pct") is not None and sessions_mom["change_pct"] <= -10:
        suggestions.append(make_suggestion(
            "Investigate Traffic Decline",
            f"Sessions dropped {abs(sessions_mom['change_pct'])}% from last month. "
            "Check: Did ad spend decrease? Were there seasonal fluctuations? "
            "Any Google algorithm updates? Review source/medium breakdown to identify "
            "which traffic sources declined most.",
            PRIORITY_HIGH, CATEGORY_STRATEGY, "traffic",
            data_point=f"Sessions: {sessions_mom['current']} (was {sessions_mom['previous']})"
        ))

    # Conversion growth
    conversions_mom = mom.get("conversions", {})
    if conversions_mom.get("change_pct") is not None and conversions_mom["change_pct"] >= 20:
        suggestions.append(make_suggestion(
            "Scale What's Working - Conversions Up",
            f"Conversions increased {conversions_mom['change_pct']}%. "
            "Identify which sources/campaigns drove the growth and allocate more budget there. "
            "Consider increasing ad spend on the top-performing campaigns by 15-20%.",
            PRIORITY_MEDIUM, CATEGORY_STRATEGY, "growth",
            data_point=f"Conversions: {conversions_mom['current']} (+{conversions_mom['change_pct']}%)"
        ))

    return suggestions


def _meta_suggestions(meta, industry, goals, client_config):
    suggestions = []
    metrics = meta.get("metrics", {})
    scores = meta.get("scores", {})
    mom = meta.get("month_over_month", {})
    campaign_analysis = meta.get("campaign_analysis", [])

    # CTR issues
    ctr_score = scores.get("ctr", "no_data")
    ctr = metrics.get("ctr", 0)
    if ctr_score in ("below_average", "poor"):
        suggestions.append(make_suggestion(
            "Improve Meta Ad Creative - Low CTR",
            f"Meta CTR is {ctr}%, below industry benchmark. For home services ads: "
            "use before/after photos of real jobs, feature customer testimonials in video form, "
            "add urgency ('Book this week, save 10%'), test different headlines emphasizing "
            "speed/reliability/price. Use carousel ads showing multiple completed projects.",
            PRIORITY_HIGH, CATEGORY_CREATIVE, "paid_traffic",
            data_point=f"CTR: {ctr}%"
        ))

    # CPC issues
    cpc_score = scores.get("cpc", "no_data")
    cpc = metrics.get("cpc", 0)
    if cpc_score in ("below_average", "poor"):
        suggestions.append(make_suggestion(
            "Reduce Meta Cost Per Click",
            f"CPC is ${cpc}, above industry average. Strategies: "
            "refine audience targeting (homeowners in service area, age 30-65), "
            "test lookalike audiences based on past converters, "
            "improve ad relevance score with better creative matching audience interests, "
            "and test automatic placements vs manual to find cheaper inventory.",
            PRIORITY_HIGH, CATEGORY_PAID, "cost_efficiency",
            data_point=f"CPC: ${cpc}"
        ))

    # Frequency fatigue
    frequency = metrics.get("frequency", 0)
    if frequency > 3.5:
        suggestions.append(make_suggestion(
            "Address Ad Fatigue - High Frequency",
            f"Ad frequency is {frequency}x (audience seeing ads too often). "
            "Rotate in new creative every 2-3 weeks, expand the target audience, "
            "or pause campaigns temporarily to let the audience refresh. "
            "Consider testing new audience segments you haven't targeted before.",
            PRIORITY_HIGH, CATEGORY_CREATIVE, "ad_efficiency",
            data_point=f"Frequency: {frequency}x"
        ))

    # Underperforming campaigns
    underperforming = [c for c in campaign_analysis if c.get("status") == "underperforming"]
    if underperforming:
        camp_names = [c["name"] for c in underperforming[:3]]
        suggestions.append(make_suggestion(
            "Review Underperforming Campaigns",
            f"These campaigns are below benchmarks: {', '.join(camp_names)}. "
            "Review targeting, creative, and landing pages for each. "
            "Consider pausing the worst performers and reallocating budget to top campaigns.",
            PRIORITY_HIGH, CATEGORY_PAID, "campaign_optimization",
            data_point=f"{len(underperforming)} underperforming campaigns"
        ))

    # Results/leads growth opportunity
    results = metrics.get("results", 0)
    spend = metrics.get("spend", 0)
    if results > 0 and spend > 0:
        cpr = spend / results
        if "reduce_cpa" in goals and cpr > 50:
            suggestions.append(make_suggestion(
                "Optimize for Lower Cost Per Lead",
                f"Cost per lead is ${cpr:.2f}. Test: lead form ads (reduce friction), "
                "retargeting warm audiences (website visitors), "
                "Advantage+ audience optimization, and different campaign objectives. "
                "For home services, Messenger ads and call ads often have lower CPL.",
                PRIORITY_HIGH, CATEGORY_PAID, "cost_efficiency",
                data_point=f"Cost/lead: ${cpr:.2f}"
            ))

    # Budget recommendation
    if results > 0 and spend > 0 and "increase_leads" in goals:
        cpr = spend / results
        if cpr < 60:  # Good CPR, suggest scaling
            suggestions.append(make_suggestion(
                "Scale Meta Ad Budget - Good CPL",
                f"At ${cpr:.2f} per lead, there's room to scale. "
                "Increase daily budget by 20% every 3-4 days (gradual scaling keeps "
                "the algorithm stable). Monitor CPL closely during scaling.",
                PRIORITY_MEDIUM, CATEGORY_BUDGET, "growth",
                data_point=f"Current CPL: ${cpr:.2f} on ${spend} spend"
            ))

    # CPM trends
    cpm = metrics.get("cpm", 0)
    cpm_mom = mom.get("cpm", {})
    if cpm_mom.get("change_pct") is not None and cpm_mom["change_pct"] >= 20:
        suggestions.append(make_suggestion(
            "Monitor Rising CPM Costs",
            f"CPM increased {cpm_mom['change_pct']}% to ${cpm}. "
            "Could indicate increased competition or audience saturation. "
            "Test new audience segments, expand geographic targeting slightly, "
            "or shift some budget to less competitive time slots.",
            PRIORITY_MEDIUM, CATEGORY_PAID, "cost_efficiency",
            data_point=f"CPM: ${cpm} (+{cpm_mom['change_pct']}%)"
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


def _gsc_suggestions(gsc, industry, client_config):
    suggestions = []
    metrics = gsc.get("metrics", {})
    scores = gsc.get("scores", {})
    opportunities = gsc.get("keyword_opportunities", [])
    top_queries = gsc.get("top_queries", [])

    # Keyword opportunities
    if opportunities:
        top_opps = opportunities[:5]
        opp_list = ", ".join([f"'{o['query']}' (pos {o.get('position', '?')})" for o in top_opps])
        suggestions.append(make_suggestion(
            "SEO Quick Wins - Keyword Opportunities",
            f"These keywords have high impressions but rank on page 2+. "
            f"With targeted optimization they could move to page 1: {opp_list}. "
            "Create or improve dedicated pages for these terms, add them to title tags "
            "and H1s, build internal links, and consider writing supporting blog content.",
            PRIORITY_HIGH, CATEGORY_SEO, "organic_traffic",
            data_point=f"{len(opportunities)} keyword opportunities found"
        ))

    # Average position
    avg_pos = metrics.get("avg_position", 0)
    if avg_pos > 15:
        suggestions.append(make_suggestion(
            "Improve Overall SEO Visibility",
            f"Average search position is {avg_pos}, meaning most pages rank on page 2+. "
            "Focus on: optimizing title tags and meta descriptions for click-through, "
            "building local citations and Google Business Profile optimization, "
            "getting customer reviews that mention specific services and location.",
            PRIORITY_HIGH, CATEGORY_SEO, "organic_traffic",
            data_point=f"Avg position: {avg_pos}"
        ))
    elif avg_pos > 8:
        suggestions.append(make_suggestion(
            "Push Page 2 Rankings to Page 1",
            f"Average position is {avg_pos}. Many keywords are close to page 1. "
            "Focus link building efforts on pages ranking positions 8-15. "
            "Add more content depth to these pages - FAQs, how-to sections, cost guides.",
            PRIORITY_MEDIUM, CATEGORY_SEO, "organic_traffic",
            data_point=f"Avg position: {avg_pos}"
        ))

    # CTR optimization
    ctr_score = scores.get("ctr", "no_data")
    ctr = metrics.get("ctr", 0)
    if ctr_score in ("below_average", "poor"):
        suggestions.append(make_suggestion(
            "Improve Organic Click-Through Rate",
            f"Organic CTR is {ctr}%, below benchmark. Optimize meta descriptions "
            "with: specific service mentions, service area, call-to-action ("
            "'Free Estimates', 'Same-Day Service'), and structured data markup "
            "for rich snippets (reviews, price ranges, service areas).",
            PRIORITY_MEDIUM, CATEGORY_SEO, "organic_traffic",
            data_point=f"Organic CTR: {ctr}%"
        ))

    # Local SEO for home services
    services = client_config.get("primary_services", [])
    service_area = client_config.get("service_area", "")
    if services and service_area:
        # Check if service + location queries exist
        has_local = any(
            service_area.lower() in q.get("query", "").lower()
            for q in top_queries[:20]
        )
        if not has_local:
            suggestions.append(make_suggestion(
                "Create Location-Specific Service Pages",
                f"No local search queries ranking well. Create dedicated pages for "
                f"each service + location combo (e.g., '{services[0]} in {service_area}'). "
                "Include local schema markup, embed Google Maps, mention nearby landmarks "
                "and neighborhoods.",
                PRIORITY_MEDIUM, CATEGORY_SEO, "local_visibility",
            ))

    # Content gap analysis
    clicks = metrics.get("clicks", 0)
    impressions = metrics.get("impressions", 0)
    if impressions > 1000 and clicks < 50:
        suggestions.append(make_suggestion(
            "Significant Search Visibility But Low Clicks",
            f"Getting {impressions} impressions but only {clicks} clicks. "
            "The site is being shown but not clicked. Rewrite title tags to be more "
            "compelling and relevant. Add review star ratings via schema markup. "
            "Ensure each page clearly addresses search intent.",
            PRIORITY_HIGH, CATEGORY_SEO, "organic_traffic",
            data_point=f"{impressions} impressions, {clicks} clicks ({ctr}% CTR)"
        ))

    return suggestions


def _google_ads_suggestions(google_ads, industry, goals):
    suggestions = []
    metrics = google_ads.get("metrics", {})
    scores = google_ads.get("scores", {})
    campaign_analysis = google_ads.get("campaign_analysis", [])

    ctr_score = scores.get("ctr", "no_data")
    ctr = metrics.get("ctr", 0)
    if ctr_score in ("below_average", "poor"):
        suggestions.append(make_suggestion(
            "Improve Google Ads CTR",
            f"Google Ads CTR is {ctr}%, below benchmark. Tighten keyword-to-ad relevance, "
            "split ad groups by intent, and test 3-5 new RSA headline variants focused on local buyer intent.",
            PRIORITY_HIGH, CATEGORY_PAID, "paid_traffic",
            data_point=f"CTR: {ctr}%"
        ))

    cpc_score = scores.get("cpc", "no_data")
    cpc = metrics.get("cpc", 0)
    if cpc_score in ("below_average", "poor"):
        suggestions.append(make_suggestion(
            "Lower Google Ads CPC",
            f"Average CPC is ${cpc}, above benchmark. Add negative keywords weekly, improve Quality Score, "
            "and split high-cost broad groups into tighter exact and phrase match groups.",
            PRIORITY_HIGH, CATEGORY_PAID, "cost_efficiency",
            data_point=f"CPC: ${cpc}"
        ))

    cpa_score = scores.get("cost_per_result", "no_data")
    cpa = metrics.get("cost_per_result", 0)
    if cpa_score in ("below_average", "poor"):
        suggestions.append(make_suggestion(
            "Reduce Google Ads CPA",
            f"Cost per conversion is ${cpa}. Audit search terms and move budget to campaigns with strongest conversion intent.",
            PRIORITY_HIGH, CATEGORY_PAID, "cost_efficiency",
            data_point=f"CPA: ${cpa}"
        ))

    underperforming = [campaign for campaign in campaign_analysis if campaign.get("status") == "underperforming"]
    if underperforming:
        names = [campaign.get("name", "Campaign") for campaign in underperforming[:3]]
        suggestions.append(make_suggestion(
            "Tune Underperforming Google Ads Campaigns",
            f"These campaigns are under target: {', '.join(names)}. Reallocate spend toward top converters and tighten targeting.",
            PRIORITY_HIGH, CATEGORY_PAID, "campaign_optimization",
            data_point=f"{len(underperforming)} campaigns underperforming"
        ))

    results = metrics.get("results", 0)
    spend = metrics.get("spend", 0)
    if "increase_leads" in goals and results > 0 and spend > 0:
        current_cpa = spend / results
        if current_cpa < 60:
            suggestions.append(make_suggestion(
                "Scale Winning Google Ads Campaigns",
                f"Current Google Ads CPA is ${current_cpa:.2f}. Increase budget gradually (10-20%) on top campaigns while watching CPA.",
                PRIORITY_MEDIUM, CATEGORY_BUDGET, "growth",
                data_point=f"CPA: ${current_cpa:.2f}"
            ))

    return suggestions


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
                    "Start with Google Business Profile optimization and local SEO.",
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
