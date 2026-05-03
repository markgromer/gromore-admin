import unittest
from datetime import date

from src.analytics import _build_lead_pacing, analyze_google_analytics
from src.suggestions import _facebook_organic_suggestions, _ga_suggestions
from webapp.client_advisor import (
    _build_action_items,
    _build_health_summary,
    _build_kpi_cluster_card,
    build_client_dashboard,
    _explain_facebook_organic,
    ensure_dashboard_health_cluster,
)


class DashboardHealthSummaryTests(unittest.TestCase):
    def test_facebook_posting_uses_current_month_pacing(self):
        suggestions = _facebook_organic_suggestions(
            {
                "metrics": {
                    "followers": 250,
                    "organic_impressions": 180,
                    "post_engagements": 12,
                    "engagement_rate": 6.6,
                },
                "post_count": 3,
                "period": {
                    "month": "2026-05",
                    "is_current_month": True,
                    "elapsed_days": 3,
                    "days_in_month": 31,
                    "progress_pct": 9.7,
                    "early_month": True,
                },
            },
            "home_services",
            [],
        )

        titles = [s["title"] for s in suggestions]
        self.assertNotIn("Increase Facebook Posting Frequency", titles)

    def test_organic_social_action_items_do_not_pull_paid_campaigns(self):
        items = _build_action_items(
            [
                {
                    "title": "Increase Facebook Posting Frequency",
                    "detail": "Build local trust with organic Facebook posts.",
                    "category": "organic_social",
                    "data_point": "3 posts vs 1.2 paced target",
                }
            ],
            {
                "kpis": {
                    "facebook_organic": {"post_count": 3, "organic_impressions": 180, "post_engagements": 12},
                    "meta": {"spend": 26, "results": 0},
                },
                "facebook_organic_detail": {"top_posts": [{"id": "p1", "likes": 4}]},
                "meta_detail": {"campaigns": [{"name": "SDL static testimonial ads", "metrics": {"spend": 26, "results": 0}}]},
            },
        )

        relevant = items[0]["relevant_data"]
        self.assertIn("fb_organic_kpis", relevant)
        self.assertNotIn("meta_campaigns", relevant)

    def test_facebook_paid_actions_do_not_pull_google_ads_targets(self):
        items = _build_action_items(
            [
                {
                    "title": "Fix Underperforming Facebook Campaigns",
                    "detail": "Open Ads Manager and inspect the weak Facebook ads.",
                    "category": "paid_advertising",
                    "data_point": "$26 spend, 0 results",
                }
            ],
            {
                "kpis": {
                    "meta": {"spend": 26, "results": 0},
                    "google_ads": {"spend": 340, "results": 0},
                },
                "meta_detail": {"campaigns": [{"name": "Meta Haters", "metrics": {"spend": 26, "results": 0}}]},
                "google_ads_detail": {"campaigns": [{"name": "Search Campaign", "metrics": {"spend": 340, "results": 0}}]},
            },
        )

        relevant = items[0]["relevant_data"]
        self.assertIn("meta_campaigns", relevant)
        self.assertNotIn("google_ads_campaigns", relevant)

    def test_current_month_leads_use_month_to_date_pacing(self):
        pacing = _build_lead_pacing("2026-04", 15, 12, today=date(2026, 4, 8))

        self.assertTrue(pacing["is_current_month"])
        self.assertEqual(pacing["elapsed_days"], 8)
        self.assertEqual(pacing["days_in_month"], 30)
        self.assertEqual(pacing["expected_to_date"], 4.0)
        self.assertEqual(pacing["status"], "ahead")
        self.assertTrue(pacing["on_track"])

    def test_health_summary_describes_ahead_of_pace_leads(self):
        analysis = {
            "kpi_status": {
                "targets": {"leads": 15, "cpa": 50, "roas": None},
                "actual": {"paid_leads": 12, "blended_cpa": 42.5, "blended_roas": None},
                "evaluation": {
                    "leads": {
                        "target": 15,
                        "actual": 12,
                        "expected_to_date": 4.0,
                        "elapsed_days": 8,
                        "days_in_month": 30,
                        "pace_ratio": 3.0,
                        "pace_status": "ahead",
                        "pace_label": "Ahead of pace",
                        "is_current_month": True,
                        "on_track": True,
                    },
                    "cpa": {
                        "target": 50,
                        "actual": 42.5,
                        "gap_pct": -15,
                        "on_track": True,
                    },
                },
            }
        }

        summary = _build_health_summary(
            analysis,
            actions=[{"mission_name": "Review lead quality"}, {"mission_name": "Keep budget stable"}],
            overall_grade="A",
            overall_score=4.8,
        )

        self.assertEqual(summary["label"], "Ahead of pace")
        self.assertEqual(summary["tone"], "positive")
        self.assertIn("12 leads so far", summary["numbers"])
        self.assertIn("15 target this month", summary["numbers"])
        self.assertIn("4.0 paced target by today", summary["numbers"])
        self.assertIn("ahead of plan", summary["summary"].lower())
        self.assertEqual(summary["actions"], ["Review lead quality", "Keep budget stable"])

    def test_dashboard_health_cluster_groups_signals_into_owner_buckets(self):
        dashboard = {
            "channels": {
                "google_ads": {
                    "cards": [
                        {"metric": "Click Rate", "value": "6.4%", "status": "good"},
                        {"metric": "Cost Per Lead", "value": "$39.00", "status": "great"},
                    ]
                },
                "facebook_ads": {
                    "cards": [
                        {"metric": "Click Rate", "value": "1.9%", "status": "good"},
                    ]
                },
                "seo": {
                    "cards": [
                        {"metric": "Clicks from Google", "value": "31", "status": "warning"},
                    ]
                },
                "website": {
                    "cards": [
                        {"metric": "Website Conversions", "value": "12 (4.8%)", "status": "good"},
                    ]
                },
            },
            "kpi_status": {
                "targets": {"leads": 15, "cpa": 50},
                "actual": {"paid_leads": 12, "blended_cpa": 42.5},
                "evaluation": {
                    "leads": {
                        "target": 15,
                        "actual": 12,
                        "expected_to_date": 4.0,
                        "elapsed_days": 8,
                        "days_in_month": 30,
                        "pace_ratio": 3.0,
                        "pace_status": "ahead",
                        "is_current_month": True,
                        "on_track": True,
                    },
                    "cpa": {
                        "target": 50,
                        "actual": 42.5,
                        "gap_pct": -15,
                        "on_track": True,
                    },
                },
            },
            "health_summary": {
                "tone": "positive",
                "label": "Ahead of pace",
                "summary": "Lead flow is ahead of plan and efficiency is healthy.",
                "numbers": ["12 leads so far"],
                "actions": ["Review lead quality", "Keep budget stable"],
                "meter_pct": 88,
                "grade": "A",
                "grade_label": "Excellent - your marketing is performing well across the board",
                "cpa_on_track": True,
                "roas_on_track": None,
            },
        }

        ensure_dashboard_health_cluster(dashboard)
        cluster = dashboard["health_cluster"]

        self.assertEqual([card["label"] for card in cluster["cards"]], ["Paid Ads", "Organic", "Website", "KPIs"])
        self.assertEqual(cluster["cards"][0]["state_label"], "Healthy")
        self.assertEqual(cluster["cards"][1]["state_label"], "Fix")
        self.assertIn("12 leads vs 15 target", cluster["cards"][3]["primary_metric"])
        self.assertEqual(cluster["cards"][3]["next_step"], "Review lead quality")

    def test_facebook_organic_explainer_surfaces_site_clicks(self):
        summary = _explain_facebook_organic({
            "metrics": {
                "followers": 320,
                "organic_impressions": 1800,
                "post_engagements": 74,
                "engagement_rate": 4.1,
                "post_clicks": 19,
            },
            "post_count": 8,
            "top_posts": [
                {"clicks": 7},
                {"clicks": 6},
            ],
        })

        clicks_card = next(card for card in summary["cards"] if card["metric"] == "Website Clicks from Social")

        self.assertEqual(clicks_card["value"], "19")
        self.assertEqual(clicks_card["status"], "good")
        self.assertIn("clicks toward your website", clicks_card["explanation"])

    def test_early_current_month_does_not_create_sharp_traffic_drop_suggestion(self):
        suggestions = _ga_suggestions(
            {
                "metrics": {"sessions": 11},
                "scores": {},
                "period": {"is_current_month": True, "early_month": True},
                "month_over_month": {
                    "sessions": {"current": 11, "previous": 487, "change_pct": -97.7}
                },
            },
            goals={},
            top_landing_pages=[],
        )

        self.assertFalse(any("Traffic Dropped" in suggestion["title"] for suggestion in suggestions))

    def test_early_current_month_thin_sample_does_not_create_conversion_mission(self):
        suggestions = _ga_suggestions(
            {
                "metrics": {"sessions": 18, "conversions": 0, "conversion_rate": 0},
                "scores": {"conversion_rate": "poor"},
                "period": {"is_current_month": True, "early_month": True},
                "month_over_month": {},
            },
            goals={},
            top_landing_pages=[],
        )

        self.assertFalse(any("Conversion Rate" in suggestion["title"] for suggestion in suggestions))

    def test_kpi_cluster_uses_paid_efficiency_when_targets_are_missing(self):
        card = _build_kpi_cluster_card(
            {
                "channels": {
                    "google_ads": {
                        "cards": [
                            {"metric": "Click Rate", "status": "good", "value": "5.4%"},
                            {"metric": "Cost Per Click", "status": "good", "value": "$2.10"},
                        ]
                    },
                    "facebook_ads": {
                        "cards": [
                            {"metric": "Cost Per Lead", "status": "warning", "value": "$64.00"},
                        ]
                    },
                },
                "kpi_status": {
                    "targets": {"leads": None, "cpa": None, "roas": None},
                    "actual": {"paid_spend": 320, "paid_leads": 8, "blended_cpa": 40},
                    "evaluation": {},
                },
            },
            health_summary={"actions": [], "grade": "B", "summary": ""},
        )

        self.assertNotEqual(card["display_score"], "--")
        self.assertIn("$320 spend, 8 paid leads", card["primary_metric"])
        self.assertIn("paid efficiency signals", card["detail"])

    def test_ga4_organic_source_feeds_organic_dashboard_gauge(self):
        ga = analyze_google_analytics(
            {
                "totals": {"sessions": 120, "conversions": 4},
                "by_source": {
                    "google / organic": {"sessions": 46, "users": 39, "conversions": 2},
                    "bing / organic": {"sessions": 8, "users": 7, "conversions": 0},
                    "google / cpc": {"sessions": 22, "users": 20, "conversions": 1},
                },
            },
            prev_ga_data=None,
            benchmarks_website={},
            month="2026-05",
        )

        dashboard = build_client_dashboard(
            {
                "google_analytics": ga,
                "search_console": {"metrics": {"clicks": 0, "impressions": 0, "avg_position": 0}, "scores": {}},
                "overall_grade": "B",
                "overall_score": 3.8,
                "kpi_status": {"targets": {}, "actual": {}, "evaluation": {}},
            },
            suggestions=[],
            brand={},
        )

        self.assertEqual(dashboard["channels"]["organic_search"]["cards"][0]["value"], "54")
        organic_card = next(card for card in dashboard["health_cluster"]["cards"] if card["key"] == "organic")
        self.assertIn("Organic Website Sessions: 54", organic_card["primary_metric"])
        self.assertNotEqual(organic_card["display_score"], "--")

    def test_missions_include_diagnostics_and_research_checks(self):
        ga = analyze_google_analytics(
            {
                "totals": {"sessions": 90, "conversions": 0, "conversion_rate": 0},
                "by_source": {"google / organic": {"sessions": 40, "users": 35, "conversions": 0}},
                "by_page": [
                    {"page": "/quote", "sessions": 70, "conversions": 0, "bounce_rate": 82.0}
                ],
            },
            prev_ga_data=None,
            benchmarks_website={"conversion_rate_good": 5},
            month="2026-05",
        )
        suggestions = _ga_suggestions(
            ga,
            goals={},
            top_landing_pages=ga.get("top_landing_pages") or [],
        )

        dashboard = build_client_dashboard(
            {
                "google_analytics": ga,
                "overall_grade": "C",
                "overall_score": 3.0,
                "kpi_status": {"targets": {}, "actual": {}, "evaluation": {}},
                "period": {"month": "2026-05", "is_current_month": True, "elapsed_days": 3, "days_in_month": 31, "progress_pct": 9.7, "early_month": True},
            },
            suggestions=suggestions,
            brand={},
        )

        self.assertTrue(dashboard["actions"])
        action = dashboard["actions"][0]
        self.assertTrue(action["diagnostics"])
        self.assertTrue(action["research_questions"])
        self.assertIn("confidence", action)


if __name__ == "__main__":
    unittest.main()
