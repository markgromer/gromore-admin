import unittest

from webapp.client_advisor import _build_action_cards


class MissionActionExecutionTests(unittest.TestCase):
    def test_website_actions_become_delegate_ready_with_exact_targets(self):
        analysis = {
            "client_config": {"display_name": "Scoop Doggy Logs", "industry": "pet waste removal"},
            "google_analytics": {
                "metrics": {"sessions": 240, "conversions": 2, "conversion_rate": 0.83, "bounce_rate": 78.4},
            },
            "top_landing_pages": [
                {"page": "/dog-poop-removal", "sessions": 92, "conversions": 0, "bounce_rate": 81.2},
                {"page": "/weekly-yard-cleaning", "sessions": 74, "conversions": 1, "bounce_rate": 76.8},
            ],
            "search_console": {
                "top_pages": [{"page": "/dog-poop-removal", "clicks": 18, "impressions": 420, "ctr": 4.2, "position": 11.4}],
                "keyword_opportunities": [{"query": "dog poop removal tucson", "page": "/dog-poop-removal", "impressions": 420, "position": 11.4}],
            },
            "highlights": [],
            "concerns": [],
        }
        suggestions = [
            {
                "title": "Reduce Landing Page Bounce Rate",
                "detail": "The main service pages are leaking traffic before people call or request a quote.",
                "category": "website",
                "priority": "high",
                "data_point": "Bounce rate: 78.4%",
            }
        ]

        actions = _build_action_cards(analysis, suggestions, brand={})

        self.assertEqual(len(actions), 1)
        action = actions[0]
        self.assertEqual(action["execution_mode"], "delegate")
        self.assertTrue(action["delegate_message"])
        self.assertIn("/dog-poop-removal", action["delegate_message"])
        self.assertIn("what changed on each page", action["delegate_message"])
        self.assertIn("contact path obvious without scrolling", action["delegate_message"])
        self.assertEqual(action["steps"][0], "Copy the developer brief below and send it today.")
        self.assertNotIn("analytics.google.com", " ".join(action["steps"]))
        self.assertTrue(action["exact_targets"])

    def test_meta_only_paid_mission_routes_to_ads_manager(self):
        analysis = {
            "client_config": {"display_name": "Ace Plumbing", "industry": "plumbing"},
            "meta_business": {
                "metrics": {"spend": 1200, "results": 18, "cost_per_result": 66.67, "ctr": 1.9},
                "campaign_analysis": [
                    {"campaign_name": "Plumbing Leads", "spend": 840, "results": 14, "cost_per_result": 60.0}
                ],
                "top_ads": [
                    {"ad_name": "Emergency Drain Ad", "ctr": 2.4, "results": 9, "cost_per_result": 48.0}
                ],
            },
            "highlights": [],
            "concerns": [],
        }
        suggestions = [
            {
                "title": "Clone Your Best Ad",
                "detail": "Duplicate the current winner and test one new headline.",
                "category": "paid_advertising",
                "priority": "high",
                "data_point": "Best ad CTR: 2.4%",
            }
        ]

        actions = _build_action_cards(analysis, suggestions, brand={})

        self.assertEqual(len(actions), 1)
        action = actions[0]
        self.assertEqual(action["platform_url"], "https://business.facebook.com/adsmanager")
        self.assertEqual(action["platform_label"], "Open Ads Manager")
        self.assertIn("business.facebook.com/adsmanager", " ".join(action["steps"]))
        self.assertNotIn("ads.google.com", " ".join(action["steps"]))

    def test_low_volume_local_page_mission_gets_rewritten(self):
        analysis = {
            "client_config": {"display_name": "Scoop Doggy Logs", "industry": "pet waste removal"},
            "google_analytics": {
                "metrics": {"sessions": 240, "conversions": 2, "conversion_rate": 0.83, "bounce_rate": 78.4},
            },
            "top_landing_pages": [
                {"page": "/dog-poop-removal", "sessions": 92, "conversions": 0, "bounce_rate": 81.2},
            ],
            "search_console": {
                "top_pages": [{"page": "/dog-poop-removal", "clicks": 18, "impressions": 120, "ctr": 4.2, "position": 11.4}],
                "keyword_opportunities": [{"query": "dog poop removal tucson", "page": "/dog-poop-removal", "impressions": 120, "position": 11.4}],
            },
            "highlights": [],
            "concerns": [],
        }
        suggestions = [
            {
                "title": "Build More Local Pages",
                "detail": "Create new city pages to win more local traffic.",
                "category": "seo",
                "priority": "high",
                "data_point": "Top opportunity: 120 impressions",
            }
        ]

        actions = _build_action_cards(analysis, suggestions, brand={})

        self.assertEqual(len(actions), 1)
        action = actions[0]
        self.assertEqual(action["mission_name"], "Tighten The Page You Have")
        self.assertIn("not a missing page", action["why"])
        self.assertIn("Please do not build new city or local pages yet.", action["delegate_message"])
        self.assertEqual(action["platform_url"], "")

    def test_strategy_diagnostic_mission_stays_direct_for_owner(self):
        analysis = {
            "client_config": {"display_name": "Ace Plumbing", "industry": "plumbing"},
            "google_analytics": {
                "metrics": {"sessions": 212, "conversions": 8, "conversion_rate": 3.77, "bounce_rate": 54.2},
                "month_over_month": {"sessions": {"current": 212, "previous": 377, "change_pct": -43.8}},
            },
            "top_sources": [
                {"source": "google / organic", "sessions": 96, "conversions": 4},
                {"source": "google / cpc", "sessions": 58, "conversions": 3},
            ],
            "top_converting_sources": [
                {"source": "google / organic", "conversions": 4},
            ],
            "top_landing_pages": [
                {"page": "/emergency-plumber", "sessions": 82, "conversions": 3, "bounce_rate": 49.2},
            ],
            "search_console": {
                "metrics": {"clicks": 140, "impressions": 2100, "ctr": 6.7, "avg_position": 10.4},
                "top_queries": [
                    {"query": "emergency plumber mesa", "clicks": 41, "impressions": 510, "ctr": 8.0, "position": 5.6},
                ],
            },
            "google_ads": {
                "metrics": {"spend": 900, "results": 12, "clicks": 180, "cpc": 5.0},
                "campaign_analysis": [
                    {"campaign_name": "Emergency Search", "spend": 540, "results": 7},
                ],
            },
            "highlights": [],
            "concerns": [],
        }
        suggestions = [
            {
                "title": "Traffic Dropped 44% - Down to 212 Sessions",
                "detail": "Sessions fell from 377 to 212. Check which traffic sources declined and whether ad activity changed.",
                "category": "strategy",
                "priority": "high",
                "data_point": "Sessions: 212 (was 377)",
            }
        ]

        actions = _build_action_cards(analysis, suggestions, brand={})

        self.assertEqual(len(actions), 1)
        action = actions[0]
        self.assertEqual(action["execution_mode"], "direct")
        self.assertEqual(action["delegate_message"], "")
        self.assertEqual(action["delegate_to"], "")
        self.assertEqual(action["platform_url"], "https://analytics.google.com")
        self.assertIn("analytics.google.com", " ".join(action["steps"]))
        self.assertIn("google / organic", " ".join(action["steps"]))
        self.assertIn("/emergency-plumber", " ".join(action["steps"]))
        self.assertIn("emergency plumber mesa", " ".join(action["steps"]))
        self.assertIn("Emergency Search", " ".join(action["steps"]))


if __name__ == "__main__":
    unittest.main()