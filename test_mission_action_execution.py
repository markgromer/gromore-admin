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
        self.assertEqual(action["steps"][0], "Copy the developer brief below and send it today.")
        self.assertNotIn("analytics.google.com", " ".join(action["steps"]))
        self.assertTrue(action["exact_targets"])


if __name__ == "__main__":
    unittest.main()