import unittest
from unittest.mock import patch

from src.suggestions import generate_suggestions
from webapp.report_runner import _attach_gbp_context


class _StubDb:
    pass


class GbpSuggestionSuppressionTests(unittest.TestCase):
    def test_healthy_gbp_rewrites_generic_gbp_optimization_copy(self):
        analysis = {
            "client_config": {"goals": []},
            "google_analytics": {
                "metrics": {"sessions": 240, "conversions": 2},
            },
            "meta_business": {
                "metrics": {"results": 20},
            },
            "search_console": {
                "metrics": {"avg_position": 18, "clicks": 80, "impressions": 2200, "ctr": 3.6},
                "top_queries": [],
                "top_pages": [],
            },
            "google_business_profile": {
                "connected": True,
                "status": "VERIFIED",
                "review_count": 42,
                "completeness": {"score": 100},
                "audit": {
                    "overall_score": 92,
                    "quests": [
                        {"field": "verification", "complete": True},
                        {"field": "address", "complete": True},
                        {"field": "phone", "complete": True},
                        {"field": "website", "complete": True},
                        {"field": "hours", "complete": True},
                    ],
                },
            },
        }

        suggestions = generate_suggestions(analysis)
        seo_visibility = next(s for s in suggestions if s["title"].startswith("Overall SEO Visibility Weak"))
        paid_dependency = next(s for s in suggestions if s["title"] == "Over-Reliant on Paid Traffic")

        self.assertNotIn("Google Business Profile optimization", seo_visibility["detail"])
        self.assertIn("local citations", seo_visibility["detail"])
        self.assertNotIn("Google Business Profile optimization", paid_dependency["detail"])
        self.assertIn("service-area page coverage", paid_dependency["detail"])

    @patch("webapp.google_business.run_gbp_audit")
    @patch("webapp.google_business.build_gbp_context")
    def test_report_runner_attaches_gbp_audit_context(self, mock_build_gbp_context, mock_run_gbp_audit):
        mock_build_gbp_context.return_value = {
            "error": None,
            "status": "VERIFIED",
            "review_count": 18,
            "rating": 4.9,
            "completeness": {"score": 100},
        }
        mock_run_gbp_audit.return_value = {"overall_score": 91, "quests": []}
        analysis = {"client_config": {"display_name": "Ace Plumbing"}}

        _attach_gbp_context(_StubDb(), {"id": 7}, analysis)

        self.assertIn("google_business_profile", analysis)
        self.assertTrue(analysis["google_business_profile"]["connected"])
        self.assertEqual(analysis["google_business_profile"]["audit"]["overall_score"], 91)
        self.assertIs(analysis["google_business_profile"], analysis["gbp"])


if __name__ == "__main__":
    unittest.main()