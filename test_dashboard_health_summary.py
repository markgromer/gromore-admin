import unittest
from datetime import date

from src.analytics import _build_lead_pacing
from webapp.client_advisor import _build_health_summary, ensure_dashboard_health_cluster


class DashboardHealthSummaryTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()