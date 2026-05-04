import unittest
import uuid

from src.analytics import build_full_analysis
from webapp.client_advisor import _build_health_summary, _explain_kpis


class CrmLeadReportingTests(unittest.TestCase):
    def test_crm_leads_backstop_paid_platform_zeroes(self):
        analysis = build_full_analysis(
            f"crm-leads-{uuid.uuid4().hex}",
            "2026-04",
            {
                "google_ads": {
                    "totals": {"spend": 100, "results": 0, "clicks": 12, "impressions": 300, "ctr": 4, "cpc": 8.33},
                    "by_campaign": {},
                },
                "crm_revenue": {
                    "source": "jobber",
                    "totals": {
                        "revenue": 250,
                        "closed_deals": 1,
                        "leads": 1,
                        "new_clients": 1,
                    },
                },
            },
            {
                "display_name": "CRM Brand",
                "industry": "plumbing",
                "kpi_target_leads": 1,
                "kpi_target_cpa": 150,
            },
        )

        actual = analysis["kpi_status"]["actual"]
        leads_eval = analysis["kpi_status"]["evaluation"]["leads"]

        self.assertEqual(actual["paid_leads"], 0)
        self.assertEqual(actual["crm_leads"], 1)
        self.assertEqual(actual["total_leads"], 1)
        self.assertEqual(actual["lead_source"], "crm")
        self.assertTrue(leads_eval["on_track"])
        self.assertEqual(leads_eval["actual"], 1)

    def test_client_kpi_cards_use_total_leads_not_paid_only(self):
        analysis = {
            "kpi_status": {
                "targets": {"leads": 1},
                "actual": {"paid_leads": 0, "crm_leads": 1, "total_leads": 1, "lead_source": "crm"},
                "evaluation": {
                    "leads": {
                        "target": 1,
                        "actual": 1,
                        "source": "crm",
                        "on_track": True,
                        "is_current_month": False,
                    }
                },
            }
        }

        cards = _explain_kpis(analysis)
        summary = _build_health_summary(analysis, actions=[], overall_grade="B", overall_score=4)

        self.assertEqual(cards[0]["label"], "Total Leads")
        self.assertEqual(cards[0]["actual"], "1")
        self.assertIn("Source: crm", cards[0]["explanation"])
        self.assertIn("1 leads so far", summary["numbers"])
        self.assertIn("hitting your lead target", summary["summary"])

    def test_organic_conversions_lower_blended_acquisition_cost(self):
        analysis = build_full_analysis(
            f"organic-leads-{uuid.uuid4().hex}",
            "2026-04",
            {
                "google_ads": {
                    "totals": {"spend": 200, "results": 2, "clicks": 20, "impressions": 500, "ctr": 4, "cpc": 10},
                    "by_campaign": {},
                },
                "google_analytics": {
                    "totals": {"sessions": 100, "conversions": 3, "conversion_rate": 3},
                    "by_source": {
                        "google / organic": {"sessions": 60, "users": 45, "conversions": 2},
                        "direct / none": {"sessions": 40, "users": 30, "conversions": 1},
                    },
                },
            },
            {
                "display_name": "Organic Brand",
                "industry": "plumbing",
                "kpi_target_leads": 4,
                "kpi_target_cpa": 75,
            },
        )

        actual = analysis["kpi_status"]["actual"]

        self.assertEqual(actual["paid_leads"], 2)
        self.assertEqual(actual["organic_conversions"], 2)
        self.assertEqual(actual["total_leads"], 4)
        self.assertEqual(actual["paid_cpa"], 100)
        self.assertEqual(actual["blended_cpa"], 50)
        self.assertEqual(actual["lead_source"], "paid_and_organic")
        self.assertEqual(actual["organic_share"], 50.0)


if __name__ == "__main__":
    unittest.main()
