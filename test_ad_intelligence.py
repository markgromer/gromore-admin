import unittest

from src.ad_intelligence import build_ad_intelligence
from src.suggestions import generate_suggestions
from webapp.ad_builder import _build_ad_context
from webapp.client_advisor import build_client_dashboard


BRAND = {
    "display_name": "Northside Plumbing",
    "industry": "plumbing",
    "service_area": "Columbus",
    "primary_services": ["drain cleaning", "water heater repair"],
    "kpi_target_cpa": 80,
}


def _analysis():
    return {
        "client_id": "northside",
        "month": "2026-04",
        "client_config": BRAND,
        "period": {"month": "2026-04", "is_current_month": False},
        "google_ads": {
            "metrics": {"spend": 500, "results": 4, "cost_per_result": 125, "clicks": 80, "impressions": 3000, "ctr": 2.67},
            "campaign_analysis": [
                {
                    "name": "Emergency Plumbing Search",
                    "metrics": {"spend": 260, "results": 4, "cost_per_result": 65, "clicks": 40, "impressions": 1200, "ctr": 3.33},
                    "status": "ok",
                },
                {
                    "name": "Broad Match Experiments",
                    "metrics": {"spend": 180, "results": 0, "clicks": 30, "impressions": 2000, "ctr": 1.5},
                    "status": "ok",
                },
            ],
            "search_terms": [
                {
                    "term": "free plumbing classes",
                    "campaign_name": "Broad Match Experiments",
                    "ad_group_name": "Broad",
                    "spend": 48,
                    "clicks": 6,
                    "results": 0,
                    "cpc": 8,
                },
                {
                    "term": "emergency plumber columbus",
                    "campaign_name": "Emergency Plumbing Search",
                    "ad_group_name": "Emergency",
                    "spend": 110,
                    "clicks": 10,
                    "results": 3,
                    "cpc": 11,
                },
            ],
        },
        "meta_business": {
            "metrics": {"spend": 300, "results": 2, "cost_per_result": 150, "frequency": 4.2, "clicks": 50, "impressions": 2500, "ctr": 2},
            "campaign_analysis": [
                {
                    "name": "Water Heater Leads",
                    "metrics": {"spend": 300, "results": 2, "cost_per_result": 150, "frequency": 4.2, "clicks": 50, "impressions": 2500, "ctr": 2},
                    "status": "ok",
                }
            ],
            "top_ads": [
                {"ad_name": "Old coupon graphic", "spend": 75, "results": 0, "ctr": 0.4},
            ],
        },
        "highlights": [],
        "concerns": [],
        "overall_grade": "C",
        "overall_score": 2.8,
        "kpi_status": {"targets": {"cpa": 80}, "actual": {"paid_spend": 800, "paid_leads": 6, "blended_cpa": 133.33}, "evaluation": {}},
    }


class AdIntelligenceTests(unittest.TestCase):
    def test_detects_waste_scale_and_creative_fatigue(self):
        intel = build_ad_intelligence(_analysis(), BRAND)
        keys = {finding["key"] for finding in intel["findings"]}

        self.assertIn("google_search_term_waste", keys)
        self.assertIn("campaign_zero_result_spend", keys)
        self.assertIn("campaign_scale_candidate", keys)
        self.assertIn("meta_frequency_fatigue", keys)
        self.assertEqual(intel["summary"]["target_cpa"], 80)
        term_finding = next(item for item in intel["findings"] if item["key"] == "google_search_term_waste")
        self.assertTrue(any("free plumbing classes" in item for item in term_finding["evidence"]))

    def test_reports_enterprise_data_gaps(self):
        analysis = _analysis()
        analysis["google_ads"]["search_terms"] = []
        analysis["meta_business"]["top_ads"] = []
        intel = build_ad_intelligence(analysis, {**BRAND, "kpi_target_cpa": ""})
        gap_keys = {gap["key"] for gap in intel["data_gaps"]}

        self.assertIn("google_search_terms_missing", gap_keys)
        self.assertIn("meta_ad_level_missing", gap_keys)

    def test_suggestions_use_unified_ad_intelligence(self):
        analysis = _analysis()
        analysis["ad_intelligence"] = build_ad_intelligence(analysis, BRAND)
        titles = [item["title"] for item in generate_suggestions(analysis)]

        self.assertIn("Stop paying for non-converting search terms", titles)
        self.assertIn("Cut spend from campaigns with no results", titles)

    def test_dashboard_and_ad_builder_receive_ad_intelligence(self):
        analysis = _analysis()
        suggestions = generate_suggestions(analysis)
        dashboard = build_client_dashboard(analysis, suggestions, BRAND)
        context = _build_ad_context(analysis, BRAND)

        self.assertIn("ad_intelligence", dashboard)
        self.assertTrue(dashboard["ad_intelligence"]["findings"])
        self.assertTrue(context["ad_intelligence"]["findings"])
        self.assertTrue(context["data_available"]["has_ad_intelligence_findings"])


if __name__ == "__main__":
    unittest.main()
