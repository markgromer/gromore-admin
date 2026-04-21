import unittest

from webapp.campaign_manager import (
    _build_quality_scorecard,
    _upgrade_campaign_plan_quality,
    _validate_plan_copy,
)


BRAND = {
    "display_name": "Northside Plumbing",
    "industry": "plumbing",
    "target_audience": "homeowners in Columbus",
    "active_offers": "$79 diagnostic visit",
    "year_founded": "2012",
    "license_info": "OH Licensed Plumbing Contractor",
}


class CampaignPlanQualityTests(unittest.TestCase):
    def test_google_plan_upgrade_adds_depth_and_specificity(self):
        raw_plan = {
            "platform": "google",
            "campaign_name": "New Campaign",
            "daily_budget": 15,
            "ad_groups": [
                {
                    "name": "Ad Group",
                    "keywords": ["keyword 1"],
                    "negative_keywords": [],
                    "headlines": ["headline 1"],
                    "descriptions": ["description 1"],
                }
            ],
            "campaign_negative_keywords": [],
            "location_targeting": "",
            "rationale": "",
        }

        upgraded = _upgrade_campaign_plan_quality(
            raw_plan,
            BRAND,
            "emergency plumber",
            "Columbus, OH",
            900,
            notes="Same-day scheduling available",
        )

        self.assertEqual(len(upgraded["ad_groups"]), 3)
        self.assertTrue(all(len(group["keywords"]) >= 6 for group in upgraded["ad_groups"]))
        self.assertTrue(all(len(group["headlines"]) >= 8 for group in upgraded["ad_groups"]))
        self.assertTrue(all(len(group["descriptions"]) >= 3 for group in upgraded["ad_groups"]))
        self.assertIn("jobs", upgraded["campaign_negative_keywords"])
        self.assertIn("Columbus", upgraded["location_targeting"])
        self.assertIn("buyer intent", upgraded["rationale"].lower())

        scorecard = _build_quality_scorecard(
            upgraded,
            brand=BRAND,
            service="emergency plumber",
            location="Columbus, OH",
        )
        check_map = {item["name"]: item for item in scorecard["checks"]}

        self.assertGreaterEqual(scorecard["score"], 80)
        self.assertTrue(check_map["Service Specificity"]["passed"])
        self.assertTrue(check_map["Structure Strength"]["passed"])
        self.assertTrue(check_map["CTA Clarity"]["passed"])

    def test_meta_plan_upgrade_builds_real_ad_sets(self):
        raw_plan = {
            "platform": "meta",
            "campaign_name": "descriptive campaign name",
            "objective": "OUTCOME_LEADS",
            "daily_budget": 12,
            "ad_sets": [
                {
                    "name": "ad set name",
                    "targeting_description": "who this targets and why",
                    "age_min": 25,
                    "age_max": 65,
                    "ad_copy": [
                        {
                            "headline": "attention-grabbing headline",
                            "primary_text": "compelling ad body text that drives action",
                            "description": "short description",
                            "call_to_action": "GET_QUOTE",
                        }
                    ],
                }
            ],
            "location_targeting": "",
            "rationale": "",
        }

        upgraded = _upgrade_campaign_plan_quality(
            raw_plan,
            BRAND,
            "water heater repair",
            "Columbus, OH",
            1200,
            notes="Offer weekend appointments",
        )

        self.assertEqual(len(upgraded["ad_sets"]), 3)
        self.assertTrue(all(len(adset["ad_copy"]) >= 3 for adset in upgraded["ad_sets"]))
        self.assertTrue(all(adset["targeting_description"] for adset in upgraded["ad_sets"]))
        self.assertTrue(all(adset["interests"] for adset in upgraded["ad_sets"]))

        headlines = [
            copy["headline"]
            for adset in upgraded["ad_sets"]
            for copy in adset["ad_copy"]
        ]
        self.assertTrue(any("Columbus" in headline for headline in headlines))
        self.assertTrue(any(copy["call_to_action"] for adset in upgraded["ad_sets"] for copy in adset["ad_copy"]))

        warnings = _validate_plan_copy(upgraded)["warnings"]
        self.assertFalse(any("placeholder" in warning.lower() for warning in warnings))

        scorecard = _build_quality_scorecard(
            upgraded,
            brand=BRAND,
            service="water heater repair",
            location="Columbus, OH",
        )
        check_map = {item["name"]: item for item in scorecard["checks"]}

        self.assertGreaterEqual(scorecard["score"], 80)
        self.assertTrue(check_map["Structure Strength"]["passed"])
        self.assertTrue(check_map["Brand Or Local Proof"]["passed"])


if __name__ == "__main__":
    unittest.main()