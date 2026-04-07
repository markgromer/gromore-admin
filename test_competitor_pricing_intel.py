import unittest

from webapp.competitor_intel import (
    _extract_structured_price_mentions,
    _summarize_pricing_snapshot,
    summarize_market_pricing,
)


class CompetitorPricingIntelTests(unittest.TestCase):
    def test_extracts_structured_price_mentions_from_public_html(self):
        html = """
        <html>
          <body>
            <main>
              <h2>Drain Cleaning Specials</h2>
              <p>Drain cleaning starting at $79 for standard lines.</p>
              <h2>Water Heater Installation</h2>
              <p>New water heater installation from $1,299 with financing from $99/month.</p>
              <p>Free estimate on sewer line replacement.</p>
            </main>
          </body>
        </html>
        """
        service_terms = [
            "Drain Cleaning",
            "Water Heater Installation",
            "Sewer Line Replacement",
        ]

        items = _extract_structured_price_mentions(
            "https://example.com/pricing",
            html,
            service_terms,
        )

        self.assertGreaterEqual(len(items), 4)

        drain = next((item for item in items if item["service"] == "Drain Cleaning" and item["amount_min"] == 79.0), None)
        self.assertIsNotNone(drain)
        self.assertEqual(drain["price_type"], "starting_at")

        heater = next((item for item in items if item["service"] == "Water Heater Installation" and item["amount_min"] == 1299.0), None)
        self.assertIsNotNone(heater)

        membership = next((item for item in items if item["amount_min"] == 99.0), None)
        self.assertIsNotNone(membership)
        self.assertEqual(membership["price_type"], "membership")

        free_estimate = next((item for item in items if item["amount_min"] == 0.0), None)
        self.assertIsNotNone(free_estimate)
        self.assertEqual(free_estimate["price_type"], "free_offer")

    def test_pricing_snapshot_summary_uses_scraped_amounts(self):
        items = [
            {"service": "Drain Cleaning", "amount_min": 79.0, "amount_max": None, "price_type": "starting_at", "confidence": 0.9},
            {"service": "Water Heater Installation", "amount_min": 1299.0, "amount_max": None, "price_type": "flat_rate", "confidence": 0.82},
            {"service": "Sewer Line Replacement", "amount_min": 0.0, "amount_max": None, "price_type": "free_offer", "confidence": 0.65},
        ]

        summary = _summarize_pricing_snapshot(items)

        self.assertEqual(summary["sample_count"], 3)
        self.assertEqual(summary["billable_sample_count"], 2)
        self.assertEqual(summary["price_min"], 79.0)
        self.assertEqual(summary["price_max"], 1299.0)
        self.assertEqual(summary["price_avg"], 689.0)
        self.assertEqual(summary["confidence_band"], "high")

    def test_market_pricing_summary_rolls_up_competitor_reports(self):
        reports = [
            {
                "competitor": {"name": "Alpha Plumbing"},
                "pricing": {
                    "items": [
                        {"amount_min": 79.0, "price_type": "starting_at"},
                        {"amount_min": 129.0, "price_type": "flat_rate"},
                    ]
                },
            },
            {
                "competitor": {"name": "Beta Plumbing"},
                "pricing": {
                    "items": [
                        {"amount_min": 99.0, "price_type": "membership"},
                        {"amount_min": 299.0, "price_type": "flat_rate"},
                    ]
                },
            },
        ]

        summary = summarize_market_pricing(reports)

        self.assertEqual(summary["competitors_with_pricing"], 2)
        self.assertEqual(summary["billable_source_count"], 4)
        self.assertEqual(summary["average_price"], 151.5)
        self.assertEqual(summary["lowest_price"], 79.0)
        self.assertEqual(summary["highest_price"], 299.0)


if __name__ == "__main__":
    unittest.main()
