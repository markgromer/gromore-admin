import unittest
from unittest.mock import patch

from webapp import google_business


class _StubDb:
    def get_brand(self, brand_id):
        return {
            "id": brand_id,
            "google_place_id": "place-123",
            "google_maps_api_key": "api-key",
        }


class GoogleBusinessDescriptionStatusTests(unittest.TestCase):
    @patch("webapp.google_business.get_place_details")
    def test_missing_editorial_summary_is_treated_as_unverified(self, mock_get_place_details):
        mock_get_place_details.return_value = {
            "displayName": {"text": "Acme Plumbing"},
            "formattedAddress": "123 Main St",
            "nationalPhoneNumber": "555-123-4567",
            "websiteUri": "https://example.com",
            "primaryTypeDisplayName": {"text": "Plumber"},
            "regularOpeningHours": {"periods": [{"open": {"day": 1}}]},
            "photos": [{"name": "photo-1"}],
            "userRatingCount": 7,
            "businessStatus": "OPERATIONAL",
        }

        gbp_ctx = google_business.build_gbp_context(_StubDb(), 1)

        self.assertEqual(gbp_ctx["description"], "")
        self.assertEqual(gbp_ctx["description_status"], "unverified")

        score, grade, detail, micro = google_business._score_field("description", gbp_ctx)
        self.assertEqual(score, 100)
        self.assertEqual(grade, "pass")
        self.assertIn("did not expose description status", detail)
        self.assertEqual(micro, [])

    @patch("webapp.google_business.get_place_details")
    def test_editorial_summary_text_counts_as_present_description(self, mock_get_place_details):
        mock_get_place_details.return_value = {
            "displayName": {"text": "Acme Plumbing"},
            "formattedAddress": "123 Main St",
            "nationalPhoneNumber": "555-123-4567",
            "websiteUri": "https://example.com",
            "primaryTypeDisplayName": {"text": "Plumber"},
            "regularOpeningHours": {"periods": [{"open": {"day": 1}}]},
            "photos": [{"name": "photo-1"}],
            "userRatingCount": 7,
            "businessStatus": "OPERATIONAL",
            "editorialSummary": {"text": "We provide full-service plumbing, water heater repair, and drain cleaning across the metro area."},
        }

        gbp_ctx = google_business.build_gbp_context(_StubDb(), 1)

        self.assertEqual(gbp_ctx["description_status"], "present")
        self.assertIn("full-service plumbing", gbp_ctx["description"])


if __name__ == "__main__":
    unittest.main()