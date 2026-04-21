import unittest
from unittest.mock import Mock, patch

from webapp.competitor_intel import _scrape_google_places


class GooglePlacesMatchingTests(unittest.TestCase):
    @patch("webapp.competitor_intel.requests.post")
    def test_scrape_google_places_prefers_exact_gbp_cid_match(self, mock_post):
        mock_post.return_value = Mock(
            status_code=200,
            json=lambda: {
                "places": [
                    {
                        "displayName": {"text": "Acme Plumbing Pros"},
                        "id": "wrong-place",
                        "rating": 4.9,
                        "userRatingCount": 480,
                        "types": ["plumber"],
                        "formattedAddress": "10 Wrong Way",
                        "websiteUri": "https://other-example.com",
                        "googleMapsUri": "https://www.google.com/maps?cid=999999999999999999",
                    },
                    {
                        "displayName": {"text": "Acme Plumbing"},
                        "id": "right-place",
                        "rating": 4.6,
                        "userRatingCount": 115,
                        "types": ["plumber"],
                        "formattedAddress": "20 Main Street",
                        "websiteUri": "https://acme.example.com",
                        "googleMapsUri": "https://www.google.com/maps?cid=123456789012345678",
                    },
                ]
            },
        )

        result = _scrape_google_places(
            {
                "name": "Acme Plumbing",
                "website": "https://acme.example.com",
                "google_maps_url": "https://www.google.com/maps?cid=123456789012345678",
                "gbp_cid": "123456789012345678",
            },
            "maps-key",
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["place_id"], "right-place")
        self.assertIn("Exact GBP CID match", result["match_reasons"])
        self.assertEqual(result["candidate_count"], 2)


if __name__ == "__main__":
    unittest.main()