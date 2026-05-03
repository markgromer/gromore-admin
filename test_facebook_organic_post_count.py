import unittest
from unittest.mock import patch

from webapp.api_bridge import _combine_facebook_organic_reports, _pull_meta_organic


class _FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.text = str(payload)

    def json(self):
        return self._payload


class FacebookOrganicPostCountTests(unittest.TestCase):
    @patch("requests.get")
    def test_uses_count_hint_when_post_listing_returns_no_rows(self, mock_get):
        def fake_get(url, params=None, timeout=0):
            params = params or {}

            if url == "https://graph.facebook.com/v21.0/page123":
                return _FakeResponse({
                    "name": "Test Page",
                    "fan_count": 180,
                    "followers_count": 220,
                })

            if url == "https://graph.facebook.com/v21.0/page123/insights":
                metric = params.get("metric")
                metric_values = {
                    "page_impressions_organic": 123,
                    "page_post_engagements": 12,
                }
                value = metric_values.get(metric)
                if value is None:
                    return _FakeResponse({"data": []})
                return _FakeResponse({
                    "data": [
                        {
                            "name": metric,
                            "values": [{"value": value}],
                        }
                    ]
                })

            if url in {
                "https://graph.facebook.com/v21.0/page123/published_posts",
                "https://graph.facebook.com/v21.0/page123/posts",
                "https://graph.facebook.com/v21.0/page123/feed",
            }:
                if params.get("summary") == "true" and params.get("access_token") == "user_token" and url.endswith("/published_posts"):
                    return _FakeResponse({
                        "data": [],
                        "summary": {"total_count": 4},
                    })
                return _FakeResponse({"data": []})

            raise AssertionError(f"Unexpected URL: {url}")

        mock_get.side_effect = fake_get

        result = _pull_meta_organic(
            "page123",
            "page_token",
            "2026-04-01",
            "2026-04-30",
            user_access_token="user_token",
        )

        self.assertEqual(result["post_count"], 4)
        self.assertEqual(result["metrics"]["organic_impressions"], 123)
        self.assertEqual(result["metrics"]["post_engagements"], 12)
        self.assertEqual(result["metrics"]["_debug"]["post_count_source"], "count_hint")

    def test_combines_main_and_ads_page_organic_reports(self):
        result = _combine_facebook_organic_reports([
            {
                "page_id": "main123",
                "page_role": "primary",
                "metrics": {
                    "page_name": "Main Page",
                    "followers": 100,
                    "organic_impressions": 50,
                    "post_engagements": 10,
                    "post_clicks": 3,
                },
                "top_posts": [{"id": "p1", "likes": 4, "comments": 1, "shares": 0}],
                "post_count": 2,
            },
            {
                "page_id": "ads456",
                "page_role": "ads",
                "metrics": {
                    "page_name": "Ads Page",
                    "followers": 40,
                    "organic_impressions": 30,
                    "post_engagements": 6,
                    "post_clicks": 2,
                },
                "top_posts": [{"id": "p2", "likes": 8, "comments": 0, "shares": 0}],
                "post_count": 1,
            },
        ])

        self.assertEqual(result["metrics"]["tracked_page_count"], 2)
        self.assertEqual(result["metrics"]["followers"], 140)
        self.assertEqual(result["metrics"]["organic_impressions"], 80)
        self.assertEqual(result["metrics"]["post_engagements"], 16)
        self.assertEqual(result["metrics"]["post_clicks"], 5)
        self.assertEqual(result["post_count"], 3)
        self.assertEqual(result["top_posts"][0]["page_role"], "ads")
        self.assertEqual(len(result["pages"]), 2)


if __name__ == "__main__":
    unittest.main()
