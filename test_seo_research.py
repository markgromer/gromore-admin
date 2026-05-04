import json
import unittest
from unittest.mock import patch

from webapp import seo_research


class FakeDB:
    def __init__(self, settings=None):
        self.settings = dict(settings or {})

    def get_setting(self, key, default=""):
        return self.settings.get(key, default)

    def save_setting(self, key, value):
        self.settings[key] = value


class FakeResponse:
    status_code = 200

    text = "{}"

    def json(self):
        return {
            "choices": [
                {
                    "message": {
                        "content": json.dumps(
                            {
                                "summary": "Organic service pages can reduce blended CPA.",
                                "pages_to_create_or_update": [{"page": "Drain Cleaning", "reason": "High-intent local demand"}],
                                "mission_candidates": [{"title": "Build a drain cleaning page"}],
                                "sources": ["provider search"],
                            }
                        )
                    }
                }
            ],
            "usage": {"total_tokens": 321},
        }


class SeoResearchTests(unittest.TestCase):
    def test_config_prefers_brand_openrouter_key(self):
        db = FakeDB({"openrouter_api_key": "global-key"})
        brand = {
            "id": 7,
            "seo_research_enabled": "1",
            "seo_research_provider": "openrouter",
            "ai_openrouter_api_key": "brand-key",
            "seo_research_model": "sonar",
        }

        config = seo_research.seo_research_config(db, brand)

        self.assertTrue(config["enabled"])
        self.assertEqual(config["api_key"], "brand-key")
        self.assertEqual(config["model"], "perplexity/sonar")

    def test_cached_research_skips_network(self):
        db = FakeDB()
        brand = {
            "id": 8,
            "display_name": "Cache Plumbing",
            "seo_research_enabled": "1",
            "seo_research_provider": "openrouter",
            "ai_openrouter_api_key": "brand-key",
        }

        with patch("webapp.seo_research.requests.post", return_value=FakeResponse()) as post:
            first = seo_research.run_seo_research(db, brand, query="seo opportunities")
            second = seo_research.run_seo_research(db, brand, query="seo opportunities")

        self.assertTrue(first["ok"])
        self.assertFalse(first["cached"])
        self.assertTrue(second["ok"])
        self.assertTrue(second["cached"])
        self.assertEqual(post.call_count, 1)

    def test_daily_limit_blocks_uncached_calls(self):
        db = FakeDB()
        brand = {
            "id": 9,
            "display_name": "Limited HVAC",
            "seo_research_enabled": "1",
            "seo_research_provider": "openrouter",
            "ai_openrouter_api_key": "brand-key",
            "seo_research_daily_limit": "1",
        }

        with patch("webapp.seo_research.requests.post", return_value=FakeResponse()) as post:
            first = seo_research.run_seo_research(db, brand, query="first")
            second = seo_research.run_seo_research(db, brand, query="second")

        self.assertTrue(first["ok"])
        self.assertFalse(second["ok"])
        self.assertIn("Daily SEO research limit reached", second["error"])
        self.assertEqual(post.call_count, 1)

    def test_openrouter_payload_uses_perplexity_model(self):
        db = FakeDB()
        brand = {
            "id": 10,
            "display_name": "Model Test",
            "seo_research_enabled": "1",
            "seo_research_provider": "openrouter",
            "ai_openrouter_api_key": "brand-key",
            "seo_research_model": "perplexity/sonar-pro",
        }

        with patch("webapp.seo_research.requests.post", return_value=FakeResponse()) as post:
            result = seo_research.run_seo_research(db, brand, query="organic vs paid")

        self.assertTrue(result["ok"])
        _, kwargs = post.call_args
        self.assertEqual(kwargs["json"]["model"], "perplexity/sonar-pro")
        self.assertEqual(kwargs["headers"]["Authorization"], "Bearer brand-key")
        self.assertEqual(post.call_args.args[0], seo_research.OPENROUTER_CHAT_URL)


if __name__ == "__main__":
    unittest.main()
