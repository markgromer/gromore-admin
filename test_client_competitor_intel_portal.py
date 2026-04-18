import json
import os
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
_BOOTSTRAP_DB = str(_TEST_ROOT / "gromore-competitor-intel-bootstrap.db")
os.environ.setdefault("DATABASE_PATH", _BOOTSTRAP_DB)
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("APP_URL", "http://localhost:5000")

from webapp.app import create_app


class ClientCompetitorIntelPortalTests(unittest.TestCase):
    def setUp(self):
        self.db_file = _TEST_ROOT / f"competitor-intel-{uuid.uuid4().hex}.db"
        os.environ["DATABASE_PATH"] = str(self.db_file)
        os.environ["SECRET_KEY"] = "test-secret"
        os.environ["APP_URL"] = "http://localhost:5000"

        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)
        self.client = self.app.test_client()

        with self.app.app_context():
            self.brand_id = self.app.db.create_brand({
                "slug": f"competitor-intel-{uuid.uuid4().hex[:8]}",
                "display_name": "Competitor Intel Brand",
            })
            self.user_id = self.app.db.create_client_user(
                self.brand_id,
                f"owner-{uuid.uuid4().hex[:8]}@example.com",
                "Password123",
                "Owner User",
            )

        with self.client.session_transaction() as session:
            session["client_user_id"] = self.user_id
            session["client_brand_id"] = self.brand_id
            session["client_role"] = "owner"
            session["client_brand_name"] = "Competitor Intel Brand"

    def tearDown(self):
        for key in ("DATABASE_PATH", "SECRET_KEY", "APP_URL"):
            os.environ.pop(key, None)
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    def _add_competitor(self, name, website=""):
        with self.app.app_context():
            return self.app.db.add_competitor(self.brand_id, name, website=website)

    def test_competitor_page_shows_scan_health_and_bulk_actions(self):
        alpha_id = self._add_competitor("Alpha Plumbing", website="https://alpha.example.com")
        self._add_competitor("Beta Plumbing", website="https://beta.example.com")

        with self.app.app_context():
            self.app.db.upsert_competitor_intel(alpha_id, self.brand_id, "google_places", json.dumps({"rating": 4.8, "review_count": 120}))
            self.app.db.upsert_competitor_intel(alpha_id, self.brand_id, "meta_ads", json.dumps({"active_ad_count": 3, "sample_ads": []}))
            self.app.db.upsert_competitor_intel(alpha_id, self.brand_id, "website", json.dumps({"title": "Alpha Plumbing", "description": "Fast plumbing help"}))
            self.app.db.upsert_competitor_intel(alpha_id, self.brand_id, "pricing", json.dumps({"summary": {"sample_count": 2, "price_min": 79, "price_max": 249}}))
            self.app.db.upsert_competitor_intel(alpha_id, self.brand_id, "research", json.dumps({"pricing_strategy": "Hold premium and defend trust.", "counter_moves": []}))

        response = self.client.get("/client/competitors")
        html = response.get_data(as_text=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn("Scan all competitors", html)
        self.assertIn("Use this intel in W.A.R.R.E.N.", html)
        self.assertIn("Needs first scan", html)
        self.assertIn("Run first scan", html)
        self.assertIn("Ready", html)
        self.assertIn("Alpha Plumbing", html)
        self.assertIn("Beta Plumbing", html)

    @patch("webapp.competitor_intel.refresh_competitor_intel")
    def test_single_competitor_rescan_uses_refresh_pipeline(self, mock_refresh):
        mock_refresh.return_value = {"_errors": []}
        alpha_id = self._add_competitor("Alpha Plumbing", website="https://alpha.example.com")

        response = self.client.post(f"/client/competitors/{alpha_id}/refresh")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(mock_refresh.call_count, 1)
        self.assertTrue(mock_refresh.call_args.kwargs["force"])

    @patch("webapp.competitor_intel.refresh_competitor_intel")
    def test_bulk_rescan_refreshes_every_competitor(self, mock_refresh):
        mock_refresh.return_value = {"_errors": []}
        self._add_competitor("Alpha Plumbing", website="https://alpha.example.com")
        self._add_competitor("Beta Plumbing", website="https://beta.example.com")

        response = self.client.post("/client/competitors/refresh-all")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(mock_refresh.call_count, 2)
        for call in mock_refresh.call_args_list:
            self.assertTrue(call.kwargs["force"])


if __name__ == "__main__":
    unittest.main()