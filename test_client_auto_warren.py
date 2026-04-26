import json
import os
import unittest
import uuid
from pathlib import Path

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
_BOOTSTRAP_DB = str(_TEST_ROOT / "gromore-auto-warren-bootstrap.db")
os.environ.setdefault("DATABASE_PATH", _BOOTSTRAP_DB)
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("APP_URL", "http://localhost:5000")

from webapp.app import create_app


class ClientAutoWarrenTests(unittest.TestCase):
    def setUp(self):
        self.db_file = _TEST_ROOT / f"auto-warren-{uuid.uuid4().hex}.db"
        os.environ["DATABASE_PATH"] = str(self.db_file)
        os.environ["SECRET_KEY"] = "test-secret"
        os.environ["APP_URL"] = "http://localhost:5000"

        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)
        self.client = self.app.test_client()

        with self.app.app_context():
            self.brand_id = self.app.db.create_brand({
                "slug": f"auto-warren-{uuid.uuid4().hex[:8]}",
                "display_name": "Auto Warren Brand",
                "sales_bot_enabled": 1,
                "gsc_site_url": "https://example.com",
            })
            self.user_id = self.app.db.create_client_user(
                self.brand_id,
                f"owner-{uuid.uuid4().hex[:8]}@example.com",
                "Password123",
                "Owner User",
            )
            self.app.db.create_report(self.brand_id, "2026-04", "internal.html", "client.html")
            self.app.db.create_lead_thread(self.brand_id, {
                "lead_name": "Jane Lead",
                "lead_phone": "+15555550123",
                "channel": "sms",
                "status": "engaged",
                "last_inbound_at": "2026-04-24 12:00:00",
                "last_outbound_at": "2026-04-23 12:00:00",
                "summary": "Asked for a quote.",
            })
            self.app.db.upsert_dashboard_snapshot(
                self.brand_id,
                "2026-04",
                json.dumps({
                    "health_summary": {"summary": "Lead flow is ahead, but follow-up matters today."},
                    "actions": [{
                        "key": "review_lead_quality",
                        "mission_name": "Review lead quality",
                        "why": "Lead quality determines whether ad spend is turning into real jobs.",
                        "data_point": "12 leads against a 15 lead monthly target.",
                        "reward": "Keep the best lead sources funded.",
                    }],
                }),
                source="test",
            )

        with self.client.session_transaction() as session:
            session["client_user_id"] = self.user_id
            session["client_brand_id"] = self.brand_id
            session["client_role"] = "owner"
            session["client_name"] = "Owner User"
            session["client_brand_name"] = "Auto Warren Brand"

    def tearDown(self):
        for key in ("DATABASE_PATH", "SECRET_KEY", "APP_URL"):
            os.environ.pop(key, None)
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    def test_auto_warren_route_is_separate_and_data_backed(self):
        response = self.client.get("/client/auto")
        html = response.get_data(as_text=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn("Auto WARREN", html)
        self.assertIn("One clear next step, based on real data.", html)
        self.assertIn("Reply to 1 lead waiting on you", html)
        self.assertIn("Jane Lead", html)
        self.assertIn("Warren lead threads", html)
        self.assertIn("No fake certainty", html)
        self.assertIn("/client/dashboard?month=2026-04", html)

    def test_auto_warren_nav_link_appears_on_overview_without_replacing_it(self):
        response = self.client.get("/client/dashboard")
        html = response.get_data(as_text=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn("Auto WARREN", html)
        self.assertIn(">Overview<", html)
        self.assertIn('/client/auto"', html)

    def test_auto_warren_missing_data_becomes_setup_step(self):
        with self.app.app_context():
            other_brand_id = self.app.db.create_brand({
                "slug": f"auto-warren-empty-{uuid.uuid4().hex[:8]}",
                "display_name": "Empty Auto Brand",
            })
            other_user_id = self.app.db.create_client_user(
                other_brand_id,
                f"empty-{uuid.uuid4().hex[:8]}@example.com",
                "Password123",
                "Empty Owner",
            )

        with self.client.session_transaction() as session:
            session["client_user_id"] = other_user_id
            session["client_brand_id"] = other_brand_id
            session["client_role"] = "owner"
            session["client_name"] = "Empty Owner"
            session["client_brand_name"] = "Empty Auto Brand"

        response = self.client.get("/client/auto")
        html = response.get_data(as_text=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn("Connect one data source first", html)
        self.assertIn("Warren should not guess", html)


if __name__ == "__main__":
    unittest.main()
