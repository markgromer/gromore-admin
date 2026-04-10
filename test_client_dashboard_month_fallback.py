import json
import os
import unittest
import uuid
from pathlib import Path

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
_BOOTSTRAP_DB = str(_TEST_ROOT / "gromore-dashboard-fallback-bootstrap.db")
os.environ.setdefault("DATABASE_PATH", _BOOTSTRAP_DB)
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("APP_URL", "http://localhost:5000")

from webapp.app import create_app


class ClientDashboardMonthFallbackTests(unittest.TestCase):
    def setUp(self):
        self.db_file = _TEST_ROOT / f"dashboard-fallback-{uuid.uuid4().hex}.db"
        os.environ["DATABASE_PATH"] = str(self.db_file)
        os.environ["SECRET_KEY"] = "test-secret"
        os.environ["APP_URL"] = "http://localhost:5000"

        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)
        self.client = self.app.test_client()

        with self.app.app_context():
            self.brand_id = self.app.db.create_brand({
                "slug": f"dashboard-fallback-{uuid.uuid4().hex[:8]}",
                "display_name": "Dashboard Fallback Brand",
            })
            self.user_id = self.app.db.create_client_user(
                self.brand_id,
                f"owner-{uuid.uuid4().hex[:8]}@example.com",
                "Password123",
                "Owner User",
            )
            self.app.db.create_report(self.brand_id, "2026-03", "internal.html", "client.html")
            self.app.db.upsert_dashboard_snapshot(
                self.brand_id,
                "2026-03",
                json.dumps({"health_summary": {"summary": "March data ready."}}),
                source="test",
            )

        with self.client.session_transaction() as session:
            session["client_user_id"] = self.user_id
            session["client_brand_id"] = self.brand_id
            session["client_role"] = "owner"
            session["client_name"] = "Owner User"
            session["client_brand_name"] = "Dashboard Fallback Brand"

    def tearDown(self):
        for key in ("DATABASE_PATH", "SECRET_KEY", "APP_URL"):
            os.environ.pop(key, None)
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    def test_dashboard_page_defaults_to_latest_available_month(self):
        response = self.client.get("/client/dashboard")
        html = response.get_data(as_text=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn('value="2026-03"', html)
        self.assertIn("Overview is showing 2026-03 instead", html)

    def test_dashboard_data_returns_fallback_month_metadata(self):
        response = self.client.get("/client/dashboard/data")
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["month"], "2026-03")
        self.assertTrue(payload["used_month_fallback"])
        self.assertEqual(payload["dashboard"]["health_summary"]["summary"], "March data ready.")


if __name__ == "__main__":
    unittest.main()