import os
import unittest
import uuid
from pathlib import Path

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
_BOOTSTRAP_DB = str(_TEST_ROOT / "gromore-warren-onboarding-bootstrap.db")
os.environ.setdefault("DATABASE_PATH", _BOOTSTRAP_DB)
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("APP_URL", "http://localhost:5000")

from webapp.app import create_app


class ClientWarrenOnboardingTests(unittest.TestCase):
    def setUp(self):
        self.db_file = _TEST_ROOT / f"warren-onboarding-{uuid.uuid4().hex}.db"
        os.environ["DATABASE_PATH"] = str(self.db_file)
        os.environ["SECRET_KEY"] = "test-secret"
        os.environ["APP_URL"] = "http://localhost:5000"

        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)
        self.client = self.app.test_client()

        with self.app.app_context():
            self.brand_id = self.app.db.create_brand({
                "slug": f"warren-onboarding-{uuid.uuid4().hex[:8]}",
                "display_name": "Warren Onboarding Brand",
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
            session["client_name"] = "Owner User"
            session["client_brand_name"] = "Warren Onboarding Brand"

    def tearDown(self):
        for key in ("DATABASE_PATH", "SECRET_KEY", "APP_URL"):
            os.environ.pop(key, None)
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    def test_dashboard_redirects_first_run_owner_into_warren_onboarding(self):
        response = self.client.get("/client/dashboard", follow_redirects=False)

        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.headers["Location"].endswith("/client/warren-onboarding"))

    def test_warren_onboarding_page_renders_guided_setup_copy(self):
        response = self.client.get("/client/warren-onboarding")
        html = response.get_data(as_text=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn("Warren Guided Setup", html)
        self.assertIn("2 core steps", html)
        self.assertIn("Connect your first ad channel", html)
        self.assertIn("Confirm your business basics", html)
        self.assertIn("their own W.A.R.R.E.N.", html)
        self.assertIn("0 of 10 interview answers saved", html)

    def test_warren_help_page_includes_onboarding_replay_link(self):
        response = self.client.get("/client/help?guide=warren")
        html = response.get_data(as_text=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn("Redo Warren Onboarding", html)
        self.assertIn("/client/warren-onboarding/restart", html)
        self.assertIn("0 of 10 owner interview answers", html)

    def test_onboarding_interview_answer_saves_to_session_and_brand(self):
        response = self.client.post(
            "/client/warren-onboarding/interview",
            data={
                "question_key": "website",
                "answer": "https://warren-example.com",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        self.assertIn("/client/warren-onboarding", response.headers["Location"])

        with self.app.app_context():
            brand = self.app.db.get_brand(self.brand_id)
            session_state = self.app.db.get_client_onboarding_session(self.brand_id, self.user_id)

        self.assertEqual(brand["website"], "https://warren-example.com")
        self.assertIsNotNone(session_state)
        self.assertEqual(session_state["profile"]["website"], "https://warren-example.com")

        page = self.client.get("/client/warren-onboarding")
        html = page.get_data(as_text=True)
        self.assertIn("1 of 10 interview answers saved", html)
        self.assertIn("https://warren-example.com", html)

    def test_dismissing_warren_onboarding_allows_dashboard_access(self):
        dismiss_response = self.client.post(
            "/client/warren-onboarding/status",
            json={"action": "dismiss"},
        )
        dismiss_payload = dismiss_response.get_json()

        self.assertEqual(dismiss_response.status_code, 200)
        self.assertTrue(dismiss_payload["ok"])
        self.assertTrue(dismiss_payload["warren_onboarding"]["is_dismissed"])

        dashboard_response = self.client.get("/client/dashboard", follow_redirects=False)
        self.assertEqual(dashboard_response.status_code, 200)
        self.assertIn("Overview", dashboard_response.get_data(as_text=True))

    def test_restart_route_clears_dismissed_onboarding_and_redirects(self):
        self.client.post(
            "/client/warren-onboarding/status",
            json={"action": "dismiss"},
        )

        response = self.client.get("/client/warren-onboarding/restart", follow_redirects=False)

        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.headers["Location"].endswith("/client/warren-onboarding"))

        page = self.client.get("/client/warren-onboarding")
        self.assertEqual(page.status_code, 200)
        self.assertIn("Warren Guided Setup", page.get_data(as_text=True))

    def test_completed_core_setup_stops_forced_redirect(self):
        with self.app.app_context():
            self.app.db.update_brand_text_field(self.brand_id, "website", "https://example.com")
            self.app.db.update_brand_text_field(self.brand_id, "service_area", "Phoenix")
            self.app.db.update_brand_text_field(self.brand_id, "primary_services", "Google Ads")
            conn = self.app.db._conn()
            conn.execute(
                "UPDATE brands SET google_ads_customer_id = ?, updated_at = datetime('now') WHERE id = ?",
                ("123-456-7890", self.brand_id),
            )
            conn.commit()
            conn.close()
            self.app.db.upsert_connection(self.brand_id, "google", {"account_id": "123", "account_name": "Test Google"})

        response = self.client.get("/client/dashboard", follow_redirects=False)

        self.assertEqual(response.status_code, 200)
        self.assertIn("Overview", response.get_data(as_text=True))


if __name__ == "__main__":
    unittest.main()