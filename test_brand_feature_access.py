import os
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
_BOOTSTRAP_DB = str(_TEST_ROOT / "gromore-brand-feature-bootstrap.db")
os.environ.setdefault("DATABASE_PATH", _BOOTSTRAP_DB)
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("APP_URL", "http://localhost:5000")

from webapp.app import create_app


class BrandFeatureAccessTests(unittest.TestCase):
    def setUp(self):
        self.db_file = _TEST_ROOT / f"brand-feature-{uuid.uuid4().hex}.db"
        os.environ["DATABASE_PATH"] = str(self.db_file)
        os.environ["SECRET_KEY"] = "test-secret"
        os.environ["APP_URL"] = "http://localhost:5000"

        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)
        self.client = self.app.test_client()

        with self.app.app_context():
            self.admin_id = self.app.db.create_user(f"admin-{uuid.uuid4().hex[:8]}@example.com", "Password123", "Admin User")
            self.brand_id = self.app.db.create_brand({
                "slug": f"brand-feature-{uuid.uuid4().hex[:8]}",
                "display_name": "Feature Test Brand",
            })
            self.client_user_id = self.app.db.create_client_user(
                self.brand_id,
                f"owner-{uuid.uuid4().hex[:8]}@example.com",
                "Password123",
                "Owner User",
            )

    def tearDown(self):
        for key in ("DATABASE_PATH", "SECRET_KEY", "APP_URL"):
            os.environ.pop(key, None)
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    def _login_admin(self):
        with self.client.session_transaction() as session:
            session["user_id"] = self.admin_id
            session["user_name"] = "Admin User"

    def _login_client(self):
        with self.client.session_transaction() as session:
            session["client_user_id"] = self.client_user_id
            session["client_brand_id"] = self.brand_id
            session["client_role"] = "owner"
            session["client_name"] = "Owner User"
            session["client_brand_name"] = "Feature Test Brand"

    def test_brand_settings_save_feature_states_and_contacts(self):
        self._login_admin()

        response = self.client.post(
            f"/brands/{self.brand_id}/settings",
            data={
                "section": "features",
                "feature_state_dashboard": "on",
                "feature_state_blog": "upgrade",
                "feature_state_campaigns": "off",
                "upgrade_dev_email": "dev@example.com",
                "upgrade_contact_emails": "designer@example.com\nowner@example.com",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        with self.app.app_context():
            brand = self.app.db.get_brand(self.brand_id)
            feature_access = self.app.db.get_brand_feature_access(self.brand_id)

        self.assertEqual(feature_access["blog"], "upgrade")
        self.assertEqual(feature_access["campaigns"], "off")
        self.assertEqual(brand["upgrade_dev_email"], "dev@example.com")
        self.assertIn("designer@example.com", brand["upgrade_contact_emails"])

    def test_upgrade_state_redirects_client_to_upgrade_page(self):
        with self.app.app_context():
            self.app.db.update_brand_feature_access(self.brand_id, {"blog": "upgrade"})

        self._login_client()
        response = self.client.get("/client/blog", follow_redirects=False)

        self.assertEqual(response.status_code, 302)
        self.assertIn("/client/upgrade/blog", response.headers["Location"])

        upgrade_response = self.client.get("/client/upgrade/blog")
        html = upgrade_response.get_data(as_text=True)
        self.assertEqual(upgrade_response.status_code, 200)
        self.assertIn("Upgrade Available", html)
        self.assertIn("Blog", html)

    @patch("webapp.email_sender.send_bulk_email")
    def test_upgrade_page_can_email_saved_contacts(self, mock_send_bulk_email):
        mock_send_bulk_email.return_value = 2
        with self.app.app_context():
            self.app.db.update_brand_feature_access(self.brand_id, {"blog": "upgrade"})
            self.app.db.update_brand_text_field(self.brand_id, "upgrade_dev_email", "dev@example.com")
            self.app.db.update_brand_text_field(self.brand_id, "upgrade_contact_emails", "designer@example.com")

        self._login_client()
        response = self.client.post(
            "/client/upgrade/blog/email-contacts",
            data={
                "subject": "Feature unlock",
                "message": "Please enable the blog feature.",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        recipients = mock_send_bulk_email.call_args[0][1]
        self.assertEqual(len(recipients), 2)
        self.assertEqual(recipients[0]["email"], "dev@example.com")
        self.assertEqual(recipients[1]["email"], "designer@example.com")


if __name__ == "__main__":
    unittest.main()