import os
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
_BOOTSTRAP_DB = str(_TEST_ROOT / "gromore-beta-broadcast-bootstrap.db")
os.environ.setdefault("DATABASE_PATH", _BOOTSTRAP_DB)
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("APP_URL", "http://localhost:5000")

from webapp.app import create_app


class BetaBroadcastEmailTests(unittest.TestCase):
    def setUp(self):
        self.db_file = _TEST_ROOT / f"beta-broadcast-{uuid.uuid4().hex}.db"
        os.environ["DATABASE_PATH"] = str(self.db_file)
        os.environ["SECRET_KEY"] = "test-secret"
        os.environ["APP_URL"] = "http://localhost:5000"

        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)
        self.client = self.app.test_client()

        with self.app.app_context():
            self.admin_id = self.app.db.create_user(f"ops-{uuid.uuid4().hex[:8]}@example.com", "Password123", "Ops Admin")
            brand_id = self.app.db.create_brand({
                "slug": f"broadcast-brand-{uuid.uuid4().hex[:8]}",
                "display_name": "Broadcast Brand",
            })
            self.app.db.create_client_user(
                brand_id,
                f"client-{uuid.uuid4().hex[:8]}@example.com",
                "Password123",
                "Client User",
            )
            approved_id = self.app.db.create_beta_tester({
                "name": "Approved Beta",
                "email": f"approved-{uuid.uuid4().hex[:8]}@example.com",
                "business_name": "Approved Co",
            })
            rejected_id = self.app.db.create_beta_tester({
                "name": "Rejected Beta",
                "email": f"rejected-{uuid.uuid4().hex[:8]}@example.com",
                "business_name": "Rejected Co",
            })
            self.app.db.update_beta_tester_status(approved_id, "approved")
            self.app.db.update_beta_tester_status(rejected_id, "rejected")

        with self.client.session_transaction() as session:
            session["user_id"] = self.admin_id
            session["user_name"] = "Ops Admin"

    def tearDown(self):
        for key in ("DATABASE_PATH", "SECRET_KEY", "APP_URL"):
            os.environ.pop(key, None)
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    @patch("webapp.email_sender.send_bulk_email")
    def test_beta_broadcast_sends_only_to_active_beta_audience(self, mock_send_bulk_email):
        mock_send_bulk_email.return_value = 1

        response = self.client.post(
            "/beta/broadcast",
            data={
                "audience": "beta_users",
                "subject": "Zoom invite",
                "message": "Join us Friday at 2pm.",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        recipients = mock_send_bulk_email.call_args[0][1]
        self.assertEqual(len(recipients), 1)
        self.assertIn("approved", recipients[0]["email"])


if __name__ == "__main__":
    unittest.main()