import os
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from werkzeug.security import generate_password_hash

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
_BOOTSTRAP_DB = str(_TEST_ROOT / "gromore-password-reset-bootstrap.db")
os.environ.setdefault("DATABASE_PATH", _BOOTSTRAP_DB)
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("APP_URL", "http://localhost:5000")

from webapp.app import create_app


class ClientPasswordResetTests(unittest.TestCase):
    def setUp(self):
        self._db_file = _TEST_ROOT / f"password-reset-{uuid.uuid4().hex}.db"
        self.db_path = str(self._db_file)
        os.environ["DATABASE_PATH"] = self.db_path
        os.environ["SECRET_KEY"] = "test-secret"
        os.environ["APP_URL"] = "http://localhost:5000"

        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)
        self.client = self.app.test_client()

        with self.app.app_context():
            db = self.app.db
            self.brand_id = db.create_brand({
                "slug": "case-test-brand",
                "display_name": "Case Test Brand",
            })
            conn = db._conn()
            cur = conn.execute(
                """
                INSERT INTO client_users (brand_id, email, password_hash, display_name, is_active)
                VALUES (?, ?, ?, ?, 1)
                """,
                (
                    self.brand_id,
                    "MixedCase@Example.com",
                    generate_password_hash("OldPassword1"),
                    "Case Test User",
                ),
            )
            self.user_id = cur.lastrowid
            conn.commit()
            conn.close()

    def tearDown(self):
        os.environ.pop("DATABASE_PATH", None)
        os.environ.pop("SECRET_KEY", None)
        os.environ.pop("APP_URL", None)
        if self._db_file.exists():
            self._db_file.unlink()
        wal_path = Path(self.db_path + "-wal")
        shm_path = Path(self.db_path + "-shm")
        if wal_path.exists():
            wal_path.unlink()
        if shm_path.exists():
            shm_path.unlink()

    def _login_client(self):
        with self.client.session_transaction() as session:
            session["client_user_id"] = self.user_id
            session["client_brand_id"] = self.brand_id
            session["client_name"] = "Case Test User"
            session["client_brand_name"] = "Case Test Brand"
            session["client_role"] = "owner"

    def test_client_login_accepts_case_insensitive_email(self):
        with patch("webapp.client_portal._warm_client_snapshots_async", return_value=None):
            response = self.client.post(
                "/client/login",
                data={"email": "mixedcase@example.com", "password": "OldPassword1"},
                follow_redirects=False,
            )

        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.headers["Location"].endswith("/client/dashboard"))

    def test_password_reset_flow_works_for_legacy_mixed_case_email(self):
        with patch("webapp.email_sender.send_password_reset_email") as send_reset_email:
            response = self.client.post(
                "/client/forgot-password",
                data={"email": "mixedcase@example.com"},
                base_url="https://portal.example.com",
                follow_redirects=True,
            )

        self.assertEqual(response.status_code, 200)
        send_reset_email.assert_called_once()
        reset_url = send_reset_email.call_args.args[3]
        self.assertTrue(reset_url.startswith("https://portal.example.com/client/reset-password/"))

        with self.app.app_context():
            user = self.app.db.get_client_user(self.user_id)
            token = user["password_reset_token"]
            self.assertTrue(token)

        reset_response = self.client.post(
            f"/client/reset-password/{token}",
            data={"password": "NewPassword1", "confirm_password": "NewPassword1"},
            follow_redirects=False,
        )

        self.assertEqual(reset_response.status_code, 302)
        self.assertTrue(reset_response.headers["Location"].endswith("/client/login"))

        with self.app.app_context():
            user = self.app.db.get_client_user(self.user_id)
            self.assertEqual(user["password_reset_token"], "")
            authenticated = self.app.db.authenticate_client("mixedcase@example.com", "NewPassword1")
            self.assertIsNotNone(authenticated)

    def test_client_can_change_password_from_settings(self):
        self._login_client()

        response = self.client.post(
            "/client/settings/password",
            data={
                "current_password": "OldPassword1",
                "new_password": "ChangedPassword1",
                "confirm_password": "ChangedPassword1",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.headers["Location"].endswith("/client/settings"))

        with self.app.app_context():
            self.assertIsNone(self.app.db.authenticate_client("mixedcase@example.com", "OldPassword1"))
            self.assertIsNotNone(self.app.db.authenticate_client("mixedcase@example.com", "ChangedPassword1"))

    def test_client_change_password_rejects_wrong_current_password(self):
        self._login_client()

        response = self.client.post(
            "/client/settings/password",
            data={
                "current_password": "WrongPassword1",
                "new_password": "ChangedPassword1",
                "confirm_password": "ChangedPassword1",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.headers["Location"].endswith("/client/settings"))

        with self.app.app_context():
            self.assertIsNotNone(self.app.db.authenticate_client("mixedcase@example.com", "OldPassword1"))
            self.assertIsNone(self.app.db.authenticate_client("mixedcase@example.com", "ChangedPassword1"))


if __name__ == "__main__":
    unittest.main()
