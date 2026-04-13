import os
import unittest
import uuid
from pathlib import Path

from werkzeug.security import generate_password_hash

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
_BOOTSTRAP_DB = str(_TEST_ROOT / "gromore-va-bootstrap.db")
os.environ.setdefault("DATABASE_PATH", _BOOTSTRAP_DB)
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("APP_URL", "http://localhost:5000")

from webapp.app import create_app


class ClientVAServicesTests(unittest.TestCase):
    def setUp(self):
        self._db_file = _TEST_ROOT / f"va-services-{uuid.uuid4().hex}.db"
        self.db_path = str(self._db_file)
        os.environ["DATABASE_PATH"] = self.db_path
        os.environ["SECRET_KEY"] = "test-secret"
        os.environ["APP_URL"] = "http://localhost:5000"

        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)
        self.client = self.app.test_client()

        with self.app.app_context():
            db = self.app.db
            self.brand_id = db.create_brand(
                {
                    "slug": "va-test-brand",
                    "display_name": "VA Test Brand",
                }
            )
            conn = db._conn()
            conn.execute(
                """
                INSERT INTO client_users (brand_id, email, password_hash, display_name, is_active)
                VALUES (?, ?, ?, ?, 1)
                """,
                (
                    self.brand_id,
                    "va@example.com",
                    generate_password_hash("Password123"),
                    "VA Owner",
                ),
            )
            conn.commit()
            conn.close()

    def tearDown(self):
        os.environ.pop("DATABASE_PATH", None)
        os.environ.pop("SECRET_KEY", None)
        os.environ.pop("APP_URL", None)
        if self._db_file.exists():
            self._db_file.unlink()
        for suffix in ("-wal", "-shm"):
            path = Path(self.db_path + suffix)
            if path.exists():
                path.unlink()

    def test_va_services_page_requires_login(self):
        response = self.client.get("/client/va", follow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.headers["Location"].endswith("/client/login"))

    def test_logged_in_client_can_view_va_coming_soon_page(self):
        login_response = self.client.post(
            "/client/login",
            data={"email": "va@example.com", "password": "Password123"},
            follow_redirects=False,
        )
        self.assertEqual(login_response.status_code, 302)

        response = self.client.get("/client/va")
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("VA Desk is coming soon", body)
        self.assertIn("10 tokens/hr", body)
        self.assertIn("25 tokens/hr", body)


if __name__ == "__main__":
    unittest.main()
