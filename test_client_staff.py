import os
import unittest
import uuid
from pathlib import Path

from werkzeug.security import generate_password_hash

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
_BOOTSTRAP_DB = str(_TEST_ROOT / "gromore-staff-bootstrap.db")
os.environ.setdefault("DATABASE_PATH", _BOOTSTRAP_DB)
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("APP_URL", "http://localhost:5000")

from webapp.app import create_app


class ClientStaffTests(unittest.TestCase):
    def setUp(self):
        self._db_file = _TEST_ROOT / f"staff-{uuid.uuid4().hex}.db"
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
                    "slug": "staff-test-brand",
                    "display_name": "Staff Test Brand",
                }
            )
            conn = db._conn()
            cur = conn.execute(
                """
                INSERT INTO client_users (brand_id, email, password_hash, display_name, role, is_active)
                VALUES (?, ?, ?, ?, 'owner', 1)
                """,
                (
                    self.brand_id,
                    "owner@example.com",
                    generate_password_hash("Password123"),
                    "Staff Owner",
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
        for suffix in ("-wal", "-shm"):
            path = Path(self.db_path + suffix)
            if path.exists():
                path.unlink()

    def test_staff_page_renders_for_owner(self):
        with self.client.session_transaction() as session:
            session["client_user_id"] = self.user_id
            session["client_brand_id"] = self.brand_id
            session["client_brand_name"] = "Staff Test Brand"
            session["client_role"] = "owner"

        response = self.client.get("/client/staff")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Staff Management", body)
        self.assertIn("Invite Team Member", body)


if __name__ == "__main__":
    unittest.main()