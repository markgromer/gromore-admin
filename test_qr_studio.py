import os
import unittest
import uuid
from pathlib import Path

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
_BOOTSTRAP_DB = str(_TEST_ROOT / "gromore-qr-bootstrap.db")
os.environ.setdefault("DATABASE_PATH", _BOOTSTRAP_DB)
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("APP_URL", "http://localhost:5000")

from webapp.app import create_app


class ClientQrStudioTests(unittest.TestCase):
    def setUp(self):
        self.db_file = _TEST_ROOT / f"qr-{uuid.uuid4().hex}.db"
        os.environ["DATABASE_PATH"] = str(self.db_file)
        os.environ["SECRET_KEY"] = "test-secret"
        os.environ["APP_URL"] = "http://localhost:5000"

        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False, SERVER_NAME="localhost")
        self.client = self.app.test_client()

        with self.app.app_context():
            self.brand_id = self.app.db.create_brand({
                "slug": f"qr-{uuid.uuid4().hex[:8]}",
                "display_name": "QR Test Brand",
                "brand_primary_color": "#0f766e",
            })
            self.user_id = self.app.db.create_client_user(
                self.brand_id,
                f"qr-owner-{uuid.uuid4().hex[:8]}@example.com",
                "Password123",
                "Owner User",
            )

        with self.client.session_transaction() as session:
            session["client_user_id"] = self.user_id
            session["client_brand_id"] = self.brand_id
            session["client_role"] = "owner"
            session["client_brand_name"] = "QR Test Brand"

    def tearDown(self):
        for key in ("DATABASE_PATH", "SECRET_KEY", "APP_URL"):
            os.environ.pop(key, None)
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    def test_qr_studio_creates_png_and_tracks_redirect(self):
        response = self.client.get("/client/qr-studio")
        self.assertEqual(response.status_code, 200)
        self.assertIn("QR Studio", response.get_data(as_text=True))

        response = self.client.post(
            "/client/qr-studio",
            data={
                "name": "Review Card",
                "target_url": "example.com/review",
                "foreground_color": "#0f766e",
                "background_color": "#ffffff",
                "frame_text": "Scan for reviews",
                "module_style": "circle",
                "badge_shape": "monogram",
                "frame_style": "side_callout",
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)

        with self.app.app_context():
            qr_codes = self.app.db.get_qr_codes(self.brand_id)
            qr = qr_codes[0]

        self.assertEqual(qr["name"], "Review Card")
        self.assertEqual(qr["target_url"], "https://example.com/review")
        self.assertEqual(qr["module_style"], "circle")
        self.assertEqual(qr["badge_shape"], "monogram")
        self.assertEqual(qr["frame_style"], "side_callout")

        png_response = self.client.get(f"/client/qr-studio/{qr['id']}/png")
        self.assertEqual(png_response.status_code, 200)
        self.assertEqual(png_response.mimetype, "image/png")
        self.assertGreater(len(png_response.data), 1000)

        redirect_response = self.client.get(f"/q/{qr['tracking_slug']}", follow_redirects=False)
        self.assertEqual(redirect_response.status_code, 302)
        self.assertEqual(redirect_response.headers["Location"], "https://example.com/review")

        with self.app.app_context():
            refreshed = self.app.db.get_qr_code(qr["id"], self.brand_id)
            summary = self.app.db.get_qr_scan_summary(self.brand_id)

        self.assertEqual(refreshed["scans"], 1)
        self.assertEqual(summary[qr["id"]]["scan_count"], 1)


if __name__ == "__main__":
    unittest.main()
