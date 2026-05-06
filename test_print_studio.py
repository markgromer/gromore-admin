import os
import unittest
import uuid
from pathlib import Path

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
_BOOTSTRAP_DB = str(_TEST_ROOT / "gromore-print-bootstrap.db")
os.environ.setdefault("DATABASE_PATH", _BOOTSTRAP_DB)
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("APP_URL", "http://localhost:5000")

from webapp.app import create_app


class ClientPrintStudioTests(unittest.TestCase):
    def setUp(self):
        self.db_file = _TEST_ROOT / f"print-{uuid.uuid4().hex}.db"
        os.environ["DATABASE_PATH"] = str(self.db_file)
        os.environ["SECRET_KEY"] = "test-secret"
        os.environ["APP_URL"] = "http://localhost:5000"

        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False, SERVER_NAME="localhost")
        self.client = self.app.test_client()

        with self.app.app_context():
            self.brand_id = self.app.db.create_brand({
                "slug": f"print-{uuid.uuid4().hex[:8]}",
                "display_name": "Print Test Brand",
                "brand_primary_color": "#0f766e",
                "brand_secondary_color": "#f59e0b",
                "website": "printbrand.example",
                "phone": "555-0100",
            })
            self.user_id = self.app.db.create_client_user(
                self.brand_id,
                f"print-owner-{uuid.uuid4().hex[:8]}@example.com",
                "Password123",
                "Owner User",
            )
            self.qr_id = self.app.db.create_qr_code(
                self.brand_id,
                "Quote QR",
                "https://example.com/quote",
                f"qr{uuid.uuid4().hex[:10]}",
                frame_text="Scan for quote",
            )

        with self.client.session_transaction() as session:
            session["client_user_id"] = self.user_id
            session["client_brand_id"] = self.brand_id
            session["client_role"] = "owner"
            session["client_brand_name"] = "Print Test Brand"

    def tearDown(self):
        for key in ("DATABASE_PATH", "SECRET_KEY", "APP_URL"):
            os.environ.pop(key, None)
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    def test_print_studio_creates_exports_and_archives_material(self):
        response = self.client.get("/client/print-studio")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Print Studio", response.get_data(as_text=True))

        response = self.client.post(
            "/client/print-studio",
            data={
                "material_type": "yard_sign",
                "name": "Neighborhood Yard Sign",
                "headline": "Clean yards start here",
                "subheadline": "Weekly pet waste removal for busy homeowners.",
                "offer": "$25 off your first month",
                "cta": "Scan to book service",
                "phone": "555-0100",
                "website": "printbrand.example",
                "qr_code_id": str(self.qr_id),
                "primary_color": "#0f766e",
                "accent_color": "#f59e0b",
                "background_color": "#ffffff",
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)

        with self.app.app_context():
            materials = self.app.db.get_print_materials(self.brand_id)
            material = materials[0]

        self.assertEqual(material["name"], "Neighborhood Yard Sign")
        self.assertEqual(material["material_type"], "yard_sign")
        self.assertEqual(material["qr_code_id"], self.qr_id)

        png_response = self.client.get(f"/client/print-studio/{material['id']}/png")
        self.assertEqual(png_response.status_code, 200)
        self.assertEqual(png_response.mimetype, "image/png")
        self.assertGreater(len(png_response.data), 1000)

        delete_response = self.client.post(
            f"/client/print-studio/{material['id']}/delete",
            follow_redirects=False,
        )
        self.assertEqual(delete_response.status_code, 302)

        with self.app.app_context():
            active_materials = self.app.db.get_print_materials(self.brand_id)
            archived = self.app.db.get_print_material(material["id"], self.brand_id)

        self.assertEqual(active_materials, [])
        self.assertEqual(archived["active"], 0)


if __name__ == "__main__":
    unittest.main()
