import os
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
_BOOTSTRAP_DB = str(_TEST_ROOT / "gromore-heatmap-bootstrap.db")
os.environ.setdefault("DATABASE_PATH", _BOOTSTRAP_DB)
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("APP_URL", "http://localhost:5000")

from webapp.app import create_app


class ClientHeatmapTests(unittest.TestCase):
    def setUp(self):
        self.db_file = _TEST_ROOT / f"heatmap-{uuid.uuid4().hex}.db"
        os.environ["DATABASE_PATH"] = str(self.db_file)
        os.environ["SECRET_KEY"] = "test-secret"
        os.environ["APP_URL"] = "http://localhost:5000"

        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)
        self.client = self.app.test_client()

        with self.app.app_context():
            self.brand_id = self.app.db.create_brand({
                "slug": f"heatmap-{uuid.uuid4().hex[:8]}",
                "display_name": "Heatmap Test Brand",
            })
            self.user_id = self.app.db.create_client_user(
                self.brand_id,
                f"owner-{uuid.uuid4().hex[:8]}@example.com",
                "Password123",
                "Owner User",
            )
            self.app.db.update_brand_text_field(self.brand_id, "google_maps_api_key", "maps-key")
            self.app.db.update_brand_number_field(self.brand_id, "business_lat", 33.4484)
            self.app.db.update_brand_number_field(self.brand_id, "business_lng", -112.0740)

        with self.client.session_transaction() as session:
            session["client_user_id"] = self.user_id
            session["client_brand_id"] = self.brand_id
            session["client_role"] = "owner"
            session["client_brand_name"] = "Heatmap Test Brand"

    def tearDown(self):
        for key in ("DATABASE_PATH", "SECRET_KEY", "APP_URL"):
            os.environ.pop(key, None)
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    def test_heatmap_page_renders_map_first_controls(self):
        response = self.client.get("/client/heatmap")
        html = response.get_data(as_text=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn("Move the scan center", html)
        self.assertIn("Use business location", html)
        self.assertIn("Run Heatmap", html)
        self.assertIn("Estimated Places calls", html)

    def test_heatmap_api_bootstrap_returns_brand_and_active_scan(self):
        with self.app.app_context():
            self.app.db.save_heatmap_scan(
                self.brand_id,
                "plumber",
                5,
                3,
                33.5001,
                -112.1999,
                '[{"row": 0, "col": 0, "lat": 33.5001, "lng": -112.1999, "rank": 2}]',
                2,
            )

        response = self.client.get("/client/api/heatmap")
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["brand"]["google_maps_api_key"], "maps-key")
        self.assertEqual(payload["active_scan"]["keyword"], "plumber")
        self.assertEqual(payload["active_scan"]["results"][0]["rank"], 2)
        self.assertEqual(payload["scans"][0]["center_lat"], 33.5001)

    @patch("webapp.heatmap.scan_grid")
    @patch("webapp.heatmap.calc_search_radius_m")
    @patch("webapp.heatmap.generate_grid")
    @patch("webapp.heatmap.clean_keyword")
    def test_heatmap_scan_uses_custom_center_and_persists_it(
        self,
        mock_clean_keyword,
        mock_generate_grid,
        mock_calc_search_radius_m,
        mock_scan_grid,
    ):
        mock_clean_keyword.return_value = ("plumber", False)
        mock_generate_grid.return_value = [{"row": 0, "col": 0, "lat": 33.5001, "lng": -112.1999}]
        mock_calc_search_radius_m.return_value = 5000
        mock_scan_grid.return_value = ([{"row": 0, "col": 0, "lat": 33.5001, "lng": -112.1999, "rank": 2}], {})

        response = self.client.post(
            "/client/heatmap/scan",
            json={
                "keyword": "plumber",
                "radius_miles": 3,
                "grid_size": 5,
                "center_lat": 33.5001,
                "center_lng": -112.1999,
            },
        )
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["center_lat"], 33.5001)
        self.assertEqual(payload["center_lng"], -112.1999)
        self.assertIsInstance(payload["scan_id"], int)
        mock_generate_grid.assert_called_once_with(33.5001, -112.1999, 3.0, 5)

        with self.app.app_context():
            saved_scan = self.app.db.get_heatmap_scan(payload["scan_id"])

        self.assertEqual(saved_scan["center_lat"], 33.5001)
        self.assertEqual(saved_scan["center_lng"], -112.1999)
        self.assertEqual(saved_scan["keyword"], "plumber")

    def test_heatmap_scan_rejects_invalid_custom_center(self):
        response = self.client.post(
            "/client/heatmap/scan",
            json={
                "keyword": "plumber",
                "radius_miles": 3,
                "grid_size": 5,
                "center_lat": "bad-lat",
                "center_lng": -112.1999,
            },
        )
        payload = response.get_json()

        self.assertEqual(response.status_code, 400)
        self.assertFalse(payload["ok"])
        self.assertIn("invalid", payload["error"].lower())


if __name__ == "__main__":
    unittest.main()