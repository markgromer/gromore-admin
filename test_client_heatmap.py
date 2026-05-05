import os
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, patch

from webapp.heatmap import (
    _extract_place_id_from_maps_href,
    _match_business,
    _normalize_browser_maps_result,
    _search_places,
    scan_grid,
    summarize_competitor_landscape,
)

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
        self.assertIn("Estimated Google Maps lookups", html)

    def test_heatmap_api_bootstrap_returns_brand_and_active_scan(self):
        with self.app.app_context():
            self.app.db.save_heatmap_scan(
                self.brand_id,
                "plumber",
                5,
                3,
                33.5001,
                -112.1999,
                '[{"row": 0, "col": 0, "lat": 33.5001, "lng": -112.1999, "rank": 2, "competitors": [{"rank": 1, "name": "Rival Plumbing", "place_id": "rival-1", "address": "123 Main St", "is_target": false}]}]',
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
        self.assertEqual(payload["active_scan"]["competitor_summary"][0]["name"], "Rival Plumbing")

    def test_heatmap_api_bootstrap_uses_fallback_maps_key(self):
        with self.app.app_context():
            self.app.db.update_brand_text_field(self.brand_id, "google_maps_api_key", "")

        with patch.dict(os.environ, {"GOOGLE_MAPS_API_KEY": "fallback-key"}, clear=False):
            response = self.client.get("/client/api/heatmap")

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["brand"]["google_maps_api_key"], "fallback-key")

    @patch("webapp.heatmap.verify_place_id")
    def test_heatmap_api_bootstrap_hydrates_location_from_place_id(self, mock_verify_place_id):
        mock_verify_place_id.return_value = {
            "name": "Heatmap Test Brand",
            "lat": 32.0869,
            "lng": -110.8243,
        }

        with self.app.app_context():
            self.app.db.update_brand_number_field(self.brand_id, "business_lat", 0)
            self.app.db.update_brand_number_field(self.brand_id, "business_lng", 0)
            self.app.db.update_brand_text_field(self.brand_id, "google_place_id", "place-123")

        response = self.client.get("/client/api/heatmap")
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["brand"]["business_lat"], 32.0869)
        self.assertEqual(payload["brand"]["business_lng"], -110.8243)
        mock_verify_place_id.assert_called_once_with("maps-key", "place-123")

        with self.app.app_context():
            refreshed = self.app.db.get_brand(self.brand_id)

        self.assertEqual(float(refreshed["business_lat"]), 32.0869)
        self.assertEqual(float(refreshed["business_lng"]), -110.8243)

    @patch("webapp.heatmap.scan_grid")
    @patch("webapp.heatmap.verify_place_id")
    @patch("webapp.heatmap.calc_search_radius_m")
    @patch("webapp.heatmap.generate_grid")
    @patch("webapp.heatmap.clean_keyword")
    def test_heatmap_scan_uses_custom_center_and_persists_it(
        self,
        mock_clean_keyword,
        mock_generate_grid,
        mock_calc_search_radius_m,
        mock_verify_place_id,
        mock_scan_grid,
    ):
        mock_clean_keyword.return_value = ("plumber", False)
        mock_generate_grid.return_value = [{"row": 0, "col": 0, "lat": 33.5001, "lng": -112.1999}]
        mock_calc_search_radius_m.return_value = 5000
        mock_verify_place_id.return_value = {"name": "Heatmap Test Brand Phoenix"}
        mock_scan_grid.return_value = ([{
            "row": 0,
            "col": 0,
            "lat": 33.5001,
            "lng": -112.1999,
            "rank": 2,
            "competitors": [
                {"rank": 1, "name": "Rival Plumbing", "place_id": "rival-1", "address": "123 Main St", "is_target": False},
                {"rank": 2, "name": "Heatmap Test Brand Phoenix", "place_id": "place-123", "address": "456 Oak Ave", "is_target": True},
            ],
        }], {})

        with self.app.app_context():
            self.app.db.update_brand_text_field(self.brand_id, "google_place_id", "place-123")

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
        self.assertEqual(mock_scan_grid.call_args.kwargs["alternate_names"], ["Heatmap Test Brand Phoenix"])
        self.assertEqual(payload["competitor_summary"][0]["name"], "Rival Plumbing")

        with self.app.app_context():
            saved_scan = self.app.db.get_heatmap_scan(payload["scan_id"])

        self.assertEqual(saved_scan["center_lat"], 33.5001)
        self.assertEqual(saved_scan["center_lng"], -112.1999)
        self.assertEqual(saved_scan["keyword"], "plumber")

    @patch("webapp.client_portal._start_heatmap_scan_job")
    @patch("webapp.heatmap.verify_place_id")
    @patch("webapp.heatmap.calc_search_radius_m")
    @patch("webapp.heatmap.generate_grid")
    @patch("webapp.heatmap.clean_keyword")
    def test_heatmap_scan_returns_pending_for_large_grid(
        self,
        mock_clean_keyword,
        mock_generate_grid,
        mock_calc_search_radius_m,
        mock_verify_place_id,
        mock_start_heatmap_scan_job,
    ):
        mock_clean_keyword.return_value = ("pet waste removal", False)
        mock_generate_grid.return_value = [
            {"row": index // 7, "col": index % 7, "lat": 33.5 + index * 0.0001, "lng": -112.1 - index * 0.0001}
            for index in range(49)
        ]
        mock_calc_search_radius_m.return_value = 5000
        mock_verify_place_id.return_value = {"name": "Heatmap Test Brand Phoenix"}

        with self.app.app_context():
            self.app.db.update_brand_text_field(self.brand_id, "google_place_id", "place-123")

        response = self.client.post(
            "/client/heatmap/scan",
            json={
                "keyword": "pet waste removal",
                "radius_miles": 10,
                "grid_size": 7,
                "center_lat": 33.5001,
                "center_lng": -112.1999,
            },
        )
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["pending"])
        self.assertEqual(payload["status"], "pending")
        mock_start_heatmap_scan_job.assert_called_once()

        with self.app.app_context():
            saved_scan = self.app.db.get_heatmap_scan(payload["scan_id"])

        self.assertEqual(saved_scan["status"], "pending")

    @patch("webapp.heatmap.scan_grid")
    @patch("webapp.heatmap.verify_place_id")
    @patch("webapp.heatmap.calc_search_radius_m")
    @patch("webapp.heatmap.generate_grid")
    @patch("webapp.heatmap.clean_keyword")
    def test_heatmap_scan_uses_fallback_maps_key(
        self,
        mock_clean_keyword,
        mock_generate_grid,
        mock_calc_search_radius_m,
        mock_verify_place_id,
        mock_scan_grid,
    ):
        mock_clean_keyword.return_value = ("plumber", False)
        mock_generate_grid.return_value = [{"row": 0, "col": 0, "lat": 33.5001, "lng": -112.1999}]
        mock_calc_search_radius_m.return_value = 5000
        mock_verify_place_id.return_value = {"name": "Heatmap Test Brand Phoenix"}
        mock_scan_grid.return_value = ([{"row": 0, "col": 0, "lat": 33.5001, "lng": -112.1999, "rank": 2, "competitors": []}], {})

        with self.app.app_context():
            self.app.db.update_brand_text_field(self.brand_id, "google_maps_api_key", "")
            self.app.db.update_brand_text_field(self.brand_id, "google_place_id", "place-123")

        with patch.dict(os.environ, {"GOOGLE_MAPS_API_KEY": "fallback-key"}, clear=False):
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

        self.assertEqual(response.status_code, 200)
        mock_verify_place_id.assert_called_once_with("fallback-key", "place-123")
        self.assertEqual(mock_scan_grid.call_args.args[0], "fallback-key")

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

    def test_match_business_normalizes_place_ids(self):
        places = [{"id": "places/place-123", "displayName": {"text": "Different Listing Name"}}]

        rank = _match_business(places, "Heatmap Test Brand", place_id="place-123")

        self.assertEqual(rank, 1)

    def test_match_business_accepts_verified_listing_alias(self):
        places = [{"id": "place-123", "displayName": {"text": "Heatmap Test Brand Phoenix"}}]

        rank = _match_business(
            places,
            "Heatmap Test Brand",
            alternate_names=["Heatmap Test Brand Phoenix"],
        )

        self.assertEqual(rank, 1)

    def test_match_business_accepts_franchise_market_suffix_alias(self):
        places = [{"id": "", "displayName": {"text": "DoodyCalls of Wake County"}}]

        rank = _match_business(places, "DoodyCalls of Raleigh")

        self.assertEqual(rank, 1)

    def test_extract_place_id_from_maps_href_reads_bang_19_token(self):
        href = "https://www.google.com/maps/place/Test/@33.4,-112.0,17z/data=!4m6!3m5!1s0x872b123456789abc:0xdef!8m2!3d33.4!4d-112.0!16s%2Fg%2F11c2abc!19sChIJ123PlaceToken"

        place_id = _extract_place_id_from_maps_href(href)

        self.assertEqual(place_id, "ChIJ123PlaceToken")

    def test_normalize_browser_maps_result_maps_scraped_row(self):
        place = _normalize_browser_maps_result({
            "name": "Rival Plumbing",
            "href": "https://www.google.com/maps/place/Rival/@33.4,-112.0,17z/data=!4m6!3m5!1s0x872b123456789abc:0xdef!8m2!3d33.4!4d-112.0!16s%2Fg%2F11c2abc!19sChIJBrowserPlaceId",
            "text": "Rival Plumbing · 123 Main St Phoenix, AZ 85001",
        })

        self.assertEqual(place["displayName"]["text"], "Rival Plumbing")
        self.assertEqual(place["id"], "ChIJBrowserPlaceId")
        self.assertEqual(place["formattedAddress"], "123 Main St Phoenix, AZ 85001")
        self.assertEqual(place["source"], "browser_maps")

    def test_competitor_summary_aggregates_grid_results(self):
        summary = summarize_competitor_landscape([
            {
                "row": 0,
                "col": 0,
                "rank": 3,
                "competitors": [
                    {"rank": 1, "name": "Rival Plumbing", "place_id": "rival-1", "address": "123 Main St", "is_target": False},
                    {"rank": 2, "name": "Heatmap Test Brand", "place_id": "place-123", "address": "456 Oak Ave", "is_target": True},
                ],
            },
            {
                "row": 0,
                "col": 1,
                "rank": 0,
                "competitors": [
                    {"rank": 2, "name": "Rival Plumbing", "place_id": "rival-1", "address": "123 Main St", "is_target": False},
                    {"rank": 3, "name": "Second Rival", "place_id": "rival-2", "address": "789 Elm St", "is_target": False},
                ],
            },
        ])

        self.assertEqual(summary[0]["name"], "Rival Plumbing")
        self.assertEqual(summary[0]["grid_share"], 2)
        self.assertEqual(summary[0]["best_rank"], 1)
        self.assertEqual(summary[0]["avg_rank"], 1.5)

    @patch("webapp.heatmap.requests.get")
    @patch("webapp.heatmap.requests.post")
    def test_search_places_uses_find_place_fallback_for_brand_queries(self, mock_post, mock_get):
        new_resp = Mock(status_code=200, text='{}')
        new_resp.json.return_value = {"places": []}
        mock_post.return_value = new_resp

        legacy_resp = Mock(status_code=200)
        legacy_resp.raise_for_status.return_value = None
        legacy_resp.json.return_value = {"status": "ZERO_RESULTS", "results": []}

        nearby_resp = Mock(status_code=200)
        nearby_resp.raise_for_status.return_value = None
        nearby_resp.json.return_value = {"status": "ZERO_RESULTS", "results": []}

        find_place_resp = Mock(status_code=200)
        find_place_resp.json.return_value = {
            "status": "OK",
            "candidates": [
                {
                    "place_id": "place-123",
                    "name": "Heatmap Test Brand Phoenix",
                    "formatted_address": "456 Oak Ave",
                }
            ],
        }

        mock_get.side_effect = [legacy_resp, nearby_resp, find_place_resp]

        places, diag = _search_places(
            "maps-key",
            "Heatmap Test Brand Phoenix",
            33.4484,
            -112.0740,
            radius_m=5000,
            brand_query=True,
            fallback_queries=["Heatmap Test Brand Phoenix"],
        )

        self.assertEqual(len(places), 1)
        self.assertEqual(places[0]["id"], "place-123")
        self.assertEqual(diag["find_place"]["count"], 1)

    @patch("webapp.heatmap.requests.get")
    @patch("webapp.heatmap.requests.post")
    def test_search_places_selects_provider_where_target_matches(self, mock_post, mock_get):
        new_resp = Mock(status_code=200, text='{}')
        new_resp.json.return_value = {
            "places": [
                {"id": "other-1", "displayName": {"text": "Other Plumbing"}},
                {"id": "other-2", "displayName": {"text": "Second Plumbing"}},
            ]
        }
        mock_post.return_value = new_resp

        legacy_resp = Mock(status_code=200)
        legacy_resp.raise_for_status.return_value = None
        legacy_resp.json.return_value = {
            "status": "OK",
            "results": [
                {"place_id": "place-123", "name": "Heatmap Test Brand Phoenix", "formatted_address": "456 Oak Ave"},
                {"place_id": "other-3", "name": "Third Plumbing", "formatted_address": "789 Elm St"},
            ],
        }

        nearby_resp = Mock(status_code=200)
        nearby_resp.raise_for_status.return_value = None
        nearby_resp.json.return_value = {"status": "ZERO_RESULTS", "results": []}
        mock_get.side_effect = [legacy_resp, nearby_resp]

        places, diag = _search_places(
            "maps-key",
            "plumber",
            33.4484,
            -112.0740,
            radius_m=5000,
            match_business_name="Heatmap Test Brand",
            match_place_id="place-123",
            match_alternate_names=["Heatmap Test Brand Phoenix"],
        )

        self.assertEqual(places[0]["id"], "place-123")
        self.assertEqual(diag["selected_provider"]["provider"], "legacy_api")
        self.assertEqual(diag["selected_provider"]["target_rank"], 1)

    def test_scan_grid_uses_places_when_browser_misses_linked_target(self):
        class FakeLocator:
            @property
            def first(self):
                return self

            def wait_for(self, **_kwargs):
                return None

            def count(self):
                return 1

            def evaluate(self, *_args, **_kwargs):
                return None

        class FakePage:
            def goto(self, *_args, **_kwargs):
                return None

            def wait_for_load_state(self, *_args, **_kwargs):
                return None

            def locator(self, *_args, **_kwargs):
                return FakeLocator()

            def wait_for_timeout(self, *_args, **_kwargs):
                return None

            def evaluate(self, *_args, **_kwargs):
                return [{
                    "rank": 1,
                    "name": "Other Plumbing",
                    "href": "https://www.google.com/maps/place/Other",
                    "aria": "Other Plumbing",
                    "text": "Other Plumbing 123 Main St",
                }]

            def title(self):
                return "Google Maps"

        class FakeContext:
            def set_geolocation(self, _location):
                return None

            def new_page(self):
                return FakePage()

        class FakeBrowser:
            def new_context(self, **_kwargs):
                return FakeContext()

        class FakeChromium:
            def launch(self, **_kwargs):
                return FakeBrowser()

        class FakePlaywright:
            chromium = FakeChromium()

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        mock_search_places = Mock()
        with patch("importlib.import_module") as mock_import_module, \
                patch.dict(scan_grid.__globals__, {"_search_places": mock_search_places}):
            mock_import_module.return_value = Mock(sync_playwright=lambda: FakePlaywright())
            mock_search_places.return_value = (
                [{"id": "place-123", "displayName": {"text": "Heatmap Test Brand Phoenix"}, "formattedAddress": "456 Oak Ave"}],
                {"selected_provider": {"provider": "legacy_api", "target_rank": 1}, "legacy_api": {"count": 1}},
            )

            results, debug = scan_grid(
                "maps-key",
                "plumber",
                "Heatmap Test Brand",
                [{"row": 0, "col": 0, "lat": 33.4484, "lng": -112.0740}],
                place_id="place-123",
                alternate_names=["Heatmap Test Brand Phoenix"],
                target_place_ids=["place-123"],
            )

            mock_search_places.assert_called_once()

        self.assertEqual(results[0]["rank"], 1)
        self.assertEqual(results[0]["competitors"][0]["place_id"], "place-123")
        self.assertTrue(results[0]["competitors"][0]["is_target"])
        self.assertEqual(debug["rank_provider"], "legacy_api")

    @patch("webapp.heatmap.scan_grid")
    @patch("webapp.heatmap.verify_place_id")
    @patch("webapp.heatmap.calc_search_radius_m")
    @patch("webapp.heatmap.generate_grid")
    @patch("webapp.heatmap.clean_keyword")
    def test_heatmap_scan_marks_brand_queries_for_fallback(
        self,
        mock_clean_keyword,
        mock_generate_grid,
        mock_calc_search_radius_m,
        mock_verify_place_id,
        mock_scan_grid,
    ):
        mock_clean_keyword.return_value = ("Heatmap Test Brand Phoenix", False)
        mock_generate_grid.return_value = [{"row": 0, "col": 0, "lat": 33.5001, "lng": -112.1999}]
        mock_calc_search_radius_m.return_value = 5000
        mock_verify_place_id.return_value = {"name": "Heatmap Test Brand Phoenix"}
        mock_scan_grid.return_value = ([{"row": 0, "col": 0, "lat": 33.5001, "lng": -112.1999, "rank": 1, "competitors": []}], {})

        with self.app.app_context():
            self.app.db.update_brand_text_field(self.brand_id, "google_place_id", "place-123")

        response = self.client.post(
            "/client/heatmap/scan",
            json={
                "keyword": "Heatmap Test Brand Phoenix",
                "radius_miles": 3,
                "grid_size": 5,
                "center_lat": 33.5001,
                "center_lng": -112.1999,
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(mock_scan_grid.call_args.kwargs["brand_query"])


if __name__ == "__main__":
    unittest.main()
