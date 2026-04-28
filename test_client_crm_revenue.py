import os
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
_BOOTSTRAP_DB = str(_TEST_ROOT / "gromore-client-crm-revenue-bootstrap.db")
os.environ.setdefault("DATABASE_PATH", _BOOTSTRAP_DB)
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("APP_URL", "http://localhost:5000")

from webapp.app import create_app


class ClientCrmRevenueTests(unittest.TestCase):
    def setUp(self):
        self.db_file = _TEST_ROOT / f"client-crm-revenue-{uuid.uuid4().hex}.db"
        os.environ["DATABASE_PATH"] = str(self.db_file)
        os.environ["SECRET_KEY"] = "test-secret"
        os.environ["APP_URL"] = "http://localhost:5000"

        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)
        self.client = self.app.test_client()

        with self.app.app_context():
            self.brand_id = self.app.db.create_brand({
                "slug": f"client-crm-revenue-{uuid.uuid4().hex[:8]}",
                "display_name": "CRM Revenue Brand",
            })
            self.app.db.update_brand_text_field(self.brand_id, "crm_type", "sweepandgo")
            self.app.db.update_brand_text_field(self.brand_id, "crm_api_key", "test-sng-token")
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
            session["client_brand_name"] = "CRM Revenue Brand"

    def tearDown(self):
        for key in ("DATABASE_PATH", "SECRET_KEY", "APP_URL"):
            os.environ.pop(key, None)
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    @patch("webapp.report_runner.get_analysis_and_suggestions_for_brand", return_value=(None, None))
    @patch("webapp.crm_bridge.sng_sync_revenue")
    @patch("webapp.crm_bridge.sng_get_cached_revenue")
    @patch("webapp.crm_bridge.sng_get_free_quotes")
    @patch("webapp.crm_bridge.sng_get_leads")
    @patch("webapp.crm_bridge.sng_get_active_no_subscription")
    @patch("webapp.crm_bridge.sng_get_inactive_clients")
    @patch("webapp.crm_bridge.sng_get_active_clients")
    @patch("webapp.crm_bridge.sng_count_jobs")
    @patch("webapp.crm_bridge.sng_count_happy_dogs")
    @patch("webapp.crm_bridge.sng_count_happy_clients")
    @patch("webapp.crm_bridge.sng_count_active_clients")
    def test_crm_data_auto_syncs_when_revenue_cache_missing(
        self,
        mock_count_active,
        mock_count_happy,
        mock_count_dogs,
        mock_count_jobs,
        mock_active_clients,
        mock_inactive,
        mock_no_sub,
        mock_leads,
        mock_quotes,
        mock_get_cached,
        mock_sync,
        mock_analysis,
    ):
        mock_count_active.return_value = ({"data": 12}, None)
        mock_count_happy.return_value = ({"data": 9}, None)
        mock_count_dogs.return_value = ({"data": 27}, None)
        mock_count_jobs.return_value = ({"data": 144}, None)
        mock_active_clients.return_value = ({"data": [], "paginate": {"total": 0}}, None)
        mock_inactive.return_value = ({"data": [], "paginate": {"total": 0}}, None)
        mock_no_sub.return_value = ({"data": [], "paginate": {"total": 0}}, None)
        mock_leads.return_value = ({"data": [], "paginate": {"total": 0}}, None)
        mock_quotes.return_value = ({"free_quotes": []}, None)
        mock_get_cached.return_value = {}
        mock_sync.return_value = {
            "mrr": 8420.0,
            "estimated_clv": 151560.0,
            "churn_cost_total": 303120.0,
            "avg_client_monthly_value": 467.78,
            "inactive_clients": 2,
            "synced_at": "2026-04-20 09:30:00",
            "revenue_month": "2026-03",
        }

        response = self.client.get("/client/crm/data")
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["revenue"]["mrr"], 8420.0)
        mock_get_cached.assert_called_once()
        mock_sync.assert_called_once()

    @patch("webapp.report_runner.get_analysis_and_suggestions_for_brand", return_value=(None, None))
    @patch("webapp.crm_bridge.sng_sync_revenue")
    @patch("webapp.crm_bridge.sng_get_cached_revenue")
    @patch("webapp.crm_bridge.sng_get_free_quotes")
    @patch("webapp.crm_bridge.sng_get_leads")
    @patch("webapp.crm_bridge.sng_get_active_no_subscription")
    @patch("webapp.crm_bridge.sng_get_inactive_clients")
    @patch("webapp.crm_bridge.sng_get_active_clients")
    @patch("webapp.crm_bridge.sng_count_jobs")
    @patch("webapp.crm_bridge.sng_count_happy_dogs")
    @patch("webapp.crm_bridge.sng_count_happy_clients")
    @patch("webapp.crm_bridge.sng_count_active_clients")
    def test_crm_data_uses_fresh_cached_revenue_without_sync(
        self,
        mock_count_active,
        mock_count_happy,
        mock_count_dogs,
        mock_count_jobs,
        mock_active_clients,
        mock_inactive,
        mock_no_sub,
        mock_leads,
        mock_quotes,
        mock_get_cached,
        mock_sync,
        mock_analysis,
    ):
        mock_count_active.return_value = ({"data": 12}, None)
        mock_count_happy.return_value = ({"data": 9}, None)
        mock_count_dogs.return_value = ({"data": 27}, None)
        mock_count_jobs.return_value = ({"data": 144}, None)
        mock_active_clients.return_value = ({"data": [], "paginate": {"total": 0}}, None)
        mock_inactive.return_value = ({"data": [], "paginate": {"total": 0}}, None)
        mock_no_sub.return_value = ({"data": [], "paginate": {"total": 0}}, None)
        mock_leads.return_value = ({"data": [], "paginate": {"total": 0}}, None)
        mock_quotes.return_value = ({"free_quotes": []}, None)
        mock_get_cached.return_value = {
            "mrr": 7900.0,
            "estimated_clv": 142200.0,
            "churn_cost_total": 284400.0,
            "avg_client_monthly_value": 438.89,
            "inactive_clients": 2,
            "synced_at": "2099-04-20 09:30:00",
            "revenue_month": "2026-03",
        }

        response = self.client.get("/client/crm/data")
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["revenue"]["mrr"], 7900.0)
        mock_get_cached.assert_called_once()
        mock_sync.assert_not_called()

    @patch("webapp.report_runner.get_analysis_and_suggestions_for_brand", return_value=(None, None))
    @patch("webapp.crm_bridge.sng_sync_revenue")
    @patch("webapp.crm_bridge.sng_get_cached_revenue")
    @patch("webapp.crm_bridge.sng_get_free_quotes")
    @patch("webapp.crm_bridge.sng_get_leads")
    @patch("webapp.crm_bridge.sng_get_active_no_subscription")
    @patch("webapp.crm_bridge.sng_get_inactive_clients")
    @patch("webapp.crm_bridge.sng_get_active_clients")
    @patch("webapp.crm_bridge.sng_count_jobs")
    @patch("webapp.crm_bridge.sng_count_happy_dogs")
    @patch("webapp.crm_bridge.sng_count_happy_clients")
    @patch("webapp.crm_bridge.sng_count_active_clients")
    def test_crm_data_builds_opportunity_snapshot_when_revenue_unavailable(
        self,
        mock_count_active,
        mock_count_happy,
        mock_count_dogs,
        mock_count_jobs,
        mock_active_clients,
        mock_inactive,
        mock_no_sub,
        mock_leads,
        mock_quotes,
        mock_get_cached,
        mock_sync,
        mock_analysis,
    ):
        mock_count_active.return_value = ({"data": 12}, None)
        mock_count_happy.return_value = ({"data": 9}, None)
        mock_count_dogs.return_value = ({"data": 27}, None)
        mock_count_jobs.return_value = ({"data": 144}, None)
        mock_active_clients.return_value = ({"data": [], "paginate": {"total": 12}}, None)
        mock_inactive.return_value = ({"data": [], "paginate": {"total": 5}}, None)
        mock_no_sub.return_value = ({"data": [], "paginate": {"total": 3}}, None)
        mock_leads.return_value = ({"data": [], "paginate": {"total": 7}}, None)
        mock_quotes.return_value = ({"free_quotes": [{"id": 1}, {"id": 2}]}, None)
        mock_get_cached.return_value = {
            "mrr": 0.0,
            "estimated_clv": 0.0,
            "churn_cost_total": 0.0,
            "avg_client_monthly_value": 0.0,
            "inactive_clients": 5,
            "synced_at": "2099-04-20 09:30:00",
            "revenue_month": "2026-03",
            "debug_note": "SNG sync returned no payments in sampled client_details responses and found no accepted-payment events in webhook history for 2026-03.",
        }

        response = self.client.get("/client/crm/data")
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["revenue"]["availability"], "unavailable")
        self.assertEqual(payload["crm_snapshot"]["panel_mode"], "opportunity")
        self.assertEqual(payload["crm_snapshot"]["subscribed_clients"], 9)
        self.assertEqual(payload["crm_snapshot"]["follow_up_queue"], 12)

        self.assertEqual(payload["crm_snapshot"]["warm_pipeline_total"], 9)
        self.assertEqual(payload["crm_snapshot"]["growth_surface_total"], 17)
        self.assertEqual(payload["crm_snapshot"]["subscription_coverage_pct"], 75)
        self.assertEqual(payload["crm_snapshot"]["subscription_gap_pct"], 25)
        self.assertEqual(payload["crm_snapshot"]["happy_client_rate_pct"], 75)
        self.assertEqual(payload["crm_snapshot"]["free_quotes_total"], 2)
        self.assertEqual(payload["crm_snapshot"]["happy_clients"], 9)
        self.assertEqual(len(payload["crm_snapshot"]["priorities"]), 3)
        self.assertEqual(payload["crm_snapshot"]["priorities"][0]["count"], 3)
        mock_sync.assert_not_called()

    def test_ghl_crm_page_uses_import_workspace_without_sng_loader(self):
        with self.app.app_context():
            self.app.db.update_brand_text_field(self.brand_id, "crm_type", "gohighlevel")
            self.app.db.update_brand_text_field(self.brand_id, "crm_api_key", "test-ghl-token")

        response = self.client.get("/client/crm")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Import GoHighLevel Leads", response.data)
        self.assertIn(b"CRM import workspace", response.data)
        self.assertNotIn(b"Pulling data from Sweep and Go", response.data)


if __name__ == "__main__":
    unittest.main()
