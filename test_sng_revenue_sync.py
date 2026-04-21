import unittest
from unittest.mock import patch

from webapp.crm_bridge import (
    _sng_extract_payments,
    _sng_sum_payments_for_month,
    _sng_sum_webhook_payments_for_month,
    sng_sync_revenue,
)


class _DummyDb:
    def __init__(self):
        self.finance_upserts = []
        self.saved_settings = {}

    def upsert_brand_month_finance(self, brand_id, month, **kwargs):
        self.finance_upserts.append((brand_id, month, kwargs))

    def save_setting(self, key, value):
        self.saved_settings[key] = value


class SngRevenueSyncTests(unittest.TestCase):
    def test_extract_payments_accepts_top_level_payment_list(self):
        payload = [
            {
                "amount": "120.00",
                "tip_amount": "15.00",
                "status": "succeeded",
                "date": "2026-03-12",
            }
        ]

        payments = _sng_extract_payments(payload)

        self.assertEqual(len(payments), 1)
        self.assertEqual(payments[0]["amount"], "120.00")

    @patch("webapp.crm_bridge.sng_get_client_details")
    def test_sum_payments_handles_client_detail_list_payload(self, mock_get_client_details):
        mock_get_client_details.return_value = ([
            {
                "amount": "120.00",
                "tip_amount": "15.00",
                "status": "succeeded",
                "date": "2026-03-12",
            },
            {
                "amount": "90.00",
                "status": "failed",
                "date": "2026-03-13",
            },
        ], None)

        revenue, payment_count, diagnostics = _sng_sum_payments_for_month(
            brand={"crm_api_key": "token"},
            client_ids=["rcl_test_client"],
            month_prefix="2026-03",
        )

        self.assertEqual(revenue, 135.0)
        self.assertEqual(payment_count, 1)
        self.assertEqual(diagnostics["errors"], 0)
        self.assertEqual(diagnostics["clients_with_payments"], 1)
        self.assertEqual(diagnostics["sample_response_keys"], ["payments"])

    @patch("webapp.crm_bridge.sng_list_webhook_events")
    def test_sum_webhook_payments_uses_accepted_payment_events(self, mock_list_webhook_events):
        mock_list_webhook_events.side_effect = [
            ({
                "data": [
                    {
                        "event_type": "client:client_payment_accepted",
                        "payment": {
                            "id": "pay_123",
                            "amount": "120.00",
                            "tip_amount": "15.00",
                            "created_at": "2026-03-12 10:00:00",
                        },
                    },
                    {
                        "event_type": "client:invoice_finalized",
                        "invoice": {
                            "id": "inv_1",
                            "amount": "80.00",
                            "created_at": "2026-03-13 09:00:00",
                        },
                    },
                ],
                "paginate": {"total_pages": 1},
            }, None)
        ]

        revenue, payment_count, diagnostics = _sng_sum_webhook_payments_for_month(
            brand={"crm_api_key": "token"},
            month_prefix="2026-03",
        )

        self.assertEqual(revenue, 135.0)
        self.assertEqual(payment_count, 1)
        self.assertEqual(diagnostics["matched_events"], 1)
        self.assertEqual(diagnostics["sample_event"]["payment_id"], "pay_123")

    @patch("webapp.crm_bridge._sng_sum_webhook_payments_for_month")
    @patch("webapp.crm_bridge._sng_sum_payments_for_month")
    @patch("webapp.crm_bridge.sng_get_active_clients")
    @patch("webapp.crm_bridge.sng_count_jobs")
    @patch("webapp.crm_bridge.sng_get_inactive_clients")
    @patch("webapp.crm_bridge.sng_count_active_clients")
    def test_sync_revenue_falls_back_to_webhook_history(
        self,
        mock_count_active_clients,
        mock_get_inactive_clients,
        mock_count_jobs,
        mock_get_active_clients,
        mock_sum_payments,
        mock_sum_webhook_payments,
    ):
        mock_count_active_clients.return_value = ({"data": 2}, None)
        mock_get_inactive_clients.return_value = ({"data": [], "paginate": {"total": 1, "total_pages": 1}}, None)
        mock_count_jobs.return_value = ({"data": 9}, None)
        mock_get_active_clients.return_value = ({
            "data": [
                {"client": "rcl_1", "subscription_names": "Weekly"},
                {"client": "rcl_2", "subscription_names": "Weekly"},
            ],
            "paginate": {"total_pages": 1},
        }, None)
        mock_sum_payments.return_value = (0.0, 0, {"all_payment_months": {}, "all_payment_statuses": {}, "first_error": None})
        mock_sum_webhook_payments.return_value = (210.0, 2, {"rows_seen": 4, "matched_events": 2})

        db = _DummyDb()
        snapshot = sng_sync_revenue(
            brand={"id": 9, "crm_api_key": "token"},
            db=db,
            max_sample=10,
            month="2026-03",
        )

        self.assertEqual(snapshot["mrr"], 210.0)
        self.assertEqual(snapshot["payment_count"], 2)
        self.assertEqual(snapshot["data_source"], "webhook_payment_history")
        self.assertEqual(snapshot["scale_factor"], 1)
        self.assertEqual(snapshot["webhook_diagnostics"]["matched_events"], 2)
        self.assertEqual(db.finance_upserts[0][2]["notes"], "SNG sync from webhook payment history")


if __name__ == "__main__":
    unittest.main()