import unittest
from unittest.mock import patch

from webapp.crm_bridge import _sng_extract_payments, _sng_sum_payments_for_month


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


if __name__ == "__main__":
    unittest.main()