import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from webapp.crm_bridge import _next_uniform_billing_date, sng_get_payment_reminder_candidates
from webapp.database import WebDB
from webapp.warren_billing import process_payment_reminders


_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)


class WarrenPaymentReminderTests(unittest.TestCase):
    def setUp(self):
        self.db_file = _TEST_ROOT / f"payment-reminders-{uuid.uuid4().hex}.db"
        self.db = WebDB(str(self.db_file))
        self.db.init()
        self.brand_id = self.db.create_brand({
            "slug": f"payment-brand-{uuid.uuid4().hex[:8]}",
            "display_name": "Payment Reminder Co",
        })
        self.db.update_brand_text_field(self.brand_id, "crm_type", "sweepandgo")
        self.db.update_brand_text_field(self.brand_id, "crm_api_key", "sng-test-token")
        self.db.update_brand_number_field(self.brand_id, "sales_bot_payment_reminders_enabled", 1)
        self.db.update_brand_number_field(self.brand_id, "sales_bot_payment_reminder_days_before", 3)
        self.db.update_brand_number_field(self.brand_id, "sales_bot_payment_reminder_billing_day", 11)
        self.db.update_brand_text_field(self.brand_id, "sales_bot_payment_reminder_channels", "email,sms")
        self.db.update_brand_text_field(self.brand_id, "quo_api_key", "quo-test-key")
        self.db.update_brand_text_field(self.brand_id, "quo_phone_number", "+15550001111")
        self.brand = self.db.get_brand(self.brand_id)

    def tearDown(self):
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    def test_next_uniform_billing_date_rolls_forward(self):
        from datetime import date

        due_date = _next_uniform_billing_date(11, today=date(2026, 4, 8))
        self.assertEqual(due_date.isoformat(), "2026-04-11")

        rolled = _next_uniform_billing_date(1, today=date(2026, 4, 8))
        self.assertEqual(rolled.isoformat(), "2026-05-01")

    def test_active_clients_become_reminder_candidates(self):
        from datetime import date

        active_clients_payload = {
            "data": [
                {
                    "client": "rcl_123",
                    "name": "Taylor Client",
                    "email": "taylor@example.com",
                    "phone": "+15555550123",
                }
            ],
            "paginate": {"total_pages": 1},
        }

        with patch("webapp.crm_bridge.sng_get_active_clients", return_value=(active_clients_payload, None)):
            candidates, error = sng_get_payment_reminder_candidates(
                self.brand,
                billing_day=11,
                days_before=3,
                today=date(2026, 4, 8),
            )

        self.assertIsNone(error)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["external_client_id"], "rcl_123")
        self.assertEqual(candidates[0]["due_date"], "2026-04-11")

    def test_process_payment_reminders_dedupes_after_send(self):
        from datetime import date

        candidate = {
            "external_client_id": "rcl_123",
            "client_name": "Taylor Client",
            "client_email": "taylor@example.com",
            "client_phone": "+15555550123",
            "due_date": "2026-04-11",
            "due_date_obj": date(2026, 4, 11),
            "days_before": 3,
            "last_payment_date": "2026-03-11",
            "last_payment_amount": 49.0,
        }

        app_config = {
            "SMTP_HOST": "smtp.example.com",
            "SMTP_PORT": 587,
            "SMTP_USER": "test",
            "SMTP_PASSWORD": "secret",
            "SMTP_FROM_EMAIL": "billing@example.com",
        }

        with patch("webapp.warren_billing.sng_get_payment_reminder_candidates", return_value=([candidate], None)), \
             patch("webapp.warren_billing.send_simple_email") as send_email, \
             patch("webapp.warren_billing.send_transactional_sms", return_value=(True, "sent")) as send_sms:
            stats_first = process_payment_reminders(self.db, app_config, today=date(2026, 4, 8))
            stats_second = process_payment_reminders(self.db, app_config, today=date(2026, 4, 8))

        self.assertEqual(stats_first["sent"], 2)
        self.assertEqual(stats_first["failed"], 0)
        self.assertGreaterEqual(stats_second["skipped"], 2)
        send_email.assert_called_once()
        send_sms.assert_called_once()

        email_row = self.db.get_client_billing_reminder(self.brand_id, "rcl_123", "2026-04-11", "email")
        sms_row = self.db.get_client_billing_reminder(self.brand_id, "rcl_123", "2026-04-11", "sms")
        self.assertEqual(email_row["status"], "sent")
        self.assertEqual(sms_row["status"], "sent")


if __name__ == "__main__":
    unittest.main()