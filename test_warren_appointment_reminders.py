import unittest
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

from webapp.crm_bridge import sng_get_day_ahead_appointment_candidates
from webapp.database import WebDB
from webapp.warren_appointments import process_appointment_reminders


_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)


class WarrenAppointmentReminderTests(unittest.TestCase):
    def setUp(self):
        self.db_file = _TEST_ROOT / f"appointment-reminders-{uuid.uuid4().hex}.db"
        self.db = WebDB(str(self.db_file))
        self.db.init()
        self.brand_id = self.db.create_brand({
            "slug": f"appointment-brand-{uuid.uuid4().hex[:8]}",
            "display_name": "Appointment Reminder Co",
        })
        self.db.update_brand_text_field(self.brand_id, "crm_type", "sweepandgo")
        self.db.update_brand_text_field(self.brand_id, "crm_api_key", "sng-test-token")
        self.db.update_brand_number_field(self.brand_id, "sales_bot_appointment_reminders_enabled", 1)
        self.db.update_brand_text_field(self.brand_id, "sales_bot_appointment_reminder_send_time", "17:00")
        self.db.update_brand_text_field(self.brand_id, "sales_bot_appointment_reminder_timezone", "America/New_York")
        self.db.update_brand_text_field(self.brand_id, "sales_bot_appointment_reminder_channels", "sms,email")
        self.db.update_brand_number_field(self.brand_id, "sales_bot_appointment_reminder_respect_client_channel", 1)
        self.db.update_brand_text_field(self.brand_id, "quo_api_key", "quo-test-key")
        self.db.update_brand_text_field(self.brand_id, "quo_phone_number", "+15550001111")
        self.brand = self.db.get_brand(self.brand_id)

    def tearDown(self):
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    def test_dispatch_board_candidates_filter_and_dedupe(self):
        dispatch_payload = {
            "data": [
                {
                    "id": 0,
                    "client_location_id": 22,
                    "client_id": 7,
                    "full_name": "Taylor Client",
                    "email": "taylor@example.com",
                    "cell_phone": "+15555550123",
                    "status_name": "pending",
                    "type": "recurring",
                    "assigned_to_name": "Jordan Tech",
                    "address": "123 Main St",
                    "city": "Albany",
                    "state_name": "NY",
                    "channel": "sms",
                    "on_the_way": 1,
                },
                {
                    "id": 0,
                    "client_location_id": 22,
                    "client_id": 7,
                    "full_name": "Taylor Client",
                    "email": "taylor@example.com",
                    "cell_phone": "+15555550123",
                    "status_name": "pending",
                    "type": "recurring",
                    "assigned_to_name": "Jordan Tech",
                    "address": "123 Main St",
                    "city": "Albany",
                    "state_name": "NY",
                },
                {
                    "id": 991,
                    "full_name": "Skip Me",
                    "email": "skip@example.com",
                    "cell_phone": "+15555550000",
                    "status_name": "completed",
                    "type": "recurring",
                },
            ]
        }
        with patch("webapp.crm_bridge.sng_get_dispatch_board", return_value=(dispatch_payload, None)):
            candidates, error = sng_get_day_ahead_appointment_candidates(self.brand, date(2026, 4, 18))

        self.assertIsNone(error)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["preferred_channel"], "sms")
        self.assertEqual(candidates[0]["appointment_date"], "2026-04-18")

    def test_process_appointment_reminders_respects_send_time_and_dedupes(self):
        candidate = {
            "appointment_key": "job:123",
            "appointment_date": "2026-04-18",
            "appointment_date_obj": date(2026, 4, 18),
            "client_name": "Taylor Client",
            "client_email": "taylor@example.com",
            "client_phone": "+15555550123",
            "assigned_to_name": "Jordan Tech",
            "address": "123 Main St, Albany, NY",
            "preferred_channel": "sms",
            "prefers_sms": True,
            "prefers_email": False,
            "job_id": "123",
            "status_name": "pending",
        }
        app_config = {
            "SMTP_HOST": "smtp.example.com",
            "SMTP_PORT": 587,
            "SMTP_USER": "test",
            "SMTP_PASSWORD": "secret",
            "SMTP_FROM_EMAIL": "appointments@example.com",
        }
        before_send_time = datetime(2026, 4, 17, 20, 30, tzinfo=timezone.utc)
        after_send_time = datetime(2026, 4, 17, 21, 30, tzinfo=timezone.utc)

        with patch("webapp.warren_appointments.sng_get_day_ahead_appointment_candidates", return_value=([candidate], None)), \
             patch("webapp.warren_appointments.send_simple_email") as send_email, \
             patch("webapp.warren_appointments.send_transactional_sms", return_value=(True, "sent")) as send_sms:
            stats_early = process_appointment_reminders(self.db, app_config, now=before_send_time)
            stats_first = process_appointment_reminders(self.db, app_config, now=after_send_time)
            stats_second = process_appointment_reminders(self.db, app_config, now=after_send_time)

        self.assertEqual(stats_early["brands"], 0)
        self.assertEqual(stats_first["sent"], 1)
        self.assertEqual(stats_first["failed"], 0)
        self.assertGreaterEqual(stats_second["skipped"], 1)
        send_sms.assert_called_once()
        send_email.assert_not_called()

        sms_row = self.db.get_client_billing_reminder(
            self.brand_id,
            "job:123",
            "2026-04-18",
            "sms",
            reminder_type="appointment_day_ahead",
        )
        self.assertEqual(sms_row["status"], "sent")


if __name__ == "__main__":
    unittest.main()