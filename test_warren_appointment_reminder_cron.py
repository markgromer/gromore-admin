import os
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch


_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
_BOOTSTRAP_DB = str(_TEST_ROOT / "gromore-appointment-cron-bootstrap.db")
os.environ.setdefault("DATABASE_PATH", _BOOTSTRAP_DB)
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("APP_URL", "http://localhost:5000")

from webapp.app import create_app


class WarrenAppointmentReminderCronAuthTests(unittest.TestCase):
    def setUp(self):
        self.db_file = _TEST_ROOT / f"appointment-cron-{uuid.uuid4().hex}.db"
        os.environ["DATABASE_PATH"] = str(self.db_file)
        os.environ["SECRET_KEY"] = "test-secret"
        os.environ["CRON_SECRET"] = "cron-only-secret"
        os.environ["APP_URL"] = "http://localhost:5000"

        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)
        self.client = self.app.test_client()

    def tearDown(self):
        for key in ("DATABASE_PATH", "SECRET_KEY", "CRON_SECRET", "APP_URL"):
            os.environ.pop(key, None)
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    def test_appointment_cron_accepts_secret_key_when_cron_secret_is_set(self):
        with patch(
            "webapp.warren_appointments.process_appointment_reminders",
            return_value={"brands": 1, "sent": 1, "failed": 0, "skipped": 0, "candidates": 1, "errors": []},
        ) as process_reminders:
            response = self.client.post(
                "/jobs/cron/warren-appointment-reminders",
                headers={"Authorization": "Bearer test-secret"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["ok"])
        process_reminders.assert_called_once()


if __name__ == "__main__":
    unittest.main()