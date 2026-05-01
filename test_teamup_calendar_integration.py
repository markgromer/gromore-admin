import json
import os
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
os.environ.setdefault("DATABASE_PATH", str(_TEST_ROOT / "teamup-bootstrap.db"))
os.environ.setdefault("SECRET_KEY", "test-secret")

from webapp.app import create_app
from webapp.connection_health import evaluate_brand_connection_health
from webapp.teamup_calendar import teamup_create_event, teamup_list_events, teamup_test_connection
from webapp.warren_appointments import process_appointment_reminders


class _Resp:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = text if text else json.dumps(self._payload)

    def json(self):
        return self._payload


class TeamupCalendarIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.db_file = _TEST_ROOT / f"teamup-{uuid.uuid4().hex}.db"
        os.environ["DATABASE_PATH"] = str(self.db_file)
        os.environ["SECRET_KEY"] = "test-secret"
        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False, APP_URL="http://localhost")
        self.client = self.app.test_client()
        with self.app.app_context():
            self.brand_id = self.app.db.create_brand({
                "slug": f"teamup-brand-{uuid.uuid4().hex[:8]}",
                "display_name": "Teamup Brand",
            })
            self.app.db.upsert_brand_integration_config(self.brand_id, "teamup_calendar", {
                "calendar_key": "ks1234567890abcdef",
                "api_key": "teamup-api-key",
                "subcalendar_id": "42",
                "webhook_secret": "teamup-secret",
            })

    def tearDown(self):
        os.environ.pop("DATABASE_PATH", None)
        os.environ.pop("SECRET_KEY", None)
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    def test_teamup_live_test_uses_api_key_and_calendar_key(self):
        calls = []

        def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
            calls.append((method, url, headers or {}, params or {}))
            self.assertEqual(headers.get("Teamup-Token"), "teamup-api-key")
            self.assertEqual(url, "https://api.teamup.com/ks1234567890abcdef/subcalendars")
            return _Resp(payload={"subcalendars": [{"id": 42, "name": "Bookings"}]})

        with self.app.app_context():
            config = self.app.db.get_brand_integration_config(self.brand_id, "teamup_calendar")["config"]
            with patch("webapp.teamup_calendar.requests.request", side_effect=fake_request):
                ok, message = teamup_test_connection(config)

        self.assertTrue(ok)
        self.assertIn("Found 1 sub-calendar", message)
        self.assertEqual(calls[0][0], "GET")

    def test_teamup_can_list_and_create_events(self):
        requests_seen = []

        def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
            requests_seen.append((method, url, params, json))
            if method == "GET":
                self.assertEqual(params["startDate"], "2026-05-01")
                self.assertEqual(params["endDate"], "2026-05-07")
                return _Resp(payload={"events": [{"id": "evt_1", "title": "Estimate", "start_dt": "2026-05-02T10:00:00"}]})
            self.assertEqual(method, "POST")
            self.assertEqual(json["subcalendar_ids"], [42])
            self.assertEqual(json["title"], "New Estimate")
            return _Resp(payload={"event": {"id": "evt_2", "title": json["title"], "start_dt": json["start_dt"], "end_dt": json["end_dt"]}})

        with self.app.app_context():
            config = self.app.db.get_brand_integration_config(self.brand_id, "teamup_calendar")["config"]
            with patch("webapp.teamup_calendar.requests.request", side_effect=fake_request):
                events = teamup_list_events(config, "2026-05-01", "2026-05-07")
                created = teamup_create_event(
                    config,
                    title="New Estimate",
                    start_dt="2026-05-03T09:00:00",
                    end_dt="2026-05-03T10:00:00",
                    location="Tucson",
                )

        self.assertEqual(events[0]["external_event_id"], "evt_1")
        self.assertEqual(created["external_event_id"], "evt_2")
        self.assertEqual(requests_seen[1][1], "https://api.teamup.com/ks1234567890abcdef/events")

    def test_teamup_webhook_requires_secret_and_records_event(self):
        with self.app.app_context():
            brand = self.app.db.get_brand(self.brand_id)
        payload = {
            "event_type": "event.updated",
            "event": {
                "id": "evt_teamup_1",
                "title": "Follow-up visit",
                "start_dt": "2026-05-02T15:00:00",
                "end_dt": "2026-05-02T16:00:00",
                "location": "Customer home",
            },
        }

        rejected = self.client.post(f"/webhooks/teamup/{brand['slug']}", json=payload)
        self.assertEqual(rejected.status_code, 403)

        accepted = self.client.post(
            f"/webhooks/teamup/{brand['slug']}",
            json=payload,
            headers={"Authorization": "Bearer teamup-secret"},
        )
        self.assertEqual(accepted.status_code, 200)
        self.assertTrue(accepted.get_json()["ok"])

        with self.app.app_context():
            events = self.app.db.get_integration_webhook_events(self.brand_id, provider="teamup_calendar")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["external_event_id"], "evt_teamup_1")
        self.assertEqual(events[0]["event_type"], "event.updated")
        self.assertIn("Follow-up visit", events[0]["detail"])

    def test_connection_health_reports_teamup_readiness(self):
        with self.app.app_context():
            brand = self.app.db.get_brand(self.brand_id)
            items = evaluate_brand_connection_health(self.app.db, brand, persist=False)

        by_key = {item["key"]: item for item in items}
        self.assertEqual(by_key["teamup_calendar"]["status"], "warn")
        self.assertIn("webhook", by_key["teamup_calendar"]["detail"].lower())

    def test_client_settings_route_can_test_teamup(self):
        with self.app.app_context():
            user_id = self.app.db.create_client_user(self.brand_id, "owner@example.test", "Password123", "Owner")
        with self.client.session_transaction() as session:
            session["client_user_id"] = user_id
            session["client_brand_id"] = self.brand_id
            session["client_name"] = "Owner"
            session["client_brand_name"] = "Teamup Brand"

        def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
            return _Resp(payload={"subcalendars": [{"id": 42, "name": "Bookings"}]})

        with patch("webapp.teamup_calendar.requests.request", side_effect=fake_request):
            response = self.client.post("/client/integration/teamup_calendar/test")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["ok"])

    def test_teamup_events_feed_appointment_reminders(self):
        with self.app.app_context():
            self.app.db.update_brand_number_field(self.brand_id, "sales_bot_appointment_reminders_enabled", 1)
            self.app.db.update_brand_text_field(self.brand_id, "sales_bot_appointment_reminder_channels", "sms")
            self.app.db.update_brand_text_field(self.brand_id, "sales_bot_appointment_reminder_send_time", "08:00")
            self.app.db.update_brand_text_field(self.brand_id, "sales_bot_appointment_reminder_timezone", "America/Phoenix")

        def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
            self.assertEqual(method, "GET")
            self.assertTrue(url.endswith("/events"))
            return _Resp(payload={"events": [{
                "id": "evt_reminder_1",
                "title": "Service visit",
                "start_dt": "2026-05-02T10:00:00",
                "end_dt": "2026-05-02T11:00:00",
                "who": "Jane Customer +15205550123",
                "location": "123 Main St",
            }]})

        with patch("webapp.teamup_calendar.requests.request", side_effect=fake_request), patch(
            "webapp.warren_appointments.send_transactional_sms",
            return_value=(True, "sent"),
        ):
            with self.app.app_context():
                stats = process_appointment_reminders(
                    self.app.db,
                    self.app.config,
                    now=datetime(2026, 5, 1, 16, 0, tzinfo=timezone.utc),
                    brand_ids=[self.brand_id],
                    ignore_send_time=True,
                )
                reminders = self.app.db.get_brand_client_billing_reminders(self.brand_id, reminder_type="appointment_day_ahead")

        self.assertEqual(stats["sent"], 1)
        self.assertEqual(len(reminders), 1)
        self.assertEqual(reminders[0]["external_client_id"].startswith("teamup:"), True)


if __name__ == "__main__":
    unittest.main()
