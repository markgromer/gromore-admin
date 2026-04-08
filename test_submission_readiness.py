import unittest

from webapp.app import create_app
from webapp.warren_sender import _messenger_response_window_open


class SubmissionSurfaceTests(unittest.TestCase):
    def setUp(self):
        self.app = create_app()
        self.app.config["TESTING"] = True
        self.client = self.app.test_client()

    def test_public_legal_pages_load(self):
        for path in ("/privacy", "/terms", "/meta/data-deletion"):
            resp = self.client.get(path)
            self.assertEqual(resp.status_code, 200, path)


class MessengerPolicyTests(unittest.TestCase):
    def test_response_window_open_with_recent_inbound(self):
        thread = {"last_inbound_at": "2026-04-08T10:00:00"}
        from datetime import datetime

        self.assertTrue(
            _messenger_response_window_open(thread, now=datetime(2026, 4, 9, 9, 59, 59))
        )

    def test_response_window_closed_after_24_hours(self):
        thread = {"last_inbound_at": "2026-04-08T10:00:00"}
        from datetime import datetime

        self.assertFalse(
            _messenger_response_window_open(thread, now=datetime(2026, 4, 9, 10, 0, 1))
        )

    def test_response_window_closed_without_inbound_timestamp(self):
        self.assertFalse(_messenger_response_window_open({}))


if __name__ == "__main__":
    unittest.main()