import base64
import hashlib
import hmac
import json
import os
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
os.environ.setdefault("DATABASE_PATH", str(_TEST_ROOT / "enterprise-integrations-bootstrap.db"))
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("SQUARE_APP_ID", "sq0id-test")
os.environ.setdefault("SQUARE_APP_SECRET", "sq0secret-test")

from webapp.app import create_app
from webapp.connection_health import evaluate_brand_connection_health
from webapp.crm_bridge import pull_square_payment_revenue, square_payment_test_connection


class _Resp:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = text or json.dumps(self._payload)

    def json(self):
        return self._payload


class EnterprisePaymentIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.db_file = _TEST_ROOT / f"enterprise-integrations-{uuid.uuid4().hex}.db"
        os.environ["DATABASE_PATH"] = str(self.db_file)
        os.environ["SECRET_KEY"] = "test-secret"
        os.environ["SQUARE_APP_ID"] = "sq0id-test"
        os.environ["SQUARE_APP_SECRET"] = "sq0secret-test"
        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False, APP_URL="http://localhost")
        self.client = self.app.test_client()
        with self.app.app_context():
            self.brand_id = self.app.db.create_brand({
                "slug": f"square-brand-{uuid.uuid4().hex[:8]}",
                "display_name": "Square Brand",
            })
            for field, value in {
                "payment_provider": "square",
                "payment_api_key": "expired-token",
                "payment_refresh_token": "refresh-token",
                "payment_token_expires_at": "2020-01-01T00:00:00Z",
                "payment_webhook_secret": "square-signature-key",
            }.items():
                self.app.db.update_brand_text_field(self.brand_id, field, value)

    def tearDown(self):
        os.environ.pop("DATABASE_PATH", None)
        os.environ.pop("SECRET_KEY", None)
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    def test_square_refreshes_token_and_pulls_paginated_payments(self):
        with self.app.app_context():
            brand = self.app.db.get_brand(self.brand_id)

            def fake_post(url, json=None, headers=None, timeout=None):
                self.assertEqual(url, "https://connect.squareup.com/oauth2/token")
                self.assertEqual(json["grant_type"], "refresh_token")
                return _Resp(payload={
                    "access_token": "fresh-token",
                    "refresh_token": "fresh-refresh",
                    "expires_at": "2099-01-01T00:00:00Z",
                    "merchant_id": "merchant-1",
                })

            get_calls = []

            def fake_get(url, headers=None, params=None, timeout=None):
                get_calls.append((url, dict(params or {}), headers.get("Authorization")))
                self.assertEqual(headers.get("Authorization"), "Bearer fresh-token")
                if "cursor" not in (params or {}):
                    return _Resp(payload={
                        "payments": [
                            {"status": "COMPLETED", "total_money": {"amount": 1200}, "refunded_money": {"amount": 200}},
                        ],
                        "cursor": "next",
                    })
                return _Resp(payload={
                    "payments": [
                        {"status": "COMPLETED", "total_money": {"amount": 2500}},
                        {"status": "CANCELED", "total_money": {"amount": 9999}},
                    ]
                })

            with patch("webapp.crm_bridge.requests.post", side_effect=fake_post), patch("webapp.crm_bridge.requests.get", side_effect=fake_get):
                revenue, count, error = pull_square_payment_revenue(brand, "2026-05")

            self.assertIsNone(error)
            self.assertEqual(revenue, 35.0)
            self.assertEqual(count, 2)
            self.assertEqual(len(get_calls), 2)
            refreshed = self.app.db.get_brand(self.brand_id)
            self.assertEqual(refreshed["payment_api_key"], "fresh-token")
            self.assertEqual(refreshed["payment_refresh_token"], "fresh-refresh")
            self.assertEqual(refreshed["payment_merchant_id"], "merchant-1")

    def test_square_test_connection_verifies_locations_and_payments_scope(self):
        with self.app.app_context():
            brand = self.app.db.get_brand(self.brand_id)
            brand["payment_token_expires_at"] = "2099-01-01T00:00:00Z"

            def fake_get(url, headers=None, params=None, timeout=None):
                if url.endswith("/v2/locations"):
                    return _Resp(payload={"locations": [{"name": "Main"}]})
                if url.endswith("/v2/payments"):
                    self.assertEqual(params, {"limit": 1})
                    return _Resp(payload={"payments": []})
                return _Resp(404, text="not found")

            with patch("webapp.crm_bridge.requests.get", side_effect=fake_get):
                message, error = square_payment_test_connection(brand)

            self.assertIsNone(error)
            self.assertIn("payment-read token accepted", message)

    def test_square_webhook_requires_signature_and_records_event(self):
        with self.app.app_context():
            brand = self.app.db.get_brand(self.brand_id)
        payload = {
            "event_id": "evt-square-1",
            "type": "payment.updated",
            "data": {
                "object": {
                    "payment": {
                        "id": "pay_1",
                        "status": "APPROVED",
                        "created_at": "2026-05-01T12:00:00Z",
                        "total_money": {"amount": 4200, "currency": "USD"},
                    }
                }
            },
        }
        raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        notification_url = f"http://localhost/webhooks/square/{brand['slug']}"
        signature = base64.b64encode(
            hmac.new(b"square-signature-key", notification_url.encode("utf-8") + raw, hashlib.sha256).digest()
        ).decode("utf-8")

        bad = self.client.post(f"/webhooks/square/{brand['slug']}", data=raw, content_type="application/json")
        self.assertEqual(bad.status_code, 403)

        good = self.client.post(
            f"/webhooks/square/{brand['slug']}",
            data=raw,
            content_type="application/json",
            headers={"X-Square-HmacSha256-Signature": signature},
        )
        self.assertEqual(good.status_code, 200)
        self.assertTrue(good.get_json()["ok"])

        with self.app.app_context():
            events = self.app.db.get_payment_webhook_events(self.brand_id, provider="square")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["external_event_id"], "evt-square-1")
        self.assertEqual(events[0]["amount"], 42.0)
        self.assertEqual(events[0]["month"], "2026-05")

    def test_connection_health_reports_razorsync_and_square_readiness(self):
        with self.app.app_context():
            self.app.db.update_brand_text_field(self.brand_id, "crm_type", "razorsync")
            self.app.db.update_brand_text_field(self.brand_id, "crm_api_key", "razor-token")
            self.app.db.update_brand_text_field(self.brand_id, "crm_server_url", "demo-portal")
            self.app.db.update_brand_text_field(self.brand_id, "payment_token_expires_at", "2099-01-01T00:00:00Z")
            brand = self.app.db.get_brand(self.brand_id)
            items = evaluate_brand_connection_health(self.app.db, brand, persist=False)

        by_key = {item["key"]: item for item in items}
        self.assertEqual(by_key["razorsync"]["status"], "ok")
        self.assertIn(by_key["square"]["status"], {"warn", "ok"})
        self.assertIn("Square", by_key["square"]["label"])

    def test_square_oauth_connect_explains_missing_platform_credentials(self):
        self.app.config["SQUARE_APP_ID"] = ""
        self.app.config["SQUARE_APP_SECRET"] = ""
        with self.app.app_context():
            client_user_id = self.app.db.create_client_user(self.brand_id, "owner@example.test", "Password123", "Owner")
        with self.client.session_transaction() as session:
            session["client_user_id"] = client_user_id
            session["client_brand_id"] = self.brand_id
            session["client_name"] = "Owner"
            session["client_brand_name"] = "Square Brand"

        response = self.client.get("/client/settings/payment-provider/square/connect", follow_redirects=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Square OAuth needs the platform App ID and App Secret", response.data)
        self.assertIn(b"Square OAuth not configured", response.data)


if __name__ == "__main__":
    unittest.main()
