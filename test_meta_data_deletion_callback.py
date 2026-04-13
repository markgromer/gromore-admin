import base64
import hashlib
import hmac
import json
import os
import unittest
import uuid
from pathlib import Path

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
_BOOTSTRAP_DB = str(_TEST_ROOT / "gromore-meta-deletion-bootstrap.db")
os.environ.setdefault("DATABASE_PATH", _BOOTSTRAP_DB)
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("APP_URL", "http://localhost:5000")

from webapp.app import create_app


def _encode_b64url(raw_bytes):
    return base64.urlsafe_b64encode(raw_bytes).decode("utf-8").rstrip("=")


def build_signed_request(payload, app_secret):
    payload_json = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    encoded_payload = _encode_b64url(payload_json)
    signature = hmac.new(
        app_secret.encode("utf-8"),
        encoded_payload.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    encoded_signature = _encode_b64url(signature)
    return f"{encoded_signature}.{encoded_payload}"


class MetaDataDeletionCallbackTests(unittest.TestCase):
    def setUp(self):
        self._db_file = _TEST_ROOT / f"meta-deletion-{uuid.uuid4().hex}.db"
        self.db_path = str(self._db_file)
        os.environ["DATABASE_PATH"] = self.db_path
        os.environ["SECRET_KEY"] = "test-secret"
        os.environ["APP_URL"] = "https://app.example.com"

        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)
        self.client = self.app.test_client()
        self.meta_secret = "meta-secret"
        self.meta_user_id = "meta-user-123"

        with self.app.app_context():
            db = self.app.db
            db.save_setting("meta_app_secret", self.meta_secret)
            self.brand_id = db.create_brand(
                {
                    "slug": "meta-delete-brand",
                    "display_name": "Meta Delete Brand",
                    "facebook_page_id": "page-123",
                }
            )
            self.thread_id = db.upsert_lead_thread(
                self.brand_id,
                "messenger",
                self.meta_user_id,
                data={
                    "lead_name": "Messenger Lead",
                    "source": "meta_messenger",
                },
            )
            db.add_lead_message(
                self.thread_id,
                direction="inbound",
                role="lead",
                channel="messenger",
                external_message_id="msg-1",
                content="Hello",
                metadata={"sender_psid": self.meta_user_id, "page_id": "page-123"},
            )

    def tearDown(self):
        os.environ.pop("DATABASE_PATH", None)
        os.environ.pop("SECRET_KEY", None)
        os.environ.pop("APP_URL", None)
        if self._db_file.exists():
            self._db_file.unlink()
        for suffix in ("-wal", "-shm"):
            path = Path(self.db_path + suffix)
            if path.exists():
                path.unlink()

    def test_callback_accepts_signed_request_and_exposes_status_url(self):
        signed_request = build_signed_request(
            {
                "algorithm": "HMAC-SHA256",
                "issued_at": 1712947200,
                "user_id": self.meta_user_id,
            },
            self.meta_secret,
        )

        response = self.client.post(
            "/meta/data-deletion/callback",
            data={"signed_request": signed_request},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertIn("confirmation_code", payload)
        self.assertEqual(
            payload["url"],
            f"https://app.example.com/meta/data-deletion/status/{payload['confirmation_code']}",
        )

        with self.app.app_context():
            db = self.app.db
            request_row = db.get_meta_deletion_request(payload["confirmation_code"])
            self.assertIsNotNone(request_row)
            self.assertEqual(request_row["status"], "completed")
            self.assertEqual(request_row["deleted_thread_count"], 1)

            conn = db._conn()
            remaining = conn.execute(
                "SELECT COUNT(1) FROM lead_threads WHERE id = ?",
                (self.thread_id,),
            ).fetchone()[0]
            conn.close()
            self.assertEqual(remaining, 0)

        status_response = self.client.get(
            f"/meta/data-deletion/status/{payload['confirmation_code']}"
        )
        self.assertEqual(status_response.status_code, 200)
        status_payload = status_response.get_json()
        self.assertEqual(status_payload["status"], "completed")
        self.assertEqual(status_payload["deleted_thread_count"], 1)

    def test_callback_rejects_invalid_signature(self):
        signed_request = build_signed_request(
            {
                "algorithm": "HMAC-SHA256",
                "issued_at": 1712947200,
                "user_id": self.meta_user_id,
            },
            "wrong-secret",
        )

        response = self.client.post(
            "/meta/data-deletion/callback",
            data={"signed_request": signed_request},
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("Invalid signed_request signature", response.get_json()["error"])


if __name__ == "__main__":
    unittest.main()
