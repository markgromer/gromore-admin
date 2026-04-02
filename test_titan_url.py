"""Quick test: verify the exact Titan launch URL GroMore builds."""
import base64, hashlib, hmac, json, time, uuid
from urllib.parse import urlencode, urlparse, parse_qs

def _to_base64url(value):
    if isinstance(value, str):
        value = value.encode("utf-8")
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")

# Simulate config
config = {
    "TITAN_BASE_URL": "https://titan-syndicate.onrender.com",
    "TITAN_UPSTREAM_ISSUER": "gromore",
    "TITAN_EXTERNAL_APP": "gromore",
    "TITAN_UPSTREAM_SSO_SECRET": "test-secret-123",
}

user = {"id": 1, "email": "mark@example.com", "display_name": "Mark G", "role": "owner"}
brand = {"id": 5, "titan_snapshot_id": "", "titan_account_id": "", "titan_ghl_location_id": "abc123"}

now = int(time.time())
payload = {
    "issuer": "gromore",
    "external_app": "gromore",
    "external_user_id": "1",
    "external_brand_id": "5",
    "email": "mark@example.com",
    "display_name": "Mark G",
    "role": "owner",
    "titan_snapshot_id": None,
    "titan_account_id": None,
    "ghl_location_id": "abc123",
    "iat": now,
    "exp": now + 300,
    "nonce": str(uuid.uuid4()),
}

encoded_payload = _to_base64url(json.dumps(payload, separators=(",", ":")))
signature = _to_base64url(
    hmac.new(
        "test-secret-123".encode("utf-8"),
        encoded_payload.encode("utf-8"),
        hashlib.sha256,
    ).digest()
)
token = f"{encoded_payload}.{signature}"

query = {"token": token, "next": "/"}
url = f"https://titan-syndicate.onrender.com/sso/upstream?{urlencode(query)}"

print("=== TOKEN (first 80 chars) ===")
print(token[:80] + "...")
print()
print("=== FULL URL ===")
print(url)
print()
print("=== DECODED PAYLOAD ===")
parts = token.split(".")
padded = parts[0] + "=" * (4 - len(parts[0]) % 4)
decoded = base64.urlsafe_b64decode(padded)
print(json.dumps(json.loads(decoded), indent=2))
print()
print("=== URL STRUCTURE ===")
parsed = urlparse(url)
print(f"scheme:  {parsed.scheme}")
print(f"netloc:  {parsed.netloc}")
print(f"path:    {parsed.path}")
qs = parse_qs(parsed.query)
print(f"query keys: {list(qs.keys())}")
print(f"next value: {qs.get('next', ['?'])}")
print(f"token present: {'token' in qs}")
print(f"token length: {len(qs.get('token', [''])[0])}")
