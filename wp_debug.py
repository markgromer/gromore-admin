"""Quick WP REST API diagnostic - run once then delete"""
import requests, base64, json, sys

url = "https://nopoop.life"

# Step 1: REST API probe (no auth)
print("=== Step 1: REST API probe (no auth) ===")
r = requests.get(f"{url}/wp-json/", timeout=15)
print(f"Status: {r.status_code}")
api = r.json()
print(f"Site name: {api.get('name', '?')}")
print(f"Auth methods: {list(api.get('authentication', {}).keys())}")
print()

# Step 2: users/me with NO auth
print("=== Step 2: users/me NO auth ===")
r2 = requests.get(f"{url}/wp-json/wp/v2/users/me", timeout=15)
print(f"Status: {r2.status_code}")
try:
    j2 = r2.json()
    print(f"Code: {j2.get('code')}")
    print(f"Message: {j2.get('message')}")
except:
    print(f"Body: {r2.text[:200]}")
print()

# Step 3: users/me WITH Basic auth (dummy password to test header passthrough)
print("=== Step 3: users/me WITH Basic auth (dummy pass) ===")
user = "Parker"
dummy_pass = "xxxx xxxx xxxx xxxx xxxx xxxx"
token = base64.b64encode(f"{user}:{dummy_pass}".encode()).decode()
headers = {"Authorization": f"Basic {token}"}
r3 = requests.get(f"{url}/wp-json/wp/v2/users/me", headers=headers, timeout=15)
print(f"Status: {r3.status_code}")
try:
    j3 = r3.json()
    print(f"Code: {j3.get('code')}")
    print(f"Message: {j3.get('message')}")
except:
    print(f"Body: {r3.text[:200]}")
print()

# Key diagnostic: if Step 2 and Step 3 return the SAME error code,
# the Authorization header is being stripped before WP sees it.
# Step 4: Try URL-embedded credentials (bypasses header stripping)
print("=== Step 4: URL-embedded auth ===")
from urllib.parse import quote
r4 = requests.get(
    f"https://{quote(user)}:{quote(dummy_pass)}@nopoop.life/wp-json/wp/v2/users/me",
    timeout=15,
)
print(f"Status: {r4.status_code}")
try:
    j4 = r4.json()
    print(f"Code: {j4.get('code')}")
    print(f"Message: {j4.get('message')}")
except:
    print(f"Body: {r4.text[:200]}")
print()

# Step 5: Custom X-GM-Auth header (requires mu-plugin on WP side)
print("=== Step 5: X-GM-Auth custom header ===")
headers5 = {"Authorization": f"Basic {token}", "X-GM-Auth": f"Basic {token}"}
r5 = requests.get(f"{url}/wp-json/wp/v2/users/me", headers=headers5, timeout=15)
print(f"Status: {r5.status_code}")
try:
    j5 = r5.json()
    print(f"Code: {j5.get('code')}")
    print(f"Message: {j5.get('message')}")
    if j5.get("code") in ("incorrect_password", "invalid_username"):
        print("** X-GM-Auth IS reaching WordPress! mu-plugin is working. **")
except:
    print(f"Body: {r5.text[:200]}")
print()

print("=== DIAGNOSIS ===")
try:
    code2 = r2.json().get("code", "")
    code3 = r3.json().get("code", "")
    code4 = r4.json().get("code", "")
    if code4 in ("invalid_username", "incorrect_password") or r4.status_code == 200:
        print("URL-embedded auth WORKS! We can use this as a workaround.")
    elif code2 == code3 == "rest_not_logged_in":
        print("CONFIRMED: Authorization header stripped by nginx proxy.")
        print("Need mu-plugin workaround on WordPress side.")
    else:
        print(f"step2={code2}, step3={code3}, step4={code4}")
except Exception as e:
    print(f"Parse error: {e}")
