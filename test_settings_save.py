"""Quick test to reproduce the settings save 500 error."""
import requests
import re
import sys

BASE = "http://127.0.0.1:5001"
s = requests.Session()

# Get login page for CSRF
r = s.get(f"{BASE}/login")
m = re.search(r'csrf-token.*?content="(.+?)"', r.text)
token = m.group(1) if m else ""
print(f"CSRF token: {token[:20]}...")

# Login
r = s.post(f"{BASE}/login", data={
    "username": "admin",
    "password": "gromore2026",
    "csrf_token": token,
}, allow_redirects=True)
print(f"Login: {r.status_code} -> {r.url}")
if "/login" in r.url:
    print("Login failed - check credentials")
    sys.exit(1)

# Get settings page for fresh CSRF
r = s.get(f"{BASE}/settings")
print(f"Settings GET: {r.status_code}")
m2 = re.search(r'csrf-token.*?content="(.+?)"', r.text)
token2 = m2.group(1) if m2 else ""

# Save openai settings
r = s.post(f"{BASE}/settings", data={
    "section": "openai",
    "openai_api_key": "sk-proj-testkey12345",
    "openai_model": "gpt-4o-mini",
    "openai_model_custom": "",
    "openai_model_competitor": "",
    "openai_model_competitor_custom": "",
    "ai_chat_system_prompt": "test prompt",
    "csrf_token": token2,
}, allow_redirects=True)

print(f"OpenAI save: {r.status_code}")
if r.status_code >= 400:
    print("ERROR RESPONSE:")
    print(r.text[:3000])
elif "OpenAI settings saved" in r.text:
    print("SUCCESS - flash message found")
else:
    print("No success flash found, checking for errors...")
    if "error" in r.text[:1000].lower():
        print("Error indicator found in page")
