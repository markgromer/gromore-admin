"""WP REST API auth diagnostic v3 - comprehensive"""
import requests, base64

url = "https://nopoop.life"
user = "Parker"
dummy = "xxxx xxxx xxxx xxxx xxxx xxxx"
token = base64.b64encode(f"{user}:{dummy}".encode()).decode()

def check(label, resp):
    print(f"  Status: {resp.status_code}")
    gm = resp.headers.get("X-GM-Plugin", "")
    if gm:
        print(f"  X-GM-Plugin: {gm}  <-- mu-plugin IS loaded")
    try:
        j = resp.json()
        code = j.get("code", "")
        msg = j.get("message", "")
        print(f"  WP code: {code}")
        print(f"  WP msg:  {msg}")
        if code in ("incorrect_password", "invalid_username"):
            print(f"  ** AUTH REACHING WP via {label} **")
            return "works"
        if resp.status_code == 200:
            return "works"
        return "stripped" if code == "rest_not_logged_in" else "other"
    except:
        print(f"  Body: {resp.text[:150]}")
        return "error"

results = {}

print("1. No auth (baseline)")
r = requests.get(f"{url}/wp-json/wp/v2/users/me", timeout=15)
results["no_auth"] = check("none", r)
print()

print("2. Authorization header")
r = requests.get(f"{url}/wp-json/wp/v2/users/me",
    headers={"Authorization": f"Basic {token}"}, timeout=15)
results["auth_header"] = check("Authorization", r)
print()

print("3. X-GM-Auth custom header")
r = requests.get(f"{url}/wp-json/wp/v2/users/me",
    headers={"X-GM-Auth": f"Basic {token}"}, timeout=15)
results["x_gm_auth"] = check("X-GM-Auth", r)
print()

print("4. REDIRECT_HTTP_AUTHORIZATION (via query)")
r = requests.get(f"{url}/wp-json/wp/v2/users/me",
    headers={"Authorization": f"Basic {token}",
             "X-GM-Auth": f"Basic {token}"}, timeout=15)
results["both_headers"] = check("both headers", r)
print()

print("5. Probe mu-plugins file")
r = requests.get(f"{url}/wp-content/mu-plugins/gm-auth-fix.php", timeout=15)
if r.status_code == 200:
    print("  File found and returned 200 (PHP executed, probably blank output)")
    mu_exists = "exists"
elif r.status_code == 403:
    print("  403 Forbidden - file EXISTS but directory listing blocked")
    mu_exists = "exists"
elif r.status_code == 404:
    print("  404 NOT FOUND - mu-plugin file does not exist!")
    mu_exists = "missing"
else:
    print(f"  Status {r.status_code}")
    mu_exists = "unknown"
print()

print("6. Check /wp-json/gm/v1/verify (custom endpoint)")
r = requests.post(f"{url}/wp-json/gm/v1/verify",
    json={"user": user, "pass": dummy}, timeout=15)
print(f"  Status: {r.status_code}")
print(f"  Body: {r.text[:200]}")
print()

print("=" * 50)
print("RESULTS")
print("=" * 50)
for k, v in results.items():
    label = "OK" if v == "works" else "FAIL" if v == "stripped" else v
    print(f"  {k:20s} -> {label}")
print(f"  {'mu-plugin file':20s} -> {mu_exists}")

working = [k for k, v in results.items() if v == "works"]
if working:
    print(f"\nAuth works via: {', '.join(working)}")
else:
    if mu_exists == "missing":
        print("\nmu-plugin file NOT FOUND. It was never created on the server.")
        print("Create: wp-content/mu-plugins/gm-auth-fix.php")
    else:
        print("\nmu-plugin may exist but isn't working.")
        print("Need to switch to POST-body auth approach.")
