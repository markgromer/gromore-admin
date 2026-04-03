"""
SNG API Field Discovery Probe
Run on Render console or locally with SNG credentials.
Usage: python tools/sng_probe.py <api_token> [org_slug]
"""
import sys
import json
import requests
from datetime import datetime

BASE = "https://openapi.sweepandgo.com"

def probe(token, org_slug=None):
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    print("=" * 60)
    print("SNG API FIELD DISCOVERY")
    print("=" * 60)

    # 1. Active clients - sample first record
    print("\n--- ACTIVE CLIENTS (page 1, first record) ---")
    r = requests.get(f"{BASE}/api/v1/clients/active", headers=headers, params={"page": 1})
    if r.ok:
        data = r.json()
        clients = data.get("data", [])
        print(f"Total clients in response: {len(clients)}")
        if data.get("paginate"):
            print(f"Pagination: {json.dumps(data['paginate'], indent=2)}")
        if clients:
            c = clients[0]
            print(f"\nALL KEYS on first client record ({len(c)} fields):")
            for k in sorted(c.keys()):
                v = c[k]
                display = str(v)[:100] if v is not None else "null"
                print(f"  {k}: {display}")
    else:
        print(f"ERROR: {r.status_code} - {r.text[:200]}")

    # 2. Dispatch board - today
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"\n--- DISPATCH BOARD ({today}) ---")
    r = requests.get(f"{BASE}/api/v1/dispatch_board/jobs_for_date",
                     headers=headers, params={"date": today})
    if r.ok:
        data = r.json()
        jobs = data.get("data", [])
        print(f"Jobs today: {len(jobs)}")
        if jobs:
            j = jobs[0]
            print(f"\nALL KEYS on first job record ({len(j)} fields):")
            for k in sorted(j.keys()):
                v = j[k]
                display = str(v)[:100] if v is not None else "null"
                print(f"  {k}: {display}")
        else:
            # Try yesterday
            from datetime import timedelta
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            print(f"No jobs today, trying yesterday ({yesterday})...")
            r2 = requests.get(f"{BASE}/api/v1/dispatch_board/jobs_for_date",
                             headers=headers, params={"date": yesterday})
            if r2.ok:
                data2 = r2.json()
                jobs2 = data2.get("data", [])
                print(f"Jobs yesterday: {len(jobs2)}")
                if jobs2:
                    j = jobs2[0]
                    print(f"\nALL KEYS on first job record ({len(j)} fields):")
                    for k in sorted(j.keys()):
                        v = j[k]
                        display = str(v)[:100] if v is not None else "null"
                        print(f"  {k}: {display}")
    else:
        print(f"ERROR: {r.status_code} - {r.text[:200]}")

    # 3. Client details (if we got a client ID)
    print("\n--- CLIENT DETAILS (first active client) ---")
    r = requests.get(f"{BASE}/api/v1/clients/active", headers=headers, params={"page": 1})
    if r.ok:
        clients = r.json().get("data", [])
        if clients:
            client_id = clients[0].get("client") or clients[0].get("id") or clients[0].get("client_id")
            if client_id:
                print(f"Probing client: {client_id}")
                r2 = requests.post(f"{BASE}/api/v2/clients/client_details",
                                   headers=headers, json={"client": client_id})
                if r2.ok:
                    cd = r2.json()
                    if isinstance(cd, dict):
                        print(f"\nALL TOP-LEVEL KEYS ({len(cd)} fields):")
                        for k in sorted(cd.keys()):
                            v = cd[k]
                            if isinstance(v, (dict, list)):
                                display = f"[{type(v).__name__}: {len(v)} items]"
                            else:
                                display = str(v)[:100] if v is not None else "null"
                            print(f"  {k}: {display}")
                        # Drill into nested objects
                        for k in sorted(cd.keys()):
                            v = cd[k]
                            if isinstance(v, dict) and v:
                                print(f"\n  >> {k} (dict with {len(v)} keys):")
                                for sk in sorted(v.keys()):
                                    sv = v[sk]
                                    display = str(sv)[:100] if sv is not None else "null"
                                    print(f"       {sk}: {display}")
                            elif isinstance(v, list) and v:
                                print(f"\n  >> {k} (list with {len(v)} items, first item keys):")
                                if isinstance(v[0], dict):
                                    for sk in sorted(v[0].keys()):
                                        sv = v[0][sk]
                                        display = str(sv)[:100] if sv is not None else "null"
                                        print(f"       {sk}: {display}")
                                else:
                                    print(f"       [0]: {str(v[0])[:200]}")
                else:
                    print(f"ERROR: {r2.status_code} - {r2.text[:300]}")
            else:
                print("No client_id found in first client record")
    else:
        print(f"ERROR getting clients: {r.status_code}")

    # 4. Free quotes
    print("\n--- FREE QUOTES (sample) ---")
    r = requests.get(f"{BASE}/api/v2/free_quotes", headers=headers)
    if r.ok:
        data = r.json()
        quotes = data.get("free_quotes", data.get("data", []))
        print(f"Quotes: {len(quotes)}")
        if quotes:
            q = quotes[0]
            print(f"\nALL KEYS on first quote ({len(q)} fields):")
            for k in sorted(q.keys()):
                v = q[k]
                display = str(v)[:100] if v is not None else "null"
                print(f"  {k}: {display}")
    else:
        print(f"ERROR: {r.status_code} - {r.text[:200]}")

    # 5. Inactive clients - sample
    print("\n--- INACTIVE CLIENTS (first record) ---")
    r = requests.get(f"{BASE}/api/v1/clients/inactive", headers=headers, params={"page": 1})
    if r.ok:
        data = r.json()
        clients = data.get("data", [])
        print(f"Inactive clients: {len(clients)}")
        if clients:
            c = clients[0]
            print(f"\nALL KEYS ({len(c)} fields):")
            for k in sorted(c.keys()):
                v = c[k]
                display = str(v)[:100] if v is not None else "null"
                print(f"  {k}: {display}")
    else:
        print(f"ERROR: {r.status_code} - {r.text[:200]}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python tools/sng_probe.py <api_token> [org_slug]")
        sys.exit(1)
    token = sys.argv[1]
    slug = sys.argv[2] if len(sys.argv) > 2 else None
    probe(token, slug)
