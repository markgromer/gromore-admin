from webapp.app import app
import json

with app.test_request_context():
    from webapp.client_portal import AGENT_ROSTER
    print(f"Agent roster: {len(AGENT_ROSTER)} agents")
    for a in AGENT_ROSTER:
        print(f"  {a['key']:8s}  {a['name']:15s}  {a['role']}")
    print()
    print("JSON serializable:", bool(json.dumps(AGENT_ROSTER)))
