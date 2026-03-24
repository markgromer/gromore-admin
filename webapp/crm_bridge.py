"""
CRM integration bridge - pushes lead/conversion events to external CRMs.
Supports GoHighLevel, HubSpot, Sweep and Go, Jobber, and generic webhooks.
"""

import logging
import requests

log = logging.getLogger(__name__)

TIMEOUT = 15


def push_lead(brand, lead_data):
    """
    Push a lead event to the brand's configured CRM.

    brand: dict with crm_type, crm_api_key, crm_webhook_url, crm_pipeline_id,
           crm_server_url (for MCP-backed CRMs like Sweep and Go)
    lead_data: dict with name, email, phone, source, notes, etc.

    Returns (success: bool, detail: str)
    """
    crm_type = (brand.get("crm_type") or "").strip().lower()
    if not crm_type or crm_type == "none":
        return False, "No CRM configured for this brand"

    dispatch = {
        "gohighlevel": _push_gohighlevel,
        "hubspot": _push_hubspot,
        "sweepandgo": _push_sweepandgo,
        "jobber": _push_jobber,
        "webhook": _push_webhook,
    }

    handler = dispatch.get(crm_type)
    if not handler:
        return False, f"Unknown CRM type: {crm_type}"

    try:
        return handler(brand, lead_data)
    except Exception as e:
        log.exception("CRM push failed for %s", crm_type)
        return False, str(e)


def _push_gohighlevel(brand, lead_data):
    api_key = (brand.get("crm_api_key") or "").strip()
    if not api_key:
        return False, "GoHighLevel API key not configured"

    pipeline_id = (brand.get("crm_pipeline_id") or "").strip()

    payload = {
        "firstName": lead_data.get("first_name", ""),
        "lastName": lead_data.get("last_name", ""),
        "email": lead_data.get("email", ""),
        "phone": lead_data.get("phone", ""),
        "source": lead_data.get("source", "Ad Platform"),
        "tags": lead_data.get("tags", []),
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    # Create contact
    resp = requests.post(
        "https://rest.gohighlevel.com/v1/contacts/",
        json=payload,
        headers=headers,
        timeout=TIMEOUT,
    )

    if resp.status_code not in (200, 201):
        return False, f"GHL contact create failed: {resp.status_code} {resp.text[:200]}"

    contact_id = resp.json().get("contact", {}).get("id")

    # If pipeline configured, create an opportunity
    if pipeline_id and contact_id:
        opp_payload = {
            "pipelineId": pipeline_id,
            "name": f"{lead_data.get('first_name', '')} {lead_data.get('last_name', '')}".strip() or "New Lead",
            "contactId": contact_id,
            "status": "open",
            "source": lead_data.get("source", "Ad Platform"),
        }
        opp_resp = requests.post(
            "https://rest.gohighlevel.com/v1/pipelines/opportunities/",
            json=opp_payload,
            headers=headers,
            timeout=TIMEOUT,
        )
        if opp_resp.status_code not in (200, 201):
            log.warning("GHL opportunity create failed: %s", opp_resp.text[:200])

    return True, f"Contact created: {contact_id}"


def _push_hubspot(brand, lead_data):
    api_key = (brand.get("crm_api_key") or "").strip()
    if not api_key:
        return False, "HubSpot API key not configured"

    payload = {
        "properties": {
            "firstname": lead_data.get("first_name", ""),
            "lastname": lead_data.get("last_name", ""),
            "email": lead_data.get("email", ""),
            "phone": lead_data.get("phone", ""),
            "hs_lead_status": "NEW",
            "lead_source": lead_data.get("source", "Ad Platform"),
        }
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    resp = requests.post(
        "https://api.hubapi.com/crm/v3/objects/contacts",
        json=payload,
        headers=headers,
        timeout=TIMEOUT,
    )

    if resp.status_code not in (200, 201):
        return False, f"HubSpot contact create failed: {resp.status_code} {resp.text[:200]}"

    contact_id = resp.json().get("id")

    # If pipeline configured, create a deal
    pipeline_id = (brand.get("crm_pipeline_id") or "").strip()
    if pipeline_id and contact_id:
        deal_payload = {
            "properties": {
                "dealname": f"{lead_data.get('first_name', '')} {lead_data.get('last_name', '')}".strip() or "New Lead",
                "pipeline": pipeline_id,
                "dealstage": "appointmentscheduled",
            },
            "associations": [
                {
                    "to": {"id": contact_id},
                    "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 3}],
                }
            ],
        }
        deal_resp = requests.post(
            "https://api.hubapi.com/crm/v3/objects/deals",
            json=deal_payload,
            headers=headers,
            timeout=TIMEOUT,
        )
        if deal_resp.status_code not in (200, 201):
            log.warning("HubSpot deal create failed: %s", deal_resp.text[:200])

    return True, f"Contact created: {contact_id}"


def _push_webhook(brand, lead_data):
    webhook_url = (brand.get("crm_webhook_url") or "").strip()
    if not webhook_url:
        return False, "Webhook URL not configured"

    payload = {
        "brand": brand.get("display_name"),
        "lead": lead_data,
    }

    resp = requests.post(
        webhook_url,
        json=payload,
        timeout=TIMEOUT,
    )

    if resp.status_code not in (200, 201, 202, 204):
        return False, f"Webhook returned {resp.status_code}: {resp.text[:200]}"

    return True, f"Webhook delivered ({resp.status_code})"


# ──────────────────────────────────────────────
# Sweep and Go (via MCP server proxy on Render)
# ──────────────────────────────────────────────

def _sng_call(brand, tool_name, arguments=None):
    """Call a tool on the Sweep and Go MCP server."""
    server_url = (brand.get("crm_server_url") or "").strip().rstrip("/")
    if not server_url:
        return None, "Sweep and Go MCP server URL not configured"

    api_key = (brand.get("crm_api_key") or "").strip()
    org_slug = (brand.get("crm_pipeline_id") or "").strip()
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["x-sng-api-key"] = api_key
    if org_slug:
        headers["x-sng-org-slug"] = org_slug

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": tool_name,
            "arguments": arguments or {},
        },
    }

    # Post directly to the configured URL (should already include /mcp)
    try:
        resp = requests.post(
            server_url,
            json=payload,
            headers=headers,
            timeout=TIMEOUT,
        )
    except Exception as e:
        return None, f"SNG MCP request failed: {str(e)[:150]}"

    if resp.status_code not in (200, 201):
        return None, f"SNG MCP returned {resp.status_code}: {resp.text[:200]}"

    data = resp.json()
    if "error" in data:
        return None, f"SNG MCP error: {data['error'].get('message', str(data['error']))[:200]}"

    result = data.get("result", {})
    # MCP tools/call returns {"content": [{"type": "text", "text": "..."}]}
    content_list = result.get("content", [])
    if content_list:
        import json
        text = content_list[0].get("text", "")
        try:
            return json.loads(text), None
        except (ValueError, TypeError):
            return {"raw": text}, None
    return result, None


def _push_sweepandgo(brand, lead_data):
    """Create a customer in Sweep and Go via MCP server."""
    first_name = lead_data.get("first_name", "")
    last_name = lead_data.get("last_name", "")
    full_name = f"{first_name} {last_name}".strip()

    args = {
        "name": full_name or "New Lead",
        "email": lead_data.get("email", ""),
        "phone": lead_data.get("phone", ""),
    }

    # Add address fields if present
    if lead_data.get("address"):
        args["address"] = lead_data["address"]
    if lead_data.get("city"):
        args["city"] = lead_data["city"]
    if lead_data.get("state"):
        args["state"] = lead_data["state"]
    if lead_data.get("zip"):
        args["zip"] = lead_data["zip"]

    # Add notes/source as a note
    notes_parts = []
    if lead_data.get("source"):
        notes_parts.append(f"Source: {lead_data['source']}")
    if lead_data.get("notes"):
        notes_parts.append(lead_data["notes"])
    if notes_parts:
        args["notes"] = " | ".join(notes_parts)

    result, error = _sng_call(brand, "create_customer", args)
    if error:
        return False, error

    customer_id = ""
    if isinstance(result, dict):
        customer_id = result.get("id", result.get("customer_id", ""))

    return True, f"SNG customer created: {customer_id}"


def pull_sweepandgo_revenue(brand, month=None):
    """Pull completed job revenue from Sweep and Go for a given month.
    Returns (revenue, job_count, error_or_None)."""
    if not month:
        from datetime import datetime
        month = datetime.now().strftime("%Y-%m")

    # Get jobs for this month
    result, error = _sng_call(brand, "list_jobs", {
        "status": "completed",
        "from_date": f"{month}-01",
        "to_date": f"{month}-31",
    })
    if error:
        return 0, 0, error

    jobs = []
    if isinstance(result, dict):
        jobs = result.get("jobs", result.get("data", []))
        if not isinstance(jobs, list):
            jobs = [result] if result.get("id") else []
    elif isinstance(result, list):
        jobs = result

    total_revenue = 0.0
    for job in jobs:
        amount = job.get("total", job.get("amount", job.get("price", job.get("revenue", 0))))
        try:
            total_revenue += float(amount or 0)
        except (TypeError, ValueError):
            pass

    return total_revenue, len(jobs), None


def pull_sweepandgo_customers(brand, limit=50):
    """Pull recent customers from Sweep and Go.
    Returns (customers_list, error_or_None)."""
    result, error = _sng_call(brand, "list_customers", {"limit": limit})
    if error:
        return [], error

    customers = []
    if isinstance(result, dict):
        customers = result.get("customers", result.get("data", []))
    elif isinstance(result, list):
        customers = result

    return customers, None


# ──────────────────────────────────────────────
# Jobber (GraphQL API)
# ──────────────────────────────────────────────

def _jobber_graphql(brand, query, variables=None):
    """Execute a Jobber GraphQL query."""
    api_key = (brand.get("crm_api_key") or "").strip()
    if not api_key:
        return None, "Jobber API key not configured"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    payload = {"query": query}
    if variables:
        payload["variables"] = variables

    try:
        resp = requests.post(
            "https://api.getjobber.com/api/graphql",
            json=payload,
            headers=headers,
            timeout=TIMEOUT,
        )
    except Exception as e:
        return None, f"Jobber request failed: {str(e)[:150]}"

    if resp.status_code != 200:
        return None, f"Jobber returned {resp.status_code}: {resp.text[:200]}"

    data = resp.json()
    if data.get("errors"):
        msg = data["errors"][0].get("message", str(data["errors"][0]))
        return None, f"Jobber GraphQL error: {msg[:200]}"

    return data.get("data", {}), None


def _push_jobber(brand, lead_data):
    """Create a client in Jobber, optionally create a request."""
    first_name = lead_data.get("first_name", "")
    last_name = lead_data.get("last_name", "")

    # Build phones list
    phones = []
    if lead_data.get("phone"):
        phones.append({"number": lead_data["phone"], "primary": True})

    # Build emails list
    emails = []
    if lead_data.get("email"):
        emails.append({"address": lead_data["email"], "primary": True})

    mutation = """
    mutation CreateClient($input: ClientCreateInput!) {
        clientCreate(input: $input) {
            client {
                id
                firstName
                lastName
            }
            userErrors {
                message
                path
            }
        }
    }
    """

    variables = {
        "input": {
            "firstName": first_name or "New",
            "lastName": last_name or "Lead",
            "phones": phones,
            "emails": emails,
        }
    }

    result, error = _jobber_graphql(brand, mutation, variables)
    if error:
        return False, error

    client_data = (result.get("clientCreate") or {})
    user_errors = client_data.get("userErrors", [])
    if user_errors:
        return False, f"Jobber: {user_errors[0].get('message', 'Validation error')}"

    client = client_data.get("client", {})
    client_id = client.get("id", "")

    # Create a request (work order) if we have a source/notes
    if client_id and (lead_data.get("source") or lead_data.get("notes")):
        req_title = lead_data.get("source", "New Lead from Ad Platform")
        req_details = lead_data.get("notes", "")
        _jobber_create_request(brand, client_id, req_title, req_details)

    return True, f"Jobber client created: {client_id}"


def _jobber_create_request(brand, client_id, title, details=""):
    """Create a request (work order) for an existing Jobber client."""
    mutation = """
    mutation CreateRequest($input: RequestCreateInput!) {
        requestCreate(input: $input) {
            request {
                id
                title
            }
            userErrors {
                message
                path
            }
        }
    }
    """

    variables = {
        "input": {
            "clientId": client_id,
            "title": title[:255] if title else "New Lead",
            "details": details[:2000] if details else "",
        }
    }

    result, error = _jobber_graphql(brand, mutation, variables)
    if error:
        log.warning("Jobber request create failed: %s", error)
    return result, error


def pull_jobber_revenue(brand, month=None):
    """Pull completed invoice revenue from Jobber for a given month.
    Returns (revenue, invoice_count, error_or_None)."""
    if not month:
        from datetime import datetime
        month = datetime.now().strftime("%Y-%m")

    query = """
    query Invoices($filter: InvoiceFilterAttributes) {
        invoices(filter: $filter, first: 200) {
            nodes {
                id
                total
                depositTotal
                createdAt
            }
            totalCount
        }
    }
    """

    variables = {
        "filter": {
            "status": "paid",
            "createdAtRange": {
                "from": f"{month}-01",
                "to": f"{month}-31",
            },
        }
    }

    result, error = _jobber_graphql(brand, query, variables)
    if error:
        return 0, 0, error

    invoices_data = result.get("invoices", {})
    nodes = invoices_data.get("nodes", [])

    total_revenue = 0.0
    for inv in nodes:
        amount = inv.get("total", 0)
        try:
            total_revenue += float(amount or 0)
        except (TypeError, ValueError):
            pass

    return total_revenue, len(nodes), None
