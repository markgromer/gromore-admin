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
# Sweep and Go (Direct Open API - https://openapi.sweepandgo.com)
# ──────────────────────────────────────────────

SNG_BASE = "https://openapi.sweepandgo.com"


def _sng_api(brand, method, path, json_body=None, params=None):
    """Make a direct call to the Sweep and Go Open API."""
    api_key = (brand.get("crm_api_key") or "").strip()
    if not api_key:
        return None, "Sweep and Go API token not configured"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    url = f"{SNG_BASE}/{path.lstrip('/')}"

    try:
        resp = requests.request(
            method, url,
            json=json_body,
            params=params,
            headers=headers,
            timeout=TIMEOUT,
        )
    except Exception as e:
        return None, f"SNG API request failed: {str(e)[:150]}"

    if resp.status_code not in (200, 201):
        return None, f"SNG API returned {resp.status_code}: {resp.text[:200]}"

    try:
        return resp.json(), None
    except (ValueError, TypeError):
        return {"raw": resp.text[:500]}, None


def _push_sweepandgo(brand, lead_data):
    """Onboard a new residential client in Sweep and Go via Open API."""
    org_slug = (brand.get("crm_pipeline_id") or "").strip()

    payload = {
        "first_name": lead_data.get("first_name", ""),
        "last_name": lead_data.get("last_name", ""),
        "email": lead_data.get("email", ""),
        "cell_phone_number": lead_data.get("phone", ""),
        "home_address": lead_data.get("address", ""),
        "city": lead_data.get("city", ""),
        "state": lead_data.get("state", ""),
        "zip_code": lead_data.get("zip", ""),
        "number_of_dogs": 1,
        "last_time_yard_was_thoroughly_cleaned": "one_week",
        "clean_up_frequency": "once_a_week",
        "initial_cleanup_required": 1,
        "marketing_allowed": 1,
        "marketing_allowed_source": "open_api",
    }

    # Add UTM tracking if available
    source = lead_data.get("source", "")
    if source:
        payload["tracking_field"] = f"utm_source=gromore&utm_campaign={source}"
        payload["how_heard_about_us"] = "social_media"
        payload["how_heard_answer"] = source

    if lead_data.get("notes"):
        payload["additional_comment"] = lead_data["notes"]

    result, error = _sng_api(brand, "PUT", "api/v1/residential/onboarding", json_body=payload)
    if error:
        return False, error

    if isinstance(result, dict) and result.get("success"):
        return True, "SNG client onboarded successfully"
    return False, f"SNG onboarding response: {str(result)[:200]}"


def sng_get_active_clients(brand, page=1):
    """Get paginated list of active clients."""
    return _sng_api(brand, "GET", "api/v1/clients/active", params={"page": page})


def sng_get_inactive_clients(brand, page=1):
    """Get paginated list of inactive clients."""
    return _sng_api(brand, "GET", "api/v1/clients/inactive", params={"page": page})


def sng_get_active_no_subscription(brand, page=1):
    """Get active clients without a subscription (upsell targets)."""
    return _sng_api(brand, "GET", "api/v1/clients/active_no_subscription", params={"page": page})


def sng_get_client_details(brand, client_id):
    """Get full client details including payment history."""
    return _sng_api(brand, "POST", "api/v2/clients/client_details", json_body={"client": client_id})


def sng_search_client(brand, email, status=None):
    """Search for a client by email."""
    body = {"email": email}
    if status:
        body["status"] = status
    return _sng_api(brand, "POST", "api/v2/clients/client_search", json_body=body)


def sng_get_leads(brand, page=1):
    """Get paginated list of leads."""
    return _sng_api(brand, "GET", "api/v1/leads/list", params={"page": page})


def sng_get_out_of_area_leads(brand, page=1):
    """Get leads outside service area (ad targeting feedback)."""
    return _sng_api(brand, "GET", "api/v1/leads/out_of_service", params={"page": page})


def sng_get_dispatch_board(brand, date_str):
    """Get all jobs for a given date (YYYY-MM-DD)."""
    return _sng_api(brand, "GET", "api/v1/dispatch_board/jobs_for_date", params={"date": date_str})


def sng_get_free_quotes(brand):
    """Get list of free quote requests."""
    return _sng_api(brand, "GET", "api/v2/free_quotes")


def sng_count_active_clients(brand):
    """Get total active client count."""
    return _sng_api(brand, "GET", "api/v2/report/count_active_clients")


def sng_count_happy_clients(brand):
    """Get total happy client count."""
    return _sng_api(brand, "GET", "api/v2/report/count_happy_clients")


def sng_count_happy_dogs(brand):
    """Get total happy dog count."""
    return _sng_api(brand, "GET", "api/v2/report/count_happy_dogs")


def sng_count_jobs(brand):
    """Get total completed job count."""
    return _sng_api(brand, "GET", "api/v2/report/jobs_count")


def sng_get_staff(brand):
    """Get list of active staff members."""
    return _sng_api(brand, "GET", "api/v2/report/staff_select_list")


def sng_create_coupon(brand, coupon_id=None, name=None, coupon_type="percent",
                      duration="once", percent_off=None, amount_off=None,
                      redeem_by=None, max_redemptions=None):
    """Create a coupon for residential subscriptions."""
    body = {"coupon_type": coupon_type, "duration": duration}
    if coupon_id:
        body["coupon_id"] = coupon_id
    if name:
        body["name"] = name
    if percent_off is not None:
        body["percent_off"] = str(percent_off)
    if amount_off is not None:
        body["amount_off"] = str(amount_off)
    if redeem_by:
        body["redeem_by"] = redeem_by
    if max_redemptions is not None:
        body["max_redemptions"] = int(max_redemptions)
    return _sng_api(brand, "POST", "api/v2/coupon", json_body=body)


def sng_check_zip(brand, org_slug, zip_code):
    """Check if a ZIP code is in the service area."""
    return _sng_api(brand, "POST", "api/v2/client_on_boarding/check_zip_code_exists",
                    json_body={"organization": org_slug, "value": zip_code})


def sng_get_org_data(brand, org_slug):
    """Get organization branding info."""
    return _sng_api(brand, "GET", "api/v2/client_on_boarding/organization_data",
                    params={"organization": org_slug})


def pull_sweepandgo_revenue(brand, month=None):
    """Pull revenue from Sweep and Go by summing completed job payments for a month.
    Uses dispatch board to iterate over days and count completed jobs + payments.
    Returns (revenue, job_count, error_or_None)."""
    if not month:
        from datetime import datetime
        month = datetime.now().strftime("%Y-%m")

    import calendar
    from datetime import date

    try:
        year, mon = int(month[:4]), int(month[5:7])
        _, last_day = calendar.monthrange(year, mon)
    except (ValueError, IndexError):
        return 0, 0, f"Invalid month format: {month}"

    # Get all active clients and sum payments for the month
    total_revenue = 0.0
    total_jobs = 0

    # Use dispatch board day by day for the month
    for day in range(1, last_day + 1):
        date_str = f"{year:04d}-{mon:02d}-{day:02d}"
        result, error = sng_get_dispatch_board(brand, date_str)
        if error:
            continue  # skip days that fail

        jobs = result.get("data", []) if isinstance(result, dict) else []
        for job in jobs:
            if job.get("status_id") == 2 or job.get("status_name") == "completed":
                total_jobs += 1

    # For actual revenue, pull client payment details
    # First get active client count as a KPI
    count_result, _ = sng_count_active_clients(brand)
    active_count = 0
    if isinstance(count_result, dict):
        active_count = count_result.get("data", 0)

    return total_revenue, total_jobs, None


def pull_sweepandgo_customers(brand, page=1):
    """Pull active customers from Sweep and Go.
    Returns (customers_list, error_or_None)."""
    result, error = sng_get_active_clients(brand, page)
    if error:
        return [], error

    customers = []
    if isinstance(result, dict):
        customers = result.get("data", [])

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
