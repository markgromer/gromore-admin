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


def _sng_collect_all_clients(brand, fetcher):
    """Paginate through an SNG client list endpoint and return all client records."""
    all_clients = []
    page = 1
    while True:
        result, error = fetcher(brand, page)
        if error or not isinstance(result, dict):
            break
        clients = result.get("data", [])
        all_clients.extend(clients)
        paginate = result.get("paginate") or {}
        if page >= (paginate.get("total_pages") or 1):
            break
        page += 1
    return all_clients


def _sng_sum_payments_for_month(brand, client_ids, month_prefix):
    """Call client_details for each client and sum succeeded payments in given month.
    month_prefix is like '2026-03'. Returns (total_revenue, payment_count)."""
    total_revenue = 0.0
    payment_count = 0
    for cid in client_ids:
        result, error = sng_get_client_details(brand, cid)
        if error:
            log.warning("client_details error for %s: %s", cid, error)
            continue
        if not isinstance(result, dict):
            log.warning("client_details non-dict for %s: %s", cid, type(result))
            continue
        payments = result.get("payments") or []
        for pmt in payments:
            if pmt.get("status") != "succeeded":
                continue
            pmt_date = (pmt.get("date") or "")
            if not pmt_date.startswith(month_prefix):
                continue
            try:
                total_revenue += float(pmt.get("amount") or 0)
            except (ValueError, TypeError):
                pass
            payment_count += 1
        if not payments:
            log.debug("client_details for %s returned 0 payments (keys: %s)",
                       cid, list(result.keys()) if isinstance(result, dict) else "N/A")
    log.info("SNG payment sum: %d clients, %d payments, $%.2f for %s",
             len(client_ids), payment_count, total_revenue, month_prefix)
    return round(total_revenue, 2), payment_count


def pull_sweepandgo_revenue(brand, month=None):
    """Pull real revenue from Sweep and Go payment history for a month.
    Iterates active clients, calls client_details, sums succeeded payments.
    Returns (revenue, payment_count, error_or_None)."""
    from datetime import datetime

    if not month:
        month = datetime.now().strftime("%Y-%m")

    # Validate month format
    try:
        int(month[:4])
        int(month[5:7])
    except (ValueError, IndexError):
        return 0, 0, f"Invalid month format: {month}"

    # Collect all active client IDs
    active = _sng_collect_all_clients(brand, sng_get_active_clients)
    client_ids = []
    for c in active:
        cid = c.get("client") or c.get("id") or c.get("client_id")
        if cid:
            client_ids.append(cid)

    if not client_ids:
        return 0, 0, "No active clients found"

    total_revenue, payment_count = _sng_sum_payments_for_month(brand, client_ids, month)
    return total_revenue, payment_count, None


def sng_estimate_revenue_snapshot(brand):
    """Revenue intelligence using real SNG payment data.
    Samples one page of clients and extrapolates to avoid 249+ API calls.
    Returns dict with mrr, avg_client_value, active/inactive counts, etc."""
    from datetime import datetime

    now = datetime.now()
    current_month = now.strftime("%Y-%m")

    # Get counts from report API (fast, single calls)
    active_count = 0
    r, _ = sng_count_active_clients(brand)
    if isinstance(r, dict):
        active_count = r.get("data", 0) or 0

    inactive_count = 0
    r, _ = sng_get_inactive_clients(brand, page=1)
    if isinstance(r, dict):
        inactive_count = (r.get("paginate") or {}).get("total", len(r.get("data", [])))

    jobs_count = 0
    r, _ = sng_count_jobs(brand)
    if isinstance(r, dict):
        jobs_count = r.get("data", 0) or 0

    # Sample page 1 of active clients (typically 10) and get their real payments
    sample_ids = []
    r, _ = sng_get_active_clients(brand, page=1)
    if isinstance(r, dict):
        for c in (r.get("data") or []):
            cid = c.get("client") or c.get("id") or c.get("client_id")
            if cid:
                sample_ids.append(cid)

    sample_revenue = 0.0
    sample_payments = 0
    sample_size = len(sample_ids)

    if sample_ids:
        sample_revenue, sample_payments = _sng_sum_payments_for_month(
            brand, sample_ids, current_month
        )

    # Extrapolate from sample to full active client base
    if sample_size > 0 and active_count > 0:
        scale = active_count / sample_size
        mrr = round(sample_revenue * scale, 2)
        payment_count = int(sample_payments * scale)
    else:
        mrr = 0.0
        payment_count = 0

    # Calculate average client monthly value from real data
    avg_client_monthly = round(mrr / active_count, 2) if active_count > 0 and mrr > 0 else 0
    # Estimated annual client value (avg retention ~18 months in service businesses)
    avg_retention_months = 18
    estimated_clv = round(avg_client_monthly * avg_retention_months, 2)
    # Churn cost: inactive clients x CLV (value lost)
    churn_cost = round(inactive_count * estimated_clv, 2) if mrr > 0 else 0

    return {
        "active_clients": active_count,
        "inactive_clients": inactive_count,
        "total_jobs": jobs_count,
        "mrr": mrr,
        "payment_count": payment_count,
        "sample_size": sample_size,
        "avg_client_monthly_value": avg_client_monthly,
        "estimated_clv": estimated_clv,
        "churn_cost_total": churn_cost,
        "avg_retention_months": avg_retention_months,
        "data_source": "real_payments_sampled",
    }


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
