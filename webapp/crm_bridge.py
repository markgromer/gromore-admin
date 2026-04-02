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


def _sng_collect_all_client_ids(brand):
    """Paginate through active clients and return all client string IDs."""
    all_ids = []
    page = 1
    while True:
        result, error = sng_get_active_clients(brand, page)
        if error or not isinstance(result, dict):
            break
        for c in (result.get("data") or []):
            cid = c.get("client") or c.get("id") or c.get("client_id")
            if cid:
                all_ids.append(cid)
        paginate = result.get("paginate") or {}
        if page >= (paginate.get("total_pages") or 1):
            break
        page += 1
    return all_ids


def _sng_sum_payments_for_month(brand, client_ids, month_prefix):
    """Call client_details for each client and sum succeeded payments in given month.
    month_prefix is like '2026-03'. Returns (total_revenue, payment_count, diagnostics)."""
    total_revenue = 0.0
    payment_count = 0
    diag = {
        "errors": 0,
        "clients_with_payments": 0,
        "clients_without_payments": 0,
        "all_payment_statuses": {},
        "all_payment_months": {},
        "sample_response_keys": None,
        "sample_payment": None,
        "first_error": None,
    }
    for i, cid in enumerate(client_ids):
        result, error = sng_get_client_details(brand, cid)
        if error:
            diag["errors"] += 1
            if not diag["first_error"]:
                diag["first_error"] = f"{cid}: {error}"
            continue
        if not isinstance(result, dict):
            diag["errors"] += 1
            if not diag["first_error"]:
                diag["first_error"] = f"{cid}: non-dict response ({type(result).__name__})"
            continue

        # Capture the keys from first successful response
        if diag["sample_response_keys"] is None:
            diag["sample_response_keys"] = list(result.keys())

        payments = result.get("payments") or []
        if payments:
            diag["clients_with_payments"] += 1
            # Capture first payment as sample
            if diag["sample_payment"] is None:
                diag["sample_payment"] = {k: str(v)[:100] for k, v in payments[0].items()}
        else:
            diag["clients_without_payments"] += 1

        for pmt in payments:
            # Track all statuses and months seen
            status = pmt.get("status") or "unknown"
            diag["all_payment_statuses"][status] = diag["all_payment_statuses"].get(status, 0) + 1

            pmt_date = (pmt.get("date") or "")
            if len(pmt_date) >= 7:
                pmt_month = pmt_date[:7]
                diag["all_payment_months"][pmt_month] = diag["all_payment_months"].get(pmt_month, 0) + 1

            if pmt.get("status") != "succeeded":
                continue
            if not pmt_date.startswith(month_prefix):
                continue
            try:
                total_revenue += float(pmt.get("amount") or 0)
            except (ValueError, TypeError):
                pass
            payment_count += 1

    log.info("SNG payment sum: %d clients, %d payments, $%.2f for %s | diag: with_pmts=%d, without=%d, errors=%d",
             len(client_ids), payment_count, total_revenue, month_prefix,
             diag["clients_with_payments"], diag["clients_without_payments"], diag["errors"])
    return round(total_revenue, 2), payment_count, diag


def sng_sync_revenue(brand, db, max_sample=50):
    """Revenue sync: samples up to max_sample clients from the previous complete
    month, then extrapolates to the full client base. Stores results in
    brand_month_finance + settings cache.
    Designed to complete within ~60s (50 clients x ~1s each).
    Returns the snapshot dict."""
    import json
    from datetime import datetime, timedelta

    brand_id = brand.get("id") or brand.get("brand_id")
    now = datetime.now()

    # Use previous complete month (current month may have incomplete billing)
    first_of_this_month = now.replace(day=1)
    last_month_end = first_of_this_month - timedelta(days=1)
    rev_month = last_month_end.strftime("%Y-%m")

    log.info("SNG revenue sync for brand %s, month %s (sample=%d)",
             brand_id, rev_month, max_sample)

    # Get counts (fast, single calls each)
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

    # Collect client IDs - paginate until we have enough for the sample
    sample_ids = []
    page = 1
    while len(sample_ids) < max_sample:
        result, error = sng_get_active_clients(brand, page)
        if error or not isinstance(result, dict):
            break
        for c in (result.get("data") or []):
            cid = c.get("client") or c.get("id") or c.get("client_id")
            if cid:
                sample_ids.append(cid)
            if len(sample_ids) >= max_sample:
                break
        paginate = result.get("paginate") or {}
        if page >= (paginate.get("total_pages") or 1):
            break
        page += 1

    sample_size = len(sample_ids)
    log.info("SNG sync: got %d sample client IDs (of %d active)", sample_size, active_count)

    sample_revenue = 0.0
    sample_payments = 0
    diag = {}
    if sample_ids:
        sample_revenue, sample_payments, diag = _sng_sum_payments_for_month(
            brand, sample_ids, rev_month
        )

    # Extrapolate from sample to full active client base
    if sample_size > 0 and active_count > 0:
        scale = active_count / sample_size
        mrr = round(sample_revenue * scale, 2)
        payment_count = int(sample_payments * scale)
    else:
        mrr = sample_revenue
        payment_count = sample_payments
        scale = 1

    # Calculate derived metrics
    avg_client_monthly = round(mrr / active_count, 2) if active_count > 0 and mrr > 0 else 0
    avg_retention_months = 18
    estimated_clv = round(avg_client_monthly * avg_retention_months, 2)
    churn_cost = round(inactive_count * estimated_clv, 2) if mrr > 0 else 0

    snapshot = {
        "active_clients": active_count,
        "inactive_clients": inactive_count,
        "total_jobs": jobs_count,
        "mrr": mrr,
        "payment_count": payment_count,
        "sample_size": sample_size,
        "sample_revenue": sample_revenue,
        "scale_factor": round(scale, 2),
        "revenue_month": rev_month,
        "avg_client_monthly_value": avg_client_monthly,
        "estimated_clv": estimated_clv,
        "churn_cost_total": churn_cost,
        "avg_retention_months": avg_retention_months,
        "synced_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "sync_status": "done",
        "data_source": "real_payments_sampled" if sample_size < active_count else "real_payments_full",
        "diagnostics": diag,
    }

    # Store in brand_month_finance for ROAS pipeline
    if db and brand_id:
        try:
            db.upsert_brand_month_finance(
                brand_id, rev_month,
                closed_revenue=mrr,
                closed_deals=payment_count,
                notes=f"SNG sync ({sample_size} of {active_count} clients sampled)"
            )
        except Exception as exc:
            log.warning("Failed to upsert brand_month_finance: %s", exc)

        # Cache the full snapshot in settings table
        try:
            cache_key = f"sng_revenue_cache_{brand_id}"
            db.save_setting(cache_key, json.dumps(snapshot))
        except Exception as exc:
            log.warning("Failed to cache revenue snapshot: %s", exc)

    log.info("SNG revenue sync done: brand=%s month=%s revenue=$%.2f (sample=%d, scale=%.1fx)",
             brand_id, rev_month, mrr, sample_size, scale)
    return snapshot


def sng_get_cached_revenue(brand, db):
    """Read cached revenue snapshot from the settings table.
    Fast - no API calls. Returns the cached dict or empty dict.
    Also merges live KPIs (active clients, jobs) for freshness."""
    import json

    brand_id = brand.get("id") or brand.get("brand_id")
    cache_key = f"sng_revenue_cache_{brand_id}"

    cached = {}
    try:
        raw = db.get_setting(cache_key, "")
        if raw:
            cached = json.loads(raw)
    except Exception:
        pass

    # Always get live KPIs (these are single fast API calls)
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

    # Merge live counts into cached data
    cached["active_clients"] = active_count
    cached["inactive_clients"] = inactive_count
    cached["total_jobs"] = jobs_count

    # Recalculate churn cost with current inactive count if we have revenue data
    if cached.get("avg_client_monthly_value") and cached["avg_client_monthly_value"] > 0:
        avg_retention = cached.get("avg_retention_months", 18)
        clv = cached["avg_client_monthly_value"] * avg_retention
        cached["estimated_clv"] = round(clv, 2)
        cached["churn_cost_total"] = round(inactive_count * clv, 2)

    return cached


def pull_sweepandgo_revenue(brand, month=None):
    """Pull real revenue from Sweep and Go payment history for a month.
    Iterates ALL active clients, calls client_details, sums succeeded payments.
    Returns (revenue, payment_count, error_or_None)."""
    from datetime import datetime

    if not month:
        month = datetime.now().strftime("%Y-%m")

    try:
        int(month[:4])
        int(month[5:7])
    except (ValueError, IndexError):
        return 0, 0, f"Invalid month format: {month}"

    client_ids = _sng_collect_all_client_ids(brand)
    if not client_ids:
        return 0, 0, "No active clients found"

    total_revenue, payment_count = _sng_sum_payments_for_month(brand, client_ids, month)
    return total_revenue, payment_count, None


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


# ── GoHighLevel Revenue Pull ──────────────────────────────────

def ghl_list_pipelines(brand):
    """List all pipelines in the GHL sub-account. Returns (list, error)."""
    api_key = (brand.get("crm_api_key") or "").strip()
    if not api_key:
        return [], "GoHighLevel API key not configured"
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        resp = requests.get(
            "https://rest.gohighlevel.com/v1/pipelines/",
            headers=headers, timeout=TIMEOUT,
        )
        if resp.status_code != 200:
            return [], f"GHL API error {resp.status_code}: {resp.text[:200]}"
        return resp.json().get("pipelines", []), None
    except requests.RequestException as exc:
        return [], f"Network error: {exc}"


def ghl_test_connection(brand):
    """Quick connection test: list pipelines. Returns (message, error)."""
    pipelines, error = ghl_list_pipelines(brand)
    if error:
        return None, error
    return f"Connected - {len(pipelines)} pipeline(s) found", None


def pull_gohighlevel_revenue(brand, month=None):
    """Pull won/closed opportunity revenue from GoHighLevel for a given month.
    Uses the pipeline configured in crm_pipeline_id (or first pipeline if blank).
    Returns (revenue, deal_count, error_or_None)."""
    from datetime import datetime
    import calendar

    api_key = (brand.get("crm_api_key") or "").strip()
    if not api_key:
        return 0, 0, "GoHighLevel API key not configured"

    if not month:
        month = datetime.now().strftime("%Y-%m")

    try:
        year = int(month[:4])
        mon = int(month[5:7])
    except (ValueError, IndexError):
        return 0, 0, f"Invalid month format: {month}"

    # Epoch-ms range for the target month
    start_ts = int(datetime(year, mon, 1).timestamp() * 1000)
    last_day = calendar.monthrange(year, mon)[1]
    end_ts = int(datetime(year, mon, last_day, 23, 59, 59).timestamp() * 1000)

    headers = {"Authorization": f"Bearer {api_key}"}

    # Resolve pipeline
    pipeline_id = (brand.get("crm_pipeline_id") or "").strip()
    if not pipeline_id:
        pipelines, err = ghl_list_pipelines(brand)
        if err:
            return 0, 0, err
        if not pipelines:
            return 0, 0, "No pipelines found in GoHighLevel account"
        pipeline_id = pipelines[0].get("id", "")

    # Fetch opportunities from that pipeline
    total_revenue = 0.0
    deal_count = 0
    page = 1

    while True:
        try:
            resp = requests.get(
                f"https://rest.gohighlevel.com/v1/pipelines/{pipeline_id}/opportunities",
                headers=headers, timeout=TIMEOUT,
            )
        except requests.RequestException as exc:
            return 0, 0, f"Network error: {exc}"

        if resp.status_code != 200:
            return 0, 0, f"GHL API error {resp.status_code}: {resp.text[:200]}"

        data = resp.json()
        opportunities = data.get("opportunities", [])

        for opp in opportunities:
            status = (opp.get("status") or "").lower()
            if status not in ("won", "closed"):
                continue
            # Check if the opportunity closed within our month
            closed_at = opp.get("lastStatusChangeAt") or opp.get("updatedAt") or 0
            if isinstance(closed_at, str):
                try:
                    closed_at = int(datetime.fromisoformat(closed_at.replace("Z", "+00:00")).timestamp() * 1000)
                except (ValueError, TypeError):
                    closed_at = 0
            if start_ts <= closed_at <= end_ts:
                try:
                    total_revenue += float(opp.get("monetaryValue") or 0)
                except (TypeError, ValueError):
                    pass
                deal_count += 1

        # GHL v1 opportunities endpoint does not paginate - break after first call
        break

    return total_revenue, deal_count, None
