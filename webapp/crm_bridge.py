"""
CRM integration bridge - pushes lead/conversion events to external CRMs.
Supports GoHighLevel, HubSpot, and generic webhooks.
"""

import logging
import requests

log = logging.getLogger(__name__)

TIMEOUT = 15


def push_lead(brand, lead_data):
    """
    Push a lead event to the brand's configured CRM.

    brand: dict with crm_type, crm_api_key, crm_webhook_url, crm_pipeline_id
    lead_data: dict with name, email, phone, source, notes, etc.

    Returns (success: bool, detail: str)
    """
    crm_type = (brand.get("crm_type") or "").strip().lower()
    if not crm_type or crm_type == "none":
        return False, "No CRM configured for this brand"

    dispatch = {
        "gohighlevel": _push_gohighlevel,
        "hubspot": _push_hubspot,
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
