"""Quo (formerly OpenPhone) SMS integration.

Docs: https://www.quo.com/docs/api-reference/introduction
Auth: API key in Authorization header (no Bearer prefix).
Send: POST https://api.openphone.com/v1/messages
"""

import logging
import requests

log = logging.getLogger(__name__)

BASE_URL = "https://api.openphone.com/v1"


def _headers(api_key):
    return {
        "Authorization": api_key,
        "Content-Type": "application/json",
    }


def send_sms(api_key, from_number, to_phone, content):
    """Send an SMS via Quo.

    Args:
        api_key: Quo workspace API key.
        from_number: Quo phone number in E.164 format (e.g. +15551234567).
        to_phone: Recipient phone in E.164 format.
        content: Message text.

    Returns:
        (success_bool, response_dict_or_error_string)
    """
    if not api_key or not from_number or not to_phone:
        return False, "Missing api_key, from_number, or to_phone"

    # Normalize to E.164
    to_phone = to_phone.strip()
    if not to_phone.startswith("+"):
        to_phone = "+1" + to_phone.lstrip("1").replace("-", "").replace(" ", "").replace("(", "").replace(")", "")

    try:
        resp = requests.post(
            f"{BASE_URL}/messages",
            headers=_headers(api_key),
            json={
                "content": content,
                "from": from_number,
                "to": [to_phone],
            },
            timeout=15,
        )
        if resp.status_code in (200, 201, 202):
            return True, resp.json()
        log.warning("Quo SMS failed (%s): %s", resp.status_code, resp.text[:300])
        return False, f"HTTP {resp.status_code}: {resp.text[:200]}"
    except Exception as exc:
        log.exception("Quo SMS error: %s", exc)
        return False, str(exc)


def get_phone_numbers(api_key):
    """Fetch available phone numbers from the Quo workspace.

    Returns:
        (list_of_dicts, error_string_or_None)
    """
    if not api_key:
        return [], "No API key provided"
    try:
        resp = requests.get(
            f"{BASE_URL}/phone-numbers",
            headers=_headers(api_key),
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            numbers = data.get("data", [])
            return numbers, None
        return [], f"HTTP {resp.status_code}: {resp.text[:200]}"
    except Exception as exc:
        log.exception("Quo phone numbers error: %s", exc)
        return [], str(exc)


def test_connection(api_key):
    """Quick connectivity check - fetch phone numbers list.

    Returns:
        (success_bool, detail_string)
    """
    numbers, err = get_phone_numbers(api_key)
    if err:
        return False, err
    return True, f"Connected. {len(numbers)} phone number(s) found."
