"""
Google Drive integration helper.

Handles token refresh, folder structure creation, file upload/download,
and listing files within the brand's Drive folder tree.
"""
import io
import json
import re
import logging
from datetime import datetime, timedelta
from urllib.parse import quote

import requests
from flask import current_app

logger = logging.getLogger(__name__)

GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
DRIVE_API = "https://www.googleapis.com/drive/v3"
DRIVE_UPLOAD_API = "https://www.googleapis.com/upload/drive/v3"
SHEETS_API = "https://sheets.googleapis.com/v4/spreadsheets"

# Subfolders auto-created inside the brand's root Drive folder
SUBFOLDER_NAMES = ["Creatives", "Ads", "Images", "Reports"]


def _extract_folder_id(raw):
    """Extract a Google Drive folder ID from a raw string (could be an ID or full URL)."""
    if not raw:
        return ""
    raw = raw.strip()
    m = re.search(r'folders/([a-zA-Z0-9_-]+)', raw)
    if m:
        return m.group(1)
    return raw


# ── Token helpers ──

def _get_google_tokens(db, brand_id):
    """Return the google connection dict or None."""
    conns = db.get_brand_connections(brand_id)
    google = conns.get("google")
    if not google or google.get("status") != "connected":
        return None
    return google


def _refresh_access_token(db, brand_id, connection):
    """Use the refresh_token to get a new access_token. Returns new token or None."""
    refresh_token = connection.get("refresh_token", "")
    if not refresh_token:
        logger.warning("No refresh_token for brand %s", brand_id)
        return None

    client_id = (db.get_setting("google_client_id", "") or "").strip()
    client_secret = (db.get_setting("google_client_secret", "") or "").strip()
    if not client_id or not client_secret:
        logger.warning("Google OAuth credentials not configured")
        return None

    resp = requests.post(GOOGLE_TOKEN_URL, data={
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }, timeout=30)

    if resp.status_code != 200:
        logger.warning("Token refresh failed: %s", resp.text[:300])
        return None

    data = resp.json()
    new_token = data.get("access_token", "")
    expiry = ""
    if "expires_in" in data:
        expiry = (datetime.now() + timedelta(seconds=data["expires_in"])).isoformat()

    # Persist new access token (keep existing refresh_token)
    db.upsert_connection(brand_id, "google", {
        "access_token": new_token,
        "refresh_token": refresh_token,
        "token_expiry": expiry,
        "scopes": connection.get("scopes", ""),
        "account_id": connection.get("account_id", ""),
        "account_name": connection.get("account_name", ""),
    })
    return new_token


def get_valid_access_token(db, brand_id):
    """Return a valid access_token string, refreshing if needed. Returns None on failure."""
    conn = _get_google_tokens(db, brand_id)
    if not conn:
        return None

    token = conn.get("access_token", "")
    expiry = conn.get("token_expiry", "")

    # Check if token is expired or will expire within 5 min
    if expiry:
        try:
            exp_dt = datetime.fromisoformat(expiry)
            if datetime.now() >= exp_dt - timedelta(minutes=5):
                token = _refresh_access_token(db, brand_id, conn)
        except (ValueError, TypeError):
            pass

    if not token:
        token = _refresh_access_token(db, brand_id, conn)

    return token or None


# ── Drive API wrappers ──

def _drive_headers(access_token):
    return {"Authorization": f"Bearer {access_token}"}


def _find_subfolder(access_token, parent_id, name):
    """Find a subfolder by name inside parent_id. Returns folder ID or None."""
    q = (
        f"'{parent_id}' in parents"
        f" and name = '{name}'"
        " and mimeType = 'application/vnd.google-apps.folder'"
        " and trashed = false"
    )
    resp = requests.get(f"{DRIVE_API}/files", params={
        "q": q,
        "fields": "files(id,name)",
        "pageSize": 1,
    }, headers=_drive_headers(access_token), timeout=15)

    if resp.status_code != 200:
        return None
    files = resp.json().get("files", [])
    return files[0]["id"] if files else None


def _create_folder(access_token, parent_id, name):
    """Create a folder inside parent_id. Returns new folder ID or None."""
    meta = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    resp = requests.post(f"{DRIVE_API}/files", json=meta,
                         headers={**_drive_headers(access_token),
                                  "Content-Type": "application/json"},
                         timeout=15)
    if resp.status_code in (200, 201):
        return resp.json().get("id")
    logger.warning("Drive create folder failed: %s", resp.text[:300])
    return None


def ensure_folder_structure(db, brand_id):
    """
    Make sure the brand's root Drive folder has the standard subfolders.
    Returns a dict mapping subfolder name to its ID, e.g.
    {"Creatives": "abc123", "Ads": "def456", ...}
    Returns None if Drive is not set up.
    """
    brand = db.get_brand(brand_id)
    root_id = _extract_folder_id(brand.get("google_drive_folder_id") or "")
    if not root_id:
        return None

    token = get_valid_access_token(db, brand_id)
    if not token:
        return None

    result = {}
    for name in SUBFOLDER_NAMES:
        folder_id = _find_subfolder(token, root_id, name)
        if not folder_id:
            folder_id = _create_folder(token, root_id, name)
        if folder_id:
            result[name] = folder_id

    return result if result else None


def get_subfolder_id(db, brand_id, subfolder_name):
    """Return the ID of a specific subfolder, creating it if needed."""
    brand = db.get_brand(brand_id)
    root_id = _extract_folder_id(brand.get("google_drive_folder_id") or "")
    if not root_id:
        return None

    token = get_valid_access_token(db, brand_id)
    if not token:
        return None

    folder_id = _find_subfolder(token, root_id, subfolder_name)
    if not folder_id:
        folder_id = _create_folder(token, root_id, subfolder_name)
    return folder_id


def upload_file(db, brand_id, subfolder_name, filename, file_bytes, mime_type="image/png"):
    """
    Upload a file into the specified subfolder of the brand's Drive tree.
    Returns {"id": ..., "name": ..., "webViewLink": ...} or None.
    """
    folder_id = get_subfolder_id(db, brand_id, subfolder_name)
    if not folder_id:
        logger.warning("Drive upload: could not resolve subfolder '%s' for brand %s", subfolder_name, brand_id)
        return None

    token = get_valid_access_token(db, brand_id)
    if not token:
        logger.warning("Drive upload: no valid access token for brand %s", brand_id)
        return None

    # Multipart upload: metadata + file content
    metadata = json.dumps({"name": filename, "parents": [folder_id]})

    boundary = "----DriveUploadBoundary"
    body = (
        f"--{boundary}\r\n"
        f"Content-Type: application/json; charset=UTF-8\r\n\r\n"
        f"{metadata}\r\n"
        f"--{boundary}\r\n"
        f"Content-Type: {mime_type}\r\n\r\n"
    ).encode("utf-8") + file_bytes + f"\r\n--{boundary}--\r\n".encode("utf-8")

    resp = requests.post(
        f"{DRIVE_UPLOAD_API}/files?uploadType=multipart&fields=id,name,webViewLink",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": f"multipart/related; boundary={boundary}",
        },
        data=body,
        timeout=60,
    )

    if resp.status_code in (200, 201):
        return resp.json()
    logger.warning("Drive upload failed (%s): %s", resp.status_code, resp.text[:300])
    return None


def list_files(db, brand_id, subfolder_name, max_results=50):
    """
    List files in a specific subfolder (or root folder if subfolder_name is None).
    Returns list of dicts with id, name, mimeType, webViewLink, thumbnailLink, modifiedTime.
    """
    if subfolder_name is None:
        # List from root folder directly
        brand = db.get_brand(brand_id)
        folder_id = _extract_folder_id(brand.get("google_drive_folder_id") or "")
    else:
        folder_id = get_subfolder_id(db, brand_id, subfolder_name)
    if not folder_id:
        logger.warning("Drive list_files: no folder_id for brand %s subfolder=%s", brand_id, subfolder_name)
        return []

    token = get_valid_access_token(db, brand_id)
    if not token:
        logger.warning("Drive list_files: no valid token for brand %s", brand_id)
        return []

    q = f"'{folder_id}' in parents and trashed = false"
    resp = requests.get(f"{DRIVE_API}/files", params={
        "q": q,
        "fields": "files(id,name,mimeType,webViewLink,thumbnailLink,modifiedTime,size)",
        "pageSize": max_results,
        "orderBy": "modifiedTime desc",
    }, headers=_drive_headers(token), timeout=15)

    if resp.status_code != 200:
        logger.warning("Drive list_files failed (%s): %s", resp.status_code, resp.text[:300])
        return []
    return resp.json().get("files", [])


def browse_folder(db, brand_id, folder_id=None, max_results=50):
    """
    List folders and all files inside a given folder_id.
    If folder_id is None, uses the brand's root Drive folder.
    Returns {"folders": [...], "files": [...], "folder_id": "..."}.
    """
    brand = db.get_brand(brand_id)
    root_id = _extract_folder_id(brand.get("google_drive_folder_id") or "")
    if not root_id:
        return {"folders": [], "files": [], "folder_id": None}

    target_id = folder_id or root_id
    token = get_valid_access_token(db, brand_id)
    if not token:
        return {"folders": [], "files": [], "folder_id": target_id}

    headers = _drive_headers(token)

    # Get folders
    q_folders = f"'{target_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    resp_f = requests.get(f"{DRIVE_API}/files", params={
        "q": q_folders,
        "fields": "files(id,name)",
        "pageSize": 30,
        "orderBy": "name",
    }, headers=headers, timeout=15)
    folders = resp_f.json().get("files", []) if resp_f.status_code == 200 else []

    # Get ALL non-folder files
    q_files = f"'{target_id}' in parents and mimeType != 'application/vnd.google-apps.folder' and trashed = false"
    resp_i = requests.get(f"{DRIVE_API}/files", params={
        "q": q_files,
        "fields": "files(id,name,mimeType,webViewLink,thumbnailLink,modifiedTime,size)",
        "pageSize": max_results,
        "orderBy": "modifiedTime desc",
    }, headers=headers, timeout=15)
    files = resp_i.json().get("files", []) if resp_i.status_code == 200 else []

    return {"folders": folders, "files": files, "folder_id": target_id, "is_root": target_id == root_id}


def list_all_images(db, brand_id, max_results=40):
    """
    List image files from the root folder AND all its subfolders (one level deep).
    Returns a deduplicated list sorted by modifiedTime desc.
    """
    brand = db.get_brand(brand_id)
    root_id = _extract_folder_id(brand.get("google_drive_folder_id") or "")
    if not root_id:
        return []

    token = get_valid_access_token(db, brand_id)
    if not token:
        return []

    headers = _drive_headers(token)
    fields = "files(id,name,mimeType,webViewLink,thumbnailLink,modifiedTime,size)"

    # Step 1: find all subfolders in root
    q_folders = f"'{root_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    resp = requests.get(f"{DRIVE_API}/files", params={
        "q": q_folders, "fields": "files(id,name)", "pageSize": 20,
    }, headers=headers, timeout=15)
    subfolder_ids = []
    if resp.status_code == 200:
        subfolder_ids = [f["id"] for f in resp.json().get("files", [])]

    # Step 2: query root + each subfolder for image files
    all_folder_ids = [root_id] + subfolder_ids
    # Build a single OR query: ('id1' in parents or 'id2' in parents ...) and mimeType contains 'image/'
    parent_clauses = " or ".join(f"'{fid}' in parents" for fid in all_folder_ids)
    q_images = f"({parent_clauses}) and mimeType contains 'image/' and trashed = false"

    resp = requests.get(f"{DRIVE_API}/files", params={
        "q": q_images, "fields": fields, "pageSize": max_results,
        "orderBy": "modifiedTime desc",
    }, headers=headers, timeout=20)

    if resp.status_code != 200:
        logger.warning("Drive list_all_images failed (%s): %s", resp.status_code, resp.text[:300])
        return []
    return resp.json().get("files", [])


def download_file(db, brand_id, file_id):
    """Download file content by ID. Returns (bytes, mime_type) or (None, None)."""
    token = get_valid_access_token(db, brand_id)
    if not token:
        return None, None

    resp = requests.get(
        f"{DRIVE_API}/files/{file_id}",
        params={"alt": "media"},
        headers=_drive_headers(token),
        timeout=60,
    )
    if resp.status_code == 200:
        return resp.content, resp.headers.get("Content-Type", "application/octet-stream")
    return None, None


def _extract_sheet_id(raw):
    """Extract a Google Sheet ID from a raw string or full spreadsheet URL."""
    if not raw:
        return ""
    raw = raw.strip()
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", raw)
    if m:
        return m.group(1)
    return raw


def _sheet_title(value, fallback="Warren Memory"):
    title = str(value or fallback).strip()
    title = re.sub(r"[\[\]\:\*\?\/\\]+", " ", title)
    title = re.sub(r"[\x00-\x1f\x7f]+", " ", title)
    title = re.sub(r"\s+", " ", title).strip()
    return (title or fallback)[:100]


def _sheet_range(worksheet, cells):
    escaped = str(worksheet or "").replace("'", "''")
    return quote(f"'{escaped}'!{cells}", safe="'!:")


def _sheet_column_name(index):
    index = max(1, int(index or 1))
    letters = []
    while index:
        index, remainder = divmod(index - 1, 26)
        letters.append(chr(65 + remainder))
    return "".join(reversed(letters))


def _sheet_table_range(worksheet, width, row_count=None):
    end_col = _sheet_column_name(width)
    if row_count:
        return _sheet_range(worksheet, f"A1:{end_col}{max(1, int(row_count))}")
    return _sheet_range(worksheet, f"A:{end_col}")


def _get_sheet_metadata(access_token, sheet_id):
    resp = requests.get(
        f"{SHEETS_API}/{sheet_id}",
        params={"fields": "sheets(properties(sheetId,title))"},
        headers=_drive_headers(access_token),
        timeout=15,
    )
    if resp.status_code != 200:
        logger.warning("Sheets metadata failed (%s): %s", resp.status_code, resp.text[:300])
        return {}
    sheets = (resp.json() or {}).get("sheets") or []
    return {
        ((sheet.get("properties") or {}).get("title") or ""): (sheet.get("properties") or {}).get("sheetId")
        for sheet in sheets
    }


def _ensure_worksheet(access_token, sheet_id, worksheet):
    worksheet = _sheet_title(worksheet)
    metadata = _get_sheet_metadata(access_token, sheet_id)
    if worksheet in metadata:
        return metadata[worksheet]

    auth_headers = {**_drive_headers(access_token), "Content-Type": "application/json"}
    resp = requests.post(
        f"{SHEETS_API}/{sheet_id}:batchUpdate",
        headers=auth_headers,
        json={"requests": [{"addSheet": {"properties": {"title": worksheet}}}]},
        timeout=15,
    )
    if resp.status_code not in (200, 201):
        logger.warning("Sheets add tab failed (%s): %s", resp.status_code, resp.text[:300])
        return None
    replies = (resp.json() or {}).get("replies") or []
    try:
        return replies[0]["addSheet"]["properties"]["sheetId"]
    except Exception:
        metadata = _get_sheet_metadata(access_token, sheet_id)
        return metadata.get(worksheet)


def _format_sheet_table(access_token, sheet_id, sheet_gid, column_count, row_count=1):
    if sheet_gid is None:
        return
    requests_payload = [
        {
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_gid, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount",
            }
        },
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_gid,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": max(1, int(column_count or 1)),
                },
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {"bold": True},
                        "backgroundColor": {"red": 0.92, "green": 0.95, "blue": 1.0},
                    }
                },
                "fields": "userEnteredFormat(textFormat,backgroundColor)",
            }
        },
        {
            "setBasicFilter": {
                "filter": {
                    "range": {
                        "sheetId": sheet_gid,
                        "startRowIndex": 0,
                        "endRowIndex": max(1, int(row_count or 1)),
                        "startColumnIndex": 0,
                        "endColumnIndex": max(1, int(column_count or 1)),
                    }
                }
            }
        },
        {
            "autoResizeDimensions": {
                "dimensions": {
                    "sheetId": sheet_gid,
                    "dimension": "COLUMNS",
                    "startIndex": 0,
                    "endIndex": max(1, int(column_count or 1)),
                }
            }
        },
    ]
    try:
        requests.post(
            f"{SHEETS_API}/{sheet_id}:batchUpdate",
            headers={**_drive_headers(access_token), "Content-Type": "application/json"},
            json={"requests": requests_payload},
            timeout=15,
        )
    except Exception as exc:
        logger.warning("Sheets table formatting failed: %s", exc)


def write_sheet_table(db, brand_id, worksheet_name, headers, rows):
    """Replace a worksheet with a rectangular table.

    This is used for Warren's current operating brain tables where the latest
    state matters more than an append-only event log.
    """
    brand = db.get_brand(brand_id)
    sheet_id = _extract_sheet_id((brand or {}).get("google_drive_sheet_id") or "")
    if not sheet_id:
        return {"ok": False, "error": "No Google Sheet ID configured."}

    token = get_valid_access_token(db, brand_id)
    if not token:
        return {"ok": False, "error": "No valid Google token."}

    worksheet = _sheet_title(worksheet_name, "Warren Brain")
    headers = [str(value or "")[:120] for value in (headers or [])]
    rows = rows or []
    width = max([len(headers)] + [len(row or []) for row in rows] + [1])
    if len(headers) < width:
        headers += [f"Column {idx + 1}" for idx in range(len(headers), width)]

    values = [headers]
    for row in rows:
        cleaned = [str(value or "")[:50000] for value in (row or [])]
        if len(cleaned) < width:
            cleaned += [""] * (width - len(cleaned))
        values.append(cleaned[:width])

    auth_headers = {**_drive_headers(token), "Content-Type": "application/json"}
    sheet_gid = _ensure_worksheet(token, sheet_id, worksheet)
    table_range = _sheet_table_range(worksheet, width, len(values))
    clear_range = _sheet_table_range(worksheet, max(width, 26))
    try:
        clear_resp = requests.post(
            f"{SHEETS_API}/{sheet_id}/values/{clear_range}:clear",
            headers=auth_headers,
            json={},
            timeout=15,
        )
        if clear_resp.status_code not in (200, 204):
            logger.warning("Sheets clear failed (%s): %s", clear_resp.status_code, clear_resp.text[:300])

        update_resp = requests.put(
            f"{SHEETS_API}/{sheet_id}/values/{table_range}",
            params={"valueInputOption": "USER_ENTERED"},
            headers=auth_headers,
            json={"values": values},
            timeout=20,
        )
        if update_resp.status_code not in (200, 201):
            logger.warning("Sheets table update failed (%s): %s", update_resp.status_code, update_resp.text[:300])
            return {"ok": False, "error": update_resp.text[:300]}
        _format_sheet_table(token, sheet_id, sheet_gid, width, len(values))
        return {"ok": True, "updatedRange": (update_resp.json() or {}).get("updatedRange", "")}
    except Exception as exc:
        logger.warning("Sheets table write failed for brand %s: %s", brand_id, exc)
        return {"ok": False, "error": str(exc)[:300]}


def append_sheet_row(db, brand_id, worksheet_name, headers, row_values):
    """Append one row to the brand's configured Google Sheet.

    Returns a small result dict. This is best-effort by design so Warren memory
    and task updates still succeed if Sheets access is missing or temporarily down.
    """
    brand = db.get_brand(brand_id)
    sheet_id = _extract_sheet_id((brand or {}).get("google_drive_sheet_id") or "")
    if not sheet_id:
        return {"ok": False, "error": "No Google Sheet ID configured."}

    token = get_valid_access_token(db, brand_id)
    if not token:
        return {"ok": False, "error": "No valid Google token."}

    worksheet = _sheet_title(worksheet_name, "Warren Memory")
    headers = [str(value or "")[:120] for value in (headers or [])]
    row_values = [str(value or "")[:50000] for value in (row_values or [])]
    headers_len = max(len(headers), len(row_values), 1)
    if len(headers) < headers_len:
        headers += [f"Column {idx + 1}" for idx in range(len(headers), headers_len)]
    if len(row_values) < headers_len:
        row_values += [""] * (headers_len - len(row_values))

    auth_headers = {**_drive_headers(token), "Content-Type": "application/json"}
    encoded_header_range = _sheet_range(worksheet, f"A1:{_sheet_column_name(headers_len)}1")
    append_range = _sheet_table_range(worksheet, headers_len)
    try:
        header_resp = requests.get(
            f"{SHEETS_API}/{sheet_id}/values/{encoded_header_range}",
            headers=_drive_headers(token),
            timeout=15,
        )
        header_is_empty = header_resp.status_code == 200 and not (header_resp.json().get("values") or [])
        if header_resp.status_code == 400:
            # The tab probably does not exist. Create it, then write headers.
            _ensure_worksheet(token, sheet_id, worksheet)
            header_is_empty = True
        elif header_resp.status_code not in (200, 404):
            logger.warning("Sheets header check failed (%s): %s", header_resp.status_code, header_resp.text[:300])

        if header_is_empty:
            requests.put(
                f"{SHEETS_API}/{sheet_id}/values/{encoded_header_range}",
                params={"valueInputOption": "USER_ENTERED"},
                headers=auth_headers,
                json={"values": [headers]},
                timeout=15,
            )

        append_resp = requests.post(
            f"{SHEETS_API}/{sheet_id}/values/{append_range}:append",
            params={"valueInputOption": "USER_ENTERED", "insertDataOption": "INSERT_ROWS"},
            headers=auth_headers,
            json={"values": [row_values]},
            timeout=20,
        )
        if append_resp.status_code in (200, 201):
            return {"ok": True, "updatedRange": (append_resp.json() or {}).get("updates", {}).get("updatedRange", "")}
        logger.warning("Sheets append failed (%s): %s", append_resp.status_code, append_resp.text[:300])
        return {"ok": False, "error": append_resp.text[:300]}
    except Exception as exc:
        logger.warning("Sheets append failed for brand %s: %s", brand_id, exc)
        return {"ok": False, "error": str(exc)[:300]}


def setup_brand_drive(db, brand_id):
    """
    One-call setup: ensure folder structure exists and return mapping.
    Intended to be called when user first configures their Drive folder.
    """
    brand = db.get_brand(brand_id)
    raw_id = (brand.get("google_drive_folder_id") or "").strip()
    root_id = _extract_folder_id(raw_id)
    if not root_id:
        return {"ok": False, "error": "No Drive folder ID provided."}

    # Check scopes BEFORE attempting any Drive operations
    conns = db.get_brand_connections(brand_id)
    google = conns.get("google", {})
    scopes = (google.get("scopes") or "").lower()
    if "drive" not in scopes:
        return {"ok": False, "error": "Your Google connection does not include Drive permissions yet. Click 'Reconnect Google With Drive Access' above, complete the Google sign-in, then save your Drive settings again."}

    token = get_valid_access_token(db, brand_id)
    if not token:
        return {"ok": False, "error": "Could not obtain a valid Google access token. Try reconnecting Google."}

    folders = ensure_folder_structure(db, brand_id)
    if not folders:
        return {"ok": False, "error": "Could not create subfolders in the Drive folder. Make sure the folder ID is correct and the Google account has edit access to it."}
    return {"ok": True, "folders": folders}
