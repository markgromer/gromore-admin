"""Due-post publisher for social channels WARREN must publish itself."""

import json
import logging


log = logging.getLogger(__name__)


def _meta_tokens_for_brand(db, brand_id, brand):
    from webapp.api_bridge import _get_meta_token, _get_page_access_token

    connections = db.get_brand_connections(brand_id)
    meta_conn = connections.get("meta")
    if not meta_conn or meta_conn.get("status") != "connected":
        raise ValueError("Meta account not connected.")
    user_token = _get_meta_token(db, brand_id, meta_conn)
    if not user_token:
        raise ValueError("Meta token expired.")
    page_id = str((brand or {}).get("facebook_page_id") or "").strip()
    page_token = _get_page_access_token(page_id, user_token) if page_id else ""
    return user_token, page_token


def _publish_instagram_post(db, brand, post):
    import requests
    from webapp.client_portal import _resolve_scheduler_image_url

    brand_id = int(post.get("brand_id") or 0)
    user_token, page_token = _meta_tokens_for_brand(db, brand_id, brand)
    access_tokens = []
    for token in (page_token, user_token):
        if token and token not in access_tokens:
            access_tokens.append(token)
    if not access_tokens:
        raise ValueError("Meta publishing token unavailable.")

    try:
        metadata = json.loads(post.get("platform_metadata") or "{}")
    except Exception:
        metadata = {}
    instagram_id = str(metadata.get("instagram_account_id") or brand.get("instagram_account_id") or "").strip()
    if not instagram_id:
        raise ValueError("Instagram Professional Account ID is missing.")

    image_url = str(metadata.get("graph_image_url") or "").strip()
    if not image_url:
        image_url = _resolve_scheduler_image_url(post.get("image_url") or "", brand_id)
    if not image_url:
        raise ValueError("Instagram image URL is missing.")

    message = str(post.get("message") or "").strip()
    last_error = ""
    for access_token in access_tokens:
        create_resp = requests.post(
            f"https://graph.facebook.com/v21.0/{instagram_id}/media",
            data={
                "access_token": access_token,
                "image_url": image_url,
                "caption": message,
            },
            timeout=45,
        )
        create_data = create_resp.json() if create_resp.content else {}
        creation_id = str(create_data.get("id") or "").strip()
        if create_resp.status_code != 200 or not creation_id:
            last_error = ((create_data or {}).get("error") or {}).get("message") or create_resp.text[:300]
            continue

        publish_resp = requests.post(
            f"https://graph.facebook.com/v21.0/{instagram_id}/media_publish",
            data={"access_token": access_token, "creation_id": creation_id},
            timeout=45,
        )
        publish_data = publish_resp.json() if publish_resp.content else {}
        media_id = str(publish_data.get("id") or "").strip()
        if publish_resp.status_code == 200 and media_id:
            db.update_scheduled_post_status(post["id"], "published", fb_post_id=media_id, external_post_id=media_id)
            return media_id
        last_error = ((publish_data or {}).get("error") or {}).get("message") or publish_resp.text[:300]

    raise ValueError(f"Instagram publish rejected: {last_error or 'Unknown Meta error'}")


def process_due_social_posts(db, limit=50):
    stats = {"published": 0, "failed": 0, "skipped": 0}
    due_posts = db.get_due_social_posts(limit=limit)
    for post in due_posts:
        try:
            brand = db.get_brand(post.get("brand_id"))
            if not brand:
                stats["skipped"] += 1
                continue
            platform = str(post.get("platform") or "").strip().lower()
            if platform == "instagram":
                _publish_instagram_post(db, brand, post)
                stats["published"] += 1
            else:
                stats["skipped"] += 1
        except Exception as exc:
            stats["failed"] += 1
            db.update_scheduled_post_status(post["id"], "failed", error_message=str(exc)[:300])
            log.exception("Due social post failed: post_id=%s", post.get("id"))
    stats["checked"] = len(due_posts)
    return stats
