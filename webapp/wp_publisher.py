"""
WordPress publisher - pushes report content to a WordPress site via REST API.

Uses WordPress Application Passwords for authentication.
"""
import requests
from pathlib import Path
from bs4 import BeautifulSoup


def publish_to_wordpress(db, brand, report):
    """
    Publish a client report to WordPress as a new post/page.

    Returns {"success": True, "url": "..."} or {"success": False, "error": "..."}
    """
    wp_url = db.get_setting("wp_url", "").rstrip("/")
    wp_user = db.get_setting("wp_user", "")
    wp_app_password = db.get_setting("wp_app_password", "")

    if not wp_url or not wp_user or not wp_app_password:
        return {"success": False, "error": "WordPress not configured. Go to Settings to add WP credentials."}

    # Read client report
    client_path = report.get("client_path", "")
    if not client_path or not Path(client_path).exists():
        return {"success": False, "error": "Client report file not found"}

    with open(client_path, "r", encoding="utf-8") as f:
        html_content = f.read()

    # Extract just the body content (strip full HTML wrapper)
    soup = BeautifulSoup(html_content, "html.parser")
    body = soup.find("body")
    content_html = str(body) if body else html_content

    month = report.get("month", "")
    title = f"{brand['display_name']} - Performance Report - {month}"

    # Create post via WP REST API
    api_url = f"{wp_url}/wp-json/wp/v2/posts"

    post_data = {
        "title": title,
        "content": content_html,
        "status": "publish",
    }

    # Add category if configured
    category_id = brand.get("wp_category_id", 0)
    if category_id:
        post_data["categories"] = [category_id]

    resp = requests.post(
        api_url,
        json=post_data,
        auth=(wp_user, wp_app_password),
        timeout=30,
    )

    if resp.status_code in (200, 201):
        post = resp.json()
        return {"success": True, "url": post.get("link", "")}
    else:
        return {"success": False, "error": f"WP API error {resp.status_code}: {resp.text[:200]}"}
