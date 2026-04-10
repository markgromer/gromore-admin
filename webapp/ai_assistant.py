"""AI assistant helpers ("Warren" briefs).

Generates structured internal + client-facing briefs from an existing analysis payload.
This is intentionally on-demand and best-effort: failures should not break core reporting.
"""

import json
import logging
import math
import os
import re
from collections import deque
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

DEFAULT_OPENAI_MODEL = "gpt-4o-mini"


def _load_benchmark_reference() -> Dict[str, Any]:
    path = os.path.join(os.path.dirname(__file__), "..", "config", "benchmarks.json")
    try:
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return {}


def _format_benchmark_value(metric: str, value: Any) -> str:
    if value in (None, ""):
        return "n/a"
    if metric in {"cpc", "cpa", "cpm"}:
        return f"${float(value):.2f}"
    if metric == "frequency_cap":
        return f"{float(value):.1f}"
    if metric in {"avg_session_duration_good"}:
        return f"{int(value)}s"
    if metric in {"target_position", "good_position"}:
        return f"{float(value):.1f}"
    return f"{float(value):.1f}%"


def _build_niche_benchmark_prompt(brand: Dict[str, Any]) -> str:
    industry = (brand.get("industry") or "").strip().lower().replace(" ", "_")
    if not industry:
        return ""

    reference = _load_benchmark_reference()
    niche_parts = []
    for channel in ("google_ads", "meta_ads", "website", "seo"):
        channel_data = (reference.get(channel) or {}).get(industry)
        if not channel_data:
            continue
        metrics = ", ".join(
            f"{metric} { _format_benchmark_value(metric, value) }"
            for metric, value in channel_data.items()
        )
        niche_parts.append(f"- {channel}: {metrics}")

    if not niche_parts:
        return ""

    special_notes = []
    notes = ((reference.get("_meta") or {}).get("notes") or "").strip()
    if industry == "pet_waste_removal":
        special_notes.append(
            "Pet waste removal is a colder demand-creation niche. Do not grade it against higher-intent home service funnels like plumbing or HVAC. "
            "A website conversion rate around 1.5% can still be healthy here, especially when traffic is colder, awareness-heavy, or requires repeated exposure before inquiry."
        )
    elif notes:
        special_notes.append(
            "Use the niche benchmarks below as the primary comparison set before falling back to any broader industry intuition."
        )

    lines = [
        "NICHE BENCHMARK CALIBRATION:",
        "Judge funnel quality against this brand's actual niche before using broader local-service heuristics.",
        *niche_parts,
    ]
    if special_notes:
        lines.extend(special_notes)
    return "\n".join(lines)


# ── Warren tool definitions (OpenAI function calling) ──

WARREN_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "browse_website",
            "description": (
                "Open a specific URL and inspect one or more pages from that website. "
                "Use this when the user shares a link, asks you to review a website, "
                "mentions a draft or staging site, or wants feedback on page structure or copy."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "The exact page or website URL to inspect.",
                    },
                    "max_pages": {
                        "type": "integer",
                        "description": "How many pages to inspect from the same site.",
                        "minimum": 1,
                        "maximum": 8,
                    },
                },
                "required": ["url"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "scan_competitor_pricing",
            "description": (
                "Scan public web sources for competitor pricing using the saved competitor profiles for this brand. "
                "Use this when the user asks what competitors charge, wants pricing comparisons, or wants to position pricing."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "competitor_name": {
                        "type": "string",
                        "description": "Optional competitor name filter. Leave empty to scan all saved competitors.",
                    }
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "web_search",
            "description": (
                "Search the web for current information. Use when the user asks about "
                "something you don't have data for, wants a link, wants pricing, wants to "
                "know about a competitor, or needs any real-time information."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The search query to look up on the web.",
                    }
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "generate_image",
            "description": (
                "Generate an image using DALL-E. Use when the user asks you to create, "
                "make, design, or generate an image, graphic, illustration, or visual."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "prompt": {
                        "type": "string",
                        "description": "Detailed description of the image to generate.",
                    },
                    "size": {
                        "type": "string",
                        "enum": ["1024x1024", "1792x1024", "1024x1792"],
                        "description": "Image dimensions. Use 1024x1024 for square, 1792x1024 for landscape, 1024x1792 for portrait.",
                    },
                },
                "required": ["prompt"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "save_memory",
            "description": (
                "Save an important insight, strategy, decision, or learning about this brand "
                "to your long-term memory. Use this when you make a recommendation, note a pattern, "
                "document a strategy being tested, or record an outcome. This builds your knowledge "
                "of the business over time. Categories: strategy (a plan or approach being used), "
                "insight (a data-driven observation), decision (a choice that was made), "
                "learning (what worked or didn't)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "category": {
                        "type": "string",
                        "enum": ["strategy", "insight", "decision", "learning"],
                        "description": "Type of memory to save.",
                    },
                    "title": {
                        "type": "string",
                        "description": "Short title (5-10 words) summarizing this memory.",
                    },
                    "content": {
                        "type": "string",
                        "description": "Detailed content of the memory. Include context, numbers, and reasoning.",
                    },
                },
                "required": ["category", "title", "content"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "recall_memories",
            "description": (
                "Search your long-term memory for relevant past insights, strategies, "
                "decisions, and learnings about this brand. Use this before making recommendations "
                "to check what you've previously noted."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "What to search for in your memories about this brand.",
                    },
                    "category": {
                        "type": "string",
                        "enum": ["strategy", "insight", "decision", "learning", "all"],
                        "description": "Filter by category, or 'all' for everything.",
                    },
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "browse_drive",
            "description": (
                "Browse the brand's Google Drive folder to list files and subfolders. "
                "Use this when the user asks about files in their Drive, wants to find an image, "
                "creative, report, or any uploaded asset. You can browse the root folder or a specific subfolder."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "subfolder": {
                        "type": "string",
                        "description": (
                            "Subfolder name to browse (e.g. 'Creatives', 'Ads', 'Images', 'Reports'). "
                            "Leave empty to browse the root folder and see all subfolders."
                        ),
                    },
                    "folder_id": {
                        "type": "string",
                        "description": "Specific Google Drive folder ID to browse. Use this to navigate into a subfolder returned by a previous browse_drive call.",
                    },
                },
            },
        },
    },
]


# ── Embedding & vector search helpers ──

def _get_embedding(api_key: str, text: str) -> List[float]:
    """Get an embedding vector from OpenAI."""
    resp = requests.post(
        "https://api.openai.com/v1/embeddings",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={"model": "text-embedding-3-small", "input": text[:8000]},
        timeout=15,
    )
    if resp.status_code != 200:
        log.warning("Embedding API error: %s", resp.text[:200])
        return []
    data = resp.json()
    return data.get("data", [{}])[0].get("embedding", [])


def _cosine_similarity(a: List[float], b: List[float]) -> float:
    """Compute cosine similarity between two vectors."""
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def recall_relevant_memories(db, brand_id: int, query: str, api_key: str,
                             category: str = "all", top_k: int = 10) -> List[Dict]:
    """Retrieve memories most relevant to a query using vector similarity."""
    query_emb = _get_embedding(api_key, query)
    if not query_emb:
        # Fallback: return recent memories without ranking
        memories = db.get_warren_memories(brand_id, limit=top_k)
        return memories

    all_memories = db.get_warren_memories_with_embeddings(brand_id)
    if category and category != "all":
        all_memories = [m for m in all_memories if m["category"] == category]

    scored = []
    for mem in all_memories:
        try:
            mem_emb = json.loads(mem["embedding"])
        except (json.JSONDecodeError, TypeError):
            continue
        sim = _cosine_similarity(query_emb, mem_emb)
        scored.append((sim, mem))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [m for _, m in scored[:top_k]]


def save_memory_with_embedding(db, brand_id: int, category: str, title: str,
                               content: str, api_key: str):
    """Save a memory and generate its embedding for future search."""
    emb_text = f"{title}: {content}"
    emb = _get_embedding(api_key, emb_text)
    emb_json = json.dumps(emb) if emb else None
    db.add_warren_memory(brand_id, category, title, content, emb_json)


def get_memory_context_for_chat(db, brand_id: int, user_message: str,
                                api_key: str) -> str:
    """Build a memory context string for injecting into Warren's chat.
    Called before each chat to give Warren access to relevant past knowledge."""
    # Get recent memories across all categories
    recent = db.get_warren_memories(brand_id, limit=8)
    # Get vector-search relevant memories
    relevant = recall_relevant_memories(db, brand_id, user_message, api_key, top_k=5)

    # Merge and deduplicate
    seen_ids = set()
    combined = []
    for m in relevant + recent:
        if m["id"] not in seen_ids:
            seen_ids.add(m["id"])
            combined.append(m)

    if not combined:
        return ""

    parts = ["YOUR MEMORIES ABOUT THIS BRAND (use these to inform your response):"]
    for m in combined[:12]:
        parts.append(
            f"- [{m['category'].upper()}] {m['title']} ({m['created_at'][:10]}): {m['content'][:300]}"
        )
    parts.append(
        "\nUse these memories as context. Reference past strategies and their results. "
        "Build on what you already know. When you make new recommendations or observations, "
        "save them with save_memory so you remember next time."
    )
    return "\n".join(parts)


def _execute_competitor_pricing_scan(db, brand_id: int, competitor_name: str = "") -> str:
    """Refresh pricing intel for one or more saved competitors and summarize the evidence."""
    if not db or not brand_id:
        return "Competitor pricing scan is unavailable because the brand context is missing."

    brand = db.get_brand(brand_id)
    if not brand:
        return "Competitor pricing scan failed because the brand could not be loaded."

    competitors = db.get_competitors(brand_id) or []
    if competitor_name:
        query = competitor_name.strip().lower()
        competitors = [c for c in competitors if query in (c.get("name", "").lower())]
    if not competitors:
        return "No saved competitors matched this pricing scan. Add competitors in My Business first."

    from webapp.competitor_intel import refresh_competitor_intel

    lines = []
    for competitor in competitors[:5]:
        result = refresh_competitor_intel(
            db,
            brand,
            competitor,
            force=True,
            only_types=["pricing"],
        )
        pricing = result.get("pricing") or {}
        summary = pricing.get("summary") or {}
        items = pricing.get("items") or []

        lines.append(f"Competitor: {competitor.get('name', 'Unknown')}")
        if summary.get("sample_count"):
            lines.append(
                f"- Public pricing mentions: {summary.get('sample_count')} "
                f"(confidence: {summary.get('confidence_band', 'unknown')})"
            )
            if summary.get("price_min") is not None or summary.get("price_max") is not None:
                lines.append(
                    f"- Price range found: ${summary.get('price_min', '?')} to ${summary.get('price_max', '?')}"
                )
            for item in items[:5]:
                amount_min = item.get("amount_min")
                amount_max = item.get("amount_max")
                if amount_min is None:
                    price_text = item.get("price_type", "offer").replace("_", " ")
                elif amount_max:
                    price_text = f"${amount_min} to ${amount_max}"
                else:
                    price_text = f"${amount_min}"
                lines.append(
                    f"- {item.get('service', 'General Service')}: {price_text} | "
                    f"{item.get('price_type', 'price').replace('_', ' ')} | "
                    f"{item.get('source_url', '')}"
                )
        else:
            lines.append("- No public pricing evidence found yet.")
            for err in (pricing.get("errors") or result.get("_errors") or [])[:2]:
                lines.append(f"- Note: {err}")
        lines.append("")

    return "\n".join(lines).strip()


_URL_RE = re.compile(r"(?P<url>(?:https?://|www\.)[^\s<>'\"()]+)", re.IGNORECASE)
_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}


def _extract_urls_from_text(text: str) -> List[str]:
    urls = []
    for match in _URL_RE.finditer(text or ""):
        url = (match.group("url") or "").rstrip(".,);!?]}")
        if url and not url.lower().startswith(("http://", "https://")):
            url = "https://" + url
        if url and url not in urls:
            urls.append(url)
    return urls


def _should_prefetch_website_review(user_message: str) -> bool:
    lower_msg = (user_message or "").lower()
    review_terms = (
        "look at",
        "review",
        "audit",
        "analyze",
        "analyse",
        "check",
        "browse",
        "visit",
        "crawl",
        "site",
        "website",
        "landing page",
        "landing pages",
        "page",
        "pages",
        "draft",
        "staging",
    )
    return any(term in lower_msg for term in review_terms)


def _normalize_site_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    if not re.match(r"^https?://", url, re.IGNORECASE):
        url = "https://" + url
    return url


def _extract_page_text(soup: BeautifulSoup, max_chars: int = 1200) -> str:
    container = soup.find("main") or soup.find("article") or soup.body or soup
    if not container:
        return ""

    cloned = BeautifulSoup(str(container), "html.parser")
    for tag in cloned(["script", "style", "noscript", "svg"]):
        tag.decompose()

    text = " ".join(cloned.stripped_strings)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_chars]


def _extract_internal_links(page_url: str, soup: BeautifulSoup, limit: int = 12) -> List[str]:
    parsed_page = urlparse(page_url)
    base_host = parsed_page.netloc.lower()
    links: List[str] = []

    for anchor in soup.find_all("a", href=True):
        href = (anchor.get("href") or "").strip()
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue
        abs_url = urljoin(page_url, href)
        parsed = urlparse(abs_url)
        if parsed.scheme not in {"http", "https"}:
            continue
        if parsed.netloc.lower() != base_host:
            continue
        clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path or '/'}"
        if parsed.query:
            clean_url += f"?{parsed.query}"
        if clean_url not in links:
            links.append(clean_url)
        if len(links) >= limit:
            break
    return links


def _summarize_browsed_page(page_url: str, html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html or "", "html.parser")
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    meta_description = ""
    meta_tag = soup.find("meta", attrs={"name": re.compile(r"^description$", re.I)})
    if meta_tag:
        meta_description = (meta_tag.get("content") or "").strip()

    headings: List[str] = []
    for tag_name in ("h1", "h2"):
        for tag in soup.find_all(tag_name):
            text = tag.get_text(" ", strip=True)
            if text and text not in headings:
                headings.append(text)
            if len(headings) >= 8:
                break
        if len(headings) >= 8:
            break

    return {
        "url": page_url,
        "title": title,
        "meta_description": meta_description,
        "headings": headings[:8],
        "text_excerpt": _extract_page_text(soup),
        "internal_links": _extract_internal_links(page_url, soup),
    }


def _execute_browse_website(url: str, max_pages: int = 5) -> str:
    """Fetch a live website page and a small set of internal pages for review."""
    start_url = _normalize_site_url(url)
    if not start_url:
        return "No URL was provided to browse."

    try:
        max_pages = int(max_pages or 5)
    except (TypeError, ValueError):
        max_pages = 5
    max_pages = max(1, min(max_pages, 8))
    session = requests.Session()
    session.headers.update(_BROWSER_HEADERS)

    queue = deque([start_url])
    seen: set[str] = set()
    pages: List[Dict[str, Any]] = []
    errors: List[str] = []
    start_host = urlparse(start_url).netloc.lower()

    while queue and len(pages) < max_pages:
        current = queue.popleft()
        if current in seen:
            continue
        seen.add(current)

        try:
            resp = session.get(current, timeout=12, allow_redirects=True)
        except requests.exceptions.SSLError:
            try:
                requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]
                resp = session.get(current, timeout=12, allow_redirects=True, verify=False)
                errors.append(f"{current} -> SSL verification failed, fetched without certificate verification")
            except Exception as exc:
                errors.append(f"{current} -> request failed after SSL fallback: {exc}")
                continue
        except Exception as exc:
            errors.append(f"{current} -> request failed: {exc}")
            continue

        final_url = resp.url or current
        parsed_final = urlparse(final_url)
        if parsed_final.netloc.lower() != start_host:
            continue
        if resp.status_code >= 400:
            errors.append(f"{final_url} -> HTTP {resp.status_code}")
            continue

        content_type = (resp.headers.get("Content-Type") or "").lower()
        if "html" not in content_type and "xml" not in content_type and resp.text[:50].lstrip()[:1] != "<":
            errors.append(f"{final_url} -> unsupported content type: {content_type or 'unknown'}")
            continue

        page = _summarize_browsed_page(final_url, resp.text)
        pages.append(page)

        for link in page["internal_links"]:
            if link not in seen and link not in queue and len(queue) < (max_pages * 4):
                queue.append(link)

    if not pages:
        if errors:
            return "Website browse failed:\n" + "\n".join(errors[:5])
        return f"Website browse failed for {start_url}."

    lines = [
        f"Website browse results for {start_url}",
        f"Pages reviewed: {len(pages)}",
    ]

    for idx, page in enumerate(pages, start=1):
        lines.append(f"\nPAGE {idx}: {page['url']}")
        if page["title"]:
            lines.append(f"Title: {page['title']}")
        if page["meta_description"]:
            lines.append(f"Meta description: {page['meta_description'][:220]}")
        if page["headings"]:
            lines.append("Headings: " + " | ".join(page["headings"][:6]))
        if page["text_excerpt"]:
            lines.append(f"Visible text excerpt: {page['text_excerpt']}")
        if page["internal_links"]:
            lines.append("Internal links: " + ", ".join(page["internal_links"][:8]))

    if errors:
        lines.append("\nBrowse warnings:")
        lines.extend(errors[:5])

    return "\n".join(lines)


def _execute_web_search(query: str) -> str:
    """Fetch web results using Google Custom Search JSON API or a simple scrape fallback."""
    # Try Google Custom Search if configured (via env)
    import os
    cse_key = os.environ.get("GOOGLE_CSE_API_KEY", "")
    cse_cx = os.environ.get("GOOGLE_CSE_CX", "")
    if cse_key and cse_cx:
        try:
            resp = requests.get(
                "https://www.googleapis.com/customsearch/v1",
                params={"key": cse_key, "cx": cse_cx, "q": query, "num": 5},
                timeout=10,
            )
            if resp.status_code == 200:
                items = resp.json().get("items", [])
                results = []
                for item in items[:5]:
                    results.append(
                        f"**{item.get('title', '')}**\n"
                        f"{item.get('link', '')}\n"
                        f"{item.get('snippet', '')}"
                    )
                if results:
                    return "\n\n".join(results)
        except Exception as exc:
            log.warning("Google CSE error: %s", exc)

    # Fallback: scrape DuckDuckGo HTML search results (no key needed)
    try:
        resp = requests.get(
            "https://html.duckduckgo.com/html/",
            params={"q": query},
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"},
        )
        if resp.status_code == 200:
            # Parse result snippets from the HTML
            from html.parser import HTMLParser

            class DDGParser(HTMLParser):
                def __init__(self):
                    super().__init__()
                    self.results = []
                    self._in_result = False
                    self._in_title = False
                    self._in_snippet = False
                    self._cur = {}
                    self._text = ""

                def handle_starttag(self, tag, attrs):
                    attrs_d = dict(attrs)
                    cls = attrs_d.get("class", "")
                    if tag == "a" and "result__a" in cls:
                        self._in_title = True
                        self._text = ""
                        href = attrs_d.get("href", "")
                        # DDG wraps URLs in a redirect; extract the real URL
                        if "uddg=" in href:
                            from urllib.parse import unquote, parse_qs, urlparse as _up
                            qs = parse_qs(_up(href).query)
                            href = unquote(qs.get("uddg", [href])[0])
                        self._cur["url"] = href
                    elif tag == "a" and "result__snippet" in cls:
                        self._in_snippet = True
                        self._text = ""

                def handle_endtag(self, tag):
                    if tag == "a" and self._in_title:
                        self._in_title = False
                        self._cur["title"] = self._text.strip()
                    elif tag == "a" and self._in_snippet:
                        self._in_snippet = False
                        self._cur["snippet"] = self._text.strip()
                        if self._cur.get("title"):
                            self.results.append(self._cur)
                        self._cur = {}

                def handle_data(self, data):
                    if self._in_title or self._in_snippet:
                        self._text += data

            parser = DDGParser()
            parser.feed(resp.text)
            if parser.results:
                parts = []
                for r in parser.results[:5]:
                    parts.append(
                        f"**{r.get('title', '')}**\n"
                        f"{r.get('url', '')}\n"
                        f"{r.get('snippet', '')}"
                    )
                return "\n\n".join(parts)
    except Exception as exc:
        log.warning("DuckDuckGo HTML search error: %s", exc)

    return f"I wasn't able to find web results for '{query}'. Try being more specific, or search directly at google.com."


def _execute_image_generation(api_key: str, prompt: str, size: str = "1024x1024") -> str:
    """Generate an image with DALL-E 3 and return the URL."""
    try:
        resp = requests.post(
            "https://api.openai.com/v1/images/generations",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": "dall-e-3",
                "prompt": prompt,
                "n": 1,
                "size": size,
                "quality": "standard",
            },
            timeout=60,
        )
        if resp.status_code != 200:
            error_msg = resp.json().get("error", {}).get("message", resp.text[:200])
            return f"Image generation failed: {error_msg}"
        data = resp.json()
        url = data["data"][0].get("url", "")
        revised = data["data"][0].get("revised_prompt", "")
        if url:
            result = f"![Generated Image]({url})"
            if revised and revised != prompt:
                result += f"\n\n*Refined prompt: {revised}*"
            return result
        return "Image generation returned no URL."
    except Exception as exc:
        log.warning("DALL-E error: %s", exc)
        return f"Image generation error: {str(exc)}"

DEFAULT_CHAT_SYSTEM_PROMPT = (
    "You are W.A.R.R.E.N. (Weighted Analysis for Revenue, Reach, Engagement & Navigation), "
    "a strategic decision engine inside GroMore. You analyze marketing performance across "
    "Google Ads, Meta Ads, GA4, and Search Console, then provide one clear, high-leverage "
    "recommendation. You are not a chatbot, not a reporter, and not a data dump. You are a "
    "strategist, a budget advisor, and a decision system.\n\n"

    "CORE OBJECTIVE\n"
    "Always determine and communicate the single highest-leverage action based on current data. "
    "Never provide multiple options. Never provide vague insights. Always provide one clear direction.\n\n"

    "INPUTS\n"
    "You have full access to and correlate data across all connected platforms:\n"
    "- Google Ads (CPC, CPA, conversions, impression share, search terms, budget limits)\n"
    "- Meta Ads (CPL, CTR, CPM, frequency, creative performance, conversion rate)\n"
    "- Meta Organic (reach, engagement, post-level performance, audience growth)\n"
    "- GA4 (sessions, conversion paths, attribution signals, landing page performance, session quality)\n"
    "- Search Console (queries, impressions, CTR, position, demand trends)\n"
    "- Optional: CRM (closed deals, revenue, LTV), Call tracking (call volume, quality, conversion outcomes)\n\n"

    "UNIFIED VIEW (CRITICAL)\n"
    "You do not analyze channels in isolation. You build a unified view of performance:\n"
    "- Connect paid traffic to actual conversions (GA4 + CRM)\n"
    "- Compare channel efficiency side-by-side (Google vs Meta)\n"
    "- Identify intent vs interruption traffic differences\n"
    "- Detect demand shifts (Search Console + Google Ads)\n"
    "- Spot creative fatigue and saturation (Meta frequency + performance)\n"
    "Your recommendations are always based on how channels perform together, not individually.\n\n"

    "DECISION HIERARCHY\n"
    "Prioritize signals in this order:\n"
    "1. Revenue / Conversions\n"
    "2. Cost Efficiency (CPA / CPL)\n"
    "3. Trend Direction (improving or declining)\n"
    "4. Volume (traffic / leads)\n"
    "5. Secondary metrics (CTR, CPC, etc.)\n\n"

    "AD SPEND INTELLIGENCE (CRITICAL)\n"
    "When asked about ads, budgets, campaigns, or whether to spend more, you MUST analyze "
    "current performance first before recommending any new spend. Follow this sequence:\n"
    "1. Check current conversion rates, CPA, and ROAS across all active campaigns.\n"
    "2. Identify campaigns or ad sets that are underperforming or wasting budget.\n"
    "3. Look for levers that improve results WITHOUT spending more: pause bad keywords, "
    "fix landing pages, reallocate budget from low to high performers, add negative keywords, "
    "improve ad copy, fix targeting.\n"
    "4. Only recommend increased spend after the fundamentals are solid.\n"
    "If the data shows inefficiency, say so directly: 'Before we talk about spending more, "
    "let me show you what is leaking first.'\n"
    "Never default to 'increase budget' when there are conversion or efficiency problems to solve. "
    "Diagnose before prescribing. Fix the engine before adding fuel.\n\n"

    "DECISION SYSTEM (signal strength)\n"
    "- Strong Signal: Clear performance gap or strong trend. Style: 'I'd shift 20-30% immediately...'\n"
    "- Moderate Signal: Noticeable difference, not extreme. Style: 'I'd start shifting 10-20% and monitor...'\n"
    "- Weak/Mixed Signal: No clear direction. Style: 'I wouldn't change anything right now...'\n"
    "- Negative Signal: Performance degrading. Style: 'I'd pull back spend before it gets worse...'\n"
    "- Opportunity Signal: Strong efficiency or rising demand. Style: 'There's room to scale here...'\n\n"

    "CONFIDENCE SCALING\n"
    "Adjust tone based on certainty: Weak = cautious, Moderate = measured, Strong = decisive, Critical = urgent. "
    "Never overstate weak data. Never under-react to strong signals.\n\n"

    "OUTPUT STYLE\n"
    "Respond in a natural, conversational way while being efficient and decisive.\n"
    "- Lead with the recommendation in a natural sentence\n"
    "- Follow with a short explanation that connects the data\n"
    "- Optionally add a quick signal if it strengthens the case\n"
    "- Think: one tight paragraph or two short paragraphs\n"
    "- First sentence = clear action, next 1-2 sentences = reasoning, optional final line = signal or emphasis\n\n"

    "TONE\n"
    "Sound like a calm, experienced strategist. No fluff. No hype. No emojis. "
    "Slightly conversational. Direct and confident. "
    "Never use em dashes. Use commas, periods, colons, or regular dashes instead.\n\n"

    "CONSTRAINTS\n"
    "- Never hallucinate data\n"
    "- Never recommend changes without evidence\n"
    "- Never provide multiple conflicting options\n"
    "- Never over-explain\n"
    "- If data is insufficient: 'I'd hold for now. There's not enough data to justify a change.'\n\n"

    "IDENTITY\n"
    "You never say 'as an AI' or 'I'm just a language model.' You are W.A.R.R.E.N. "
    "You are not an assistant. You are the system that tells the client where their money should go.\n\n"

    "YOUR ENVIRONMENT\n"
    "You live inside the GroMore client portal. You have access to the client's real ad platform "
    "data, brand profile, KPI targets, and you can see which page they're on. Use all of it. "
    "When context includes live data, reference the actual numbers, campaign names, and trends. "
    "Generic answers when real data is sitting right there are not acceptable.\n\n"

    "CONNECTED DATA SOURCES (when available in context)\n"
    "- Google Analytics 4: sessions, conversions, conversion rate, traffic sources, trends\n"
    "- Google Search Console: organic clicks, impressions, CTR, average position, top queries\n"
    "- Google Ads: campaigns, ad groups, keywords, spend, conversions, CPA, CPC, Quality Score\n"
    "- Meta Ads: campaigns, ad sets, spend, results, cost per result, CPM, CTR, reach, frequency\n"
    "- CRM data (if configured): closed revenue, deals, pipeline value\n"
    "- When a data source isn't connected, just say so: 'I don't have your [source] connected "
    "yet. You can hook it up in Settings.'\n\n"

    "BRAND PROFILE FIELDS\n"
    "Brand name, industry, service area, services, website, monthly budget, goals, "
    "brand voice/tone, active offers, target audience, competitors, reporting notes, "
    "KPI targets (CPA, leads, ROAS), brand colors, logos, call tracking number. "
    "Use these to tailor everything. A plumber in Phoenix on $3k/mo gets completely different "
    "advice than a SaaS company in NYC on $50k/mo.\n\n"

    "PORTAL PAGES AND TOOLS\n"
    "You know which page the client is on. Stay relevant to that context.\n\n"

    "Dashboard (/client/dashboard): Month-over-month KPI overview. Help them read trends, "
    "not just stare at numbers.\n\n"
    "Action Plan (/client/actions): Prioritized recommendations. Focus on what to do next.\n\n"
    "Campaigns (/client/campaigns): All campaigns across platforms. Clients can pause/enable, "
    "adjust budgets ($1-$10k), add negative keywords. If they can do it from here, tell them.\n\n"
    "Campaign Creator (/client/campaigns/new): AI builds a full campaign plan from service, "
    "location, budget, and platform. Walks through to launch.\n\n"
    "Ad Builder (/client/ad-builder): Generates ad copy and headlines.\n\n"
    "Creative Center (/client/creative): Visual ad builder with templates for every format.\n\n"
    "My Business (/client/my-business): Edit brand voice, offers, audience, KPIs, colors, logos.\n\n"
    "Settings (/client/settings): Connect platforms, enter IDs, manage API keys.\n\n"

    "WEBSITE ACCESS\n"
    "If the user gives you a URL, asks you to inspect a website, or mentions a draft/staging site, "
    "you are expected to browse it. Use your website browsing capability before saying data is missing. "
    "Do not say you cannot access external websites unless a live fetch actually failed.\n\n"

    "PRICING INTEL\n"
    "When the client asks what competitors charge, how their pricing compares, or whether they should move prices, "
    "use competitor pricing evidence. Scan saved competitors first, then answer from the public pricing evidence you found. "
    "Do not guess at competitor pricing.\n\n"

    "EXPERTISE AREAS\n"
    "Google Ads, Meta Ads, Search Console, GA4, SEO, conversion optimization, sales funnels, "
    "and the psychology of clients who are nervous about their spend. You know when to push for "
    "changes and when to reassure. You always frame things in terms of business impact, "
    "not platform mechanics.\n\n"

    "HARD RULES\n"
    "1. Never fabricate data. If a metric isn't in the context, say you don't have it.\n"
    "2. Never blame 'the algorithm' without evidence from the actual data.\n"
    "3. Every recommendation ties to something in the data or is clearly flagged as an assumption.\n"
    "4. If you don't have enough info, ask 1-3 pointed questions. Don't pad with generic advice.\n"
    "5. Only recommend actions the client can actually take. If it needs platform-side work, say so.\n"
    "6. Specific beats vague. 'Pause the three campaigns over $85 CPA and shift that budget to "
    "Campaign X at $34' beats 'optimize your campaigns.'\n"
    "7. Don't recap what's already on the screen. Add new insight.\n"
    "8. Keep it under 300 words unless the question genuinely needs more. Use lists to stay scannable.\n"
    "9. When uncertain, say so honestly, then share what the data suggests.\n"
    "10. When data is missing, say what's missing and point to Settings to connect it.\n"
    "11. Reference actual campaign names, spend, conversions, and KPI targets from context."
)


def summarize_analysis_for_ai(analysis: Dict[str, Any]) -> Dict[str, Any]:
    return _summarize_analysis_for_ai(analysis)


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _pick(d: Optional[dict], path: str) -> Any:
    if not isinstance(d, dict):
        return None
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def _summarize_analysis_for_ai(analysis: Dict[str, Any]) -> Dict[str, Any]:
    client_config = analysis.get("client_config") or {}

    meta = analysis.get("meta_business") or {}
    google_ads = analysis.get("google_ads") or {}
    ga = analysis.get("google_analytics") or {}
    gsc = analysis.get("search_console") or {}
    fb_organic = analysis.get("facebook_organic") or {}

    fb_metrics = fb_organic.get("metrics") or {}

    out: Dict[str, Any] = {
        "client": {
            "name": client_config.get("display_name") or analysis.get("client_id"),
            "industry": analysis.get("industry") or client_config.get("industry"),
            "service_area": client_config.get("service_area"),
            "primary_services": client_config.get("primary_services") or [],
            "monthly_budget": _safe_float(client_config.get("monthly_budget")),
            "goals": client_config.get("goals") or [],
            "brand_voice": client_config.get("brand_voice"),
            "active_offers": client_config.get("active_offers"),
            "target_audience": client_config.get("target_audience"),
            "competitors": client_config.get("competitors"),
            "competitor_profiles": client_config.get("competitor_profiles", []),
            "reporting_notes": client_config.get("reporting_notes"),
            "kpi_target_cpa": _safe_float(client_config.get("kpi_target_cpa")),
            "kpi_target_leads": _safe_float(client_config.get("kpi_target_leads")),
            "kpi_target_roas": _safe_float(client_config.get("kpi_target_roas")),
        },
        "period": {
            "month": analysis.get("month"),
        },
        "score": {
            "overall_grade": analysis.get("overall_grade"),
            "overall_score": analysis.get("overall_score"),
        },
        "paid_summary": analysis.get("paid_summary") or {},
        "kpi_status": analysis.get("kpi_status") or {},
        "highlights": analysis.get("highlights") or [],
        "concerns": analysis.get("concerns") or [],
        "kpis": {
            "meta": {
                "spend": _safe_float(_pick(meta, "metrics.spend")),
                "results": _safe_float(_pick(meta, "metrics.results")),
                "cpr": _safe_float(_pick(meta, "metrics.cost_per_result")),
                "cpc": _safe_float(_pick(meta, "metrics.cpc")),
                "ctr": _safe_float(_pick(meta, "metrics.ctr")),
                "impressions": _safe_float(_pick(meta, "metrics.impressions")),
                "clicks": _safe_float(_pick(meta, "metrics.clicks")),
                "mom": {
                    "spend_pct": _safe_float(_pick(meta, "month_over_month.spend.change_pct")),
                    "results_pct": _safe_float(_pick(meta, "month_over_month.results.change_pct")),
                    "cpr_pct": _safe_float(_pick(meta, "month_over_month.cost_per_result.change_pct")),
                },
            },
            "ga": {
                "sessions": _safe_float(_pick(ga, "metrics.sessions")),
                "conversions": _safe_float(_pick(ga, "metrics.conversions")),
                "conversion_rate": _safe_float(_pick(ga, "metrics.conversion_rate")),
                "bounce_rate": _safe_float(_pick(ga, "metrics.bounce_rate")),
                "pages_per_session": _safe_float(_pick(ga, "metrics.pages_per_session")),
                "users": _safe_float(_pick(ga, "metrics.users")),
                "mom": {
                    "sessions_pct": _safe_float(_pick(ga, "month_over_month.sessions.change_pct")),
                    "conversions_pct": _safe_float(_pick(ga, "month_over_month.conversions.change_pct")),
                },
            },
            "gsc": {
                "clicks": _safe_float(_pick(gsc, "metrics.clicks")),
                "impressions": _safe_float(_pick(gsc, "metrics.impressions")),
                "ctr": _safe_float(_pick(gsc, "metrics.ctr")),
                "position": _safe_float(_pick(gsc, "metrics.avg_position")),
                "mom": {
                    "clicks_pct": _safe_float(_pick(gsc, "month_over_month.clicks.change_pct")),
                    "impressions_pct": _safe_float(_pick(gsc, "month_over_month.impressions.change_pct")),
                },
            },
            "google_ads": {
                "spend": _safe_float(_pick(google_ads, "metrics.spend")),
                "results": _safe_float(_pick(google_ads, "metrics.results")),
                "cpr": _safe_float(_pick(google_ads, "metrics.cost_per_result")),
                "cpc": _safe_float(_pick(google_ads, "metrics.cpc")),
                "ctr": _safe_float(_pick(google_ads, "metrics.ctr")),
                "impressions": _safe_float(_pick(google_ads, "metrics.impressions")),
                "clicks": _safe_float(_pick(google_ads, "metrics.clicks")),
                "mom": {
                    "spend_pct": _safe_float(_pick(google_ads, "month_over_month.spend.change_pct")),
                    "results_pct": _safe_float(_pick(google_ads, "month_over_month.results.change_pct")),
                    "cpr_pct": _safe_float(_pick(google_ads, "month_over_month.cost_per_result.change_pct")),
                },
            },
            "facebook_organic": {
                "followers": fb_metrics.get("followers"),
                "fans": fb_metrics.get("fans"),
                "organic_impressions": fb_metrics.get("organic_impressions"),
                "engaged_users": fb_metrics.get("engaged_users"),
                "post_engagements": fb_metrics.get("post_engagements"),
                "engagement_rate": fb_metrics.get("engagement_rate"),
                "new_fans": fb_metrics.get("new_fans"),
                "net_fans": fb_metrics.get("net_fans"),
                "page_views": fb_metrics.get("page_views"),
                "post_count": fb_organic.get("post_count", 0),
            },
        },
        "seo_detail": {
            "top_queries": (gsc.get("top_queries") or [])[:20],
            "keyword_opportunities": (gsc.get("keyword_opportunities") or [])[:20],
            "keyword_recommendations": (gsc.get("keyword_recommendations") or [])[:20],
            "top_pages": (gsc.get("top_pages") or [])[:15],
        },
        "website_detail": {
            "top_landing_pages": (analysis.get("top_landing_pages") or _pick(ga, "by_page") or [])[:15],
        },
        "google_ads_detail": {
            "campaigns": (google_ads.get("campaign_analysis") or [])[:20],
            "month_over_month": google_ads.get("month_over_month") or {},
            "search_terms": (google_ads.get("search_terms") or [])[:50],
        },
        "meta_detail": {
            "campaigns": (meta.get("campaign_analysis") or [])[:20],
            "top_ads": (meta.get("top_ads") or [])[:20],
            "month_over_month": meta.get("month_over_month") or {},
        },
        "facebook_organic_detail": {
            "top_posts": (fb_organic.get("top_posts") or [])[:10],
        },
        "competitor_watch": analysis.get("competitor_watch") or {},
    }

    # Remove empty channel objects to reduce noise
    for channel in ("meta", "ga", "gsc", "google_ads"):
        if not any(v is not None for v in (out["kpis"][channel] or {}).values() if not isinstance(v, dict)):
            mom = out["kpis"][channel].get("mom") if isinstance(out["kpis"][channel], dict) else None
            if not (isinstance(mom, dict) and any(x is not None for x in mom.values())):
                out["kpis"].pop(channel, None)

    # Remove facebook_organic if no data
    fb_kpis = out["kpis"].get("facebook_organic", {})
    if not any(v for v in fb_kpis.values() if v):
        out["kpis"].pop("facebook_organic", None)
        out.pop("facebook_organic_detail", None)

    return out


def _extract_json_from_text(text: str) -> Dict[str, Any]:
    text = (text or "").strip()
    if not text:
        raise ValueError("Empty AI response")

    try:
        return json.loads(text)
    except Exception:
        pass

    # Fallback: find first JSON object in the content
    m = re.search(r"\{[\s\S]*\}", text)
    if not m:
        raise ValueError("AI response was not valid JSON")
    return json.loads(m.group(0))


def generate_warren_brief(
    *,
    api_key: str,
    analysis: Dict[str, Any],
    suggestions: Any,
    variant: str,
    model: Optional[str] = None,
    timeout: int = 60,
) -> Dict[str, Any]:
    """Generate a structured brief.

    variant: "internal" or "client"
    """
    if not api_key:
        raise ValueError("OpenAI API key not configured")

    model = model or DEFAULT_OPENAI_MODEL
    variant = (variant or "").strip().lower()
    if variant not in {"internal", "client"}:
        raise ValueError("variant must be 'internal' or 'client'")

    analysis_summary = _summarize_analysis_for_ai(analysis)

    prompt = {
        "variant": variant,
        "analysis": analysis_summary,
        "suggestions": suggestions,
        "output_schema": {
            "executive_summary": "string, 3-6 sentences",
            "mission_critical": [
                {
                    "title": "string",
                    "why": "string",
                    "impact": "string",
                    "next_step": "string",
                }
            ],
            "quick_wins_14_days": [
                {"title": "string", "owner": "string", "next_step": "string"}
            ],
            "strategy_30_60_days": [
                {"title": "string", "hypothesis": "string", "how_to_test": "string"}
            ],
            "watchouts_next_7_days": ["string"],
            "questions": ["string"],
        },
    }

    system = (
        "You are a senior paid media + analytics strategist inside an ad agency. "
        "Generate mission-critical, concrete, prioritized guidance. "
        "Return ONLY valid JSON matching the provided output_schema. "
        "No markdown, no extra keys, no surrounding text. "
        "Be specific but do not invent metrics; if unknown, omit that point. "
        "For variant=client: keep tone polished, remove internal jargon, and avoid mentioning 'benchmarks' or grades explicitly. "
        "For variant=internal: be blunt and tactical, include account checks and next actions."
    )

    # Inject brand voice and context if available
    brand_context = analysis_summary.get("client", {})
    voice_parts = []
    if brand_context.get("brand_voice"):
        voice_parts.append(f"Brand voice/tone instructions: {brand_context['brand_voice']}")
    if brand_context.get("active_offers"):
        voice_parts.append(f"Active offers/promotions: {brand_context['active_offers']}")
    if brand_context.get("target_audience"):
        voice_parts.append(f"Target audience: {brand_context['target_audience']}")
    if brand_context.get("competitors"):
        voice_parts.append(f"Known competitors: {brand_context['competitors']}")
    if brand_context.get("reporting_notes"):
        voice_parts.append(f"Reporting notes: {brand_context['reporting_notes']}")
    if voice_parts:
        system += " " + " ".join(voice_parts)

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    resp = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers=headers,
        json={
            "model": model,
            "temperature": 0.3,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(prompt)},
            ],
            "response_format": {"type": "json_object"},
        },
        timeout=timeout,
    )

    if resp.status_code != 200:
        raise ValueError(f"OpenAI request failed ({resp.status_code}): {resp.text}")

    data = resp.json()
    content = (
        (data.get("choices") or [{}])[0]
        .get("message", {})
        .get("content", "")
    )

    brief = _extract_json_from_text(content)

    # Light sanity defaults
    brief.setdefault("mission_critical", [])
    brief.setdefault("quick_wins_14_days", [])
    brief.setdefault("strategy_30_60_days", [])
    brief.setdefault("watchouts_next_7_days", [])
    brief.setdefault("questions", [])

    return brief


def chat_with_warren(
    *,
    api_key: str,
    messages: list[dict[str, str]],
    context: Optional[Dict[str, Any]] = None,
    admin_system_prompt: str = "",
    model: Optional[str] = None,
    timeout: int = 60,
    db=None,
    brand_id: Optional[int] = None,
    canvas_image: Optional[str] = None,
) -> str:
    if not api_key:
        raise ValueError("OpenAI API key not configured")

    model = model or DEFAULT_OPENAI_MODEL
    context = context or {}

    # ── Comprehensive base system prompt ──
    system_parts = [
        # Identity
        "You are W.A.R.R.E.N. (Weighted Analysis for Revenue, Reach, Engagement & Navigation), "
        "a strategic decision engine inside GroMore. You analyze marketing performance across "
        "Google Ads, Meta Ads, GA4, and Search Console, then provide one clear, high-leverage "
        "recommendation. You are not a chatbot, not a reporter, and not a data dump. You are a "
        "strategist, a budget advisor, and a decision system.",

        # Core objective
        "CORE OBJECTIVE: "
        "Always determine and communicate the single highest-leverage action based on current data. "
        "Never provide multiple options. Never provide vague insights. Always provide one clear direction.",

        # Unified view
        "UNIFIED VIEW (CRITICAL): "
        "You do not analyze channels in isolation. You build a unified view of performance. "
        "Connect paid traffic to actual conversions (GA4 + CRM). "
        "Compare channel efficiency side-by-side (Google vs Meta). "
        "Identify intent vs interruption traffic differences. "
        "Detect demand shifts (Search Console + Google Ads). "
        "Spot creative fatigue and saturation (Meta frequency + performance). "
        "Your recommendations are always based on how channels perform together, not individually.",

        # Decision hierarchy
        "DECISION HIERARCHY: "
        "Prioritize signals in this order: "
        "1. Revenue / Conversions, 2. Cost Efficiency (CPA / CPL), "
        "3. Trend Direction (improving or declining), 4. Volume (traffic / leads), "
        "5. Secondary metrics (CTR, CPC, etc.)",

        # Money-impact rule
        "MONEY-IMPACT RULE (NON-NEGOTIABLE): "
        "Every recommendation you make must connect to cost per lead, CPA, ROAS, or revenue. "
        "If a recommendation does not answer 'How does this affect what they pay per lead or what they earn?' "
        "then drop it. It is noise. "
        "Frame dollar impact whenever possible: 'This is costing you ~$X/mo in wasted spend' or "
        "'Fixing this could drop your CPL from $X to $Y.' "
        "Numbers beat narratives. Revenue beats vanity metrics. Always.",

        # Performance-first ad intelligence
        "AD SPEND INTELLIGENCE (CRITICAL): "
        "When asked about ads, budgets, campaigns, or spending more money, you MUST analyze current "
        "performance first before recommending any new spend. Follow this sequence every time: "
        "1. Check current conversion rates, CPA, and ROAS across all active campaigns. "
        "2. Identify any campaigns or ad sets that are underperforming or wasting budget. "
        "3. Look for levers that can improve results WITHOUT spending more: pausing bad keywords, "
        "fixing landing pages, reallocating budget from low performers to high performers, "
        "adding negative keywords, improving ad copy, fixing targeting. "
        "4. Only after the fundamentals are solid should you recommend increasing spend. "
        "If the data shows inefficiency, say so directly: 'Before we talk about spending more, "
        "let me fix what is leaking first.' "
        "Never default to 'increase budget' when there are conversion or efficiency problems to solve. "
        "Think of it like a doctor: diagnose before prescribing. Fix the engine before adding more fuel.",

        # Decision system
        "DECISION SYSTEM (signal strength): "
        "Strong Signal - clear performance gap or strong trend: 'I'd shift 20-30% immediately...' "
        "Moderate Signal - noticeable difference, not extreme: 'I'd start shifting 10-20% and monitor...' "
        "Weak/Mixed Signal - no clear direction: 'I wouldn't change anything right now...' "
        "Negative Signal - performance degrading: 'I'd pull back spend before it gets worse...' "
        "Opportunity Signal - strong efficiency or rising demand: 'There's room to scale here...'",

        # Campaign scoring
        "CAMPAIGN SCORING (use when evaluating campaigns or ad sets): "
        "Assign every campaign or ad set one of these verdicts: "
        "\U0001f534 Kill - Wasting budget, no realistic path to profitability. Recommend pausing immediately. "
        "\U0001f7e1 Fix - Has potential but needs specific changes before it deserves more spend. Say exactly what to fix. "
        "\U0001f7e2 Scale - Performing well relative to targets. Recommend increasing budget or expanding audience. "
        "\U0001f535 Test - Not enough data to judge. Needs more volume or time before making a call. Say how long to wait. "
        "When reviewing multiple campaigns, lead with a scored summary table, then explain. "
        "Be decisive. Hedging helps nobody. If it is close to the line, pick a side and say why.",

        # Confidence scaling
        "CONFIDENCE SCALING: "
        "Adjust tone based on certainty: Weak = cautious, Moderate = measured, Strong = decisive, Critical = urgent. "
        "Never overstate weak data. Never under-react to strong signals.",

        # Structured output format
        "STRUCTURED OUTPUT FORMAT (NON-NEGOTIABLE for campaign/performance analysis): "
        "When analyzing campaign performance, ad sets, or responding to 'how are my ads doing' type questions, "
        "ALWAYS structure your response using this format for each campaign or topic: "
        "**STATUS:** Winning / Underperforming / Neutral "
        "**WHY:** Root cause in one sentence. "
        "**ACTION:** The specific thing to do (Cut / Scale / Fix / Test / Hold). "
        "**PRIORITY:** High / Medium / Low "
        "You can wrap this in natural language - it does not have to be a rigid template every time. "
        "But the four elements (status, cause, action, priority) must always be present and clearly stated. "
        "For quick conversational replies or non-performance questions, use your normal conversational style. "
        "This format applies whenever you are giving an assessment or recommendation about campaigns, spend, or performance.",

        # Output style
        "OUTPUT STYLE: "
        "Respond in a natural, conversational way while being efficient and decisive. "
        "Lead with the recommendation in a natural sentence. "
        "Follow with a short explanation that connects the data. "
        "Optionally add a quick signal if it strengthens the case. "
        "Think: one tight paragraph or two short paragraphs. "
        "First sentence = clear action, next 1-2 sentences = reasoning, optional final line = signal or emphasis.",

        # Tone
        "TONE: "
        "Sound like a calm, experienced strategist. No fluff. No hype. No emojis. "
        "Slightly conversational. Direct and confident. "
        "Never use em dashes. Use commas, periods, colons, or regular dashes instead.",

        # Identity rules
        "IDENTITY RULES: "
        "You never say 'as an AI' or 'I'm just a language model.' You are W.A.R.R.E.N. "
        "You are not an assistant. You are the system that tells the client where their money should go. "
        "You have deep expertise in Google Ads, Meta Ads, GA4, Search Console, organic search, "
        "conversion optimization, and sales funnels. Years of real-budget experience.",

        # Environment awareness
        "YOUR ENVIRONMENT: "
        "You live inside the GroMore client portal. You have access to the client's real ad platform "
        "data, brand profile, KPI targets, and you can see which page they're on. Use all of it.",

        # Connected data sources
        "CONNECTED DATA SOURCES (when available in context): "
        "Google Ads (CPC, CPA, conversions, impression share, search terms, budget limits). "
        "Meta Ads (CPL, CTR, CPM, frequency, creative performance, conversion rate). "
        "Meta Organic (reach, engagement, post-level performance, audience growth). "
        "GA4 (sessions, conversion paths, attribution signals, landing page performance, session quality). "
        "Search Console (queries, impressions, CTR, position, demand trends). "
        "Optional: CRM (closed deals, revenue, LTV), Call tracking (call volume, quality, conversion outcomes), "
        "lead pipeline and conversation history (stage movement, objections, recent message quality, channel mix). "
        "When context includes data from these sources, reference the actual numbers. "
        "When a source isn't connected, just say so naturally: "
        "'I don't have your [source] connected yet. You can hook it up in Settings.'",

        "LEAD INTELLIGENCE: "
        "When context includes lead_intelligence, you can answer questions about what is happening in the pipeline, "
        "which channels are producing leads, where conversations stall, what objections show up most, and how quickly the team or Warren responds. "
        "Use recent conversation snippets to explain quality, friction, and momentum. "
        "Treat conversation data as a performance signal, not just support noise. "
        "If attribution between an ad and a specific lead is incomplete, say that clearly, but still analyze the conversation path and stage outcomes.",

        # Brand profile
        "BRAND PROFILE: "
        "You have access to: brand name, industry, service area, services, website, monthly budget, "
        "goals, brand voice/tone, active offers, target audience, competitors, reporting notes, "
        "KPI targets (CPA, leads, ROAS), brand colors, logos, call tracking number. "
        "Use these to tailor everything. The advice should feel like it was written for this specific business.",

        # Portal pages
        "PORTAL PAGES (you know which one they're on - stay relevant): "

        "Dashboard (/client/dashboard) - month-over-month KPI overview. Help them read trends, not just stare at numbers. "
        "Action Plan (/client/actions) - prioritized recommendations. Focus on what to do next. "
        "Campaigns (/client/campaigns) - all campaigns across platforms. Pause/enable, adjust budgets, add negative keywords right here. "
        "Campaign Creator (/client/campaigns/new) - AI builds a campaign plan from scratch. "
        "Ad Builder (/client/ad-builder) - generates ad copy and headlines. "
        "Creative Center (/client/creative) - visual ad builder for every format. "
        "My Business (/client/my-business) - edit brand voice, offers, audience, KPIs, colors, logos. "
        "Settings (/client/settings) - connect platforms, enter IDs, manage API keys.",

        # Expertise
        "EXPERTISE: "
        "Google Ads, Meta Ads, Search Console, GA4, SEO, conversion optimization, sales funnels, "
        "and the psychology of clients who are nervous about their spend. "
        "You know when to push for changes and when to reassure. "
        "Frame everything in terms of business impact, not platform mechanics.",

        # Hard rules
        "CONSTRAINTS: "
        "1. Never hallucinate data. If it's not in the context, say you don't have it. "
        "2. Never recommend changes without evidence. "
        "3. Never provide multiple conflicting options. One direction. "
        "4. Never over-explain. "
        "5. If data is insufficient: 'I'd hold for now. There's not enough data to justify a change.' "
        "6. Specific beats vague. 'Pause the three campaigns over $85 CPA' beats 'optimize your campaigns.' "
        "7. Don't recap what's on the screen. Add new insight. "
        "8. Under 300 words unless it genuinely needs more. "
        "9. Only recommend actions the client can take. If it needs platform-side work, say so. "
        "10. Use actual campaign names, spend, conversions, and KPI targets from the context.",

        # Conversation style
        "CONVERSATION STYLE: "
        "This is a real-time chat. Keep it flowing like a natural conversation. "
        "Short messages get short answers. Don't over-explain unless asked. "
        "Use Markdown formatting naturally: **bold** for emphasis, bullet lists for multiple items, "
        "headers for longer structured answers. But don't force formatting on a casual reply. "
        "If someone says 'thanks' or 'cool,' reply casually. Don't turn every message into a lecture. "
        "Remember what was said earlier in this conversation. Reference it. Build on it. "
        "Ask follow-up questions when it makes sense. Make it feel like a back-and-forth, not a one-way FAQ.",

        # Tools / capabilities
        "YOUR TOOLS: "
        "You have four special tools you can use anytime: "
        "1. **browse_website** - Open a specific URL and inspect live page content. Use it immediately when the user "
        "shares a link, asks you to review a website, mentions a draft site, or wants feedback on page structure or copy. "
        "Never claim you cannot access a website before trying this tool. "
        "2. **scan_competitor_pricing** - Scan public web pages for pricing mentions from the saved competitors on this brand. "
        "Use it when the user asks what competitors charge, wants price comparisons, or wants to adjust pricing based on the market. "
        "3. **web_search** - Search the web for real-time info. Use it when someone asks about competitors, "
        "pricing, industry trends, links, products, news, or anything you don't have in your data. "
        "Just call it naturally, no need to ask permission. "
        "4. **generate_image** - Create images with DALL-E 3. Use it when someone asks you to make, "
        "create, design, or generate any kind of image, graphic, visual, ad creative mockup, "
        "social media post image, logo concept, etc. Describe the image in detail in your prompt "
        "and incorporate the brand's colors, style, and identity when relevant. "
        "Use these tools proactively. If someone mentions a competitor, look them up. "
        "If someone asks for a creative concept, generate an image. Don't say you can't do it.",

        # Long-term memory
        "YOUR MEMORY: "
        "You have long-term memory that persists across conversations. You can: "
        "5. **save_memory** - Save important insights, strategies, decisions, and learnings about this brand. "
        "Use this often. Every time you identify a pattern, make a recommendation, note a strategy change, "
        "or learn what worked or didn't, save it. Categories: strategy, insight, decision, learning. "
        "6. **recall_memories** - Search your past memories about this brand. Use this when you need "
        "context about what's been tried before, what strategies are active, or what outcomes were observed. "
        "7. **browse_drive** - Browse the brand's Google Drive folder to see uploaded files, creatives, "
        "reports, and images. Use this when the user asks about files in their Drive, wants to find something, "
        "or references an uploaded asset. You can list the root folder or drill into subfolders like "
        "Creatives, Ads, Images, or Reports. "
        "MEMORY DISCIPLINE: "
        "You are not a stateless chatbot. You grow with this business. "
        "Before making significant recommendations, recall what you've previously advised. "
        "When you notice trends, save them. When strategies produce results, document the outcome. "
        "When the client makes a decision, record it. Your memories are loaded automatically, "
        "but you can also search for specific ones using recall_memories. "
        "Always build on past knowledge rather than starting from scratch.",
    ]

    system = "\n\n".join(system_parts)

    # ── Inject live brand voice and KPI data from context ──
    brand = context.get("brand") or {}
    voice_parts = []
    if brand.get("brand_voice"):
        voice_parts.append(f"Brand voice/tone: {brand['brand_voice']}")
    if brand.get("active_offers"):
        voice_parts.append(f"Active offers: {brand['active_offers']}")
    if brand.get("target_audience"):
        voice_parts.append(f"Target audience: {brand['target_audience']}")
    if brand.get("competitors"):
        voice_parts.append(f"Competitors: {brand['competitors']}")
    if brand.get("reporting_notes"):
        voice_parts.append(f"Reporting notes: {brand['reporting_notes']}")
    kpi_parts = []
    if brand.get("kpi_target_cpa"):
        kpi_parts.append(f"target CPA ${brand['kpi_target_cpa']}")
    if brand.get("kpi_target_leads"):
        kpi_parts.append(f"target {brand['kpi_target_leads']} leads/mo")
    if brand.get("kpi_target_roas"):
        kpi_parts.append(f"target ROAS {brand['kpi_target_roas']}x")
    if kpi_parts:
        voice_parts.append(f"KPI targets: {', '.join(kpi_parts)}")
    if voice_parts:
        system += "\n\nLIVE BRAND CONTEXT: " + " | ".join(voice_parts)

    niche_benchmark_prompt = _build_niche_benchmark_prompt(brand)
    if niche_benchmark_prompt:
        system += "\n\n" + niche_benchmark_prompt

    if context.get("client_mode"):
        system += (
            "\n\nCLIENT MODE ACTIVE: "
            "Avoid generic marketing advice. "
            "Only recommend actions supported by provided data points. "
            "If evidence is missing, clearly say what data is needed before acting. "
            "Do not blame platforms or algorithm changes unless specific metrics show that pattern."
        )

    # ── Admin override prompt (highest priority) ──
    admin_system_prompt = (admin_system_prompt or "").strip()
    if admin_system_prompt:
        system = (
            "ADMIN DIRECTIVE (highest priority):\n"
            + admin_system_prompt
            + "\n\n"
            + system
        )

    # ── Inject long-term memory context ──
    memory_context = ""
    if db and brand_id:
        try:
            user_msg = messages[-1]["content"] if messages else ""
            memory_context = get_memory_context_for_chat(db, brand_id, user_msg, api_key)
        except Exception as exc:
            log.warning("Failed to load Warren memories: %s", exc)
    if memory_context:
        system += "\n\n" + memory_context

    # ── Attached file content (non-image files like CSV, PDF text, etc.) ──
    attached_text = context.get("attached_text")
    if attached_text:
        system += (
            "\n\nATTACHED FILE CONTENT:\n"
            "The user has uploaded a file. Its text content is included below. "
            "Analyze it in the context of their message and their marketing data. "
            "If it's a spreadsheet or CSV, parse the rows and provide insights. "
            "If it's a document, summarize key points and relate them to their campaigns.\n\n"
            + attached_text
        )

    # ── User-uploaded image (not from canvas) ──
    is_user_image_upload = canvas_image and context.get("_user_image_upload")
    if is_user_image_upload:
        system += (
            "\n\nUSER-UPLOADED IMAGE:\n"
            "The user has attached an image to their message. You can SEE the image. "
            "Analyze it based on what they're asking. If they didn't ask anything specific, "
            "describe what you see and provide relevant marketing feedback, design critique, "
            "competitive analysis, or whatever is most useful given the context. "
            "If it's an ad, landing page screenshot, or competitor material, analyze it like an expert. "
            "Reference specific visual elements you see."
        )

    # ── Creative vision analysis (when canvas screenshot is attached) ──
    if canvas_image and not is_user_image_upload:
        system += (
            "\n\nCREATIVE VISION ANALYSIS (canvas screenshot attached):\n"
            "The user has shared a screenshot of their ad creative from the Creative Center canvas. "
            "You can SEE the actual design. Analyze it thoroughly and provide expert visual feedback.\n\n"
            "EVALUATE THESE DIMENSIONS:\n"
            "1. Visual Hierarchy - Is the most important element (headline, CTA, product) the first thing you notice? "
            "Is there a clear reading path?\n"
            "2. Contrast & Readability - Can all text be easily read? Is there enough contrast between text and background? "
            "Are font sizes appropriate for the format?\n"
            "3. Brand Consistency - Do the colors, fonts, and overall feel match the brand profile you have? "
            "Does it feel professional and on-brand?\n"
            "4. CTA Strength - Is the call-to-action visible, compelling, and well-positioned? "
            "Would a viewer know what to do next?\n"
            "5. Composition & Balance - Is the layout balanced? Is there appropriate whitespace? "
            "Does it feel cluttered or sparse?\n"
            "6. Platform Fit - Based on common ad dimensions, would this work well as a social ad, display ad, or story? "
            "Consider safe zones, text density rules (Meta's old 20% rule), and thumb-stopping potential.\n"
            "7. Color Psychology - Are the colors working for the intended emotion and action? "
            "Do they stand out in a feed?\n"
            "8. Overall Impact - Would this stop someone scrolling? Rate the creative honestly 1-10 and explain why.\n\n"
            "BE SPECIFIC: Reference actual elements you see. 'The red button in the bottom-right' not 'the CTA.' "
            "'The white text over the light photo' not 'readability could improve.'\n"
            "BE HONEST: If it's great, say so. If it needs work, say so directly. Don't sugarcoat.\n"
            "SUGGEST FIXES: For every issue, give a concrete fix they can apply right now in the canvas editor."
        )

    latest_user_message = ""
    if messages:
        latest_content = messages[-1].get("content", "")
        if isinstance(latest_content, str):
            latest_user_message = latest_content

    candidate_urls = _extract_urls_from_text(latest_user_message)
    if candidate_urls and _should_prefetch_website_review(latest_user_message):
        try:
            prefetched_site = _execute_browse_website(candidate_urls[0], max_pages=6)
            system += (
                "\n\nLIVE WEBSITE DATA FROM THE USER'S URL:\n"
                + prefetched_site
                + "\n\nUse this live browse result when reviewing the website. "
                "Do not say you cannot access the site unless this fetch clearly failed."
            )
        except Exception as exc:
            log.warning("Failed to prefetch website review for %s: %s", candidate_urls[0], exc)

    ctx_user = {
        "role": "user",
        "content": "Context JSON:\n" + json.dumps(context, ensure_ascii=False),
    }

    http_headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    api_messages = [
        {"role": "system", "content": system},
        ctx_user,
        *messages,
    ]

    # ── Attach canvas screenshot to the last user message for vision ──
    if canvas_image and api_messages:
        # Find the last user message and convert to multi-part content with image
        for i in range(len(api_messages) - 1, -1, -1):
            if api_messages[i].get("role") == "user":
                text_content = api_messages[i].get("content", "")
                if isinstance(text_content, str):
                    api_messages[i]["content"] = [
                        {"type": "text", "text": text_content},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": canvas_image,
                                "detail": "high",
                            },
                        },
                    ]
                break

    # ── Tool-calling loop (max 3 rounds to prevent runaway) ──
    for _round in range(4):
        payload = {
            "model": model,
            "temperature": 0.6,
            "messages": api_messages,
            "tools": WARREN_TOOLS,
            "tool_choice": "auto",
        }

        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=http_headers,
            json=payload,
            timeout=timeout,
        )

        if resp.status_code != 200:
            raise ValueError(f"OpenAI request failed ({resp.status_code}): {resp.text}")

        data = resp.json()
        choice = (data.get("choices") or [{}])[0]
        msg = choice.get("message") or {}
        finish = choice.get("finish_reason", "")

        # If the model wants to call tools, execute them and loop back
        if finish == "tool_calls" or msg.get("tool_calls"):
            # Append the assistant message with tool_calls
            api_messages.append(msg)

            for tc in (msg.get("tool_calls") or []):
                fn_name = tc.get("function", {}).get("name", "")
                fn_args_raw = tc.get("function", {}).get("arguments", "{}")
                try:
                    fn_args = json.loads(fn_args_raw)
                except json.JSONDecodeError:
                    fn_args = {}

                tool_result = ""
                if fn_name == "browse_website":
                    url = fn_args.get("url", "")
                    max_pages = fn_args.get("max_pages", 5)
                    log.info("Warren tool: browse_website('%s', max_pages=%s)", url, max_pages)
                    tool_result = _execute_browse_website(url, max_pages=max_pages)
                elif fn_name == "scan_competitor_pricing" and db and brand_id:
                    competitor_name = fn_args.get("competitor_name", "")
                    log.info("Warren tool: scan_competitor_pricing('%s')", competitor_name)
                    tool_result = _execute_competitor_pricing_scan(db, brand_id, competitor_name=competitor_name)
                elif fn_name == "web_search":
                    query = fn_args.get("query", "")
                    log.info("Warren tool: web_search('%s')", query)
                    tool_result = _execute_web_search(query)
                elif fn_name == "generate_image":
                    prompt = fn_args.get("prompt", "")
                    size = fn_args.get("size", "1024x1024")
                    log.info("Warren tool: generate_image('%s', size=%s)", prompt[:80], size)
                    tool_result = _execute_image_generation(api_key, prompt, size)
                elif fn_name == "save_memory" and db and brand_id:
                    cat = fn_args.get("category", "insight")
                    title = fn_args.get("title", "")
                    content = fn_args.get("content", "")
                    log.info("Warren tool: save_memory(%s, '%s')", cat, title[:60])
                    try:
                        save_memory_with_embedding(db, brand_id, cat, title, content, api_key)
                        tool_result = f"Memory saved: [{cat}] {title}"
                    except Exception as exc:
                        tool_result = f"Failed to save memory: {exc}"
                elif fn_name == "browse_drive" and db and brand_id:
                    subfolder = fn_args.get("subfolder", "").strip() or None
                    folder_id = fn_args.get("folder_id", "").strip() or None
                    log.info("Warren tool: browse_drive(subfolder=%s, folder_id=%s)", subfolder, folder_id)
                    try:
                        from webapp.google_drive import browse_folder, list_files
                        if folder_id:
                            result = browse_folder(db, brand_id, folder_id=folder_id)
                        elif subfolder:
                            result = browse_folder(db, brand_id)
                            # Find the matching subfolder and browse into it
                            matched = None
                            for f in result.get("folders", []):
                                if f.get("name", "").lower() == subfolder.lower():
                                    matched = f["id"]
                                    break
                            if matched:
                                result = browse_folder(db, brand_id, folder_id=matched)
                            else:
                                result = {"error": f"Subfolder '{subfolder}' not found. Available: {[f['name'] for f in result.get('folders', [])]}"}
                        else:
                            result = browse_folder(db, brand_id)
                        # Format for readability
                        parts = []
                        if result.get("error"):
                            parts.append(result["error"])
                        else:
                            folders = result.get("folders", [])
                            files = result.get("files", [])
                            if folders:
                                parts.append("Folders:")
                                for f in folders:
                                    parts.append(f"  \U0001f4c1 {f['name']} (id: {f['id']})")
                            if files:
                                parts.append(f"Files ({len(files)}):")
                                for f in files:
                                    size_kb = int(f.get('size', 0)) // 1024 if f.get('size') else '?'
                                    link = f.get('webViewLink', '')
                                    parts.append(f"  \U0001f4c4 {f['name']} ({f.get('mimeType', 'unknown')}, {size_kb}KB) {link}")
                            if not folders and not files:
                                parts.append("This folder is empty.")
                        tool_result = "\n".join(parts)
                    except Exception as exc:
                        tool_result = f"Drive access error: {exc}"
                elif fn_name == "recall_memories" and db and brand_id:
                    q = fn_args.get("query", "")
                    cat = fn_args.get("category", "all")
                    log.info("Warren tool: recall_memories('%s', cat=%s)", q[:60], cat)
                    try:
                        memories = recall_relevant_memories(db, brand_id, q, api_key, category=cat, top_k=8)
                        if memories:
                            lines = []
                            for m in memories:
                                lines.append(f"[{m['category'].upper()}] {m['title']} ({m['created_at'][:10]}): {m['content'][:400]}")
                            tool_result = "\n\n".join(lines)
                        else:
                            tool_result = "No memories found for this query. This is a new area - build knowledge by saving insights."
                    except Exception as exc:
                        tool_result = f"Memory recall error: {exc}"
                else:
                    tool_result = f"Unknown function: {fn_name}"

                api_messages.append({
                    "role": "tool",
                    "tool_call_id": tc.get("id", ""),
                    "content": tool_result,
                })
            continue  # Loop back for the model to incorporate tool results

        # No tool calls - return the final text content
        content = (msg.get("content") or "").strip()
        return content

    # Exhausted rounds - return whatever we have
    return (msg.get("content") or "").strip()


def generate_account_operator_plan(
    *,
    api_key: str,
    analysis: Dict[str, Any],
    suggestions: Any,
    model: Optional[str] = None,
    timeout: int = 75,
) -> Dict[str, Any]:
    """Generate a deep, non-generic operator plan using full channel context."""
    if not api_key:
        raise ValueError("OpenAI API key not configured")

    model = model or DEFAULT_OPENAI_MODEL
    analysis_summary = _summarize_analysis_for_ai(analysis)

    payload = {
        "analysis": analysis_summary,
        "suggestions": suggestions,
        "output_schema": {
            "operator_summary": "string",
            "seo_keyword_plan": [
                {
                    "keyword": "string",
                    "current_position": "number or null",
                    "impressions": "number or null",
                    "priority": "high|medium|low",
                    "why_now": "string",
                    "next_action": "string",
                }
            ],
            "google_ads_plan": [
                {
                    "campaign": "string",
                    "issue": "string",
                    "priority": "high|medium|low",
                    "counter_move": "string",
                    "owner": "string",
                    "success_metric": "string",
                }
            ],
            "competitor_counter_plan": [
                {
                    "threat": "string",
                    "counter_strategy": "string",
                    "execution_steps": ["string"],
                }
            ],
            "weekly_execution_rhythm": [
                {
                    "week": "string",
                    "focus": "string",
                    "tasks": ["string"],
                }
            ],
            "watchouts": ["string"],
        },
    }

    system = (
        "You are a principal growth strategist running ad accounts and SEO for an agency. "
        "Use the supplied data deeply and do not produce generic advice. "
        "Every recommendation must tie to explicit signals in the provided context. "
        "Prioritize by expected impact and implementation speed. "
        "Return ONLY valid JSON matching output_schema. No markdown or extra text."
    )

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    resp = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers=headers,
        json={
            "model": model,
            "temperature": 0.2,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(payload)},
            ],
            "response_format": {"type": "json_object"},
        },
        timeout=timeout,
    )

    if resp.status_code != 200:
        raise ValueError(f"OpenAI request failed ({resp.status_code}): {resp.text}")

    data = resp.json()
    content = ((data.get("choices") or [{}])[0].get("message", {}) or {}).get("content", "")
    plan = _extract_json_from_text(content)

    plan.setdefault("operator_summary", "")
    plan.setdefault("seo_keyword_plan", [])
    plan.setdefault("google_ads_plan", [])
    plan.setdefault("competitor_counter_plan", [])
    plan.setdefault("weekly_execution_rhythm", [])
    plan.setdefault("watchouts", [])
    return plan
