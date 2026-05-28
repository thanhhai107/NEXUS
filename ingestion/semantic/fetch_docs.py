"""
Fetch API Documentation.

Fetches documentation from URLs provided in source config.
"""

from __future__ import annotations

import requests
# Default headers for API documentation fetching
DEFAULT_HEADERS = {
    "User-Agent": "NEXUS-SemanticAnnotator/1.0 (data-pipeline)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

DEFAULT_TIMEOUT = 30  # seconds
MAX_CONTENT_LENGTH = 100_000  # 100KB max


def fetch_api_docs(
    docs_url: str | None,
    timeout: int = DEFAULT_TIMEOUT,
    max_length: int = MAX_CONTENT_LENGTH,
) -> str | None:
    """
    Fetch API documentation from URL.
    
    Args:
        docs_url: URL of the API documentation
        timeout: Request timeout in seconds
        max_length: Maximum content length to fetch
    
    Returns:
        Documentation text, or None if fetch failed
    
    Usage:
        docs = fetch_api_docs("https://api.tfl.gov.uk/")
        if docs:
            # Use docs for LLM context
    """
    if not docs_url:
        return None
    
    # Validate URL
    if not docs_url.startswith(("http://", "https://")):
        print(f"Warning: Invalid URL scheme: {docs_url}")
        return None
    
    try:
        response = requests.get(
            docs_url,
            headers=DEFAULT_HEADERS,
            timeout=timeout,
            allow_redirects=True,
        )
        response.raise_for_status()
        
        content_type = response.headers.get("Content-Type", "")
        
        # Check if response is HTML or text
        if "text/html" in content_type or "text/plain" in content_type:
            # Truncate if too long
            content = response.text[:max_length]
            
            # Basic cleanup
            content = _clean_html(content)
            
            return content
        else:
            print(f"Warning: Unexpected content type '{content_type}' from {docs_url}")
            return None
            
    except requests.exceptions.Timeout:
        print(f"Warning: Timeout fetching docs from {docs_url}")
        return None
    except requests.exceptions.ConnectionError as e:
        print(f"Warning: Connection error fetching docs from {docs_url}: {e}")
        return None
    except requests.exceptions.HTTPError as e:
        print(f"Warning: HTTP error {e.response.status_code} fetching docs from {docs_url}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Warning: Failed to fetch docs from {docs_url}: {e}")
        return None


def fetch_api_docs_batch(
    urls: dict[str, str],
    timeout: int = DEFAULT_TIMEOUT,
) -> dict[str, str | None]:
    """
    Fetch documentation from multiple URLs.
    
    Args:
        urls: Dict of source_id -> docs_url
        timeout: Request timeout in seconds
    
    Returns:
        Dict of source_id -> documentation text (or None)
    """
    results = {}
    
    for source_id, docs_url in urls.items():
        results[source_id] = fetch_api_docs(docs_url, timeout)
    
    return results


def _clean_html(html: str) -> str:
    """
    Basic HTML cleanup to extract readable text.
    
    Removes scripts, styles, and extra whitespace.
    
    Args:
        html: Raw HTML content
    
    Returns:
        Cleaned text content
    """
    import re
    
    # Remove script and style tags with content
    html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
    
    # Remove HTML comments
    html = re.sub(r'<!--.*?-->', '', html, flags=re.DOTALL)
    
    # Remove all HTML tags
    html = re.sub(r'<[^>]+>', ' ', html)
    
    # Decode common HTML entities
    html = html.replace("&nbsp;", " ")
    html = html.replace("&lt;", "<")
    html = html.replace("&gt;", ">")
    html = html.replace("&amp;", "&")
    html = html.replace("&quot;", '"')
    html = html.replace("&#39;", "'")
    
    # Normalize whitespace
    html = re.sub(r'\s+', ' ', html)
    
    return html.strip()


def fetch_with_retry(
    docs_url: str,
    max_retries: int = 3,
    timeout: int = DEFAULT_TIMEOUT,
) -> str | None:
    """
    Fetch API docs with retry logic.
    
    Args:
        docs_url: URL to fetch
        max_retries: Maximum number of retries
        timeout: Request timeout
    
    Returns:
        Documentation text, or None if all retries failed
    """
    import time
    
    for attempt in range(max_retries):
        try:
            return fetch_api_docs(docs_url, timeout)
        except Exception:
            if attempt < max_retries - 1:
                # Exponential backoff
                wait_time = 2 ** attempt
                print(f"Retry {attempt + 1}/{max_retries} for {docs_url} after {wait_time}s")
                time.sleep(wait_time)
    
    print(f"Warning: All {max_retries} attempts failed for {docs_url}")
    return None


def extract_field_descriptions(
    docs_text: str,
    fields: list[str],
) -> dict[str, str | None]:
    """
    Try to extract field descriptions from documentation text.
    
    Simple keyword matching - not perfect but helpful.
    
    Args:
        docs_text: Documentation text
        fields: List of field names to look for
    
    Returns:
        Dict of field_name -> extracted description (or None)
    """
    import re
    
    results = {}
    docs_lower = docs_text.lower()
    
    for field in fields:
        field_lower = field.lower().replace("_", " ")
        
        # Look for patterns like "field_name: description" or "field_name - description"
        patterns = [
            rf'{re.escape(field_lower)}[:\s]+([^.!\n]{{10,100}})',
            rf'{re.escape(field)}(?:\s*:|\s*-|\s*\()\s*([^.!\n]{{10,100}})',
        ]
        
        found = False
        for pattern in patterns:
            match = re.search(pattern, docs_lower)
            if match:
                description = match.group(1).strip()
                # Clean up
                description = re.sub(r'\s+', ' ', description)
                results[field] = description[:200]  # Limit length
                found = True
                break
        
        if not found:
            results[field] = None
    
    return results
