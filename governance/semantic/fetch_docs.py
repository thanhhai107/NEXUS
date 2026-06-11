"""API Documentation Fetcher.

Fetches API documentation for semantic annotation context.
"""

from __future__ import annotations

import re


def fetch_api_docs(url: str) -> str | None:
    """Fetch API documentation from URL.
    
    Args:
        url: URL to fetch documentation from
    
    Returns:
        Documentation text or None if fetch fails
    """
    try:
        import requests

        headers = {
            "Accept": "text/html,application/json",
            "User-Agent": "NEXUS/1.0",
        }

        response = requests.get(url, headers=headers, timeout=10)

        if response.status_code == 200:
            content = response.text

            # Try to extract useful text
            if "json" in response.headers.get("Content-Type", ""):
                try:
                    data = response.json()
                    return _extract_from_json(data)
                except Exception:
                    pass

            # Extract text from HTML
            text = re.sub(r"<[^>]+>", " ", content)
            text = re.sub(r"\s+", " ", text).strip()

            return text[:5000] if len(text) > 5000 else text

    except Exception as e:
        print(f"Failed to fetch docs from {url}: {e}")

    return None


def _extract_from_json(data: dict) -> str:
    """Extract useful text from JSON API response."""
    lines = []

    def extract(obj, path=""):
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_path = f"{path}.{key}" if path else key
                if isinstance(value, str) and len(value) > 10 and len(value) < 500:
                    lines.append(f"{new_path}: {value}")
                elif isinstance(value, (dict, list)):
                    extract(value, new_path)
        elif isinstance(obj, list):
            for i, item in enumerate(obj[:10]):
                extract(item, f"{path}[{i}]")

    extract(data)
    return "\n".join(lines[:100])
