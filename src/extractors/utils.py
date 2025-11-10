thonimport logging
import random
import time
from typing import Any, Dict, Optional, Sequence, Tuple
from urllib.parse import quote_plus

import requests

logger = logging.getLogger("extractors.utils")

def build_search_url(base_url: str, keyword: str, location: str, page: int = 1) -> str:
    """
    Build a YellowPages search URL from keyword, location, and page.
    Example:
        https://www.yellowpages.com/search?search_terms=plumbers&geo_location_terms=Los+Angeles%2C+CA&page=2
    """
    keyword_param = quote_plus(keyword.strip())
    location_param = quote_plus(location.strip())
    url = (
        f"{base_url.rstrip('/')}/search?search_terms={keyword_param}"
        f"&geo_location_terms={location_param}"
    )
    if page > 1:
        url = f"{url}&page={page}"
    logger.debug("Built search URL: %s", url)
    return url

def random_delay(delay_range: Tuple[float, float]) -> None:
    """
    Sleep for a random amount of time between delay_range[0] and delay_range[1].
    """
    low, high = delay_range
    if high <= 0:
        return
    duration = random.uniform(max(0, low), max(low, high))
    logger.debug("Sleeping for %.2f seconds to throttle requests.", duration)
    time.sleep(duration)

def fetch_html(
    url: str,
    *,
    user_agent: str,
    delay_range: Tuple[float, float],
    max_retries: int = 3,
    proxies: Optional[Dict[str, str]] = None,
    timeout: float = 20.0,
) -> Optional[str]:
    """
    Fetch the HTML content from a URL with retries and basic error handling.
    Returns None if all retries fail.
    """
    headers = {
        "User-Agent": user_agent,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
    }

    for attempt in range(1, max_retries + 1):
        try:
            logger.info("Requesting (%d/%d): %s", attempt, max_retries, url)
            random_delay(delay_range)
            response = requests.get(
                url,
                headers=headers,
                proxies=proxies,
                timeout=timeout,
            )
            if response.status_code >= 500:
                logger.warning(
                    "Server error %s fetching %s (attempt %d).",
                    response.status_code,
                    url,
                    attempt,
                )
                continue
            if response.status_code != 200:
                logger.error(
                    "Non-OK status %s fetching %s (attempt %d).",
                    response.status_code,
                    url,
                    attempt,
                )
                return None
            logger.debug("Received %d bytes from %s", len(response.text), url)
            return response.text
        except requests.RequestException as exc:
            logger.warning(
                "RequestException fetching %s (attempt %d/%d): %s",
                url,
                attempt,
                max_retries,
                exc,
            )
            if attempt == max_retries:
                return None
    return None

def clean_text(value: Optional[str]) -> Optional[str]:
    """
    Normalize whitespace in text and strip surrounding spaces.
    Returns None if value is falsy after cleaning.
    """
    if value is None:
        return None
    text = " ".join(str(value).split())
    return text or None

def parse_locality(locality_text: Optional[str]) -> Dict[str, Optional[str]]:
    """
    Parse a locality string into city, state, and zip_code.
    Example:
        'Los Angeles, CA 90001' -> {'city': 'Los Angeles', 'state': 'CA', 'zip_code': '90001'}
    """
    locality_text = clean_text(locality_text) or ""
    city = state = zip_code = None

    if "," in locality_text:
        city_part, rest = locality_text.split(",", 1)
        city = clean_text(city_part)
        rest = clean_text(rest)
    else:
        rest = locality_text

    if rest:
        parts: Sequence[str] = rest.split()
        if len(parts) >= 1:
            state = parts[0]
        if len(parts) >= 2:
            zip_code = parts[1]

    return {
        "city": city,
        "state": state,
        "zip_code": zip_code,
    }

def parse_phone(phone_text: Optional[str]) -> Optional[str]:
    """
    Basic cleanup for phone numbers.
    """
    return clean_text(phone_text)

def parse_rating(rating_element: Any) -> Optional[float]:
    """
    Attempt to parse rating from various representations on YellowPages pages.
    """
    if rating_element is None:
        return None

    # Common pattern: attribute data-rating
    value = getattr(rating_element, "get", None)
    if callable(value):
        raw = rating_element.get("data-rating")
        if raw:
            try:
                return float(raw)
            except (ValueError, TypeError):
                pass

    # Fallback: try from inner text
    text = clean_text(getattr(rating_element, "text", None))
    if not text:
        return None

    for token in text.split():
        try:
            return float(token)
        except ValueError:
            continue
    return None