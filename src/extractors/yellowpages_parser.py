thonimport logging
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

from .utils import (
    build_search_url,
    clean_text,
    fetch_html,
    parse_locality,
    parse_phone,
    parse_rating,
)

logger = logging.getLogger("extractors.yellowpages")

class YellowPagesScraper:
    """
    High-level scraper for YellowPages business listings.
    """

    def __init__(
        self,
        *,
        base_url: str = "https://www.yellowpages.com",
        user_agent: str = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/130.0 Safari/537.36"
        ),
        delay_range: Tuple[float, float] = (1.0, 3.0),
        max_retries: int = 3,
        proxies: Optional[Dict[str, str]] = None,
        timeout: float = 20.0,
    ) -> None:
        self.base_url = base_url
        self.user_agent = user_agent
        self.delay_range = delay_range
        self.max_retries = max_retries
        self.proxies = proxies
        self.timeout = timeout

    def search(
        self,
        *,
        keyword: str,
        location: str,
        max_pages: int = 1,
    ) -> List[Dict[str, Any]]:
        """
        Search for businesses on YellowPages given a keyword and location.
        Returns a list of lead dictionaries with normalized fields.
        """
        all_results: List[Dict[str, Any]] = []

        for page in range(1, max_pages + 1):
            url = build_search_url(self.base_url, keyword, location, page)
            html = fetch_html(
                url,
                user_agent=self.user_agent,
                delay_range=self.delay_range,
                max_retries=self.max_retries,
                proxies=self.proxies,
                timeout=self.timeout,
            )
            if not html:
                logger.warning("Stopping search at page %d due to fetch failure.", page)
                break

            page_results = self._parse_search_page(html)
            if not page_results:
                logger.info(
                    "No results found on page %d; assuming end of listings.", page
                )
                break

            logger.info(
                "Parsed %d results from page %d.", len(page_results), page
            )
            all_results.extend(page_results)

        return all_results

    def _parse_search_page(self, html: str) -> List[Dict[str, Any]]:
        """
        Parse a YellowPages search results page into a list of business dicts.
        """
        soup = BeautifulSoup(html, "lxml")

        # YellowPages often uses <div class="result"> for each listing.
        # We'll look for multiple patterns to make this resilient.
        result_containers = soup.select("div.search-results div.result") or soup.select(
            "div.result"
        )

        leads: List[Dict[str, Any]] = []
        logger.debug("Found %d potential result containers.", len(result_containers))

        for container in result_containers:
            lead = self._parse_single_result(container)
            if lead:
                leads.append(lead)

        return leads

    def _parse_single_result(self, container: Any) -> Optional[Dict[str, Any]]:
        """
        Extract fields for a single result container.
        Returns None if it doesn't seem like a valid listing.
        """
        try:
            # Business name
            name_el = container.select_one("a.business-name span") or container.select_one(
                "a.business-name"
            )
            business_name = clean_text(getattr(name_el, "text", None))

            if not business_name:
                # This might be an ad container or other noise
                return None

            # Category
            category_el = container.select_one("div.categories a") or container.select_one(
                "div.categories"
            )
            category = clean_text(getattr(category_el, "text", None))

            # Address and locality
            address_el = container.select_one("div.street-address")
            locality_el = container.select_one("div.locality")

            address = clean_text(getattr(address_el, "text", None))
            locality_info = parse_locality(getattr(locality_el, "text", None))

            # Phone
            phone_el = container.select_one("div.phones") or container.select_one(
                "a.phone"
            )
            phone_number = parse_phone(getattr(phone_el, "text", None))

            # Website
            website_el = (
                container.select_one("a.track-visit-website")
                or container.select_one("a.website-link")
                or container.select_one("a.track-visit-website-mobile")
            )
            website = clean_text(website_el.get("href")) if website_el else None

            # Email (rarely provided directly on listing; look for mailto links)
            email_el = container.select_one('a[href^="mailto:"]')
            email = None
            if email_el is not None:
                href = email_el.get("href", "")
                if href.lower().startswith("mailto:"):
                    email = clean_text(href[len("mailto:") :])

            # Rating
            rating_el = container.select_one("div.result-rating") or container.select_one(
                "div.ratings"
            )
            rating = parse_rating(rating_el)

            lead: Dict[str, Any] = {
                "business_name": business_name,
                "category": category,
                "address": address,
                "city": locality_info.get("city"),
                "state": locality_info.get("state"),
                "zip_code": locality_info.get("zip_code"),
                "phone_number": phone_number,
                "email": email,
                "website": website,
                "rating": rating,
            }

            return lead
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to parse a result container: %s", exc, exc_info=True)
            return None