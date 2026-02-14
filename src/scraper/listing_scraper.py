"""
Step 3: Scrape individual listing pages for description + images.

The CSV gives us structured data (price, beds, baths, sqft, etc.)
but not the listing description or image URLs. For the ~20-50
listings that pass hard filters, we hit their individual pages
to extract these details.
"""

import asyncio
import json
import logging
import random
import re
from dataclasses import dataclass, field

import httpx
from selectolax.parser import HTMLParser

from .config import DEFAULT_HEADERS, ScraperConfig

logger = logging.getLogger(__name__)


@dataclass
class ListingDetails:
    """Extracted details from an individual Redfin listing page."""

    url: str = ""
    description: str = ""
    image_urls: list[str] = field(default_factory=list)
    raw_data: dict = field(default_factory=dict)


class ListingScraper:
    """Async scraper for individual Redfin listing pages."""

    def __init__(self, config: ScraperConfig | None = None):
        self.config = config or ScraperConfig()

    async def scrape_listing(
        self, url: str, client: httpx.AsyncClient
    ) -> ListingDetails:
        """Scrape a single Redfin listing page for description and images.

        Args:
            url: Full Redfin listing URL.
            client: Shared async HTTP client.

        Returns:
            ListingDetails with description and image URLs.
        """
        details = ListingDetails(url=url)

        try:
            resp = await client.get(url)
        except httpx.HTTPError as e:
            logger.warning("Failed to fetch %s: %s", url, e)
            return details

        if resp.status_code != 200:
            logger.warning("Got status %d for %s", resp.status_code, url)
            return details

        tree = HTMLParser(resp.text)

        # --- Description ---
        # Redfin puts the listing description in a div with id
        # "marketing-remarks-scroll" or class "remarks"
        desc_node = tree.css_first(
            "div#marketing-remarks-scroll, "
            "div.remarks, "
            'div[data-testid="listing-remarks"], '
            "p.marketing-remarks"
        )
        if desc_node:
            details.description = desc_node.text(strip=True)

        # --- Images ---
        # Redfin embeds listing data in script tags as JSON.
        # Look for the preloaded data that contains image URLs.
        details.image_urls = self._extract_images_from_scripts(tree)

        # If script extraction didn't work, fall back to <img> tags
        if not details.image_urls:
            details.image_urls = self._extract_images_from_tags(tree)

        # --- Embedded JSON data ---
        # Try to grab the full property data blob that Redfin embeds
        details.raw_data = self._extract_embedded_json(tree)

        logger.info(
            "Scraped %s: %d chars description, %d images",
            url, len(details.description), len(details.image_urls),
        )
        return details

    @staticmethod
    def _extract_images_from_scripts(tree: HTMLParser) -> list[str]:
        """Extract image URLs from Redfin's embedded JSON in script tags."""
        image_urls = []

        for script in tree.css("script"):
            text = script.text() or ""

            # Look for the pattern that contains listing images
            if "listingImages" not in text and "photos" not in text:
                continue

            # Try to find JSON objects containing image URLs
            # Redfin uses several patterns for embedding data
            url_pattern = re.compile(
                r'https?://ssl\.cdn-redfin\.com/[^"\'\\]+\.(?:jpg|jpeg|png|webp)',
                re.IGNORECASE,
            )
            found = url_pattern.findall(text)
            if found:
                # Deduplicate while preserving order
                seen = set()
                for u in found:
                    if u not in seen:
                        seen.add(u)
                        image_urls.append(u)
                break

        return image_urls

    @staticmethod
    def _extract_images_from_tags(tree: HTMLParser) -> list[str]:
        """Fallback: extract image URLs from <img> tags on the listing page."""
        image_urls = []
        seen = set()

        for img in tree.css("img[src*='cdn-redfin'], img[src*='ssl.cdn-redfin']"):
            src = img.attributes.get("src", "")
            if src and src not in seen and not src.endswith("_icon.png"):
                seen.add(src)
                image_urls.append(src)

        return image_urls

    @staticmethod
    def _extract_embedded_json(tree: HTMLParser) -> dict:
        """Try to extract the full embedded property data JSON."""
        for script in tree.css("script"):
            text = script.text() or ""

            # Redfin embeds data as `root.__reactServerAgent.config = {...}`
            # or in a `<script type="application/ld+json">` tag
            if "application/ld+json" in (script.attributes.get("type") or ""):
                try:
                    return json.loads(text)
                except (json.JSONDecodeError, ValueError):
                    continue

            # Look for the reactServerAgent config blob
            match = re.search(
                r'reactServerAgent\.config\s*=\s*(\{.+?\});?\s*$',
                text,
                re.DOTALL | re.MULTILINE,
            )
            if match:
                try:
                    return json.loads(match.group(1))
                except (json.JSONDecodeError, ValueError):
                    continue

        return {}

    async def scrape_listings(
        self, urls: list[str]
    ) -> list[ListingDetails]:
        """Scrape multiple listing pages with rate limiting.

        Args:
            urls: List of full Redfin listing URLs.

        Returns:
            List of ListingDetails, one per URL.
        """
        results = []

        async with httpx.AsyncClient(
            headers=DEFAULT_HEADERS,
            follow_redirects=True,
            timeout=30.0,
        ) as client:
            for i, url in enumerate(urls):
                if not url.startswith("http"):
                    url = f"https://www.redfin.com{url}"

                logger.info(
                    "Scraping listing %d/%d: %s", i + 1, len(urls), url
                )
                result = await self.scrape_listing(url, client)
                results.append(result)

                # Rate limit — be polite
                if i < len(urls) - 1:
                    delay = self.config.listing_request_delay_min + random.uniform(
                        0, self.config.listing_request_delay_max - self.config.listing_request_delay_min
                    )
                    logger.debug("Sleeping %.1fs before next request", delay)
                    await asyncio.sleep(delay)

        return results
