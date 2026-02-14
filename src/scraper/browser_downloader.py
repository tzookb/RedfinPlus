"""
Step 2: Playwright browser fallback for CSV download.

Use this when the direct httpx CSV endpoint starts rejecting
requests (rate limiting, captcha). Playwright with stealth
navigates the real Redfin search page and clicks "Download All".
"""

import logging
import os
from datetime import datetime

import pandas as pd

from .config import WatchQuery, ScraperConfig

logger = logging.getLogger(__name__)


class RedfBrowserDownloader:
    """Fallback CSV downloader using a real browser via Playwright."""

    def __init__(self, config: ScraperConfig | None = None):
        self.config = config or ScraperConfig()

    async def download(self, search_url: str) -> pd.DataFrame:
        """Navigate to a Redfin search URL and click "Download All".

        Args:
            search_url: Full Redfin search results URL, e.g.
                "https://www.redfin.com/city/29439/WA/Bothell/filter/..."

        Returns:
            pandas DataFrame parsed from the downloaded CSV.
        """
        from playwright.async_api import async_playwright

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.config.headless)
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 1080},
            )
            page = await context.new_page()

            # Apply stealth patches if available
            try:
                from playwright_stealth import stealth_async
                await stealth_async(page)
                logger.debug("Applied playwright-stealth patches")
            except ImportError:
                logger.debug(
                    "playwright-stealth not installed, proceeding without it"
                )

            logger.info("Navigating to %s", search_url)
            await page.goto(search_url, wait_until="networkidle")
            await page.wait_for_timeout(2000)

            # Find the "Download All" button/link
            download_button = page.locator('a#download-and-save')

            # Wait for it to appear (search results need to load)
            try:
                await download_button.wait_for(
                    state="visible",
                    timeout=self.config.browser_timeout,
                )
            except Exception:
                # Try alternate selector — Redfin sometimes changes the UI
                download_button = page.locator(
                    'a[href*="gis-csv"], button:has-text("Download All")'
                )
                await download_button.first.wait_for(
                    state="visible",
                    timeout=self.config.browser_timeout,
                )
                download_button = download_button.first

            # Click and capture the download
            os.makedirs(self.config.output_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            save_path = os.path.join(
                self.config.output_dir, f"redfin_browser_{timestamp}.csv"
            )

            async with page.expect_download(
                timeout=self.config.browser_timeout
            ) as download_info:
                await download_button.click()

            download = await download_info.value
            await download.save_as(save_path)
            logger.info("Browser download saved to %s", save_path)

            await browser.close()

        df = pd.read_csv(save_path)
        logger.info("Parsed %d listings from browser download", len(df))
        return df

    async def download_from_query(self, query: WatchQuery) -> pd.DataFrame:
        """Build a Redfin search URL from a WatchQuery and download via browser.

        This constructs the /city/ or /zipcode/ URL with filter params.
        Less precise than the direct CSV endpoint but works when blocked.
        """
        # Build a filter URL — Redfin's filter URL format
        base = "https://www.redfin.com/stingray/api/gis-csv"
        params = query.to_params()

        # For browser approach, navigate to the actual search page instead.
        # We construct the search page URL and let the browser handle the
        # CSV download button.
        #
        # The simplest approach: go to the search results page.
        # We'll use the gis-search page format which redirects to results.
        search_url = "https://www.redfin.com/stingray/do/gis-search?"
        param_parts = [f"{k}={v}" for k, v in params.items()]
        search_url += "&".join(param_parts)

        # Actually, the easiest is to navigate the filter URL directly.
        # But Redfin's JS routing means we need the actual search page.
        # We use a simpler approach: go to the region page.
        region_type_map = {
            2: "zipcode",
            5: "county",
            6: "city",
        }
        region_slug = region_type_map.get(query.region_type, "city")

        # For cities/zips we need the redfin path — user should pass the
        # full search URL for best results. Fall back to generic filter URL.
        filter_url = (
            f"https://www.redfin.com/{region_slug}/{query.region_id}"
        )

        logger.info(
            "Browser download for '%s' — navigating to %s",
            query.name, filter_url,
        )
        return await self.download(filter_url)
