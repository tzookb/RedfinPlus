"""
Step 1: Direct CSV download from Redfin's gis-csv endpoint.

This is the fastest and most reliable approach — no browser needed.
Redfin's "Download All" button on search results hits this endpoint,
and we can construct the URL programmatically with the right region_id,
region_type, and filter params.
"""

import logging
import os
from datetime import datetime
from io import StringIO

import httpx
import pandas as pd

from .config import REDFIN_CSV_ENDPOINT, DEFAULT_HEADERS, WatchQuery, ScraperConfig

logger = logging.getLogger(__name__)


class CSVDownloadError(Exception):
    """Raised when CSV download fails (rate-limited, blocked, etc.)."""
    pass


class RedfCSVDownloader:
    """Downloads Redfin search results as CSV via the gis-csv endpoint."""

    def __init__(self, config: ScraperConfig | None = None):
        self.config = config or ScraperConfig()
        self._client: httpx.Client | None = None

    def _get_client(self) -> httpx.Client:
        if self._client is None or self._client.is_closed:
            self._client = httpx.Client(
                headers=DEFAULT_HEADERS,
                follow_redirects=True,
                timeout=30.0,
            )
        return self._client

    def close(self):
        if self._client and not self._client.is_closed:
            self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def download(self, query: WatchQuery) -> pd.DataFrame:
        """Download CSV for a watch query and return as DataFrame.

        Args:
            query: A WatchQuery with region_id, region_type, and filters.

        Returns:
            pandas DataFrame with the raw CSV columns from Redfin.

        Raises:
            CSVDownloadError: If the request fails or returns non-CSV data.
        """
        params = query.to_params()
        client = self._get_client()

        logger.info("Downloading CSV for '%s' (region=%d, type=%d)",
                     query.name, query.region_id, query.region_type)
        logger.debug("Params: %s", params)

        try:
            resp = client.get(REDFIN_CSV_ENDPOINT, params=params)
        except httpx.HTTPError as e:
            raise CSVDownloadError(f"HTTP request failed: {e}") from e

        if resp.status_code != 200:
            raise CSVDownloadError(
                f"Got status {resp.status_code} for query '{query.name}'. "
                f"Response: {resp.text[:500]}"
            )

        content_type = resp.headers.get("content-type", "")
        if "text/csv" not in content_type and "text/plain" not in content_type:
            # Redfin might return HTML (captcha page) instead of CSV
            if "<html" in resp.text[:200].lower():
                raise CSVDownloadError(
                    f"Got HTML instead of CSV for '{query.name}' — "
                    "likely blocked or captcha. Try the browser fallback."
                )

        try:
            df = pd.read_csv(StringIO(resp.text))
        except Exception as e:
            raise CSVDownloadError(
                f"Failed to parse CSV for '{query.name}': {e}"
            ) from e

        if df.empty:
            logger.warning("CSV for '%s' returned 0 rows", query.name)
        else:
            logger.info("Downloaded %d listings for '%s'", len(df), query.name)

        return df

    def download_and_save(self, query: WatchQuery) -> tuple[pd.DataFrame, str]:
        """Download CSV and save a timestamped copy to output_dir.

        Returns:
            Tuple of (DataFrame, path_to_saved_csv).
        """
        df = self.download(query)

        os.makedirs(self.config.output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_name = query.name.replace(" ", "_").replace("/", "_")
        filename = f"redfin_{safe_name}_{timestamp}.csv"
        filepath = os.path.join(self.config.output_dir, filename)
        df.to_csv(filepath, index=False)
        logger.info("Saved CSV to %s", filepath)

        return df, filepath

    @staticmethod
    def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Normalize Redfin CSV columns to a consistent schema.

        Redfin CSV columns vary slightly over time. This maps them
        to a stable set of lowercase names matching the existing
        RFAPI.py conventions.
        """
        col_map = {
            "SALE TYPE": "sale_type",
            "SOLD DATE": "sold_date",
            "PROPERTY TYPE": "property_type",
            "ADDRESS": "address",
            "CITY": "city",
            "STATE OR PROVINCE": "state",
            "STATE": "state",
            "ZIP OR POSTAL CODE": "zip",
            "ZIP": "zip",
            "PRICE": "price",
            "BEDS": "beds",
            "BATHS": "baths",
            "LOCATION": "location",
            "SQUARE FEET": "sqft",
            "LOT SIZE": "lot_size",
            "YEAR BUILT": "year_built",
            "DAYS ON MARKET": "dom",
            "$/SQUARE FEET": "price_per_sqft",
            "HOA/MONTH": "hoa_monthly",
            "STATUS": "status",
            "NEXT OPEN HOUSE START TIME": "open_house_start",
            "NEXT OPEN HOUSE END TIME": "open_house_end",
            "URL (SEE https://www.redfin.com/buy-a-home/comparative-market-analysis FOR INFO ON PRICING)": "url",
            "URL (SEE http://www.redfin.com/buy-a-home/comparative-market-analysis FOR INFO ON PRICING)": "url",
            "SOURCE": "source",
            "MLS#": "mls_id",
            "FAVORITE": "favorite",
            "INTERESTED": "interested",
            "LATITUDE": "latitude",
            "LONGITUDE": "longitude",
        }

        renamed = {}
        for col in df.columns:
            normalized = col_map.get(col.strip().upper(), col.strip().lower())
            renamed[col] = normalized
        df = df.rename(columns=renamed)

        # Ensure numeric types
        for col in ["price", "beds", "baths", "sqft", "lot_size", "year_built",
                     "dom", "price_per_sqft", "hoa_monthly", "latitude", "longitude"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df
