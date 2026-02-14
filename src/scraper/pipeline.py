"""
Step 4: Full pipeline orchestrator.

    Step 1: Redfin CSV endpoint (bulk data, no browser needed)
               |
               v
    Step 2: Hard filter the CSV -> keep only interesting listings
               |
               v
    Step 3: Scrape individual pages for the ~20-50 that passed
             (description + images only for filtered results)
               |
               v
    Step 4: Return enriched results ready for scoring/storage/notification
"""

import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Optional

import pandas as pd

from .config import ScraperConfig, WatchQuery
from .csv_downloader import RedfCSVDownloader, CSVDownloadError
from .listing_scraper import ListingScraper, ListingDetails

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    """Result from a single watch query pipeline run."""

    query_name: str
    raw_count: int = 0
    filtered_count: int = 0
    enriched_count: int = 0
    df_raw: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_filtered: pd.DataFrame = field(default_factory=pd.DataFrame)
    listing_details: list[ListingDetails] = field(default_factory=list)
    error: Optional[str] = None


class RedfinPipeline:
    """Orchestrates the full scrape -> filter -> enrich pipeline."""

    def __init__(self, config: ScraperConfig | None = None):
        self.config = config or ScraperConfig()
        self.csv_downloader = RedfCSVDownloader(self.config)
        self.listing_scraper = ListingScraper(self.config)

    def close(self):
        self.csv_downloader.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def run_query(
        self,
        query: WatchQuery,
        filters: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
        scrape_details: bool = True,
        use_browser_fallback: bool = True,
    ) -> PipelineResult:
        """Run the full pipeline for a single watch query.

        Args:
            query: The watch query to run.
            filters: Optional function that takes a DataFrame and returns
                a filtered DataFrame. Applied after column normalization.
                If None, all rows pass through.
            scrape_details: Whether to scrape individual listing pages
                for descriptions and images (Step 3).
            use_browser_fallback: If True and the CSV endpoint fails,
                try the Playwright browser approach.

        Returns:
            PipelineResult with raw data, filtered data, and listing details.
        """
        result = PipelineResult(query_name=query.name)

        # --- Step 1: Download CSV ---
        logger.info("=" * 60)
        logger.info("Pipeline: Step 1 — CSV download for '%s'", query.name)
        logger.info("=" * 60)

        df = None
        try:
            df = self.csv_downloader.download(query)
        except CSVDownloadError as e:
            logger.warning("CSV download failed: %s", e)
            if use_browser_fallback:
                logger.info("Attempting browser fallback...")
                df = self._try_browser_fallback(query)

        if df is None or df.empty:
            result.error = "No data returned from CSV download"
            logger.error(result.error)
            return result

        # Normalize columns
        df = RedfCSVDownloader.normalize_columns(df)
        result.df_raw = df.copy()
        result.raw_count = len(df)
        logger.info("Step 1 complete: %d raw listings", result.raw_count)

        # --- Step 2: Apply hard filters ---
        logger.info("Pipeline: Step 2 — filtering")

        if filters is not None:
            df_filtered = filters(df)
        else:
            df_filtered = df

        result.df_filtered = df_filtered.copy()
        result.filtered_count = len(df_filtered)
        logger.info(
            "Step 2 complete: %d/%d listings passed filters",
            result.filtered_count, result.raw_count,
        )

        # --- Step 3: Scrape individual listing pages ---
        if scrape_details and result.filtered_count > 0:
            logger.info("Pipeline: Step 3 — scraping %d listing pages",
                        result.filtered_count)

            urls = []
            if "url" in df_filtered.columns:
                urls = df_filtered["url"].dropna().tolist()
            elif "URL" in df_filtered.columns:
                urls = df_filtered["URL"].dropna().tolist()

            if urls:
                # Prefix with domain if needed
                urls = [
                    u if u.startswith("http") else f"https://www.redfin.com{u}"
                    for u in urls
                ]

                result.listing_details = asyncio.run(
                    self.listing_scraper.scrape_listings(urls)
                )
                result.enriched_count = sum(
                    1 for d in result.listing_details
                    if d.description or d.image_urls
                )
                logger.info(
                    "Step 3 complete: enriched %d/%d listings with details",
                    result.enriched_count, result.filtered_count,
                )
            else:
                logger.warning("No URLs found in filtered data to scrape")
        else:
            logger.info("Step 3 skipped (scrape_details=%s, filtered=%d)",
                        scrape_details, result.filtered_count)

        # --- Step 4: Save results ---
        self._save_results(result)

        logger.info("=" * 60)
        logger.info(
            "Pipeline complete for '%s': %d raw -> %d filtered -> %d enriched",
            query.name, result.raw_count, result.filtered_count,
            result.enriched_count,
        )
        logger.info("=" * 60)

        return result

    def run_all(
        self,
        filters: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
        scrape_details: bool = True,
    ) -> list[PipelineResult]:
        """Run the pipeline for all configured watch queries.

        Args:
            filters: Optional filter function applied to each query's results.
            scrape_details: Whether to scrape individual listing pages.

        Returns:
            List of PipelineResults, one per watch query.
        """
        results = []
        for query in self.config.watch_queries:
            result = self.run_query(
                query, filters=filters, scrape_details=scrape_details
            )
            results.append(result)
        return results

    def _try_browser_fallback(self, query: WatchQuery) -> Optional[pd.DataFrame]:
        """Attempt to download via Playwright browser."""
        try:
            from .browser_downloader import RedfBrowserDownloader

            downloader = RedfBrowserDownloader(self.config)
            return asyncio.run(downloader.download_from_query(query))
        except ImportError:
            logger.error(
                "Playwright not installed. Install with: "
                "pip install playwright && playwright install chromium"
            )
            return None
        except Exception as e:
            logger.error("Browser fallback also failed: %s", e)
            return None

    def _save_results(self, result: PipelineResult) -> None:
        """Save pipeline results to output directory."""
        os.makedirs(self.config.output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_name = result.query_name.replace(" ", "_").replace("/", "_")

        # Save filtered CSV
        if not result.df_filtered.empty:
            csv_path = os.path.join(
                self.config.output_dir,
                f"filtered_{safe_name}_{timestamp}.csv",
            )
            result.df_filtered.to_csv(csv_path, index=False)
            logger.info("Saved filtered listings to %s", csv_path)

        # Save listing details as JSON
        if result.listing_details:
            details_path = os.path.join(
                self.config.output_dir,
                f"details_{safe_name}_{timestamp}.json",
            )
            details_data = [
                {
                    "url": d.url,
                    "description": d.description,
                    "image_urls": d.image_urls,
                }
                for d in result.listing_details
            ]
            with open(details_path, "w") as f:
                json.dump(details_data, f, indent=2)
            logger.info("Saved listing details to %s", details_path)


# --- Convenience filter builders ---

def price_filter(min_price: int = 0, max_price: int = 99_999_999):
    """Return a filter function that keeps listings within a price range."""
    def _filter(df: pd.DataFrame) -> pd.DataFrame:
        if "price" not in df.columns:
            return df
        return df[(df["price"] >= min_price) & (df["price"] <= max_price)]
    return _filter


def composite_filter(**kwargs):
    """Build a filter from keyword args.

    Supported kwargs:
        min_price, max_price, min_beds, max_beds, min_baths,
        min_sqft, max_sqft, min_year_built, max_dom,
        property_types (list of strings like ["Single Family Residential"])

    Returns:
        Filter function for use with pipeline.run_query().
    """
    def _filter(df: pd.DataFrame) -> pd.DataFrame:
        mask = pd.Series(True, index=df.index)

        if "min_price" in kwargs and "price" in df.columns:
            mask &= df["price"] >= kwargs["min_price"]
        if "max_price" in kwargs and "price" in df.columns:
            mask &= df["price"] <= kwargs["max_price"]
        if "min_beds" in kwargs and "beds" in df.columns:
            mask &= df["beds"] >= kwargs["min_beds"]
        if "max_beds" in kwargs and "beds" in df.columns:
            mask &= df["beds"] <= kwargs["max_beds"]
        if "min_baths" in kwargs and "baths" in df.columns:
            mask &= df["baths"] >= kwargs["min_baths"]
        if "min_sqft" in kwargs and "sqft" in df.columns:
            mask &= df["sqft"] >= kwargs["min_sqft"]
        if "max_sqft" in kwargs and "sqft" in df.columns:
            mask &= df["sqft"] <= kwargs["max_sqft"]
        if "min_year_built" in kwargs and "year_built" in df.columns:
            mask &= df["year_built"] >= kwargs["min_year_built"]
        if "max_dom" in kwargs and "dom" in df.columns:
            mask &= df["dom"] <= kwargs["max_dom"]
        if "property_types" in kwargs and "property_type" in df.columns:
            mask &= df["property_type"].isin(kwargs["property_types"])

        return df[mask]

    return _filter
