"""
Configuration for Redfin CSV scraper pipeline.

Watch queries encode the region_id, region_type, and filter params
captured from Redfin's "Download All" network request. To add a new
watch query:

1. Go to redfin.com and set your search filters
2. Open DevTools > Network tab
3. Click "Download All" on the results page
4. Copy the full query string from the gis-csv request
5. Store the params as a WatchQuery below
"""

import json
import os
from dataclasses import dataclass, field
from typing import Optional


REDFIN_CSV_ENDPOINT = "https://www.redfin.com/stingray/api/gis-csv"

# Region type codes (from docs/REDFIN.md)
REGION_TYPE_NEIGHBORHOOD = 1
REGION_TYPE_ZIP = 2
REGION_TYPE_COUNTY = 5
REGION_TYPE_CITY = 6

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.redfin.com/",
    "Accept": "text/csv,application/csv,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}


@dataclass
class WatchQuery:
    """A saved Redfin search that can be polled for CSV data.

    Construct from params grabbed from the network tab, or build
    programmatically with the helper kwargs.
    """

    name: str
    region_id: int
    region_type: int = REGION_TYPE_CITY

    # Filters — all optional, only included in the request when set
    min_price: Optional[int] = None
    max_price: Optional[int] = None
    min_num_beds: Optional[int] = None
    max_num_beds: Optional[int] = None
    min_num_baths: Optional[float] = None
    max_num_baths: Optional[float] = None
    min_sqft: Optional[int] = None
    max_sqft: Optional[int] = None
    min_lot_size: Optional[int] = None
    max_lot_size: Optional[int] = None
    min_year_built: Optional[int] = None
    max_year_built: Optional[int] = None
    min_stories: Optional[int] = None
    max_stories: Optional[int] = None
    property_type: Optional[int] = None  # 1=House,2=Condo,3=TH,4=MF,5=Land
    status: int = 1  # 1=active, 130=pending, 131=both
    num_homes: int = 350
    garage: Optional[bool] = None
    min_parking: Optional[int] = None
    hoa: Optional[int] = None
    time_on_market_range: Optional[str] = None

    # Raw override dict — merged last so you can pass anything Redfin accepts
    extra_params: dict = field(default_factory=dict)

    def to_params(self) -> dict:
        """Build the query-string params dict for the gis-csv endpoint."""
        params = {
            "al": 1,
            "sp": "true",
            "status": self.status,
            "num_homes": self.num_homes,
            "region_id": self.region_id,
            "region_type": self.region_type,
            "v": 8,
        }

        _optional_map = {
            "min_price": "min_price",
            "max_price": "max_price",
            "min_num_beds": "min_num_beds",
            "max_num_beds": "max_num_beds",
            "min_num_baths": "min_num_baths",
            "max_num_baths": "max_num_baths",
            "min_sqft": "min_listing_approx_size",
            "max_sqft": "max_listing_approx_size",
            "min_lot_size": "min_parcel_size",
            "max_lot_size": "max_parcel_size",
            "min_year_built": "min_year_built",
            "max_year_built": "max_year_built",
            "min_stories": "min_stories",
            "max_stories": "max_stories",
            "property_type": "uipt",
            "hoa": "hoa",
            "time_on_market_range": "time_on_market_range",
        }

        for attr, param_name in _optional_map.items():
            val = getattr(self, attr)
            if val is not None:
                params[param_name] = val

        if self.garage:
            params["gar"] = "true"
        if self.min_parking is not None:
            params["min_num_park"] = self.min_parking

        params.update(self.extra_params)
        return params


@dataclass
class ScraperConfig:
    """Top-level configuration for the scraper pipeline."""

    # Directory for cached CSV downloads and scraped data
    output_dir: str = os.path.join(
        os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
        ".generated",
        "scraper",
    )

    # Rate limiting
    listing_request_delay_min: float = 2.0  # seconds between listing requests
    listing_request_delay_max: float = 4.0  # random jitter upper bound

    # Playwright settings
    headless: bool = True
    browser_timeout: int = 30_000  # ms

    # Watch queries to poll
    watch_queries: list = field(default_factory=list)

    def save(self, path: str) -> None:
        """Persist config to JSON."""
        data = {
            "output_dir": self.output_dir,
            "listing_request_delay_min": self.listing_request_delay_min,
            "listing_request_delay_max": self.listing_request_delay_max,
            "headless": self.headless,
            "browser_timeout": self.browser_timeout,
            "watch_queries": [
                {
                    "name": q.name,
                    "region_id": q.region_id,
                    "region_type": q.region_type,
                    "min_price": q.min_price,
                    "max_price": q.max_price,
                    "min_num_beds": q.min_num_beds,
                    "max_num_beds": q.max_num_beds,
                    "min_num_baths": q.min_num_baths,
                    "max_num_baths": q.max_num_baths,
                    "min_sqft": q.min_sqft,
                    "max_sqft": q.max_sqft,
                    "min_lot_size": q.min_lot_size,
                    "max_lot_size": q.max_lot_size,
                    "min_year_built": q.min_year_built,
                    "max_year_built": q.max_year_built,
                    "min_stories": q.min_stories,
                    "max_stories": q.max_stories,
                    "property_type": q.property_type,
                    "status": q.status,
                    "num_homes": q.num_homes,
                    "garage": q.garage,
                    "min_parking": q.min_parking,
                    "hoa": q.hoa,
                    "time_on_market_range": q.time_on_market_range,
                    "extra_params": q.extra_params,
                }
                for q in self.watch_queries
            ],
        }
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f, indent=2)

    @classmethod
    def load(cls, path: str) -> "ScraperConfig":
        """Load config from JSON."""
        with open(path) as f:
            data = json.load(f)

        queries = [WatchQuery(**q) for q in data.pop("watch_queries", [])]
        return cls(watch_queries=queries, **data)
