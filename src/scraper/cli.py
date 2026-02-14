#!/usr/bin/env python3
"""
CLI entry point for the Redfin CSV scraper pipeline.

Usage examples:

    # Quick download — just grab the CSV for a region
    python -m scraper.cli download --region-id 11203 --region-type 6 --name "Miami"

    # Download with filters
    python -m scraper.cli download \
        --region-id 11203 --region-type 6 --name "Miami" \
        --min-price 400000 --max-price 800000 \
        --min-beds 3 --min-baths 2

    # Full pipeline — download, filter, scrape details
    python -m scraper.cli pipeline \
        --region-id 11203 --region-type 6 --name "Miami" \
        --min-price 400000 --max-price 800000 \
        --min-beds 3 --min-sqft 1500

    # Run from a saved config file
    python -m scraper.cli run-config --config scraper_config.json

    # Generate a config template
    python -m scraper.cli init-config --output scraper_config.json
"""

import argparse
import asyncio
import json
import logging
import sys

from .config import ScraperConfig, WatchQuery
from .csv_downloader import RedfCSVDownloader
from .listing_scraper import ListingScraper
from .pipeline import RedfinPipeline, composite_filter


def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def cmd_download(args):
    """Download CSV only (Step 1)."""
    query = WatchQuery(
        name=args.name,
        region_id=args.region_id,
        region_type=args.region_type,
        min_price=args.min_price,
        max_price=args.max_price,
        min_num_beds=args.min_beds,
        min_num_baths=args.min_baths,
        num_homes=args.num_homes,
    )

    config = ScraperConfig()
    if args.output_dir:
        config.output_dir = args.output_dir

    with RedfCSVDownloader(config) as downloader:
        df, path = downloader.download_and_save(query)
        print(f"Downloaded {len(df)} listings to {path}")
        if not df.empty:
            print(f"\nColumns: {', '.join(df.columns[:10])}...")
            print(f"\nFirst 5 rows:")
            print(df.head().to_string(index=False))


def cmd_pipeline(args):
    """Run full pipeline (Steps 1-4)."""
    query = WatchQuery(
        name=args.name,
        region_id=args.region_id,
        region_type=args.region_type,
        min_price=args.min_price,
        max_price=args.max_price,
        min_num_beds=args.min_beds,
        min_num_baths=args.min_baths,
        num_homes=args.num_homes,
    )

    config = ScraperConfig(watch_queries=[query])
    if args.output_dir:
        config.output_dir = args.output_dir

    # Build filters from CLI args
    filter_kwargs = {}
    if args.filter_min_price is not None:
        filter_kwargs["min_price"] = args.filter_min_price
    if args.filter_max_price is not None:
        filter_kwargs["max_price"] = args.filter_max_price
    if args.filter_min_beds is not None:
        filter_kwargs["min_beds"] = args.filter_min_beds
    if args.filter_min_sqft is not None:
        filter_kwargs["min_sqft"] = args.filter_min_sqft
    if args.filter_min_year is not None:
        filter_kwargs["min_year_built"] = args.filter_min_year
    if args.filter_max_dom is not None:
        filter_kwargs["max_dom"] = args.filter_max_dom

    filters = composite_filter(**filter_kwargs) if filter_kwargs else None

    with RedfinPipeline(config) as pipeline:
        result = pipeline.run_query(
            query,
            filters=filters,
            scrape_details=not args.no_scrape,
        )

    print(f"\nResults for '{result.query_name}':")
    print(f"  Raw listings:      {result.raw_count}")
    print(f"  After filtering:   {result.filtered_count}")
    print(f"  Enriched:          {result.enriched_count}")
    if result.error:
        print(f"  Error: {result.error}")


def cmd_run_config(args):
    """Run pipeline from a saved config file."""
    config = ScraperConfig.load(args.config)
    print(f"Loaded {len(config.watch_queries)} watch queries from {args.config}")

    with RedfinPipeline(config) as pipeline:
        results = pipeline.run_all(scrape_details=not args.no_scrape)

    for r in results:
        print(f"\n'{r.query_name}': {r.raw_count} raw -> "
              f"{r.filtered_count} filtered -> {r.enriched_count} enriched")


def cmd_init_config(args):
    """Generate a sample config file."""
    config = ScraperConfig(
        watch_queries=[
            WatchQuery(
                name="Miami",
                region_id=11203,
                region_type=6,
                min_price=400000,
                max_price=800000,
                min_num_beds=3,
                min_num_baths=2,
                num_homes=350,
            ),
            WatchQuery(
                name="Bothell WA",
                region_id=29439,
                region_type=6,
                min_price=500000,
                max_price=760000,
                min_num_beds=3,
                min_num_baths=2,
                property_type=1,
                garage=True,
                min_parking=2,
            ),
        ],
    )
    config.save(args.output)
    print(f"Sample config saved to {args.output}")
    print("Edit the watch queries and filters, then run with:")
    print(f"  python -m scraper.cli run-config --config {args.output}")


def main():
    parser = argparse.ArgumentParser(
        description="Redfin CSV scraper pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Enable debug logging")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- download ---
    dl = subparsers.add_parser("download", help="Download CSV only (Step 1)")
    dl.add_argument("--name", required=True, help="Name for this search")
    dl.add_argument("--region-id", type=int, required=True,
                    help="Redfin region ID")
    dl.add_argument("--region-type", type=int, default=6,
                    help="Region type (2=ZIP, 5=County, 6=City)")
    dl.add_argument("--min-price", type=int, default=None)
    dl.add_argument("--max-price", type=int, default=None)
    dl.add_argument("--min-beds", type=int, default=None)
    dl.add_argument("--min-baths", type=float, default=None)
    dl.add_argument("--num-homes", type=int, default=350)
    dl.add_argument("--output-dir", default=None)

    # --- pipeline ---
    pl = subparsers.add_parser("pipeline",
                                help="Full pipeline: download + filter + scrape")
    pl.add_argument("--name", required=True)
    pl.add_argument("--region-id", type=int, required=True)
    pl.add_argument("--region-type", type=int, default=6)
    pl.add_argument("--min-price", type=int, default=None)
    pl.add_argument("--max-price", type=int, default=None)
    pl.add_argument("--min-beds", type=int, default=None)
    pl.add_argument("--min-baths", type=float, default=None)
    pl.add_argument("--num-homes", type=int, default=350)
    pl.add_argument("--output-dir", default=None)
    pl.add_argument("--no-scrape", action="store_true",
                    help="Skip scraping individual listing pages")
    # Post-download filters (applied to the CSV data)
    pl.add_argument("--filter-min-price", type=int, default=None)
    pl.add_argument("--filter-max-price", type=int, default=None)
    pl.add_argument("--filter-min-beds", type=int, default=None)
    pl.add_argument("--filter-min-sqft", type=int, default=None)
    pl.add_argument("--filter-min-year", type=int, default=None)
    pl.add_argument("--filter-max-dom", type=int, default=None)

    # --- run-config ---
    rc = subparsers.add_parser("run-config",
                                help="Run pipeline from saved config file")
    rc.add_argument("--config", required=True, help="Path to config JSON")
    rc.add_argument("--no-scrape", action="store_true")

    # --- init-config ---
    ic = subparsers.add_parser("init-config",
                                help="Generate a sample config file")
    ic.add_argument("--output", default="scraper_config.json",
                    help="Output path for config file")

    args = parser.parse_args()
    setup_logging(args.verbose)

    commands = {
        "download": cmd_download,
        "pipeline": cmd_pipeline,
        "run-config": cmd_run_config,
        "init-config": cmd_init_config,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
