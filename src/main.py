thonimport argparse
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from extractors.yellowpages_parser import YellowPagesScraper
from outputs import exporters

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("main")

def load_settings(settings_path: Optional[str]) -> Dict[str, Any]:
    """
    Load settings from a JSON file if provided.
    Falls back to sensible defaults if file is missing or invalid.
    """
    defaults: Dict[str, Any] = {
        "base_url": "https://www.yellowpages.com",
        "user_agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/130.0 Safari/537.36"
        ),
        "delay_seconds_min": 1.0,
        "delay_seconds_max": 3.0,
        "max_retries": 3,
        "proxies": None,
        "timeout_seconds": 20,
        "output_directory": "data",
    }

    if not settings_path:
        logger.info("No settings file provided, using defaults.")
        return defaults

    try:
        with open(settings_path, "r", encoding="utf-8") as f:
            file_settings = json.load(f)
        logger.info("Loaded settings from %s", settings_path)
        merged = {**defaults, **file_settings}
        return merged
    except FileNotFoundError:
        logger.warning("Settings file %s not found, using defaults.", settings_path)
    except json.JSONDecodeError as exc:
        logger.warning(
            "Settings file %s is invalid JSON (%s), using defaults.",
            settings_path,
            exc,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to load settings from %s (%s), using defaults.", settings_path, exc)

    return defaults

def load_batch_inputs(input_path: str) -> List[Dict[str, Any]]:
    """
    Load a batch configuration JSON with a top-level 'searches' array.
    """
    try:
        with open(input_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        searches = data.get("searches", [])
        if not isinstance(searches, list):
            raise ValueError("'searches' must be a list.")
        logger.info("Loaded %d search definitions from %s", len(searches), input_path)
        return searches
    except FileNotFoundError:
        logger.error("Input config file %s not found.", input_path)
        raise
    except json.JSONDecodeError as exc:
        logger.error("Input config file %s is invalid JSON: %s", input_path, exc)
        raise
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to load input config from %s: %s", input_path, exc)
        raise

def ensure_output_dir(path: str) -> None:
    directory = os.path.dirname(path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Real Yellow Pages Lead Generator (USA version)",
    )

    # Single-run parameters
    parser.add_argument("--keyword", help="Business category or keyword, e.g. 'plumbers'")
    parser.add_argument(
        "--location",
        help="City and state, e.g. 'Los Angeles, CA'",
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=1,
        help="Number of pages to scrape for a single run (default: 1).",
    )

    # Batch mode
    parser.add_argument(
        "--input-config",
        help="Path to JSON file describing multiple searches (see data/inputs.sample.json).",
    )

    # Settings
    parser.add_argument(
        "--settings",
        help="Path to settings JSON file (see src/config/settings.example.json).",
    )

    # Output
    parser.add_argument(
        "--output",
        help=(
            "Output file path. If omitted, a timestamped JSON file will be created "
            "in the configured output directory."
        ),
    )
    parser.add_argument(
        "--format",
        choices=["json", "csv", "both"],
        default="json",
        help="Output format: json, csv, or both (default: json).",
    )

    args = parser.parse_args()
    return args

def build_default_output_path(
    output_dir: str,
    fmt: str,
    suffix: str = "",
) -> str:
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    suffix_part = f"_{suffix}" if suffix else ""
    name = f"yellowpages_leads{suffix_part}_{ts}.{fmt}"
    return os.path.join(output_dir, name)

def run_single(
    scraper: YellowPagesScraper,
    keyword: str,
    location: str,
    pages: int,
) -> List[Dict[str, Any]]:
    logger.info(
        "Running single search: keyword=%r, location=%r, pages=%d",
        keyword,
        location,
        pages,
    )
    leads = scraper.search(keyword=keyword, location=location, max_pages=pages)
    logger.info("Collected %d leads.", len(leads))
    return leads

def run_batch(
    scraper: YellowPagesScraper,
    batch_definitions: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    all_leads: List[Dict[str, Any]] = []
    for idx, definition in enumerate(batch_definitions, start=1):
        keyword = definition.get("keyword")
        location = definition.get("location")
        pages = int(definition.get("pages", 1))

        if not keyword or not location:
            logger.warning(
                "Skipping search index %d due to missing keyword or location: %s",
                idx,
                definition,
            )
            continue

        logger.info(
            "Batch search %d/%d: keyword=%r, location=%r, pages=%d",
            idx,
            len(batch_definitions),
            keyword,
            location,
            pages,
        )
        leads = scraper.search(keyword=keyword, location=location, max_pages=pages)
        # Optionally annotate with search parameters
        for lead in leads:
            lead.setdefault("_search_keyword", keyword)
            lead.setdefault("_search_location", location)
        all_leads.extend(leads)
        logger.info("Total leads accumulated so far: %d", len(all_leads))
    return all_leads

def main() -> None:
    args = parse_args()
    settings = load_settings(args.settings)

    scraper = YellowPagesScraper(
        base_url=settings["base_url"],
        user_agent=settings["user_agent"],
        delay_range=(
            float(settings.get("delay_seconds_min", 1.0)),
            float(settings.get("delay_seconds_max", 3.0)),
        ),
        max_retries=int(settings.get("max_retries", 3)),
        proxies=settings.get("proxies"),
        timeout=float(settings.get("timeout_seconds", 20)),
    )

    # Determine output paths
    output_dir = settings.get("output_directory", "data")
    if args.output:
        base_output_path = args.output
    else:
        base_output_path = build_default_output_path(output_dir, "json")

    # Collect leads
    if args.input_config:
        batch_definitions = load_batch_inputs(args.input_config)
        leads = run_batch(scraper, batch_definitions)
        suffix = "batch"
    else:
        if not args.keyword or not args.location:
            logger.error(
                "Either provide --input-config for batch mode or both --keyword and --location for a single search."
            )
            raise SystemExit(1)

        leads = run_single(
            scraper,
            keyword=args.keyword,
            location=args.location,
            pages=args.pages,
        )
        suffix = "single"

    if not leads:
        logger.warning("No leads were collected; nothing to export.")
        return

    # Decide actual output paths and export
    fmt = args.format
    exported_paths: List[str] = []

    if fmt in ("json", "both"):
        json_path = (
            base_output_path
            if base_output_path.lower().endswith(".json")
            else build_default_output_path(output_dir, "json", suffix)
        )
        ensure_output_dir(json_path)
        exporters.save_to_json(leads, json_path)
        exported_paths.append(json_path)

    if fmt in ("csv", "both"):
        csv_path = (
            base_output_path
            if base_output_path.lower().endswith(".csv")
            else build_default_output_path(output_dir, "csv", suffix)
        )
        ensure_output_dir(csv_path)
        exporters.save_to_csv(leads, csv_path)
        exported_paths.append(csv_path)

    for path in exported_paths:
        logger.info("Exported %d leads to %s", len(leads), path)

if __name__ == "__main__":
    main()