"""Run the CrossRef DOI search pipeline for researcher publications."""

import argparse
import logging

from src.google_scholar.scrape import run_doi_search

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="CrossRef DOI discovery for researchers")
    parser.add_argument("--input", default="investigadores_depurados_con_gs_man-checks.xlsx",
                       help="Input Excel file with researcher data")
    args = parser.parse_args()

    logger.info("Starting CrossRef DOI search with input: %s", args.input)
    run_doi_search(args.input)


if __name__ == "__main__":
    main()
