"""Run the Scopus author search and ID matching pipeline."""

import argparse
import logging

from src.scopus.id_match import process_researcher_files
from src.scopus.query import _get_els_client, get_author_data, search_for_author

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Scopus author matching pipeline")
    parser.add_argument("--clear-dir", action="store_true", help="Clear output directory before running")
    parser.add_argument("--mode", choices=["search", "id-match"], default="id-match",
                       help="search: query Scopus for author profiles; id-match: map Scopus DOIs to OpenAlex IDs")
    args = parser.parse_args()

    if args.mode == "id-match":
        logger.info("Running Scopus -> OpenAlex ID matching...")
        process_researcher_files(clear_dir=args.clear_dir)
    else:
        logger.info("Scopus author search mode not yet wired as standalone CLI")


if __name__ == "__main__":
    main()
