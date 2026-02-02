"""Full disambiguation pipeline — documents the execution order.

This script outlines the complete pipeline for matching Fulbright
scholars to their academic publications across multiple databases.
Each stage can also be run independently via its own script.

Pipeline stages:
  1. Scopus author search (scripts/run_scopus.py)
     Query Elsevier API for author profiles by name and affiliation.

  2. OpenAlex matching (scripts/run_openalex.py)
     Search OpenAlex API with 3-tier matching: exact name, institution,
     and topic overlap. Produces candidate profiles per researcher.

  3. Scopus -> OpenAlex ID mapping (scripts/run_scopus.py --mode id-match)
     Map Scopus publication DOIs to OpenAlex author IDs via author position.

  4. CrossRef DOI discovery (scripts/run_scholar.py)
     Search CrossRef for additional publications not found in other databases.

  5. BigQuery compilation
     Aggregate all results into unified researcher profiles.
"""

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("=== Academic Author Disambiguation Pipeline ===")
    logger.info("")
    logger.info("Run each stage independently:")
    logger.info("  1. python scripts/run_scopus.py --mode search")
    logger.info("  2. python scripts/run_openalex.py")
    logger.info("  3. python scripts/run_scopus.py --mode id-match")
    logger.info("  4. python scripts/run_scholar.py")
    logger.info("")
    logger.info("See README.md for full documentation.")


if __name__ == "__main__":
    main()
