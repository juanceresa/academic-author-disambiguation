"""Researcher publication discovery via CrossRef DOI search.

Processes a spreadsheet of researchers, searching CrossRef for matching
publications when no Google Scholar profile or DOI is already known.
Saves results incrementally to guard against interruptions.

Note: Google Scholar direct scraping was attempted via Oxylabs Web Unblocker
proxy but consistently hit rate-limiting walls. The pipeline now relies
on CrossRef as the primary DOI discovery source.
"""

import logging
import time
from pathlib import Path

import pandas as pd

from src.google_scholar.search import search_doi

logger = logging.getLogger(__name__)

DEFAULT_INPUT = "investigadores_depurados_con_gs_man-checks.xlsx"
SAVE_INTERVAL = 10


def run_doi_search(input_path: str = DEFAULT_INPUT) -> None:
    """Process researcher spreadsheet and search CrossRef for DOIs.

    Skips rows that already have a Google Scholar profile or DOI.
    Saves progress every SAVE_INTERVAL rows.
    """
    path = Path(input_path)
    if not path.exists():
        logger.error("Input file not found: %s", input_path)
        return

    df = pd.read_excel(path)
    scholar_column = "GS"

    for col in ["DOI", "DOI_Status"]:
        if col not in df.columns:
            df[col] = None

    rows_processed = 0
    for index, row in df.iterrows():
        if not pd.isna(row.get(scholar_column)) or not pd.isna(row.get("DOI")):
            continue

        name_query = str(row["Nombre y apellidos"])
        scholarship_year = int(row["Año beca"]) if not pd.isna(row.get("Año beca")) else 0
        institution = str(row["Trabajo.institucion"]) if not pd.isna(row.get("Trabajo.institucion")) else ""
        given_name = str(row["Nombre"]).strip() if not pd.isna(row.get("Nombre")) else ""

        results = search_doi(name_query, given_name, scholarship_year, institution)

        if results:
            result = results[0]
            df.at[index, "DOI"] = result["doi"]
            df.at[index, "DOI_Status"] = str(result["score"])
            logger.info("Found DOI for %s: %s (score: %s)", name_query, result["doi"], result["score"])
        else:
            logger.info("No DOI found for %s", name_query)
            df.at[index, "DOI"] = None
            df.at[index, "DOI_Status"] = None

        rows_processed += 1
        if rows_processed % SAVE_INTERVAL == 0:
            df.to_excel(path, index=False)
            logger.info("Progress saved at row %d", index)

        time.sleep(2)

    df.to_excel(path, index=False)
    logger.info("DOI search complete. Results saved to %s", input_path)
