"""Retrieve and validate publication records for matched OpenAlex authors.

Two retrieval modes:
  - Basic: Fetch works list for each candidate profile (from alex_documents_query)
  - Full validation: Cross-check author surnames, paginate all works (from open_alex_document_fill)
"""

import logging
import os
import time
from pathlib import Path

import pandas as pd
import requests
import unicodedata
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from zipfile import BadZipFile

logger = logging.getLogger(__name__)

OPENALEX_EMAIL = "jcere@umich.edu"
BASE_AUTHOR_URL = "https://api.openalex.org/authors/"

# HTTP session with retry logic for transient errors
_session = requests.Session()
_retry = Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504],
               allowed_methods=["HEAD", "GET", "OPTIONS"])
_session.mount("https://", HTTPAdapter(max_retries=_retry))
_session.mount("http://", HTTPAdapter(max_retries=_retry))


def _fetch_json(url: str, params: dict | None = None) -> dict:
    """Retrieve JSON with retry logic and SSL fallback."""
    try:
        response = _session.get(url, params=params, timeout=10)
        response.raise_for_status()
    except requests.exceptions.SSLError:
        response = _session.get(url, params=params, timeout=10, verify=False)
        response.raise_for_status()
    return response.json()


def _strip_accents(text: str) -> str:
    """Convert accented characters to unaccented equivalents."""
    normalized = unicodedata.normalize("NFD", text)
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def fetch_author_works_basic(alex_id: str, q_name: str) -> list[tuple]:
    """Fetch works for an author via the OpenAlex API (basic mode).

    Returns list of (title, doi, id, publication_year, type, cited_by_count) tuples.
    """
    if not alex_id:
        logger.info("No OpenAlex ID for '%s', skipping", q_name)
        return []

    api_id = alex_id.replace("https://openalex.org/", "https://api.openalex.org/authors/")
    url = f"{api_id}&mailto={OPENALEX_EMAIL}"

    try:
        response = requests.get(url)
        if response.status_code != 200:
            logger.warning("Error fetching profile for '%s': status %d", q_name, response.status_code)
            return []
        data = response.json()
        works_url = data["works_api_url"]
    except Exception as e:
        logger.error("Error fetching profile for '%s': %s", q_name, e)
        return []

    works_url = works_url.split('"')[1] if '"' in works_url else works_url
    try:
        response = requests.get(works_url)
        if response.status_code != 200:
            logger.warning("Error fetching works for '%s': status %d", q_name, response.status_code)
            return []
        data = response.json()
    except Exception as e:
        logger.error("Error fetching works for '%s': %s", q_name, e)
        return []

    works = []
    if data.get("meta", {}).get("count", 0) > 0:
        for work in data["results"]:
            works.append((
                work.get("title", ""),
                work.get("doi", ""),
                work.get("id", ""),
                work.get("publication_year", ""),
                work.get("type", ""),
                work.get("cited_by_count", 0),
            ))
    else:
        logger.info("No works found for '%s'", q_name)
    return works


def _get_all_works_paginated(works_api_url: str) -> list[dict]:
    """Fetch all works by following pagination links."""
    works = []
    url = works_api_url
    params: dict | None = {"per-page": 200}
    while url:
        page = _fetch_json(url, params=params)
        works.extend(page.get("results", []))
        url = page.get("meta", {}).get("next")
        time.sleep(0.1)
        params = None
    return works


def _read_raw(path: str) -> pd.DataFrame:
    """Read a file as Excel or CSV fallback, returning raw DataFrame."""
    ext = os.path.splitext(path)[1].lower()
    if ext in (".xlsx", ".xls"):
        engines = ["openpyxl"] if ext == ".xlsx" else []
        engines.append("xlrd")
        for engine in engines:
            try:
                return pd.read_excel(path, header=None, engine=engine)
            except (ValueError, BadZipFile, ImportError):
                continue
    try:
        return pd.read_csv(path, header=None)
    except Exception as e:
        raise ValueError(f"Cannot read file {path}: {e}")


def _read_with_dynamic_header(path: str, flag_col: str) -> pd.DataFrame | None:
    """Detect header row by searching for flag column, then re-read properly."""
    raw = _read_raw(path)
    header_row = None
    for idx, row in raw.iterrows():
        if row.astype(str).str.contains(flag_col, case=False, na=False).any():
            header_row = idx
            break
    if header_row is None:
        return None
    ext = os.path.splitext(path)[1].lower()
    if ext in (".xlsx", ".xls"):
        engine = "openpyxl" if ext == ".xlsx" else "xlrd"
        df = pd.read_excel(path, header=header_row, engine=engine)
    else:
        df = pd.read_csv(path, header=header_row)
    df.columns = df.columns.str.strip()
    return df


def process_validated_documents(input_dir: str, output_dir: str) -> None:
    """Full validation pipeline: verify author surnames, fetch all paginated works.

    For each researcher spreadsheet in input_dir:
      1. Find rows flagged as first author appearance
      2. Verify researcher surname appears in OpenAlex profile
      3. Retrieve all publications with pagination
      4. Write two-sheet Excel (metadata + works) to output_dir
    """
    flag = "unique_author_first_appearance"
    os.makedirs(output_dir, exist_ok=True)

    for fname in os.listdir(input_dir):
        if not fname.lower().endswith((".xlsx", ".xls", ".csv")):
            continue

        slug, _ = os.path.splitext(fname)
        parts = slug.split("_")
        name_parts = parts[:-1]
        raw_surnames = name_parts[-2:] if len(name_parts) > 2 else [name_parts[-1]]
        surnames = [_strip_accents(s).lower() for s in raw_surnames]

        out_path = os.path.join(output_dir, fname)
        if os.path.exists(out_path):
            logger.info("[Skip] %s already exists", fname)
            continue

        df = None
        try:
            df = _read_with_dynamic_header(os.path.join(input_dir, fname), flag)
        except ValueError as e:
            logger.warning("[%s] %s -> skipping", slug, e)
        if df is None or flag not in df.columns:
            logger.info("[%s] flag column not found -> skipping", slug)
            continue

        flagged = df.loc[
            (df[flag] == True) | (df[flag].astype(str).str.upper() == "TRUE")
        ]
        if flagged.empty:
            logger.info("[%s] no flagged rows -> skipping", slug)
            continue

        matched_profiles: list[str] = []
        all_publications: list[dict] = []

        for _, row in flagged.iterrows():
            author_col = next(
                (c for c in ["id", "openalex_author_id", "author_id"] if c in df.columns), None
            )
            author_id = row.get(author_col) if author_col else None
            if not author_id or pd.isna(author_id):
                continue

            profile = _fetch_json(BASE_AUTHOR_URL + str(author_id))
            display = profile.get("display_name", "")
            works_url = profile.get("works_api_url")

            norm = _strip_accents(display).lower()
            if not any(s in norm for s in surnames):
                logger.info("[%s] no surname match in '%s' -> skipping", slug, display)
                continue
            if display not in matched_profiles:
                matched_profiles.append(display)
            logger.info("[%s] matched profile '%s'", slug, display)

            pubs = _get_all_works_paginated(works_url)
            all_publications.extend(pubs)

        if not matched_profiles:
            continue

        details = pd.DataFrame([
            {
                "title": w.get("title"),
                "doi": w.get("doi"),
                "publication_year": w.get("publication_year"),
                "cited_by_count": w.get("cited_by_count"),
            }
            for w in all_publications
        ])

        summary = pd.DataFrame([{
            "display_name": ", ".join(matched_profiles),
            "works_count": len(details),
            "cited_by_count": details["cited_by_count"].fillna(0).sum(),
        }])

        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            summary.to_excel(writer, sheet_name="metadata", index=False)
            details.to_excel(writer, sheet_name="works", index=False)

        logger.info("[%s] wrote %d works", slug, len(details))

    logger.info("Document processing complete.")
