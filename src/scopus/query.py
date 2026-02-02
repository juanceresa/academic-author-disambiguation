"""Query Elsevier Scopus API for author profiles and publication data.

Uses the ElsaPy library to search by author name and affiliation,
retrieving identifiers, citation counts, and publication metadata.
"""

import json
import logging
import os
import re
from pathlib import Path

import pandas as pd
from elsapy.elsclient import ElsClient
from elsapy.elsprofile import ElsAuthor
from elsapy.elssearch import ElsSearch
from pandas import json_normalize
from unidecode import unidecode

from src.config import settings

logger = logging.getLogger(__name__)


def _get_els_client() -> ElsClient:
    """Initialize Elsevier API client with configured API key."""
    return ElsClient(settings.scopus_api_key)


def clean_query_value(text: str) -> str:
    """Remove parentheses and enclosed text from query strings."""
    return re.sub(r"\s*\(.*?\)", "", text).strip()


def clean_affiliation(ins: str) -> str:
    """Remove parenthetical text and text after commas from affiliation strings."""
    cleaned = re.sub(r"\(.*?\)", "", ins)
    if "," in cleaned:
        cleaned = cleaned.split(",")[0]
    return cleaned.strip()


def search_for_author(
    client: ElsClient, first: str, last1: str, last2: str, ins: str
) -> str | None:
    """Search Scopus for an author by name, falling back to affiliation filter.

    Args:
        client: ElsaPy client instance
        first: Author first name
        last1: Author first surname (paternal)
        last2: Author second surname (maternal)
        ins: Author institution/affiliation

    Returns:
        Scopus author ID if found, None otherwise.
    """
    first_clean = clean_query_value(first)
    last_clean = clean_query_value(f"{last1} {last2}")

    # Primary search: name only
    query = f"authlast({last_clean}) AND authfirst({first_clean})"
    logger.info("Primary search: %s", query)
    search = ElsSearch(query, "author")
    search.execute(client)
    if search.results:
        author_id = search.results[0].get("dc:identifier", "").split(":")[-1]
        if author_id:
            return author_id

    # Secondary search: add affiliation filter
    logger.info("Primary search failed for %s %s %s, trying with affiliation", first, last1, last2)
    affil_clean = clean_affiliation(ins)
    query2 = f'authlast({last_clean}) AND authfirst({first_clean}) AND AFFIL("{affil_clean}")'
    logger.info("Secondary search: %s", query2)
    search2 = ElsSearch(query2, "author")
    search2.execute(client)
    if search2.results:
        author_id = search2.results[0].get("dc:identifier", "").split(":")[-1]
        if author_id:
            return author_id

    logger.warning("Author search failed for %s %s %s", first, last1, last2)
    return None


def get_author_data(client: ElsClient, author_id: str) -> dict | None:
    """Retrieve full author profile from Scopus.

    Returns dict with identifier, names, document count, citations,
    and current affiliation. None if retrieval fails.
    """
    my_auth = ElsAuthor(uri=f"https://api.elsevier.com/content/author/author_id/{author_id}")
    if not my_auth.read(client):
        logger.warning("Failed to read author data for %s", author_id)
        return None

    logger.info("Retrieved author: %s", my_auth.full_name)
    df = json_normalize(my_auth.__dict__)
    selected_columns = [
        "_data.coredata.dc:identifier",
        "_data.coredata.prism:url",
        "_data.author-profile.preferred-name.given-name",
        "_data.author-profile.preferred-name.surname",
        "_data.coredata.document-count",
        "_data.coredata.cited-by-count",
        "_data.coredata.citation-count",
        "_data.author-profile.publication-range.@start",
        "_data.author-profile.affiliation-current.affiliation.ip-doc.afdispname",
    ]
    row = {}
    for col in selected_columns:
        row[col] = df[col].iloc[0] if col in df.columns else None

    try:
        if my_auth.read_docs(client):
            row["doc_count_retrieved"] = len(my_auth._doc_list) if my_auth._doc_list else 0
        else:
            row["doc_count_retrieved"] = 0
    except Exception as e:
        row["doc_count_retrieved"] = None
        row["doc_error"] = str(e)
    return row
