"""CrossRef-based DOI search for researcher publication matching.

Searches CrossRef for publications by author name, scoring results
based on name match quality, institutional affiliation, and publication
year proximity to scholarship date.

Note: Google Scholar direct scraping was attempted via Oxylabs proxy
but hit persistent rate-limiting walls. This module uses CrossRef
as the reliable alternative for DOI discovery.
"""

import logging
import re

from crossref.restful import Etiquette, Works
from unidecode import unidecode

from src.common.name_matching import (
    COMMON_INSTITUTION_WORDS,
    normalize_institution_name,
    parse_spanish_name,
    tokenize_name_fields,
)

logger = logging.getLogger(__name__)


def check_affiliation_or_publisher(item: dict, institution_name: str) -> bool:
    """Check if any author affiliation or publisher matches the institution."""
    from src.common.name_matching import institution_match

    for au in item.get("author", []):
        for aff in au.get("affiliation", []):
            if institution_match(institution_name, aff.get("name", "")):
                return True
    if institution_match(institution_name, item.get("publisher", "")):
        return True
    return False


def get_created_year(item: dict) -> int | None:
    """Extract publication year from CrossRef created date-parts."""
    date_parts = item.get("created", {}).get("date-parts", [])
    if date_parts and len(date_parts[0]) > 0:
        return date_parts[0][0]
    return None


def check_created_year_in_range(
    item: dict, scholarship_year: int, delta: int = 5
) -> bool:
    """Check if publication year is within +/- delta of scholarship year."""
    year = get_created_year(item)
    if year is None:
        return False
    return (scholarship_year - delta) <= year <= (scholarship_year + delta)


def compute_similarity_score(
    item: dict,
    full_name: str,
    institution_name: str,
    scholarship_year: int,
) -> int:
    """Score a CrossRef result based on name, affiliation, and year match.

    Scoring:
      +1 for perfect name token match (all query tokens in author tokens)
      +1 for institution/publisher affiliation match
      +1 for publication year within +/- 5 years of scholarship
    Returns -999 if no perfect name match found.
    """
    query_tokens = set(tokenize_name_fields(full_name))
    perfect_match = False
    for au in item.get("author", []):
        author_tokens = set(tokenize_name_fields(au.get("given", ""), au.get("family", "")))
        if query_tokens.issubset(author_tokens):
            perfect_match = True
            break
    if not perfect_match:
        return -999

    score = 1
    if check_affiliation_or_publisher(item, institution_name):
        score += 1
    if check_created_year_in_range(item, scholarship_year):
        score += 1
    return score


def search_doi(
    name_query: str,
    given_name: str,
    scholarship_year: int,
    institution_name: str,
) -> list[dict] | None:
    """Search CrossRef for a researcher's publications and return the best DOI match.

    Queries CrossRef by combined last names, scores each result using name,
    institution, and year signals. Returns at most one DOI:
      - If any result scores >= 2, returns the highest-scoring DOI
      - Otherwise falls back to exact token match
      - Returns None if no match found

    Args:
        name_query: Full name for parsing surnames
        given_name: First name for building full search name
        scholarship_year: Year of scholarship for proximity scoring
        institution_name: Institution for affiliation matching
    """
    _, apellido1, apellido2 = parse_spanish_name(name_query)
    last_name_query = apellido1 if apellido1 else ""
    if apellido2:
        last_name_query += " " + apellido2
    full_name = f"{given_name} {last_name_query}".strip()

    etiquette = Etiquette(
        "AcademicAuthorDisambiguation", "1.0",
        "https://github.com/juanceresa",
    )
    works = Works(etiquette=etiquette)

    logger.info("CrossRef search for author '%s'", last_name_query)
    results = works.query(author=last_name_query).sample(100)

    scored_items: list[tuple[int, dict]] = []
    exact_match_items: list[dict] = []

    for item in results:
        sc = compute_similarity_score(item, full_name, institution_name, scholarship_year)
        if sc > 0:
            scored_items.append((sc, item))

        # Check for exact token match as fallback
        for au in item.get("author", []):
            author_tokens = set(tokenize_name_fields(au.get("given", ""), au.get("family", "")))
            user_tokens = set(tokenize_name_fields(full_name))
            if author_tokens == user_tokens:
                exact_match_items.append(item)
                break

    # Return best scored result (threshold >= 2)
    scored_items.sort(key=lambda x: x[0], reverse=True)
    best_scored = [(sc, it) for sc, it in scored_items if sc >= 2]
    if best_scored:
        best_score, best_item = best_scored[0]
        doi = best_item.get("DOI")
        if doi:
            return [{"doi": f"https://doi.org/{doi}", "score": best_score}]

    # Fallback to exact name match
    if exact_match_items:
        doi = exact_match_items[0].get("DOI")
        if doi:
            return [{"doi": f"https://doi.org/{doi}", "score": 1}]

    return None
