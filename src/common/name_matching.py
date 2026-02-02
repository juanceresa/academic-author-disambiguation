"""Shared name normalization and matching utilities.

Consolidates duplicate name-handling logic from the OpenAlex, Scopus,
BigQuery, and Google Scholar modules into a single source of truth.
Handles Spanish naming conventions (paternal + maternal surnames).
"""

import re
import unicodedata

from fuzzywuzzy import fuzz
from unidecode import unidecode

# Words filtered out when comparing institution names
COMMON_INSTITUTION_WORDS = {
    "universidad", "university", "college", "institute", "instituto",
    "institut", "facultad", "escuela", "politecnica", "autonoma", "superior",
    "council",
}


def normalize_name(name: str) -> str:
    """Lowercase, strip accents, remove punctuation, collapse whitespace.

    Works for both Spanish and English names. Converts hyphens to spaces
    so 'acin-perez' matches 'acin perez'.
    """
    if not name or not isinstance(name, str):
        return ""
    norm = name.strip().lower()
    norm = unicodedata.normalize("NFKD", norm)
    norm = "".join(ch for ch in norm if not unicodedata.combining(ch))
    norm = re.sub(r"[-‐]", " ", norm)
    norm = "".join(ch for ch in norm if ch.isalnum() or ch.isspace())
    norm = re.sub(r"\s+", " ", norm)
    return norm.strip()


def name_to_bag(name: str) -> set[str]:
    """Split a normalized name into a set of tokens for bag-of-words matching."""
    return set(normalize_name(name).split())


def parse_spanish_name(full_name: str) -> tuple[list[str], str | None, str | None]:
    """Split a Spanish full name into (first_names, paternal_last, maternal_last).

    Example: 'Candida Acin Saiz' -> (['candida'], 'acin', 'saiz')
    If only two tokens, assumes first + single surname.
    """
    tokens = full_name.lower().split()
    if len(tokens) < 2:
        return tokens, None, None
    first_names = [tokens[0]]
    if len(tokens) == 2:
        return first_names, tokens[1], None
    return first_names, tokens[-2], tokens[-1]


def tokenize_name_fields(*fields: str) -> list[str]:
    """Combine multiple name strings, normalize, and return token list.

    Example: tokenize_name_fields('Rebeca', 'acin-perez') -> ['rebeca', 'acin', 'perez']
    """
    combined = " ".join(fields).lower()
    combined = unidecode(combined)
    combined = combined.replace("-", " ")
    combined = re.sub(r"[^\w\s]", "", combined)
    return combined.split()


def name_tokens_exact_match(tokens_a: set[str], tokens_b: set[str]) -> bool:
    """Check if two sets of name tokens are identical."""
    return tokens_a == tokens_b


def fuzzy_name_match(name1: str, name2: str, threshold: int = 90) -> bool:
    """Return True if fuzzy token-set ratio meets the threshold."""
    return fuzz.token_set_ratio(normalize_name(name1), normalize_name(name2)) >= threshold


def fuzzy_match_score(
    inv_name: str, auth_name: str, alt_names: str | None = None
) -> int:
    """Compute best fuzzy score between an investigator name and author name(s).

    Checks both the primary display name and any comma-separated alternatives.
    """
    score_primary = fuzz.token_set_ratio(inv_name, auth_name)
    if alt_names:
        alt_list = [normalize_name(n) for n in str(alt_names).split(",")]
        score_alts = [fuzz.token_set_ratio(inv_name, alt) for alt in alt_list]
        return max(score_primary, max(score_alts)) if score_alts else score_primary
    return score_primary


def normalize_institution_name(name: str) -> list[str]:
    """Normalize an institution name, removing common filler words.

    Returns a list of meaningful tokens for comparison.
    """
    name = name.lower()
    name = re.sub(r"[-]", " ", name)
    name = re.sub(r"[^\w\s]", "", name)
    name = unidecode(name)
    tokens = name.split()
    return [t for t in tokens if t not in COMMON_INSTITUTION_WORDS]


def institution_match(institution: str, affiliation: str) -> bool:
    """Return True if any meaningful token from institution appears in affiliation."""
    inst_tokens = normalize_institution_name(institution)
    aff_tokens = normalize_institution_name(affiliation)
    if not inst_tokens:
        return False
    return any(t in aff_tokens for t in inst_tokens)


def strip_accents(text: str) -> str:
    """Convert accented characters to unaccented equivalents."""
    normalized = unicodedata.normalize("NFD", text)
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
