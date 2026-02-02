"""OpenAlex author search with multi-tier matching strategy.

Implements a 3-tier candidate matching approach:
  1. Exact name or display_name_alternative match
  2. Institution affiliation match with bag-of-words name verification
  3. Topic/field overlap with existing matched candidates

Results are stored per researcher with multiple candidate profiles.
"""

import json
import logging
import re
from pathlib import Path

import pandas as pd
import requests
from google.cloud import bigquery

from src.common.name_matching import normalize_name
from src.config import settings

logger = logging.getLogger(__name__)

CACHE_FILE = Path("ins_id_cache.json")
OPENALEX_EMAIL = "jcere@umich.edu"


def load_institution_cache() -> dict[str, str]:
    """Load the institution ID cache from disk."""
    if CACHE_FILE.exists():
        return json.loads(CACHE_FILE.read_text())
    return {}


def save_institution_cache(cache: dict[str, str]) -> None:
    """Persist institution ID cache to disk."""
    CACHE_FILE.write_text(json.dumps(cache))


def resolve_institution_id(
    ins_name: str, cache: dict[str, str]
) -> str | None:
    """Look up an OpenAlex institution ID, using cache when available."""
    ins_clean = re.sub(r"\s*(\(.*?\)|,.*|/.*|-.*)", "", ins_name or "").strip()

    if ins_clean in cache:
        cached = cache[ins_clean]
        return None if cached == "MANUAL_REQUIRED" else cached

    url = f"https://api.openalex.org/institutions?search={ins_clean}&mailto={OPENALEX_EMAIL}"
    try:
        response = requests.get(url)
        if response.status_code != 200:
            logger.warning("Error fetching institution '%s': status %d", ins_clean, response.status_code)
            cache[ins_clean] = "MANUAL_REQUIRED"
            save_institution_cache(cache)
            return None
        data = response.json()
        if not data.get("results"):
            logger.info("No institution results for '%s'", ins_clean)
            cache[ins_clean] = "MANUAL_REQUIRED"
            save_institution_cache(cache)
            return None
        ins_id = data["results"][0]["id"]
        cache[ins_clean] = ins_id
        save_institution_cache(cache)
        return ins_id
    except Exception as e:
        logger.error("Error fetching institution '%s': %s", ins_clean, e)
        cache[ins_clean] = "MANUAL_REQUIRED"
        save_institution_cache(cache)
        return None


def bag_of_words_reject(
    candidate_tokens: list[str],
    full_name_tokens: list[str],
    candidate_name: str,
    full_name: str,
    reject_dict: dict[str, bool],
    candidate_alex_id: str,
) -> bool:
    """Check if candidate name tokens conflict with the expected full name tokens.

    Returns True if the candidate should be rejected.
    """
    if candidate_alex_id in reject_dict:
        return True

    for i, token in enumerate(candidate_tokens):
        if i >= len(full_name_tokens):
            logger.debug("SKIP %s — extra token '%s' not in %s", candidate_name, token, full_name)
            return True
        if len(token) < 2:
            if token != full_name_tokens[i][0]:
                logger.debug("SKIP %s — initial mismatch: expected '%s', got '%s'",
                           candidate_name, full_name_tokens[i][0], token)
                return True
        else:
            if token not in full_name_tokens:
                logger.debug("SKIP %s — token '%s' not in %s", candidate_name, token, full_name)
                return True
    return False


def gather_candidate_data(
    fs_id: str,
    candidate: dict,
    candidate_name: str,
    display_name_alternatives: list[str],
    candidate_dict: dict,
) -> None:
    """Extract and store candidate profile data."""
    other_ids = candidate.get("ids", {})
    topics = candidate.get("topics", [])

    candidate_tuple = (
        fs_id,
        candidate_name,
        display_name_alternatives,
        topics[0].get("field", {}).get("display_name", "") if topics else "",
        candidate.get("id", ""),
        ", ".join(v for k, v in other_ids.items() if k == "orcid"),
        ", ".join(v for k, v in other_ids.items() if k == "scopus"),
        candidate.get("works_count", ""),
        candidate.get("cited_by_count", ""),
        ", ".join(f"{k}: {v}" for k, v in candidate.get("summary_stats", {}).items()),
        None,  # relevance_score placeholder
        topics,
    )
    candidate_dict[fs_id].append(candidate_tuple)


def search_openalex(
    fs_id: str,
    q_name: str,
    full_name: str,
    pais: str,
    ins: str,
    candidate_dict: dict,
    reject_dict: dict,
    ins_cache: dict,
) -> None:
    """Query OpenAlex API and apply 3-tier matching for a single researcher."""
    ins_id = resolve_institution_id(ins, ins_cache)

    url = f"https://api.openalex.org/authors?search={q_name}&mailto={OPENALEX_EMAIL}"
    try:
        response = requests.get(url)
        if response.status_code != 200:
            logger.warning("Error fetching OpenAlex data for '%s': status %d", q_name, response.status_code)
            return
        data = response.json()
    except Exception as e:
        logger.error("Error fetching OpenAlex data for '%s': %s", q_name, e)
        return

    if fs_id not in candidate_dict:
        candidate_dict[fs_id] = []

    if data.get("meta", {}).get("count", 0) == 0:
        return

    results = data["results"]
    full_name_norm = normalize_name(full_name)

    # Case 1: Exact name or alternative match
    for candidate in results:
        candidate_name = candidate.get("display_name", "")
        candidate_name_norm = normalize_name(candidate_name)
        alternatives = candidate.get("display_name_alternatives", [])
        alternatives_norm = [normalize_name(alt) for alt in alternatives]

        if candidate_name_norm == full_name_norm or any(full_name_norm == alt for alt in alternatives_norm):
            candidate_alex_id = candidate.get("id", "")
            if not any(candidate_alex_id == ct[4] for ct in candidate_dict.get(fs_id, [])):
                gather_candidate_data(fs_id, candidate, candidate_name, alternatives, candidate_dict)
                logger.info("EXACT MATCH: %s -> %s", full_name, candidate_name)

    # Case 2: Institution match with bag-of-words name check
    for candidate in results:
        candidate_alex_id = candidate.get("id", "")
        candidate_name = candidate.get("display_name", "")
        full_name_tokens = normalize_name(full_name).split()
        candidate_tokens = normalize_name(candidate_name).split()

        if any(candidate_alex_id == ct[4] for ct in candidate_dict.get(fs_id, [])):
            continue
        if bag_of_words_reject(candidate_tokens, full_name_tokens, candidate_name, full_name, reject_dict, candidate_alex_id):
            reject_dict[candidate_alex_id] = True
            continue

        affiliations = candidate.get("affiliations", [])
        candidate_institutions = [
            aff.get("institution", {}).get("id", "")
            for aff in affiliations
            if aff.get("institution", {}).get("id")
        ]

        if ins_id and ins_id in candidate_institutions:
            logger.info("INSTITUTION MATCH: %s -> %s (%s)", full_name, candidate_name, ins)
            gather_candidate_data(fs_id, candidate, candidate_name, candidate.get("display_name_alternatives", []), candidate_dict)

    # Case 3: Topic matching against existing candidates
    for candidate in results:
        candidate_alex_id = candidate.get("id", "")
        candidate_name = candidate.get("display_name", "")
        full_name_tokens = sorted(normalize_name(full_name).split())
        candidate_tokens = sorted(normalize_name(candidate_name).split())

        if any(candidate_alex_id == ct[4] for ct in candidate_dict.get(fs_id, [])):
            continue
        if bag_of_words_reject(candidate_tokens, full_name_tokens, candidate_name, full_name, reject_dict, candidate_alex_id):
            reject_dict[candidate_alex_id] = True
            continue

        existing_candidates = candidate_dict.get(fs_id, [])
        potential_topics = candidate.get("topics", [])

        best_existing = max(
            (c for c in existing_candidates),
            key=lambda c: c[10] if c[10] is not None else 0,
            default=None,
        )
        if best_existing is None:
            continue

        existing_topics = best_existing[-1]
        for topic in existing_topics[:2]:
            matched = False
            for topic_cmp in potential_topics[:2]:
                if topic.get("field", {}).get("display_name", "") == topic_cmp.get("field", {}).get("display_name", ""):
                    logger.info("TOPIC MATCH: %s -> %s (field: %s)",
                              best_existing[1], candidate_name,
                              topic.get("field", {}).get("display_name", ""))
                    gather_candidate_data(fs_id, candidate, candidate_name, candidate.get("display_name_alternatives", []), candidate_dict)
                    matched = True
                    break
            if matched:
                break


def run_openalex_pipeline(df_main: pd.DataFrame) -> pd.DataFrame:
    """Process all researchers through OpenAlex matching pipeline.

    Args:
        df_main: DataFrame with columns ID, Nombre, Apellido_1, Nombre_apellidos, Pais, Trabajo_institucion

    Returns:
        DataFrame with matched candidate profiles merged with original data.
    """
    candidate_dict: dict[str, list] = {}
    reject_dict: dict[str, bool] = {}
    ins_cache = load_institution_cache()

    for _, row in df_main.iterrows():
        fs_id = row["ID"]
        query_name = f"{row['Nombre']} {row['Apellido_1']}"
        search_openalex(
            fs_id, query_name, row["Nombre_apellidos"],
            row["Pais"], row["Trabajo_institucion"],
            candidate_dict, reject_dict, ins_cache,
        )

    # Build results DataFrame
    candidate_rows = []
    for fs_id, candidates in candidate_dict.items():
        df_candidates = pd.DataFrame(
            [t[:-2] for t in candidates],
            columns=[
                "fs_id", "candidate_name", "candidate_display_name_alternatives",
                "candidate_field", "candidate_alex_id", "candidate_orc_id",
                "candidate_scopus_id", "candidate_works_count",
                "candidate_cited_by_count", "candidate_summary_stats",
            ],
        )
        candidate_rows.append(df_candidates)

    if not candidate_rows:
        return df_main

    df_candidates_final = pd.concat(candidate_rows, ignore_index=True)
    df_candidates_final = df_candidates_final.sort_values(by="fs_id").reset_index(drop=True)
    return df_main.merge(df_candidates_final, left_on="ID", right_on="fs_id", how="left")
