"""DOI-based and API-based investigator matching against OpenAlex via BigQuery.

Two-stage matching pipeline:
  Stage 1: Match investigators to OpenAlex authors via DOI lookups in BigQuery,
           using fuzzy name matching to verify candidates.
  Stage 2: For investigators still missing Alex_IDs, search the OpenAlex API
           directly by name with fuzzy matching.
"""

import logging
import time

import pandas as pd
import requests
from google.cloud import bigquery

from src.common.name_matching import fuzzy_match_score, normalize_name
from src.config import settings

logger = logging.getLogger(__name__)

FUZZY_THRESHOLD = 90


def _get_client() -> bigquery.Client:
    return bigquery.Client(project=settings.bigquery_project)


def stage1_doi_match(client: bigquery.Client, table: str = "userdb_JC.investigadores") -> pd.DataFrame:
    """Match investigators to OpenAlex authors by DOI position with fuzzy name verification.

    Queries BigQuery for works matching investigator DOIs, retrieves authorship
    info, and selects the best fuzzy-matched author per investigator.
    """
    # Load investigators
    query_inv = f"SELECT ID, DOI AS doi, Nombre_apellidos, Alex_ID, Author_Pos FROM {table}"
    df_inv = client.query(query_inv).to_dataframe()

    # Find work IDs from OpenAlex by DOI
    doi_list = df_inv["doi"].dropna().unique().tolist()
    query_works = """
    SELECT w.id AS work_id, w.doi
    FROM insyspo.publicdb_openalex_2024_10_rm.works w
    WHERE w.doi IN UNNEST(@doi_list)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("doi_list", "STRING", doi_list)]
    )
    df_works = client.query(query_works, job_config=job_config).to_dataframe()
    df_inv = df_inv.merge(df_works, on="doi", how="left")

    # Get authorship info
    df_inv["work_id"] = pd.to_numeric(df_inv["work_id"], errors="coerce")
    work_id_list = df_inv["work_id"].dropna().astype(int).tolist()

    if work_id_list:
        query_auth = """
        SELECT wa.work_id, wa.author_position, wa.author_id
        FROM insyspo.publicdb_openalex_2024_10_rm.works_authorships wa
        WHERE wa.work_id IN UNNEST(@work_id_list)
        """
        job_config_auth = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("work_id_list", "INT64", work_id_list)]
        )
        df_authorships = client.query(query_auth, job_config=job_config_auth).to_dataframe()
    else:
        df_authorships = pd.DataFrame()
    df_inv = df_inv.merge(df_authorships, on="work_id", how="left")

    # Get author details including alternative names
    df_inv["author_id"] = pd.to_numeric(df_inv["author_id"], errors="coerce")
    author_id_list = df_inv["author_id"].dropna().astype(int).tolist()

    if author_id_list:
        query_authors = """
        SELECT a.id AS author_id, a.display_name
        FROM insyspo.publicdb_openalex_2024_10_rm.authors a
        WHERE a.id IN UNNEST(@author_id_list)
        """
        job_config_authors = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("author_id_list", "INT64", author_id_list)]
        )
        df_authors = client.query(query_authors, job_config=job_config_authors).to_dataframe()

        query_alts = """
        SELECT dna.author_id, STRING_AGG(dna.display_name_alternative, ', ') AS display_name_alternatives
        FROM insyspo.publicdb_openalex_2024_10_rm.authors_display_name_alternatives dna
        WHERE dna.author_id IN UNNEST(@author_id_list)
        GROUP BY dna.author_id
        """
        df_alts = client.query(query_alts, job_config=job_config_authors).to_dataframe()
        df_authors = df_authors.merge(df_alts, on="author_id", how="left")
    else:
        df_authors = pd.DataFrame()
    df_inv = df_inv.merge(df_authors, on="author_id", how="left")

    # Fuzzy matching to select best candidate per investigator
    df_inv["normalized_inv_name"] = df_inv["Nombre_apellidos"].apply(normalize_name)
    df_inv["fuzzy_score"] = df_inv.apply(
        lambda row: fuzzy_match_score(
            row["normalized_inv_name"],
            normalize_name(row.get("display_name", "")),
            row.get("display_name_alternatives", ""),
        ),
        axis=1,
    )
    df_best = df_inv.loc[df_inv.groupby("ID")["fuzzy_score"].idxmax()].copy()
    df_best.loc[
        df_best["fuzzy_score"] < FUZZY_THRESHOLD,
        ["author_id", "author_position", "display_name", "display_name_alternatives"],
    ] = None
    df_best["Alex_ID"] = df_best["author_id"].apply(
        lambda x: f"https://openalex.org/A{int(x)}" if pd.notna(x) else None
    )
    df_best["Author_Pos"] = df_best["author_position"]
    return df_best.sort_values("ID").drop_duplicates(subset=["ID"], keep="first")


def stage2_api_match(
    client: bigquery.Client, table: str = "userdb_JC.investigadores"
) -> pd.DataFrame:
    """Search OpenAlex API directly for investigators missing Alex_IDs.

    Queries the API by name, uses fuzzy matching to verify, then merges
    results back into the full investigators table.
    """
    query_missing = f"SELECT * FROM {table} WHERE Alex_id IS NULL"
    df_missing = client.query(query_missing).to_dataframe()
    df_missing["normalized_name"] = df_missing["Nombre_apellidos"].apply(normalize_name)

    api_results = []
    for i, norm_name in enumerate(df_missing["normalized_name"]):
        url = f"https://api.openalex.org/authors?filter=display_name.search:{norm_name.replace(' ', '%20')}&mailto=jcere@umich.edu"
        try:
            response = requests.get(url).json()
            if response.get("meta", {}).get("count", 0) > 0:
                candidate = response["results"][0]
                candidate_name = candidate.get("display_name", "")
                from fuzzywuzzy import fuzz
                score = fuzz.token_set_ratio(norm_name, normalize_name(candidate_name))
                if score >= FUZZY_THRESHOLD:
                    api_results.append((candidate["id"], candidate_name))
                else:
                    api_results.append((None, None))
            else:
                api_results.append((None, None))
        except Exception as e:
            logger.error("Error fetching OpenAlex for '%s': %s", norm_name, e)
            api_results.append((None, None))

        if (i + 1) % 1000 == 0:
            logger.info("Processed %d API calls...", i + 1)
        time.sleep(0.1)

    if api_results:
        df_missing["Alex_id"], df_missing["matched_name"] = zip(*api_results)
    else:
        df_missing["Alex_id"] = None
        df_missing["matched_name"] = None

    # Merge back into full table
    df_full = client.query(f"SELECT * FROM {table}").to_dataframe()
    df_full = df_full.merge(df_missing[["ID", "Alex_id"]], on="ID", how="left", suffixes=("", "_new"))
    df_full["Alex_id"] = df_full["Alex_id"].combine_first(df_full["Alex_id_new"])
    df_full.drop(columns=["Alex_id_new"], inplace=True)
    return df_full.sort_values("ID")
