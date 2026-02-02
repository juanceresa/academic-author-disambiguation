"""Cross-database matching and result compilation via BigQuery.

Matches Scopus publication DOIs to OpenAlex work/author IDs, and
compiles aggregated results across all data sources.
"""

import glob
import logging
import os

import pandas as pd
from google.cloud import bigquery

from src.config import settings

logger = logging.getLogger(__name__)


def _get_client() -> bigquery.Client:
    return bigquery.Client(project=settings.bigquery_project)


def match_scopus_to_openalex(
    client: bigquery.Client | None = None,
    excel_dir: str = "publicaciones_scopus_excel",
) -> pd.DataFrame:
    """Match Scopus researcher publications to OpenAlex profiles via DOI.

    Loads Scopus publication Excel files, queries BigQuery for matching
    OpenAlex work IDs and authorship data, and returns merged results.
    """
    if client is None:
        client = _get_client()

    # Load all Scopus Excel files
    excel_files = glob.glob(os.path.join(excel_dir, "*.xlsx"))
    dfs = [pd.read_excel(f) for f in excel_files]
    df_inv = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    if df_inv.empty:
        logger.warning("No Scopus Excel files found in %s", excel_dir)
        return df_inv

    # Query OpenAlex works by DOI
    doi_list = df_inv["prism:doi"].dropna().str.strip().unique().tolist()
    query_works = """
    SELECT w.id AS work_id, w.doi
    FROM insyspo.publicdb_openalex_2025_03_rm.works w
    WHERE w.doi IN UNNEST(@doi_list)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("doi_list", "STRING", doi_list)]
    )
    df_works = client.query(query_works, job_config=job_config).to_dataframe()

    df_inv = df_inv.rename(columns={"prism:doi": "doi"})
    df_inv = df_inv.merge(df_works, on="doi", how="left")

    # Get authorship info
    df_inv["work_id"] = pd.to_numeric(df_inv["work_id"], errors="coerce")
    work_id_list = df_inv["work_id"].dropna().astype(int).tolist()

    if work_id_list:
        query_auth = """
        SELECT wa.work_id, wa.author_position, wa.author_id
        FROM insyspo.publicdb_openalex_2025_03_rm.works_authorships wa
        WHERE wa.work_id IN UNNEST(@work_id_list)
        """
        job_config_auth = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("work_id_list", "INT64", work_id_list)]
        )
        df_authorships = client.query(query_auth, job_config=job_config_auth).to_dataframe()
    else:
        df_authorships = pd.DataFrame()
    df_inv = df_inv.merge(df_authorships, on="work_id", how="left")

    # Get author details
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
        df_inv = df_inv.merge(df_authors, on="author_id", how="left")

    return df_inv


def compile_results(client: bigquery.Client | None = None) -> pd.DataFrame:
    """Compile and aggregate results from Scopus and OpenAlex matching tables.

    Aggregates candidate profiles per researcher (unique Alex IDs, total works
    and citations) and merges with Scopus metadata.
    """
    if client is None:
        client = _get_client()

    query_scopus = """
    SELECT
        ID, Nombre, `Apellido 1` AS Apellido1, `Apellido 2` AS Apellido2,
        `Nombre y apellidos` AS NombreYApellidos,
        `Trabajo_institucion` AS TrabajoInstitucion,
        `Año beca` AS AnioBeca, pais, GS, Scopus_ID
    FROM userdb_JC.scopus_table
    """
    df_scopus = client.query(query_scopus).to_dataframe()

    query_inv = """
    SELECT ID, candidate_alex_id, candidate_orc_id,
           candidate_works_count, candidate_cited_by_count
    FROM userdb_JC.investigadores_alexapi_3
    """
    df_inv = client.query(query_inv).to_dataframe()

    df_agg = df_inv.groupby("ID").agg({
        "candidate_alex_id": lambda x: list(set(x.dropna())),
        "candidate_orc_id": "first",
        "candidate_works_count": "sum",
        "candidate_cited_by_count": "sum",
    }).reset_index()

    return pd.merge(df_scopus, df_agg, on="ID", how="left")
