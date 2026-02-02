"""Run the OpenAlex author matching pipeline.

Loads investigators from BigQuery, searches OpenAlex API with 3-tier
matching (exact name, institution, topic), and saves results.
"""

import logging
import sys

from google.cloud import bigquery

from src.config import settings
from src.openalex.query import run_openalex_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    client = bigquery.Client(project=settings.bigquery_project)
    query = "SELECT * FROM userdb_JC.investigadores_template"
    logger.info("Loading investigators from BigQuery...")
    df_main = client.query(query).to_dataframe()
    logger.info("Loaded %d investigators", len(df_main))

    df_result = run_openalex_pipeline(df_main)
    logger.info("Pipeline complete. %d result rows", len(df_result))

    # Save to BigQuery
    table_id = "userdb_JC.investigadores_alexapi_3"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(df_result, table_id, job_config=job_config).result()
    logger.info("Results saved to %s", table_id)


if __name__ == "__main__":
    main()
