"""Silver • Ocupação Hospitalar COVID-19 — Parquets bronze → silver.ocupacao_covid.

Fluxo (TaskFlow API, Airflow 3):

    ensure_schema  ──►  transform_and_load

Decisões:
    * Lê o manifest do Bronze (``susforge.io.covid_ocupacao``) para
      resolver a partição mais recente.
    * ``transform`` + ``load`` na MESMA task: DataFrame Polars não é
      serializável via XCom.
    * Dedup defensivo por ``id_registro`` (UUID do e-SUS Notifica).
    * ``replace_partition`` faz ``DELETE WHERE _extraction_date = ?``
      antes do COPY; rerun na mesma data → mesmo resultado.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task

from susforge.config import get_settings
from susforge.io.covid_ocupacao import latest_extraction_date
from susforge.loaders.postgres import execute_ddl, replace_partition
from susforge.transformers.covid_ocupacao import transform

logger = logging.getLogger(__name__)


def _sql_dir() -> Path:
    return get_settings().project_root / "sql"


DEFAULT_ARGS = {
    "owner": "susforge",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="silver_covid_ocupacao",
    description=(
        "Silver · Ocupação COVID-19 — consolida 2020-2022, Pandera, COPY"
    ),
    start_date=datetime(2026, 6, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["silver", "covid", "assistencia-saude", "fato"],
    doc_md=__doc__,
)
def silver_covid_ocupacao() -> None:
    @task(task_id="ensure_schema")
    def ensure_schema() -> str:
        ddl = _sql_dir() / "silver" / "012_create_ocupacao_covid.sql"
        execute_ddl(ddl)
        return str(ddl)

    @task(task_id="transform_and_load")
    def transform_and_load(_ddl_marker: str) -> dict[str, object]:
        extraction_date = latest_extraction_date()
        print(f"silver/covid: extraction_date={extraction_date}")

        result = transform(extraction_date)
        print(
            f"silver/covid: anos processados={result.processed_years} | "
            f"{result.df.height:,} linhas × {result.df.width} cols"
        )

        n = replace_partition(
            result.df,
            schema="silver",
            table="ocupacao_covid",
            partition_column="_extraction_date",
            partition_value=extraction_date,
        )
        print(f"silver/covid: {n:,} linhas carregadas em silver.ocupacao_covid")
        return {
            "rows_loaded": n,
            "processed_years": result.processed_years,
        }

    transform_and_load(ensure_schema())


silver_covid_ocupacao()
