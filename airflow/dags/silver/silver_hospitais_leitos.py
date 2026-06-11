"""Silver • Hospitais e Leitos — Parquets bronze → silver.leitos_anual.

Fluxo (TaskFlow API, Airflow 3):

    ensure_schema  ──►  transform_and_load

Decisões:
    * Lê o manifest do Bronze (``susforge.io.hospitais_leitos``) para
      resolver a partição mais recente — não dependemos de XCom da
      DAG Bronze.
    * ``transform`` + ``load`` na MESMA task: DataFrame Polars não é
      serializável via XCom; combinar preserva atomicidade
      (validação Pandera → COPY em transação).
    * ``replace_partition`` faz ``DELETE WHERE _extraction_date = ?``
      antes do COPY; rodar 2× na mesma data → mesmo resultado.
    * Anos com Bronze corrompido (drift de separador) são reportados
      em ``processed_years`` / ``skipped_years`` para auditoria.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task

from susforge.config import get_settings
from susforge.io.hospitais_leitos import latest_extraction_date
from susforge.loaders.postgres import execute_ddl, replace_partition
from susforge.transformers.leitos import transform

logger = logging.getLogger(__name__)


def _sql_dir() -> Path:
    return get_settings().project_root / "sql"


DEFAULT_ARGS = {
    "owner": "susforge",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="silver_hospitais_leitos",
    description=(
        "Silver · Hospitais e Leitos — consolida 20 anos, Pandera, COPY"
    ),
    start_date=datetime(2026, 6, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["silver", "hospitais-leitos", "assistencia-saude", "fato"],
    doc_md=__doc__,
)
def silver_hospitais_leitos() -> None:
    @task(task_id="ensure_schema")
    def ensure_schema() -> str:
        ddl = _sql_dir() / "silver" / "011_create_leitos_anual.sql"
        execute_ddl(ddl)
        return str(ddl)

    @task(task_id="transform_and_load")
    def transform_and_load(_ddl_marker: str) -> dict[str, object]:
        extraction_date = latest_extraction_date()
        print(f"silver/leitos: extraction_date={extraction_date}")

        result = transform(extraction_date)
        print(
            f"silver/leitos: anos processados={result.processed_years} "
            f"pulados={result.skipped_years} | "
            f"{result.df.height:,} linhas × {result.df.width} cols"
        )

        n = replace_partition(
            result.df,
            schema="silver",
            table="leitos_anual",
            partition_column="_extraction_date",
            partition_value=extraction_date,
        )
        print(f"silver/leitos: {n:,} linhas carregadas em silver.leitos_anual")
        return {
            "rows_loaded": n,
            "processed_years": result.processed_years,
            "skipped_years": result.skipped_years,
        }

    transform_and_load(ensure_schema())


silver_hospitais_leitos()
