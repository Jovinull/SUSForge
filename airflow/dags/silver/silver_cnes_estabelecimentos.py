"""Silver • CNES Estabelecimentos — Parquet bronze → silver.estabelecimentos.

Fluxo (TaskFlow API, Airflow 3):

    ensure_schema  ──►  transform_and_load

Decisões:
    * Origem: leitura do manifest do Bronze CNES (``susforge.io.cnes``)
      — não dependemos de XCom da DAG Bronze; cada Silver pega o
      último Parquet disponível, identificando-o por ``_extraction_date``.
    * ``transform`` e ``load`` ficam na MESMA task: DataFrame Polars
      não é serializável via XCom; combinar evita re-trabalho e
      preserva atomicidade (validação Pandera → COPY em transação).
    * Idempotência: ``replace_partition`` faz
      ``DELETE WHERE _extraction_date = <data>`` antes do COPY,
      tudo em uma transação. Rodar 2× na mesma data → mesmo resultado.

Schedule ``@weekly`` — encaixa naturalmente após a Bronze CNES.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task

from susforge.config import get_settings
from susforge.io.cnes import latest_extraction_date, latest_parquet_path
from susforge.loaders.postgres import execute_ddl, replace_partition
from susforge.transformers.cnes import transform

logger = logging.getLogger(__name__)


def _sql_dir() -> Path:
    """Diretório dos DDLs Silver (montado em ``/opt/susforge/sql/``)."""
    return get_settings().project_root / "sql"


DEFAULT_ARGS = {
    "owner": "susforge",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="silver_cnes_estabelecimentos",
    description=(
        "Silver · CNES Estabelecimentos — limpeza, Pandera e COPY p/ Postgres"
    ),
    start_date=datetime(2026, 6, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["silver", "cnes", "assistencia-saude", "master-data"],
    doc_md=__doc__,
)
def silver_cnes_estabelecimentos() -> None:
    @task(task_id="ensure_schema")
    def ensure_schema() -> str:
        """Cria/garante ``silver.estabelecimentos`` (idempotente)."""
        ddl = _sql_dir() / "silver" / "010_create_estabelecimentos.sql"
        execute_ddl(ddl)
        return str(ddl)

    @task(task_id="transform_and_load")
    def transform_and_load(_ddl_marker: str) -> int:
        """Lê parquet bronze, valida Pandera e COPY para silver."""
        parquet = latest_parquet_path()
        extraction_date = latest_extraction_date()
        print(
            f"silver/cnes: parquet={parquet.name} extraction_date={extraction_date}"
        )

        df = transform(parquet, extraction_date)
        print(f"silver/cnes: validado {df.height} linhas × {df.width} colunas")

        n = replace_partition(
            df,
            schema="silver",
            table="estabelecimentos",
            partition_column="_extraction_date",
            partition_value=extraction_date,
        )
        print(f"silver/cnes: {n} linhas carregadas")
        return n

    transform_and_load(ensure_schema())


silver_cnes_estabelecimentos()
