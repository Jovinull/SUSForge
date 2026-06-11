"""Silver • UBS — Parquet bronze → silver.ubs.

Subset do CNES focado em Atenção Primária. ~48k unidades, 8 colunas.

Decisão específica: coordenadas vêm com **vírgula decimal BR**
(``"-43,9914089036"``) — o transformer normaliza para ponto antes do
cast Float.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task

from susforge.config import get_settings
from susforge.io.ubs import latest_extraction_date
from susforge.loaders.postgres import execute_ddl, replace_partition
from susforge.transformers.ubs import transform

logger = logging.getLogger(__name__)


def _sql_dir() -> Path:
    return get_settings().project_root / "sql"


DEFAULT_ARGS = {
    "owner": "susforge",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="silver_ubs",
    description="Silver · UBS — limpeza, validação Pandera, COPY para Postgres",
    start_date=datetime(2026, 6, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["silver", "ubs", "assistencia-saude", "atencao-primaria"],
    doc_md=__doc__,
)
def silver_ubs() -> None:
    @task(task_id="ensure_schema")
    def ensure_schema() -> str:
        ddl = _sql_dir() / "silver" / "013_create_ubs.sql"
        execute_ddl(ddl)
        return str(ddl)

    @task(task_id="transform_and_load")
    def transform_and_load(_ddl_marker: str) -> int:
        extraction_date = latest_extraction_date()
        df = transform(extraction_date)
        print(f"silver/ubs: {df.height:,} × {df.width} validado")
        n = replace_partition(
            df,
            schema="silver",
            table="ubs",
            partition_column="_extraction_date",
            partition_value=extraction_date,
        )
        print(f"silver/ubs: {n:,} linhas em silver.ubs")
        return n

    transform_and_load(ensure_schema())


silver_ubs()
