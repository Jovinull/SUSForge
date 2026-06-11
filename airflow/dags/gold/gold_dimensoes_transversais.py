"""Gold • Dimensões transversais — dim_tempo + dim_municipio.

Dimensões reusáveis por TODOS os fatos do projeto:

    * ``gold.dim_tempo``     — 1 linha por dia (2007 → 2030); habilita
      drill-down ano/trim/mês/sem e JOIN por ``data`` ou ``ano_mes``.
    * ``gold.dim_municipio`` — derivada das Silver; contém contagens
      de estabelecimentos e UBS por município (5k+ municípios).

ELT puro: dois SQLs idempotentes em ``sql/gold/``.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task

from susforge.config import get_settings
from susforge.loaders.postgres import count_rows, execute_ddl

logger = logging.getLogger(__name__)


def _sql_dir() -> Path:
    return get_settings().project_root / "sql" / "gold"


DEFAULT_ARGS = {
    "owner": "susforge",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="gold_dimensoes_transversais",
    description="Gold · dim_tempo (diária 2007-2030) + dim_municipio",
    start_date=datetime(2026, 6, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["gold", "kimball", "dimensoes", "transversal"],
    doc_md=__doc__,
)
def gold_dimensoes_transversais() -> None:
    @task(task_id="build_dim_tempo")
    def build_dim_tempo() -> int:
        execute_ddl(_sql_dir() / "023_dim_tempo.sql")
        total = count_rows("gold", "dim_tempo")
        print(f"gold.dim_tempo: {total:,} linhas")
        return total

    @task(task_id="build_dim_municipio")
    def build_dim_municipio(_t: int) -> int:
        execute_ddl(_sql_dir() / "024_dim_municipio.sql")
        total = count_rows("gold", "dim_municipio")
        print(f"gold.dim_municipio: {total:,} linhas")
        return total

    build_dim_municipio(build_dim_tempo())


gold_dimensoes_transversais()
