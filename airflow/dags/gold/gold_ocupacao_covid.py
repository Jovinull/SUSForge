"""Gold • Ocupação COVID-19 — fato mensal por estabelecimento.

Modelagem Kimball — fato puro agregado:

    gold.dim_estabelecimento (Master Data, já em produção)
            ▲
            │ co_cnes
            │
    gold.fato_ocupacao_covid (cnes × ano_mes, agregação SUM/AVG/MAX)

ELT puro: o SQL em ``sql/gold/022_fato_ocupacao_covid.sql`` faz
``TRUNCATE`` + ``INSERT ... SELECT`` lendo de ``silver.ocupacao_covid``.

Comparabilidade com ``gold.fato_leitos_anual``:
    * Mesmo ``co_cnes`` (7 dígitos zero-padded).
    * Mesmo formato de competência (``ano_mes = "YYYYMM"`` ≡ ``comp``).
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
    dag_id="gold_ocupacao_covid",
    description=(
        "Gold · Fato mensal de ocupação COVID-19 por estabelecimento"
    ),
    start_date=datetime(2026, 6, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["gold", "kimball", "covid", "assistencia-saude"],
    doc_md=__doc__,
)
def gold_ocupacao_covid() -> None:
    @task(task_id="build_fato_ocupacao_covid")
    def build_fato() -> int:
        sql = _sql_dir() / "022_fato_ocupacao_covid.sql"
        rows = execute_ddl(sql)
        total = count_rows("gold", "fato_ocupacao_covid")
        print(
            f"gold.fato_ocupacao_covid: {total:,} linhas "
            f"(insert rowcount={rows:,})"
        )
        return total

    build_fato()


gold_ocupacao_covid()
