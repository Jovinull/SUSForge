"""Gold • Capacidade Hospitalar — Star Schema (CNES × Leitos).

Modelagem Kimball:

    gold.dim_estabelecimento (Master Data)
            ▲
            │ co_cnes
            │
    gold.fato_leitos_anual (granularidade: cnes × ano)

ELT puro: os dois SQLs em ``sql/gold/`` fazem ``TRUNCATE`` + ``INSERT
... SELECT`` lendo das tabelas Silver. A DAG só orquestra a execução
e reporta os volumes via ``count_rows``.

Sem foreign keys físicas: data warehouse prioriza performance de
recarga; o relacionamento é mantido por convenção (lpad 7-dígitos
em ambas as tabelas) e via índices.

Schedule ``@weekly`` — roda depois que Silver de CNES e Leitos
estiverem populadas para a partição mais recente.
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
    dag_id="gold_capacidade_hospitalar",
    description=(
        "Gold · Capacidade hospitalar — Star Schema CNES × Leitos (Kimball)"
    ),
    start_date=datetime(2026, 6, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["gold", "kimball", "star-schema", "assistencia-saude"],
    doc_md=__doc__,
)
def gold_capacidade_hospitalar() -> None:
    @task(task_id="ensure_dominios")
    def ensure_dominios() -> int:
        """Cria/recarga das tabelas de domínio curadas (decodificação)."""
        silver_dir = get_settings().project_root / "sql" / "silver"
        execute_ddl(silver_dir / "014_dominio_tp_unidade.sql")
        execute_ddl(silver_dir / "015_dominio_natureza_juridica.sql")
        tp = count_rows("silver", "dominio_tp_unidade")
        nj = count_rows("silver", "dominio_natureza_juridica")
        print(f"dominios: tp_unidade={tp} natureza_jur={nj}")
        return tp + nj

    @task(task_id="build_dim_estabelecimento")
    def build_dim(_dominios: int) -> int:
        """TRUNCATE + INSERT da dimensão (recriação total)."""
        sql = _sql_dir() / "020_dim_estabelecimento.sql"
        rows = execute_ddl(sql)
        total = count_rows("gold", "dim_estabelecimento")
        print(
            f"gold.dim_estabelecimento: {total:,} linhas "
            f"(insert rowcount={rows:,})"
        )
        return total

    @task(task_id="build_fato_leitos_anual")
    def build_fato(_dim_rows: int) -> int:
        """TRUNCATE + INSERT do fato agregado (cnes × ano)."""
        sql = _sql_dir() / "021_fato_leitos_anual.sql"
        rows = execute_ddl(sql)
        total = count_rows("gold", "fato_leitos_anual")
        print(
            f"gold.fato_leitos_anual: {total:,} linhas "
            f"(insert rowcount={rows:,})"
        )
        return total

    build_fato(build_dim(ensure_dominios()))


gold_capacidade_hospitalar()
