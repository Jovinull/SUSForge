"""Bronze • Hospitais e Leitos — série histórica anual (2007 → hoje).

Padrão Ouro Bronze (vide ``susforge.io.hospitais_leitos``):
    * Particionamento por data de extração em ``raw/`` e ``staging/``.
    * Detecção de mudança por ANO via HEAD (ETag / Last-Modified).
    * Manifesto leve com ``last_extraction.years[year]``.
    * Schedule ``@weekly``: o MS atualiza o ano corrente mensalmente,
      mas ``@weekly`` reage rápido sem custo (HEADs apenas).

Fora de escopo (Silver / Gold):
    * Decodificação de domínios, tipagem estrita, mascaramento de PII,
      carga em banco.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, task

from susforge.io.hospitais_leitos import (
    DATASET,
    IngestResult,
    convert_to_parquet,
    ingest_raw,
)

DEFAULT_ARGS = {
    "owner": "susforge",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="bronze_hospitais_leitos",
    description=(
        "Bronze · Hospitais e Leitos — série histórica anual CSV→Parquet"
    ),
    start_date=datetime(2026, 6, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "hospitais-leitos", "assistencia-saude", "serie-historica"],
    doc_md=__doc__,
)
def bronze_hospitais_leitos() -> None:
    @task(task_id="ingest_raw")
    def ingest() -> dict[str, Any]:
        """Faz HEAD em cada ano; baixa só os que mudaram."""
        result = ingest_raw()
        if not result.changed:
            raise AirflowSkipException(
                f"[{DATASET}] nenhum ano novo — "
                f"inalterados={len(result.unchanged_years)} "
                f"falhados={len(result.failed_years)}"
            )
        anos = ", ".join(str(r.year) for r in result.new_records)
        print(
            f"[{DATASET}] novos {len(result.new_records)} anos: {anos} | "
            f"inalterados={len(result.unchanged_years)} "
            f"falhados={len(result.failed_years)}"
        )
        return result.model_dump(mode="json")

    @task(task_id="to_parquet")
    def to_parquet(result_dict: dict[str, Any]) -> list[str]:
        """Converte cada CSV novo em Parquet (linhagem em cada arquivo)."""
        result = IngestResult.model_validate(result_dict)
        paths = convert_to_parquet(result)
        print(f"[{DATASET}] {len(paths)} parquet(s) gerado(s)")
        return [str(p) for p in paths]

    to_parquet(ingest())


bronze_hospitais_leitos()
