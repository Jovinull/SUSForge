"""Bronze • CNES Estabelecimentos — ingestão nacional CSV → Parquet.

Fluxo (TaskFlow API, Airflow 3):

    ingest_raw  ──►  to_parquet

Origem: CKAN/DEMAS do Ministério da Saúde (S3 público).
Escopo: nacional, sem fatiamento por UF — o arquivo já é Brasil inteiro.

Fora de escopo (responsabilidade da Silver / Gold):
    * Decodificação de domínios (TP_GESTAO, CO_NATUREZA_ORGANIZACAO, …).
    * Tipagem estrita das colunas.
    * Mascaramento de PII (endereço, telefone, e-mail) — dados públicos.
    * Carga em banco — esta DAG vive 100% no data lake (raw/staging).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task

from susforge.io.cnes import (
    DATASET,
    SOURCE_URL,
    convert_to_parquet,
    ingest_raw,
)

DEFAULT_ARGS = {
    "owner": "susforge",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="bronze_cnes_estabelecimentos",
    description="Bronze · CNES Estabelecimentos — CSV → Parquet (nacional)",
    start_date=datetime(2026, 6, 1),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "cnes", "assistencia-saude", "master-data"],
    doc_md=__doc__,
    params={
        "force_download": False,
    },
)
def bronze_cnes_estabelecimentos() -> None:
    @task(task_id="ingest_raw")
    def ingest(force_download: bool = False) -> str:
        """Baixa CSV do S3 (fallback local) → ``data/raw/cnes/estabelecimentos/``."""
        path = ingest_raw(force_download=force_download)
        print(f"[{DATASET}] raw aterrissado em {path} (origem: {SOURCE_URL})")
        return str(path)

    @task(task_id="to_parquet")
    def to_parquet(raw_path: str) -> str:
        """Lê o CSV bruto com Polars e grava Parquet+zstd em ``data/staging/``."""
        path = convert_to_parquet(Path(raw_path))
        print(f"[{DATASET}] parquet gerado em {path}")
        return str(path)

    to_parquet(ingest())


bronze_cnes_estabelecimentos()
