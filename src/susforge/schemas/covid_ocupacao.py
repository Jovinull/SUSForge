"""Contrato Pandera para ``silver.ocupacao_covid``.

Granularidade Silver: 1 linha por ``(_extraction_date, id_registro)``.
``id_registro`` é o ``_id`` original do e-SUS Notifica (Parse), única
chave natural confiável do dataset.

Categorias de coluna:
    * **Ocupação clínica** (4 colunas): leitos clínicos por status do
      paciente — Suspeito, Confirmado, Covid, Hospitalar.
    * **Ocupação UTI**     (4 colunas): mesma divisão para UTI.
    * **Desfechos**        (4 colunas): óbitos e altas, ambos para
      casos suspeitos e confirmados.
    * **Localização**      (4 colunas): estado/município do paciente e
      do estabelecimento notificador.
    * **Metadados**        (origem, registros Parse, flags).
"""

from __future__ import annotations

from typing import Annotated

import polars as pl
from pandera.polars import DataFrameModel, Field
from pandera.typing.polars import Series

DatetimeUTC = Annotated[pl.Datetime, "us", "UTC"]


class OcupacaoCovidSchema(DataFrameModel):
    """Schema da tabela ``silver.ocupacao_covid``."""

    # ---- Identidade ----
    id_registro: Series[str] = Field(nullable=False)
    cnes: Series[str] = Field(nullable=False)
    data_notificacao: Series[DatetimeUTC] = Field(nullable=False)
    ano_referencia: Series[int] = Field(nullable=False, ge=2020, le=2030)

    # ---- Ocupação clínica ----
    ocupacao_suspeito_cli: Series[int] = Field(nullable=False, ge=0)
    ocupacao_confirmado_cli: Series[int] = Field(nullable=False, ge=0)
    ocupacao_covid_cli: Series[int] = Field(nullable=False, ge=0)
    ocupacao_hospitalar_cli: Series[int] = Field(nullable=False, ge=0)

    # ---- Ocupação UTI ----
    ocupacao_suspeito_uti: Series[int] = Field(nullable=False, ge=0)
    ocupacao_confirmado_uti: Series[int] = Field(nullable=False, ge=0)
    ocupacao_covid_uti: Series[int] = Field(nullable=False, ge=0)
    ocupacao_hospitalar_uti: Series[int] = Field(nullable=False, ge=0)

    # ---- Desfechos ----
    saida_suspeita_obitos: Series[int] = Field(nullable=False, ge=0)
    saida_suspeita_altas: Series[int] = Field(nullable=False, ge=0)
    saida_confirmada_obitos: Series[int] = Field(nullable=False, ge=0)
    saida_confirmada_altas: Series[int] = Field(nullable=False, ge=0)

    # ---- Localização ----
    estado_notificacao: Series[str] = Field(nullable=True)
    municipio_notificacao: Series[str] = Field(nullable=True)
    estado: Series[str] = Field(nullable=True)
    municipio: Series[str] = Field(nullable=True)

    # ---- Metadados ----
    origem: Series[str] = Field(nullable=True)
    excluido: Series[bool] = Field(nullable=True)
    validado: Series[bool] = Field(nullable=True)
    registro_created_at: Series[DatetimeUTC] = Field(nullable=True)
    registro_updated_at: Series[DatetimeUTC] = Field(nullable=True)

    # ---- Linhagem ----
    ingested_at: Series[DatetimeUTC] = Field(
        nullable=False, alias="_ingested_at"
    )
    source_file: Series[str] = Field(nullable=False, alias="_source_file")
    source_hash: Series[str] = Field(nullable=False, alias="_source_hash")
    extraction_date: Series[pl.Date] = Field(
        nullable=False, alias="_extraction_date"
    )
    cleansed_at: Series[DatetimeUTC] = Field(
        nullable=False, alias="_cleansed_at"
    )

    class Config:
        strict = True


SILVER_COLUMN_ORDER: tuple[str, ...] = (
    "id_registro",
    "cnes",
    "data_notificacao",
    "ano_referencia",
    "ocupacao_suspeito_cli",
    "ocupacao_confirmado_cli",
    "ocupacao_covid_cli",
    "ocupacao_hospitalar_cli",
    "ocupacao_suspeito_uti",
    "ocupacao_confirmado_uti",
    "ocupacao_covid_uti",
    "ocupacao_hospitalar_uti",
    "saida_suspeita_obitos",
    "saida_suspeita_altas",
    "saida_confirmada_obitos",
    "saida_confirmada_altas",
    "estado_notificacao",
    "municipio_notificacao",
    "estado",
    "municipio",
    "origem",
    "excluido",
    "validado",
    "registro_created_at",
    "registro_updated_at",
    "_ingested_at",
    "_source_file",
    "_source_hash",
    "_extraction_date",
    "_cleansed_at",
)

# Quantitativos — viram int não-nulo via cast Float→Int + fill_null(0).
QUANTITATIVE_COLUMNS: tuple[str, ...] = (
    "ocupacao_suspeito_cli",
    "ocupacao_confirmado_cli",
    "ocupacao_covid_cli",
    "ocupacao_hospitalar_cli",
    "ocupacao_suspeito_uti",
    "ocupacao_confirmado_uti",
    "ocupacao_covid_uti",
    "ocupacao_hospitalar_uti",
    "saida_suspeita_obitos",
    "saida_suspeita_altas",
    "saida_confirmada_obitos",
    "saida_confirmada_altas",
)


__all__ = [
    "QUANTITATIVE_COLUMNS",
    "SILVER_COLUMN_ORDER",
    "OcupacaoCovidSchema",
]
