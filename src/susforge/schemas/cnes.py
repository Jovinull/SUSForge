"""Contrato Pandera para ``silver.estabelecimentos``.

Define os tipos e invariantes que o DataFrame deve respeitar **antes**
de ir para o Postgres. Falha rápido em qualquer divergência: a Silver
existe justamente para garantir esse contrato — não há "tente carregar
e veja o que cai".

Tipos:
    * Códigos/descrições  → ``str`` (NULLABLE, exceto ``co_cnes``).
    * Coordenadas        → ``float`` (NULLABLE) com envelope Brasil
                           +1° de tolerância em cada direção.
    * Flags ``st_*``      → ``bool`` (NULLABLE).
    * Linhagem técnica   → ``datetime`` / ``date`` / ``str`` (NOT NULL).
"""

from __future__ import annotations

from typing import Annotated

import polars as pl
from pandera.polars import DataFrameModel, Field
from pandera.typing.polars import Series

# Tipos Datetime com timezone explícito (Pandera-Polars exige).
DatetimeUTC = Annotated[pl.Datetime, "us", "UTC"]

# Envelope geográfico do Brasil + 1° de tolerância (limpeza Silver
# zera coordenadas fora desse envelope).
LAT_MIN: float = -35.0
LAT_MAX: float = 6.0
LON_MIN: float = -75.0
LON_MAX: float = -33.0


class EstabelecimentosSchema(DataFrameModel):
    """Schema da tabela ``silver.estabelecimentos``."""

    co_cnes: Series[str] = Field(nullable=False)
    co_unidade: Series[str] = Field(nullable=True)
    co_uf: Series[str] = Field(nullable=True)
    co_ibge: Series[str] = Field(nullable=True)
    nu_cnpj_mantenedora: Series[str] = Field(nullable=True)
    no_razao_social: Series[str] = Field(nullable=True)
    no_fantasia: Series[str] = Field(nullable=True)
    co_natureza_organizacao: Series[str] = Field(nullable=True)
    ds_natureza_organizacao: Series[str] = Field(nullable=True)
    tp_gestao: Series[str] = Field(nullable=True)
    co_nivel_hierarquia: Series[str] = Field(nullable=True)
    ds_nivel_hierarquia: Series[str] = Field(nullable=True)
    co_esfera_administrativa: Series[str] = Field(nullable=True)
    ds_esfera_administrativa: Series[str] = Field(nullable=True)
    co_atividade: Series[str] = Field(nullable=True)
    tp_unidade: Series[str] = Field(nullable=True)
    co_cep: Series[str] = Field(nullable=True)
    no_logradouro: Series[str] = Field(nullable=True)
    nu_endereco: Series[str] = Field(nullable=True)
    no_bairro: Series[str] = Field(nullable=True)
    nu_telefone: Series[str] = Field(nullable=True)
    nu_latitude: Series[float] = Field(nullable=True, ge=LAT_MIN, le=LAT_MAX)
    nu_longitude: Series[float] = Field(nullable=True, ge=LON_MIN, le=LON_MAX)
    co_turno_atendimento: Series[str] = Field(nullable=True)
    ds_turno_atendimento: Series[str] = Field(nullable=True)
    nu_cnpj: Series[str] = Field(nullable=True)
    no_email: Series[str] = Field(nullable=True)
    co_natureza_jur: Series[str] = Field(nullable=True)
    st_centro_cirurgico: Series[bool] = Field(nullable=True)
    st_centro_obstetrico: Series[bool] = Field(nullable=True)
    st_centro_neonatal: Series[bool] = Field(nullable=True)
    st_atend_hospitalar: Series[bool] = Field(nullable=True)
    st_servico_apoio: Series[bool] = Field(nullable=True)
    st_atend_ambulatorial: Series[bool] = Field(nullable=True)
    co_motivo_desab: Series[str] = Field(nullable=True)
    co_ambulatorial_sus: Series[str] = Field(nullable=True)
    # ---- Linhagem ----
    ingested_at: Series[DatetimeUTC] = Field(
        nullable=False,
        alias="_ingested_at",
    )
    source_file: Series[str] = Field(nullable=False, alias="_source_file")
    source_hash: Series[str] = Field(nullable=False, alias="_source_hash")
    extraction_date: Series[pl.Date] = Field(
        nullable=False,
        alias="_extraction_date",
    )
    cleansed_at: Series[DatetimeUTC] = Field(
        nullable=False,
        alias="_cleansed_at",
    )

    class Config:
        strict = True


# Ordem das colunas como espera o DDL (e o COPY).
SILVER_COLUMN_ORDER: tuple[str, ...] = (
    "co_cnes",
    "co_unidade",
    "co_uf",
    "co_ibge",
    "nu_cnpj_mantenedora",
    "no_razao_social",
    "no_fantasia",
    "co_natureza_organizacao",
    "ds_natureza_organizacao",
    "tp_gestao",
    "co_nivel_hierarquia",
    "ds_nivel_hierarquia",
    "co_esfera_administrativa",
    "ds_esfera_administrativa",
    "co_atividade",
    "tp_unidade",
    "co_cep",
    "no_logradouro",
    "nu_endereco",
    "no_bairro",
    "nu_telefone",
    "nu_latitude",
    "nu_longitude",
    "co_turno_atendimento",
    "ds_turno_atendimento",
    "nu_cnpj",
    "no_email",
    "co_natureza_jur",
    "st_centro_cirurgico",
    "st_centro_obstetrico",
    "st_centro_neonatal",
    "st_atend_hospitalar",
    "st_servico_apoio",
    "st_atend_ambulatorial",
    "co_motivo_desab",
    "co_ambulatorial_sus",
    "_ingested_at",
    "_source_file",
    "_source_hash",
    "_extraction_date",
    "_cleansed_at",
)


__all__ = [
    "EstabelecimentosSchema",
    "LAT_MAX",
    "LAT_MIN",
    "LON_MAX",
    "LON_MIN",
    "SILVER_COLUMN_ORDER",
]
