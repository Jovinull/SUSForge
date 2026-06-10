"""Configuração tipada do SUSForge.

Usa `pydantic-settings` para ler variáveis de ambiente do arquivo `.env`
na raiz do projeto (ou exportadas no ambiente). Toda configuração que
toque credencial, host ou caminho deve passar por aqui — proibido
hardcode em DAGs ou módulos de ETL.

Exemplo de uso::

    from susforge.config import get_settings

    settings = get_settings()
    engine = create_engine(settings.database.sqlalchemy_url)
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field, SecretStr, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT: Path = Path(__file__).resolve().parents[3]
ENV_FILE: Path = PROJECT_ROOT / ".env"


class DatabaseSettings(BaseSettings):
    """Conexão com o banco analítico SUSForge (Postgres + PostGIS).

    Por padrão aponta para o serviço Postgres do `docker-compose.yml`
    acessado a partir do host (porta exposta em `POSTGRES_HOST_PORT`).
    Para execução **dentro** da rede do Compose (ex.: DAGs Airflow nos
    contêineres), sobrescreva `SUSFORGE_DB_HOST=postgres` e
    `SUSFORGE_DB_PORT=5432` no ambiente do contêiner.

    A leitura é case-insensitive e ignora variáveis extras do `.env`
    (Airflow, Metabase, etc.) para evitar acoplamento.
    """

    model_config = SettingsConfigDict(
        env_file=ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    host: str = Field(
        default="localhost",
        validation_alias="SUSFORGE_DB_HOST",
        description="Hostname do Postgres. 'postgres' dentro da rede Compose.",
    )
    port: int = Field(
        default=5432,
        validation_alias="POSTGRES_HOST_PORT",
        description="Porta do Postgres exposta no host.",
        ge=1,
        le=65535,
    )
    user: str = Field(
        validation_alias="POSTGRES_USER",
        description="Usuário do banco analítico.",
    )
    password: SecretStr = Field(
        validation_alias="POSTGRES_PASSWORD",
        description="Senha do usuário do banco (não logar).",
    )
    name: str = Field(
        default="susforge",
        validation_alias="POSTGRES_DB",
        description="Nome do banco analítico principal.",
    )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def sqlalchemy_url(self) -> str:
        """URL SQLAlchemy completa, com driver psycopg2."""
        return (
            "postgresql+psycopg2://"
            f"{self.user}:{self.password.get_secret_value()}"
            f"@{self.host}:{self.port}/{self.name}"
        )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def dsn(self) -> str:
        """DSN no formato libpq (psycopg2.connect, psql, etc.)."""
        return (
            f"host={self.host} port={self.port} dbname={self.name} "
            f"user={self.user} password={self.password.get_secret_value()}"
        )


class Settings(BaseSettings):
    """Configuração raiz do projeto.

    Agrega blocos especializados (database, futuramente airflow,
    storage, datasus, etc.). Use `get_settings()` para obter uma
    instância cacheada — evita re-leitura do `.env` a cada chamada.
    """

    model_config = SettingsConfigDict(
        env_file=ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    project_root: Path = Field(default=PROJECT_ROOT, description="Raiz do repositório.")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Retorna uma instância cacheada de `Settings`.

    Cacheado para garantir que o `.env` seja lido uma única vez por
    processo. Em testes, use `get_settings.cache_clear()` para forçar
    releitura após alterar variáveis de ambiente.
    """
    return Settings()


__all__ = [
    "DatabaseSettings",
    "Settings",
    "get_settings",
    "PROJECT_ROOT",
    "ENV_FILE",
]
