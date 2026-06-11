"""Cliente HTTP genérico para a API DEMAS / Dados Abertos do Ministério da Saúde.

URL base: ``https://apidadosabertos.saude.gov.br/``

Comportamento real observado (Jun/2026):
    * Sem autenticação — todos os endpoints públicos.
    * Paginação por ``limit`` (1..1000) + ``offset`` (NÚMERO DA PÁGINA,
      não offset de registros).
    * Cap de profundidade: offsets além de ~100 retornam vazio.
    * Resposta é um objeto ``{"<dataset_name>": [...]}`` (chave nomeada
      conforme o dataset, não um array no topo).
    * Filtros aplicados ANTES do slice global — uma página pode trazer
      bem menos itens que ``limit`` se o filtro for restritivo.

Por que cliente próprio: temos perfis específicos (retry exponencial,
session reusada, logs de throughput) que valem encapsular.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Final

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

DEMAS_BASE_URL: Final = "https://apidadosabertos.saude.gov.br"
DEFAULT_TIMEOUT_S: Final = 60
USER_AGENT: Final = "SUSForge/0.1 (+susforge.dw)"

# Limites práticos do servidor — confirmados empiricamente.
MAX_LIMIT: Final = 1000


def _make_session(
    *,
    total_retries: int = 5,
    backoff_factor: float = 0.5,
    bearer_token: str | None = None,
) -> requests.Session:
    """Session com retry exponencial em 429/5xx."""
    retry = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=8)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {"User-Agent": USER_AGENT, "Accept": "application/json"}
    )
    if bearer_token:
        session.headers["Authorization"] = f"Bearer {bearer_token}"
    return session


class DemasClient:
    """Cliente HTTP minimalista para a API DEMAS."""

    def __init__(
        self,
        *,
        base_url: str = DEMAS_BASE_URL,
        bearer_token: str | None = None,
        timeout_s: int = DEFAULT_TIMEOUT_S,
    ) -> None:
        self.base = base_url.rstrip("/")
        self.timeout = timeout_s
        self.session = _make_session(bearer_token=bearer_token)
        self._calls = 0
        self._total_time = 0.0
        self._total_bytes = 0

    def get(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """GET com retry + log de throughput."""
        url = f"{self.base}/{path.lstrip('/')}"
        t0 = time.perf_counter()
        r = self.session.get(url, params=params, timeout=self.timeout)
        elapsed = time.perf_counter() - t0
        r.raise_for_status()
        self._calls += 1
        self._total_time += elapsed
        self._total_bytes += len(r.content)
        result: dict[str, Any] = r.json()
        return result

    def paginate(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        data_key: str | None = None,
        limit: int = MAX_LIMIT,
        max_pages: int = 200,
        empty_streak_to_stop: int = 3,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Itera ``limit``/``offset`` até esvaziar.

        Args:
            path: Caminho do endpoint (relativo à base).
            params: Filtros (ex.: ``{"uf_estabelecimento": "SE"}``).
            data_key: Nome da chave que contém a lista (auto-detect se None).
            limit: Tamanho de página (≤ 1000).
            max_pages: Limite de páginas para proteger contra loop infinito.
            empty_streak_to_stop: Páginas vazias consecutivas para parar.

        Returns:
            Tupla ``(items, stats)`` — todos os registros baixados + um dict
            com métricas de execução (páginas, itens, tempo, throughput).
        """
        all_params = dict(params or {})
        all_params["limit"] = limit

        items: list[dict[str, Any]] = []
        empty_streak = 0
        last_nonempty_offset = -1
        wall_t0 = time.perf_counter()

        for offset in range(max_pages):
            all_params["offset"] = offset
            payload = self.get(path, params=all_params)

            if data_key is None:
                # Auto-detecta: pega a primeira chave cujo valor seja lista.
                for k, v in payload.items():
                    if isinstance(v, list):
                        data_key = k
                        break
                if data_key is None:
                    raise RuntimeError(
                        f"Payload sem chave de lista detectável: {list(payload)}"
                    )

            page = payload.get(data_key, [])
            if not page:
                empty_streak += 1
                logger.debug("page=%d vazia (streak=%d)", offset, empty_streak)
                if empty_streak >= empty_streak_to_stop:
                    break
                continue

            empty_streak = 0
            last_nonempty_offset = offset
            items.extend(page)
            if offset % 10 == 0 or offset < 5:
                logger.info(
                    "page=%d +%d itens (acumulado=%d)",
                    offset,
                    len(page),
                    len(items),
                )

        wall_elapsed = time.perf_counter() - wall_t0
        stats = {
            "pages_fetched": self._calls,
            "last_nonempty_offset": last_nonempty_offset,
            "total_items": len(items),
            "total_seconds": round(wall_elapsed, 2),
            "items_per_second": round(len(items) / max(wall_elapsed, 0.001), 1),
            "bytes_downloaded": self._total_bytes,
            "data_key": data_key,
        }
        return items, stats


__all__ = ["DEMAS_BASE_URL", "MAX_LIMIT", "DemasClient"]
