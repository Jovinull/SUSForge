"""Bootstrap automático do Metabase via API REST — SUSForge BI.

O que faz (idempotente — pode rodar quantas vezes quiser):

    1. Aguarda o ``GET /api/health`` voltar ``200``.
    2. Detecta se a instância precisa de setup inicial (``setup-token``
       presente em ``/api/session/properties``).
       - Se precisar: cria o admin com as constantes definidas abaixo.
       - Se não: faz login com as MESMAS credenciais (mantém idempotência
         entre execuções).
    3. Cadastra a conexão Postgres apontando para ``host=postgres``
       (rede do Compose) — nome ``"SUSForge DW"``. Pula se já existir.
    4. Cria a Collection ``"SUSForge - Governança Pública"``.
    5. Cria o Dashboard ``"Visão Executiva: Capacidade Hospitalar SUS"``
       dentro da coleção.
    6. Cria 3 cards SQL nativos (``gold.fato_leitos_anual`` ⨝
       ``gold.dim_estabelecimento``) e os atrela ao dashboard:
        a) Evolução Histórica de Leitos UTI (SUS vs Total) — line
        b) UTI Total por UF (2025) — bar
        c) Top 10 Hospitais por UTI Pico (2025) — table

Uso::

    .venv/bin/python -m susforge.utils.setup_metabase
"""

from __future__ import annotations

import logging
import os
import sys
import time
from typing import Any

import requests

from susforge.config import get_settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Constantes (públicas — printamos no terminal no final)
# ---------------------------------------------------------------------
METABASE_URL: str = os.environ.get("METABASE_URL", "http://localhost:3000")

ADMIN_EMAIL: str = "admin@susforge.local"
ADMIN_PASSWORD: str = "SusForge@2026!"  # atende requisitos do Metabase
ADMIN_FIRST_NAME: str = "SUSForge"
ADMIN_LAST_NAME: str = "Admin"
SITE_NAME: str = "SUSForge BI"
SITE_LOCALE: str = "pt_BR"

DB_NAME: str = "SUSForge DW"
COLLECTION_NAME: str = "SUSForge - Governança Pública"
DASHBOARD_NAME: str = "Visão Executiva: Capacidade Hospitalar SUS"

# ---------------------------------------------------------------------
# Cards (SQL nativo)
# ---------------------------------------------------------------------
SQL_UTI_EVOLUCAO = """
SELECT f.ano_referencia AS ano,
       sum(f.uti_total_avg)::int AS uti_total_media_mensal,
       sum(f.uti_sus_avg)::int   AS uti_sus_media_mensal
FROM gold.fato_leitos_anual f
WHERE f.ano_referencia BETWEEN 2007 AND 2025
GROUP BY 1
ORDER BY 1
""".strip()

SQL_UTI_POR_UF = """
SELECT d.sg_uf AS uf,
       sum(f.uti_total_avg)::int AS uti_total_media_mensal,
       sum(f.uti_sus_avg)::int   AS uti_sus_media_mensal
FROM gold.fato_leitos_anual f
JOIN gold.dim_estabelecimento d ON d.co_cnes = f.co_cnes
WHERE f.ano_referencia = 2025
GROUP BY d.sg_uf
ORDER BY uti_total_media_mensal DESC
""".strip()

SQL_TOP_HOSPITAIS_UTI = """
SELECT d.no_fantasia AS hospital,
       d.sg_uf       AS uf,
       f.uti_total_max AS uti_pico_2025
FROM gold.fato_leitos_anual f
JOIN gold.dim_estabelecimento d ON d.co_cnes = f.co_cnes
WHERE f.ano_referencia = 2025
  AND f.uti_total_max  > 0
ORDER BY f.uti_total_max DESC
LIMIT 10
""".strip()


# =====================================================================
# Cliente HTTP do Metabase
# =====================================================================
class MetabaseClient:
    """Wrapper fino sobre ``requests`` que injeta o header de sessão."""

    def __init__(self, base_url: str) -> None:
        self.base: str = base_url.rstrip("/")
        self.http: requests.Session = requests.Session()
        self.session_id: str | None = None

    def _request(
        self,
        method: str,
        path: str,
        *,
        expected_status: tuple[int, ...] = (200, 201, 204),
        **kwargs: Any,
    ) -> requests.Response:
        url = f"{self.base}{path}"
        headers = dict(kwargs.pop("headers", {}))
        if self.session_id:
            headers["X-Metabase-Session"] = self.session_id
        response = self.http.request(
            method, url, headers=headers, timeout=60, **kwargs
        )
        if response.status_code not in expected_status:
            raise RuntimeError(
                f"{method} {path} → {response.status_code}: {response.text[:400]}"
            )
        return response

    # ---- Lifecycle ----
    def wait_ready(self, timeout_s: int = 180) -> None:
        deadline = time.monotonic() + timeout_s
        last_err: str | None = None
        while time.monotonic() < deadline:
            try:
                r = self.http.get(f"{self.base}/api/health", timeout=5)
                if r.status_code == 200:
                    logger.info("Metabase healthy em %s", self.base)
                    return
                last_err = f"HTTP {r.status_code}"
            except requests.RequestException as exc:
                last_err = str(exc)
            time.sleep(3)
        raise TimeoutError(
            f"Metabase não healthy em {timeout_s}s (último erro: {last_err})"
        )

    def session_properties(self) -> dict[str, Any]:
        r = self._request("GET", "/api/session/properties")
        result: dict[str, Any] = r.json()
        return result

    def setup(self, token: str) -> None:
        body = {
            "token": token,
            "user": {
                "email": ADMIN_EMAIL,
                "password": ADMIN_PASSWORD,
                "first_name": ADMIN_FIRST_NAME,
                "last_name": ADMIN_LAST_NAME,
                "site_name": SITE_NAME,
            },
            "prefs": {
                "site_name": SITE_NAME,
                "site_locale": SITE_LOCALE,
                "allow_tracking": "false",
            },
            "database": None,
        }
        r = self._request("POST", "/api/setup", json=body)
        self.session_id = r.json()["id"]
        logger.info("Setup inicial concluído — admin criado e logado")

    def login(self) -> None:
        r = self._request(
            "POST",
            "/api/session",
            json={"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
        )
        self.session_id = r.json()["id"]
        logger.info("Login OK como %s", ADMIN_EMAIL)

    # ---- Recursos ----
    def list_databases(self) -> list[dict[str, Any]]:
        data = self._request("GET", "/api/database").json()
        # v0.50+ devolve {"data": [...]}; v0.42- devolve lista direto
        if isinstance(data, dict) and "data" in data:
            return list(data["data"])
        return list(data)

    def ensure_database(self) -> int:
        existing = [db for db in self.list_databases() if db.get("name") == DB_NAME]
        if existing:
            db_id = int(existing[0]["id"])
            logger.info("DB '%s' já existe (id=%d)", DB_NAME, db_id)
            return db_id

        pg = get_settings().database
        body = {
            "name": DB_NAME,
            "engine": "postgres",
            "details": {
                # Dentro da rede Compose o Metabase fala com 'postgres:5432'
                "host": "postgres",
                "port": 5432,
                "dbname": pg.name,
                "user": pg.user,
                "password": pg.password.get_secret_value(),
                "ssl": False,
                "tunnel-enabled": False,
                "schema-filters-type": "inclusion",
                "schema-filters-patterns": "gold,silver,bronze",
            },
            "is_full_sync": True,
            "is_on_demand": False,
        }
        r = self._request("POST", "/api/database", json=body)
        db_id = int(r.json()["id"])
        logger.info("DB criado (id=%d) — disparando sync", db_id)
        # Sync de schema (não bloqueia; Metabase processa em background)
        try:
            self._request("POST", f"/api/database/{db_id}/sync_schema")
        except RuntimeError as exc:
            logger.warning("sync_schema retornou erro (não crítico): %s", exc)
        return db_id

    def ensure_collection(self) -> int:
        existing_all = self._request("GET", "/api/collection").json()
        for c in existing_all:
            if c.get("name") == COLLECTION_NAME:
                cid = int(c["id"])
                logger.info("Collection '%s' já existe (id=%d)", COLLECTION_NAME, cid)
                return cid
        body = {
            "name": COLLECTION_NAME,
            "description": (
                "Painéis analíticos do SUSForge — capacidade hospitalar, "
                "atenção primária, séries históricas."
            ),
            "color": "#509EE3",
        }
        r = self._request("POST", "/api/collection", json=body)
        cid = int(r.json()["id"])
        logger.info("Collection criada (id=%d)", cid)
        return cid

    def ensure_dashboard(self, collection_id: int) -> int:
        data = self._request("GET", "/api/dashboard").json()
        for d in data:
            if d.get("name") == DASHBOARD_NAME:
                did = int(d["id"])
                logger.info("Dashboard '%s' já existe (id=%d)", DASHBOARD_NAME, did)
                return did
        body = {
            "name": DASHBOARD_NAME,
            "description": (
                "Cards executivos da capacidade hospitalar nacional, "
                "construídos a partir do Star Schema gold."
            ),
            "collection_id": collection_id,
        }
        r = self._request("POST", "/api/dashboard", json=body)
        did = int(r.json()["id"])
        logger.info("Dashboard criado (id=%d)", did)
        return did

    def ensure_card(
        self,
        *,
        name: str,
        sql: str,
        display: str,
        db_id: int,
        collection_id: int,
        visualization_settings: dict[str, Any] | None = None,
    ) -> int:
        existing = self._request("GET", "/api/card").json()
        for c in existing:
            if c.get("name") == name:
                cid = int(c["id"])
                logger.info("Card '%s' já existe (id=%d)", name, cid)
                return cid
        body = {
            "name": name,
            "dataset_query": {
                "type": "native",
                "native": {"query": sql, "template-tags": {}},
                "database": db_id,
            },
            "display": display,
            "visualization_settings": visualization_settings or {},
            "collection_id": collection_id,
        }
        r = self._request("POST", "/api/card", json=body)
        cid = int(r.json()["id"])
        logger.info("Card criado: '%s' (id=%d, display=%s)", name, cid, display)
        return cid

    def attach_cards(
        self, dashboard_id: int, card_ids: list[int]
    ) -> None:
        """Atrela cards ao dashboard via ``PUT /api/dashboard/:id``.

        Layout: 2 colunas no topo (line + bar lado a lado) e a tabela
        ocupando a largura total embaixo.
        """
        # Pega o dashboard atual para descobrir se já tem dashcards.
        current = self._request(
            "GET", f"/api/dashboard/{dashboard_id}"
        ).json()
        if current.get("dashcards"):
            logger.info(
                "Dashboard %d já possui %d card(s) — pulando attach",
                dashboard_id,
                len(current["dashcards"]),
            )
            return

        layout = [
            {"row": 0, "col": 0,  "size_x": 12, "size_y": 7},   # line
            {"row": 0, "col": 12, "size_x": 12, "size_y": 7},   # bar
            {"row": 7, "col": 0,  "size_x": 24, "size_y": 8},   # table
        ]
        dashcards = [
            {
                "id": -(i + 1),
                "card_id": card_id,
                "row": pos["row"],
                "col": pos["col"],
                "size_x": pos["size_x"],
                "size_y": pos["size_y"],
                "parameter_mappings": [],
                "visualization_settings": {},
            }
            for i, (card_id, pos) in enumerate(zip(card_ids, layout, strict=True))
        ]
        self._request(
            "PUT",
            f"/api/dashboard/{dashboard_id}",
            json={"dashcards": dashcards},
        )
        logger.info("Atrelados %d cards ao dashboard %d", len(card_ids), dashboard_id)


# =====================================================================
# Entrypoint
# =====================================================================
def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    client = MetabaseClient(METABASE_URL)
    client.wait_ready()

    props = client.session_properties()
    setup_token = props.get("setup-token")
    has_user_setup = props.get("has-user-setup")

    # has-user-setup é a flag autoritativa. O Metabase v0.51+ ainda
    # devolve setup-token mesmo após configurado — não confiar nele.
    if has_user_setup:
        logger.info("Metabase já configurado — efetuando login")
        client.login()
    elif setup_token:
        logger.info("Setup inicial necessário (token detectado)")
        client.setup(setup_token)
    else:
        logger.error(
            "Metabase não configurado E sem setup-token. "
            "Tente `docker compose restart metabase`."
        )
        return 1

    db_id = client.ensure_database()
    collection_id = client.ensure_collection()
    dashboard_id = client.ensure_dashboard(collection_id)

    card_uti = client.ensure_card(
        name="Evolução Histórica de Leitos UTI (SUS vs Total)",
        sql=SQL_UTI_EVOLUCAO,
        display="line",
        db_id=db_id,
        collection_id=collection_id,
        visualization_settings={
            "graph.dimensions": ["ano"],
            "graph.metrics": ["uti_total_media_mensal", "uti_sus_media_mensal"],
        },
    )
    card_uf = client.ensure_card(
        name="UTI Total por UF (2025)",
        sql=SQL_UTI_POR_UF,
        display="bar",
        db_id=db_id,
        collection_id=collection_id,
        visualization_settings={
            "graph.dimensions": ["uf"],
            "graph.metrics": ["uti_total_media_mensal"],
        },
    )
    card_top = client.ensure_card(
        name="Top 10 Hospitais por UTI Pico (2025)",
        sql=SQL_TOP_HOSPITAIS_UTI,
        display="table",
        db_id=db_id,
        collection_id=collection_id,
    )

    client.attach_cards(dashboard_id, [card_uti, card_uf, card_top])

    # ---- Relatório final ----
    print()
    print("=" * 64)
    print(" Metabase configurado")
    print("=" * 64)
    print(f"  URL              : {METABASE_URL}")
    print(f"  Login (e-mail)   : {ADMIN_EMAIL}")
    print(f"  Senha            : {ADMIN_PASSWORD}")
    print(f"  Database         : {DB_NAME} (id={db_id})")
    print(f"  Collection       : {COLLECTION_NAME} (id={collection_id})")
    print(f"  Dashboard        : {DASHBOARD_NAME}")
    print(f"  → Acesso direto  : {METABASE_URL}/dashboard/{dashboard_id}")
    print("=" * 64)
    return 0


if __name__ == "__main__":
    sys.exit(main())
