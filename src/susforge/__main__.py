"""Entrypoint da CLI `susforge` (registrado em pyproject.toml).

Implementação inicial mínima — comandos serão adicionados conforme
os pipelines forem entrando.
"""

from __future__ import annotations

import typer

from susforge import __version__

app = typer.Typer(
    name="susforge",
    help="CLI do SUSForge — utilitários do Data Warehouse Medalhão.",
    no_args_is_help=True,
    add_completion=False,
)


@app.callback()
def _root() -> None:
    """Ponto de entrada da CLI (força estrutura multi-comando do Typer)."""


@app.command()
def version() -> None:
    """Imprime a versão instalada do pacote."""
    typer.echo(f"susforge {__version__}")


if __name__ == "__main__":
    app()
