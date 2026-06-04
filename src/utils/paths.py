"""Funções utilitárias para criação de diretórios locais do laboratório."""

from __future__ import annotations

from pathlib import Path

from src.config.settings import Settings


def ensure_local_directories(settings: Settings | None = None) -> list[Path]:
    active_settings = settings or Settings()
    created_paths: list[Path] = []

    for path in active_settings.all_local_directories:
        path.mkdir(parents=True, exist_ok=True)
        created_paths.append(path)

    return created_paths
