"""Logger compartilhado para todos os scripts do projeto."""

from __future__ import annotations

import logging
import os
import sys

DEFAULT_LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def _normalize_level(level: str | int | None = None) -> int:
    if isinstance(level, int):
        return level

    raw_level = (level or os.getenv("LOG_LEVEL", "INFO")).strip().upper()
    normalized = logging.getLevelName(raw_level)
    if isinstance(normalized, int):
        return normalized
    raise ValueError(f"Nível de log inválido: {raw_level}")


def configure_logging(level: str | int | None = None, force: bool = False) -> logging.Logger:
    """Configura o logger raiz com formatação padronizada do projeto."""

    root_logger = logging.getLogger()
    resolved_level = _normalize_level(level)

    if force:
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)

    if not root_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(DEFAULT_LOG_FORMAT, DEFAULT_DATE_FORMAT))
        root_logger.addHandler(handler)
    else:
        for handler in root_logger.handlers:
            handler.setFormatter(logging.Formatter(DEFAULT_LOG_FORMAT, DEFAULT_DATE_FORMAT))

    root_logger.setLevel(resolved_level)
    return root_logger


def get_logger(name: str | None = None, level: str | int | None = None) -> logging.Logger:
    """Retorna um logger já alinhado com a formatação padrão do projeto."""

    configure_logging(level=level)
    return logging.getLogger(name or "aws-lakehouse-engineering-lab")


__all__ = ["configure_logging", "get_logger"]
