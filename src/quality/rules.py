"""Catálogo inicial de regras de qualidade por camada."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class QualityRule:
    name: str
    layer: str
    severity: str
    description: str


DEFAULT_QUALITY_RULES = [
    QualityRule(
        name="raw_file_presence",
        layer="raw",
        severity="high",
        description="Garante que pelo menos um arquivo de origem foi disponibilizado.",
    ),
    QualityRule(
        name="bronze_required_columns",
        layer="bronze",
        severity="high",
        description="Valida a presença das colunas mínimas definidas para a origem.",
    ),
    QualityRule(
        name="silver_deduplication",
        layer="silver",
        severity="high",
        description="Assegura que a entidade conformada não possui duplicidades indevidas.",
    ),
    QualityRule(
        name="gold_fact_dimension_integrity",
        layer="gold",
        severity="critical",
        description="Confere integridade entre fatos e dimensões na camada analítica.",
    ),
]


def rules_by_layer(layer: str) -> list[QualityRule]:
    return [rule for rule in DEFAULT_QUALITY_RULES if rule.layer == layer]
