"""Cálculo de custo simulado para execuções do laboratório."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ResourceUsage:
    storage_gb_month: float
    vcpu_minutes: float
    run_count: int
    failed_runs: int = 0


@dataclass(frozen=True, slots=True)
class SimulatedCostModel:
    storage_cost_per_gb_month: float = 0.023
    compute_cost_per_vcpu_minute: float = 0.0008
    orchestration_cost_per_run: float = 0.001
    failure_penalty_per_run: float = 0.0025

    def estimate_total(self, usage: ResourceUsage) -> float:
        total = (
            usage.storage_gb_month * self.storage_cost_per_gb_month
            + usage.vcpu_minutes * self.compute_cost_per_vcpu_minute
            + usage.run_count * self.orchestration_cost_per_run
            + usage.failed_runs * self.failure_penalty_per_run
        )
        return round(total, 4)
