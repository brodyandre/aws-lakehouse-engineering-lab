"""Helpers compartilhados para sessões Spark locais, clusterizadas e via Spark Connect."""

from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import SparkSession

from src.config.settings import Settings, local_spark_runtime_conf, prepare_local_spark_environment


def create_spark_session(
    *,
    settings: Settings | None = None,
    app_name: str | None = None,
    master: str | None = None,
    remote: str | None = None,
    extra_conf: Mapping[str, str] | None = None,
) -> SparkSession:
    """Cria uma SparkSession compatível com execução local, cluster ou Spark Connect."""

    active_settings = settings or Settings()
    resolved_app_name = app_name or active_settings.spark.app_name
    resolved_master = master or active_settings.spark.master
    resolved_remote = (remote or active_settings.spark.remote or "").strip()

    builder = SparkSession.builder.appName(resolved_app_name)
    if resolved_remote:
        builder = builder.remote(resolved_remote)
    else:
        prepare_local_spark_environment(resolved_master)
        builder = builder.master(resolved_master)

    for key, value in active_settings.spark_conf.items():
        if key in {"spark.app.name", "spark.master"}:
            continue
        builder = builder.config(key, value)

    if not resolved_remote:
        for key, value in local_spark_runtime_conf(resolved_master).items():
            builder = builder.config(key, value)

    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)

    return builder.getOrCreate()


__all__ = ["create_spark_session"]
