"""Configurações centrais do laboratório AWS Lakehouse Engineering Lab."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - compatibilidade quando dotenv não está instalado
    load_dotenv = None


VALID_LAYERS = ("raw", "bronze", "silver", "gold")
VALID_REPORT_CATEGORIES = ("pipeline_runs", "data_quality", "finops", "observability", "query")
TRUE_VALUES = {"1", "true", "yes", "on"}
FALSE_VALUES = {"0", "false", "no", "off"}


def _get_env(names: tuple[str, ...], default: str) -> str:
    for name in names:
        value = os.getenv(name)
        if value not in (None, ""):
            return value
    return default


def _get_int_env(names: tuple[str, ...], default: int) -> int:
    raw_value = _get_env(names, str(default))
    try:
        return int(raw_value)
    except ValueError as exc:
        joined_names = ", ".join(names)
        raise ValueError(f"Variável de ambiente inválida para inteiro: {joined_names}") from exc


def _get_optional_env(names: tuple[str, ...]) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value is None:
            continue
        stripped_value = value.strip()
        if stripped_value:
            return stripped_value
    return None


def _get_bool_env(names: tuple[str, ...], default: bool) -> bool:
    raw_value = _get_env(names, str(default).lower()).strip().lower()
    if raw_value in TRUE_VALUES:
        return True
    if raw_value in FALSE_VALUES:
        return False
    joined_names = ", ".join(names)
    raise ValueError(f"Variável de ambiente inválida para booleano: {joined_names}")


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _is_local_spark_master(master: str | None) -> bool:
    return bool(master) and str(master).strip().lower().startswith("local")


if load_dotenv is not None:
    load_dotenv(_project_root() / ".env", override=False)


@dataclass(frozen=True, slots=True)
class MinIOSettings:
    """Configuração de acesso ao MinIO usado como S3 local."""

    endpoint: str = field(
        default_factory=lambda: _get_env(
            ("MINIO_ENDPOINT", "S3_ENDPOINT_URL", "S3_ENDPOINT_URL_INTERNAL"),
            "http://localhost:9000",
        )
    )
    access_key: str = field(
        default_factory=lambda: _get_env(("MINIO_ACCESS_KEY", "AWS_ACCESS_KEY_ID"), "minioadmin")
    )
    secret_key: str = field(
        default_factory=lambda: _get_env(
            ("MINIO_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"),
            "minioadmin123",
        )
    )
    raw_bucket: str = field(default_factory=lambda: _get_env(("MINIO_RAW_BUCKET",), "raw"))
    bronze_bucket: str = field(default_factory=lambda: _get_env(("MINIO_BRONZE_BUCKET",), "bronze"))
    silver_bucket: str = field(default_factory=lambda: _get_env(("MINIO_SILVER_BUCKET",), "silver"))
    gold_bucket: str = field(default_factory=lambda: _get_env(("MINIO_GOLD_BUCKET",), "gold"))

    def bucket_for_layer(self, layer: str) -> str:
        bucket_map = {
            "raw": self.raw_bucket,
            "bronze": self.bronze_bucket,
            "silver": self.silver_bucket,
            "gold": self.gold_bucket,
        }
        if layer not in bucket_map:
            raise ValueError(f"Camada inválida: {layer}. Use uma das opções: {VALID_LAYERS}")
        return bucket_map[layer]


@dataclass(frozen=True, slots=True)
class SparkSettings:
    """Configuração base do Spark para execução local ou em cluster local."""

    app_name: str = field(
        default_factory=lambda: _get_env(("SPARK_APP_NAME",), "aws-lakehouse-engineering-lab")
    )
    master: str = field(
        default_factory=lambda: _get_env(("SPARK_MASTER", "SPARK_MASTER_URL"), "local[*]")
    )
    shuffle_partitions: int = field(
        default_factory=lambda: _get_int_env(("SPARK_SHUFFLE_PARTITIONS",), 8)
    )
    adaptive_query_execution: bool = field(
        default_factory=lambda: _get_bool_env(("SPARK_ADAPTIVE_QUERY_EXECUTION",), True)
    )
    timezone: str = field(default_factory=lambda: _get_env(("SPARK_TIMEZONE",), "UTC"))
    remote: str | None = field(default_factory=lambda: _get_optional_env(("SPARK_REMOTE",)))

    def as_spark_conf(self) -> dict[str, str]:
        return {
            "spark.app.name": self.app_name,
            "spark.master": self.master,
            "spark.sql.shuffle.partitions": str(self.shuffle_partitions),
            "spark.sql.adaptive.enabled": str(self.adaptive_query_execution).lower(),
            "spark.sql.session.timeZone": self.timezone,
        }


@dataclass(slots=True)
class Settings:
    """Camada de configuração central do projeto."""

    project_name: str = field(
        default_factory=lambda: _get_env(
            ("COMPOSE_PROJECT_NAME", "PROJECT_NAME"),
            "aws-lakehouse-engineering-lab",
        )
    )
    environment: str = field(default_factory=lambda: _get_env(("ENVIRONMENT",), "local"))
    aws_region: str = field(default_factory=lambda: _get_env(("AWS_REGION",), "us-east-1"))
    log_level: str = field(default_factory=lambda: _get_env(("LOG_LEVEL",), "INFO"))
    project_root: Path = field(default_factory=_project_root)
    minio: MinIOSettings = field(default_factory=MinIOSettings)
    spark: SparkSettings = field(default_factory=SparkSettings)

    @property
    def data_root(self) -> Path:
        return self.project_root / "data"

    @property
    def raw_data_path(self) -> Path:
        return self.data_root / "raw"

    @property
    def bronze_data_path(self) -> Path:
        return self.data_root / "bronze"

    @property
    def silver_data_path(self) -> Path:
        return self.data_root / "silver"

    @property
    def gold_data_path(self) -> Path:
        return self.data_root / "gold"

    @property
    def serving_data_path(self) -> Path:
        return self.data_root / "serving"

    def layer_path(self, layer: str) -> Path:
        layer_map = {
            "raw": self.raw_data_path,
            "bronze": self.bronze_data_path,
            "silver": self.silver_data_path,
            "gold": self.gold_data_path,
        }
        if layer not in layer_map:
            raise ValueError(f"Camada inválida: {layer}. Use uma das opções: {VALID_LAYERS}")
        return layer_map[layer]

    @property
    def reports_root(self) -> Path:
        return self.project_root / "reports"

    @property
    def pipeline_runs_report_path(self) -> Path:
        return self.reports_root / "pipeline_runs"

    @property
    def data_quality_report_path(self) -> Path:
        return self.reports_root / "data_quality"

    @property
    def finops_report_path(self) -> Path:
        return self.reports_root / "finops"

    @property
    def observability_report_path(self) -> Path:
        return self.reports_root / "observability"

    @property
    def query_report_path(self) -> Path:
        return self.reports_root / "query"

    def report_path(self, category: str) -> Path:
        report_map = {
            "pipeline_runs": self.pipeline_runs_report_path,
            "data_quality": self.data_quality_report_path,
            "finops": self.finops_report_path,
            "observability": self.observability_report_path,
            "query": self.query_report_path,
        }
        if category not in report_map:
            raise ValueError(
                f"Categoria de relatório inválida: {category}. "
                f"Use uma das opções: {VALID_REPORT_CATEGORIES}"
            )
        return report_map[category]

    @property
    def s3_endpoint_url(self) -> str:
        return self.minio.endpoint

    @property
    def aws_access_key_id(self) -> str:
        return self.minio.access_key

    @property
    def aws_secret_access_key(self) -> str:
        return self.minio.secret_key

    @property
    def minio_bucket_map(self) -> dict[str, str]:
        return {layer: self.minio.bucket_for_layer(layer) for layer in VALID_LAYERS}

    @property
    def spark_conf(self) -> dict[str, str]:
        spark_conf = self.spark.as_spark_conf()
        spark_conf.update(
            {
                "spark.hadoop.fs.s3a.endpoint": self.minio.endpoint,
                "spark.hadoop.fs.s3a.access.key": self.minio.access_key,
                "spark.hadoop.fs.s3a.secret.key": self.minio.secret_key,
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": str(
                    self.minio.endpoint.startswith("https://")
                ).lower(),
            }
        )
        return spark_conf

    @property
    def all_local_directories(self) -> list[Path]:
        return [
            self.raw_data_path,
            self.bronze_data_path,
            self.silver_data_path,
            self.gold_data_path,
            self.pipeline_runs_report_path,
            self.data_quality_report_path,
            self.finops_report_path,
            self.observability_report_path,
            self.query_report_path,
            self.serving_data_path,
        ]


settings = Settings()


def local_spark_runtime_env(master: str | None) -> dict[str, str]:
    if not _is_local_spark_master(master):
        return {}

    local_ip = _get_env(("SPARK_LOCAL_IP",), "127.0.0.1")
    local_hostname = _get_env(("SPARK_LOCAL_HOSTNAME",), "localhost")
    return {
        "SPARK_LOCAL_IP": local_ip,
        "SPARK_LOCAL_HOSTNAME": local_hostname,
    }


def local_spark_runtime_conf(master: str | None) -> dict[str, str]:
    if not _is_local_spark_master(master):
        return {}

    local_ip = _get_env(("SPARK_LOCAL_IP",), "127.0.0.1")
    return {
        "spark.driver.host": local_ip,
        "spark.driver.bindAddress": "127.0.0.1",
    }


def prepare_local_spark_environment(master: str | None) -> None:
    for key, value in local_spark_runtime_env(master).items():
        os.environ.setdefault(key, value)


__all__ = [
    "MinIOSettings",
    "Settings",
    "SparkSettings",
    "VALID_LAYERS",
    "VALID_REPORT_CATEGORIES",
    "local_spark_runtime_conf",
    "local_spark_runtime_env",
    "prepare_local_spark_environment",
    "settings",
]
