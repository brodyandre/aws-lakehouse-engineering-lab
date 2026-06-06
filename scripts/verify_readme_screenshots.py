"""Valida automaticamente coerência básica das screenshots do README."""

from __future__ import annotations

import json
import math
import statistics
import struct
import zlib
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
README_SCREENSHOTS_ROOT = PROJECT_ROOT / "assets" / "screenshots" / "readme"


@dataclass(frozen=True)
class ImageStats:
    path: Path
    width: int
    height: int
    size_kb: float
    mean_luma: float
    std_luma: float
    edge_density: float


@dataclass(frozen=True)
class VerificationResult:
    name: str
    ok: bool
    details: str


def _paeth(a: int, b: int, c: int) -> int:
    p = a + b - c
    pa = abs(p - a)
    pb = abs(p - b)
    pc = abs(p - c)
    if pa <= pb and pa <= pc:
        return a
    if pb <= pc:
        return b
    return c


def _read_png_pixels(path: Path) -> tuple[int, int, int, list[bytearray]]:
    data = path.read_bytes()
    if data[:8] != b"\x89PNG\r\n\x1a\n":
        raise ValueError(f"{path} is not a PNG file.")

    cursor = 8
    width = height = bit_depth = color_type = None
    idat = bytearray()

    while cursor < len(data):
        length = struct.unpack(">I", data[cursor : cursor + 4])[0]
        chunk_type = data[cursor + 4 : cursor + 8]
        chunk_data = data[cursor + 8 : cursor + 8 + length]
        cursor += 12 + length

        if chunk_type == b"IHDR":
            width, height, bit_depth, color_type, *_ = struct.unpack(">IIBBBBB", chunk_data)
        elif chunk_type == b"IDAT":
            idat.extend(chunk_data)
        elif chunk_type == b"IEND":
            break

    if bit_depth != 8 or color_type not in {2, 6}:
        raise ValueError(
            f"{path} uses unsupported PNG format (bit_depth={bit_depth}, color_type={color_type})."
        )

    channels = 4 if color_type == 6 else 3
    bytes_per_pixel = channels
    raw = zlib.decompress(bytes(idat))
    stride = width * bytes_per_pixel
    rows: list[bytearray] = []
    cursor = 0
    previous = bytearray(stride)

    for _ in range(height):
        filter_type = raw[cursor]
        cursor += 1
        scanline = bytearray(raw[cursor : cursor + stride])
        cursor += stride

        if filter_type == 1:
            for index in range(stride):
                scanline[index] = (
                    scanline[index]
                    + (scanline[index - bytes_per_pixel] if index >= bytes_per_pixel else 0)
                ) & 255
        elif filter_type == 2:
            for index in range(stride):
                scanline[index] = (scanline[index] + previous[index]) & 255
        elif filter_type == 3:
            for index in range(stride):
                left = scanline[index - bytes_per_pixel] if index >= bytes_per_pixel else 0
                up = previous[index]
                scanline[index] = (scanline[index] + ((left + up) // 2)) & 255
        elif filter_type == 4:
            for index in range(stride):
                left = scanline[index - bytes_per_pixel] if index >= bytes_per_pixel else 0
                up = previous[index]
                up_left = previous[index - bytes_per_pixel] if index >= bytes_per_pixel else 0
                scanline[index] = (scanline[index] + _paeth(left, up, up_left)) & 255
        elif filter_type != 0:
            raise ValueError(f"{path} uses unsupported PNG filter {filter_type}.")

        rows.append(scanline)
        previous = scanline

    return width, height, channels, rows


def _luma(row: bytearray, index: int) -> float:
    return 0.2126 * row[index] + 0.7152 * row[index + 1] + 0.0722 * row[index + 2]


def analyze_png(path: Path) -> ImageStats:
    width, height, channels, rows = _read_png_pixels(path)
    total_pixels = width * height
    luminance_sum = 0.0
    luminance_sq_sum = 0.0
    edge_hits = 0
    edge_total = 0

    for row in rows:
        for index in range(0, len(row), channels):
            current_luma = _luma(row, index)
            luminance_sum += current_luma
            luminance_sq_sum += current_luma * current_luma

    mean_luma = luminance_sum / total_pixels
    variance = max((luminance_sq_sum / total_pixels) - (mean_luma * mean_luma), 0.0)
    std_luma = math.sqrt(variance)

    for row_index, row in enumerate(rows):
        for index in range(0, len(row) - channels, channels):
            if abs(_luma(row, index) - _luma(row, index + channels)) > 18:
                edge_hits += 1
            edge_total += 1

        if row_index == height - 1:
            continue

        next_row = rows[row_index + 1]
        for index in range(0, len(row), channels):
            if abs(_luma(row, index) - _luma(next_row, index)) > 18:
                edge_hits += 1
            edge_total += 1

    return ImageStats(
        path=path,
        width=width,
        height=height,
        size_kb=round(path.stat().st_size / 1024, 1),
        mean_luma=round(mean_luma, 2),
        std_luma=round(std_luma, 2),
        edge_density=round(edge_hits / edge_total, 4),
    )


def file_sha256(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()


def _must(condition: bool, success: str, failure: str) -> VerificationResult:
    return VerificationResult(
        name=success.split(":")[0],
        ok=condition,
        details=success if condition else failure,
    )


def verify() -> tuple[list[VerificationResult], dict[str, ImageStats]]:
    target_paths = {
        "runtime_current": README_SCREENSHOTS_ROOT
        / "runtime"
        / "08-readme-local-services-overview.png",
        "query_current": README_SCREENSHOTS_ROOT / "query" / "10-readme-trino-query-serving.png",
        "airflow_current": README_SCREENSHOTS_ROOT / "orchestration" / "06-readme-airflow-dag.png",
        "observability_current": README_SCREENSHOTS_ROOT
        / "observability"
        / "04-readme-observability-metrics.png",
        "data_quality_current": README_SCREENSHOTS_ROOT
        / "data-quality"
        / "03-readme-data-quality-report.png",
        "airflow_reference": README_SCREENSHOTS_ROOT
        / "orchestration"
        / "09-readme-airflow-run-success.png",
        "architecture_reference": README_SCREENSHOTS_ROOT
        / "architecture"
        / "01-readme-architecture-overview.png",
        "modeling_reference": README_SCREENSHOTS_ROOT
        / "modeling"
        / "02-readme-gold-star-schema-overview.png",
        "finops_reference": README_SCREENSHOTS_ROOT
        / "finops"
        / "05-readme-finops-cost-estimation.png",
    }

    stats = {name: analyze_png(path) for name, path in target_paths.items()}

    dark_reference_lumas = [
        stats["architecture_reference"].mean_luma,
        stats["modeling_reference"].mean_luma,
        stats["data_quality_current"].mean_luma,
        stats["finops_reference"].mean_luma,
    ]
    median_dark_luma = statistics.median(dark_reference_lumas)
    airflow_reference = stats["airflow_reference"]
    runtime_current = stats["runtime_current"]
    query_current = stats["query_current"]
    airflow_current = stats["airflow_current"]
    observability_current = stats["observability_current"]
    data_quality_current = stats["data_quality_current"]
    observability_hash = file_sha256(observability_current.path)
    data_quality_hash = file_sha256(data_quality_current.path)

    results = [
        _must(
            runtime_current.width == 1600 and runtime_current.height == 900,
            "runtime_dimensions: runtime com dimensão 1600x900.",
            (
                "runtime_dimensions: esperado 1600x900, encontrado "
                f"{runtime_current.width}x{runtime_current.height}."
            ),
        ),
        _must(
            query_current.width == 1600 and query_current.height == 900,
            "query_dimensions: query com dimensão 1600x900.",
            (
                "query_dimensions: esperado 1600x900, encontrado "
                f"{query_current.width}x{query_current.height}."
            ),
        ),
        _must(
            airflow_current.width == 1600 and airflow_current.height == 900,
            "airflow_dimensions: Airflow com dimensão 1600x900.",
            (
                "airflow_dimensions: esperado 1600x900, encontrado "
                f"{airflow_current.width}x{airflow_current.height}."
            ),
        ),
        _must(
            observability_current.width == 1600 and observability_current.height == 900,
            "observability_dimensions: observability com dimensão 1600x900.",
            (
                "observability_dimensions: esperado 1600x900, encontrado "
                f"{observability_current.width}x{observability_current.height}."
            ),
        ),
        _must(
            runtime_current.mean_luma <= 60,
            "runtime_theme: runtime está em faixa escura.",
            (
                "runtime_theme: brilho médio "
                f"{runtime_current.mean_luma} acima da faixa escura esperada."
            ),
        ),
        _must(
            query_current.mean_luma <= 70,
            "query_theme: query está em faixa escura.",
            (
                "query_theme: brilho médio "
                f"{query_current.mean_luma} acima da faixa escura esperada."
            ),
        ),
        _must(
            observability_current.mean_luma <= 70,
            "observability_theme: observability está em faixa escura.",
            (
                "observability_theme: brilho médio "
                f"{observability_current.mean_luma} acima da faixa escura esperada."
            ),
        ),
        _must(
            abs(runtime_current.mean_luma - median_dark_luma) <= 20,
            "runtime_consistency: runtime coerente com referências dark do README.",
            (
                "runtime_consistency: brilho "
                f"{runtime_current.mean_luma} distante da mediana dark "
                f"{median_dark_luma}."
            ),
        ),
        _must(
            abs(query_current.mean_luma - median_dark_luma) <= 20,
            "query_consistency: query coerente com referências dark do README.",
            (
                "query_consistency: brilho "
                f"{query_current.mean_luma} distante da mediana dark "
                f"{median_dark_luma}."
            ),
        ),
        _must(
            abs(observability_current.mean_luma - median_dark_luma) <= 20,
            "observability_consistency: observability coerente com referências dark do README.",
            (
                "observability_consistency: brilho "
                f"{observability_current.mean_luma} distante da mediana dark "
                f"{median_dark_luma}."
            ),
        ),
        _must(
            abs(airflow_current.mean_luma - airflow_reference.mean_luma) <= 20,
            "airflow_theme_match: Airflow coerente com a referência visual da mesma seção.",
            (
                "airflow_theme_match: brilho "
                f"{airflow_current.mean_luma} distante da referência "
                f"{airflow_reference.mean_luma}."
            ),
        ),
        _must(
            observability_hash != data_quality_hash,
            "observability_uniqueness: observability difere da imagem de Data Quality.",
            "observability_uniqueness: observability está idêntica à imagem de Data Quality.",
        ),
        _must(
            airflow_current.edge_density >= 0.025 and airflow_current.std_luma >= 20,
            "airflow_content_density: Airflow mostra conteúdo suficiente de UI.",
            (
                "airflow_content_density: edge_density="
                f"{airflow_current.edge_density}, std={airflow_current.std_luma} "
                "abaixo do mínimo esperado."
            ),
        ),
        _must(
            runtime_current.size_kb >= 100
            and query_current.size_kb >= 100
            and airflow_current.size_kb >= 80
            and observability_current.size_kb >= 100,
            "file_weight: screenshots possuem peso compatível com capturas completas.",
            (
                "file_weight: um ou mais arquivos parecem leves demais "
                "("
                f"runtime={runtime_current.size_kb}KB, "
                f"query={query_current.size_kb}KB, "
                f"observability={observability_current.size_kb}KB, "
                f"airflow={airflow_current.size_kb}KB"
                ")."
            ),
        ),
    ]

    return results, stats


def main() -> int:
    try:
        results, stats = verify()
    except Exception as error:  # pragma: no cover - script de operação
        print(f"[error] {error}")
        return 1

    print("Screenshot verification report")
    print("=" * 30)
    for key in ("runtime_current", "observability_current", "query_current", "airflow_current"):
        item = stats[key]
        print(
            f"- {item.path.relative_to(PROJECT_ROOT)}: "
            f"{item.width}x{item.height}, {item.size_kb}KB, "
            f"luma={item.mean_luma}, std={item.std_luma}, "
            f"edge={item.edge_density}"
        )

    print("\nChecks")
    failures = 0
    for result in results:
        status = "PASS" if result.ok else "FAIL"
        print(f"[{status}] {result.details}")
        if not result.ok:
            failures += 1

    summary = {
        "ok": failures == 0,
        "checks_passed": len(results) - failures,
        "checks_failed": failures,
    }
    print("\nSummary")
    print(json.dumps(summary, ensure_ascii=False))
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
