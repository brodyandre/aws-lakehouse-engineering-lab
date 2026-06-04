#!/usr/bin/env python3
"""Ativa blocos de screenshots no README quando os arquivos existirem."""

from __future__ import annotations

import re
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
README_PATH = PROJECT_ROOT / "README.md"

SCREENSHOT_BLOCK_RE = re.compile(
    r"<!--\n"
    r"Screenshot sugerido: (?P<path>[^\n]+)\n\n"
    r"(?P<body>.*?)\n"
    r"-->",
    re.DOTALL,
)


def enable_screenshots(readme_text: str) -> tuple[str, int, int]:
    enabled_blocks = 0
    missing_blocks = 0

    def replacer(match: re.Match[str]) -> str:
        nonlocal enabled_blocks, missing_blocks

        relative_path = match.group("path").strip()
        body = match.group("body").strip("\n")
        screenshot_path = PROJECT_ROOT / relative_path

        if screenshot_path.is_file():
            enabled_blocks += 1
            return f"{body}\n"

        missing_blocks += 1
        return match.group(0)

    updated_text = SCREENSHOT_BLOCK_RE.sub(replacer, readme_text)
    return updated_text, enabled_blocks, missing_blocks


def main() -> int:
    original_text = README_PATH.read_text(encoding="utf-8")
    updated_text, enabled_blocks, missing_blocks = enable_screenshots(original_text)

    if updated_text != original_text:
        README_PATH.write_text(updated_text, encoding="utf-8")

    print(
        "README screenshots sync: "
        f"enabled={enabled_blocks}, missing={missing_blocks}, "
        f"changed={'yes' if updated_text != original_text else 'no'}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
