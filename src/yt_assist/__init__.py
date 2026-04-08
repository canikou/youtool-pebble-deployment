"""YTAssist Python port."""

from __future__ import annotations

from pathlib import Path
from typing import Any

__all__ = ["build_runtime_context", "run"]


def build_runtime_context(config_path: Path | None = None) -> Any:
    from .app import build_runtime_context as _build_runtime_context

    return _build_runtime_context(config_path)


def run(config_path: Path | None = None) -> Any:
    from .app import run as _run

    return _run(config_path)
