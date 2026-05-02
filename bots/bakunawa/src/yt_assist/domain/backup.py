from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

from .models import ExportBundle

EXPORT_SCHEMA_VERSION = 5


def _timestamp() -> datetime:
    return datetime.now(UTC)


def save_export(bundle: ExportBundle, export_dir: Path | str, label: str) -> Path:
    return save_export_at(bundle, export_dir, label, _timestamp())


async def save_export_async(bundle: ExportBundle, export_dir: Path | str, label: str) -> Path:
    return await asyncio.to_thread(save_export, bundle, export_dir, label)


def save_export_at(bundle: ExportBundle, export_dir: Path | str, label: str, timestamp: datetime) -> Path:
    export_dir = Path(export_dir)
    export_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"{label}-{timestamp.astimezone(UTC).strftime('%Y%m%d-%H%M%S')}.json"
    path = export_dir / file_name
    path.write_text(json.dumps(bundle.to_mapping(), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return path


def load_export(path: Path | str) -> ExportBundle:
    path = Path(path)
    raw = path.read_text(encoding="utf-8")
    return ExportBundle.from_mapping_or_json(raw)


async def load_export_async(path: Path | str) -> ExportBundle:
    return await asyncio.to_thread(load_export, path)

