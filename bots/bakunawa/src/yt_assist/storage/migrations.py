"""SQLite migration helpers."""

from __future__ import annotations

from pathlib import Path

import aiosqlite


async def run_migrations(connection: aiosqlite.Connection, migrations_dir: Path) -> None:
    for path in sorted(migrations_dir.glob("*.sql")):
        await connection.executescript(path.read_text(encoding="utf-8"))
    await connection.commit()
