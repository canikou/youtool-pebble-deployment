"""Single-instance runtime guard for the Discord bot."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

if os.name == "nt":  # pragma: no cover - imported conditionally by platform
    import msvcrt
else:  # pragma: no cover - imported conditionally by platform
    import fcntl


class SingleInstanceError(RuntimeError):
    """Raised when another bot instance already holds the runtime lock."""

    def __init__(self, lock_path: Path, existing_pid: int | None = None) -> None:
        self.lock_path = lock_path
        self.existing_pid = existing_pid
        message = f"another yt-assist instance is already running (lock={lock_path}"
        if existing_pid is not None:
            message += f", pid={existing_pid}"
        message += ")"
        super().__init__(message)


@dataclass(slots=True)
class SingleInstanceGuard:
    """Holds an exclusive lock for the lifetime of a running bot."""

    lock_path: Path
    pid_path: Path
    _handle: BinaryIO
    _released: bool = False

    @classmethod
    def acquire(cls, lock_path: Path, pid_path: Path) -> SingleInstanceGuard:
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        pid_path.parent.mkdir(parents=True, exist_ok=True)

        handle = lock_path.open("a+b")
        _ensure_lockfile_content(handle)
        try:
            cls._lock_handle(handle)
        except OSError as error:
            handle.close()
            raise SingleInstanceError(lock_path, _read_pid_file(pid_path)) from error

        pid_path.write_text(f"{os.getpid()}\n", encoding="utf-8")
        return cls(lock_path=lock_path, pid_path=pid_path, _handle=handle)

    def release(self) -> None:
        if self._released:
            return
        self._released = True

        try:
            try:
                current_pid = _read_pid_file(self.pid_path)
                if current_pid == os.getpid() and self.pid_path.exists():
                    self.pid_path.unlink()
            finally:
                self._unlock_handle(self._handle)
        finally:
            self._handle.close()

    @staticmethod
    def _lock_handle(handle: BinaryIO) -> None:
        if os.name == "nt":
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            return
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    @staticmethod
    def _unlock_handle(handle: BinaryIO) -> None:
        if os.name == "nt":
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            return
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _ensure_lockfile_content(handle: BinaryIO) -> None:
    handle.seek(0, os.SEEK_END)
    if handle.tell() == 0:
        handle.write(b"1")
        handle.flush()
    handle.seek(0)


def _read_pid_file(pid_path: Path) -> int | None:
    try:
        raw = pid_path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    except OSError:
        return None
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None
