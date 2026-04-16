from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parent
GRACEFUL_STOP_SECONDS = 30
FORCED_STOP_SECONDS = 10


@dataclass(frozen=True)
class BotSpec:
    name: str
    root: Path
    stop_file: Path

    @property
    def src_dir(self) -> Path:
        return self.root / "src"


BOTS = (
    BotSpec(
        name="YouTool",
        root=ROOT,
        stop_file=ROOT / "data" / "yt-assist.stop",
    ),
    BotSpec(
        name="Bakunawa Mech",
        root=ROOT / "bots" / "bakunawa",
        stop_file=ROOT / "bots" / "bakunawa" / "data" / "bakunawa-mech.stop",
    ),
)

shutdown_signal: str | None = None


def build_child_env(spec: BotSpec) -> dict[str, str]:
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        str(spec.src_dir)
        if not existing_pythonpath
        else str(spec.src_dir) + os.pathsep + existing_pythonpath
    )
    env["PYTHONUNBUFFERED"] = "1"
    return env


def ensure_layout(spec: BotSpec) -> None:
    missing: list[Path] = []
    for path in (spec.root, spec.src_dir, spec.root / "config"):
        if not path.exists():
            missing.append(path)
    if missing:
        joined = ", ".join(str(path) for path in missing)
        raise FileNotFoundError(f"{spec.name} deployment is incomplete: {joined}")


def start_bot(spec: BotSpec) -> subprocess.Popen[bytes]:
    ensure_layout(spec)
    for directory in ("data", "logs", "exports", "import"):
        (spec.root / directory).mkdir(parents=True, exist_ok=True)
    spec.stop_file.unlink(missing_ok=True)

    print(f"Starting {spec.name} from {spec.root}", flush=True)
    return subprocess.Popen(
        [sys.executable, "-u", "-m", "yt_assist"],
        cwd=spec.root,
        env=build_child_env(spec),
    )


def write_stop_file(spec: BotSpec, reason: str) -> None:
    try:
        spec.stop_file.parent.mkdir(parents=True, exist_ok=True)
        spec.stop_file.write_text(
            f"{time.strftime('%Y-%m-%dT%H:%M:%S')} {reason}\n",
            encoding="utf-8",
        )
    except OSError as error:
        print(f"Failed to write stop signal for {spec.name}: {error}", flush=True)


def request_shutdown(reason: str) -> None:
    global shutdown_signal
    if shutdown_signal is None:
        shutdown_signal = reason
        print(f"PebbleHost requested shutdown via {reason}. Stopping all bots...", flush=True)
    for spec in BOTS:
        write_stop_file(spec, reason)


def handle_signal(signum: int, _frame: object) -> None:
    request_shutdown(signal.Signals(signum).name)


def wait_for_exit(
    processes: dict[BotSpec, subprocess.Popen[bytes]],
    timeout_seconds: int,
) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if all(process.poll() is not None for process in processes.values()):
            return True
        time.sleep(0.5)
    return all(process.poll() is not None for process in processes.values())


def terminate_remaining(processes: dict[BotSpec, subprocess.Popen[bytes]], reason: str) -> None:
    request_shutdown(reason)
    if wait_for_exit(processes, GRACEFUL_STOP_SECONDS):
        return

    for spec, process in processes.items():
        if process.poll() is None:
            print(f"{spec.name} did not stop cleanly; sending terminate.", flush=True)
            process.terminate()

    if wait_for_exit(processes, FORCED_STOP_SECONDS):
        return

    for spec, process in processes.items():
        if process.poll() is None:
            print(f"{spec.name} did not terminate; killing process.", flush=True)
            process.kill()


def check_layout() -> int:
    for spec in BOTS:
        ensure_layout(spec)
        config_path = spec.root / "config" / "app.toml"
        config_status = "present" if config_path.exists() else "missing runtime config"
        print(f"{spec.name}: {spec.root} ({config_status})", flush=True)
    return 0


def run() -> int:
    for handled_signal in (signal.SIGINT, signal.SIGTERM):
        signal.signal(handled_signal, handle_signal)

    processes = {spec: start_bot(spec) for spec in BOTS}

    try:
        while True:
            if shutdown_signal is not None:
                terminate_remaining(processes, shutdown_signal)
                return max(
                    process.returncode if process.returncode is not None else 1
                    for process in processes.values()
                )

            for spec, process in processes.items():
                exit_code = process.poll()
                if exit_code is not None:
                    print(f"{spec.name} exited with status {exit_code}; stopping remaining bots.", flush=True)
                    terminate_remaining(processes, f"{spec.name} exited")
                    return exit_code

            time.sleep(1)
    finally:
        terminate_remaining(processes, "launcher exit")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in {"--check", "check"}:
        raise SystemExit(check_layout())
    raise SystemExit(run())
