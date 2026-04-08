"""Command-line entrypoints for the Python port."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from yt_assist.bot.runtime import run_console, run_discord
from yt_assist.parity.compare import compare_snapshots
from yt_assist.parity.harness import run_harness


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="yt-assist")
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run the Discord bot.")
    run_parser.add_argument(
        "--config",
        type=Path,
        default=Path("config") / "app.toml",
        help="Path to the runtime TOML config.",
    )

    console_parser = subparsers.add_parser("console", help="Run the local interactive parity console.")
    console_parser.add_argument(
        "--config",
        type=Path,
        default=Path("config") / "app.toml",
        help="Path to the runtime TOML config.",
    )
    console_parser.add_argument(
        "--command",
        type=str,
        dest="input_command",
        default=None,
        help="Run one command non-interactively and exit.",
    )

    harness_parser = subparsers.add_parser("parity-harness", help="Run the Python parity harness.")
    harness_parser.add_argument(
        "--workspace-root",
        type=Path,
        default=Path("target") / "harness-output",
        help="Directory where harness artifacts should be written.",
    )

    compare_parser = subparsers.add_parser(
        "compare-parity",
        help="Compare two parity harness snapshot JSON files.",
    )
    compare_parser.add_argument("baseline", type=Path, help="Baseline snapshot path.")
    compare_parser.add_argument("candidate", type=Path, help="Candidate snapshot path.")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    command = args.command or "run"
    default_config_path = Path("config") / "app.toml"
    config_path = getattr(args, "config", default_config_path)

    if command == "run":
        return asyncio.run(run_discord(config_path))
    if command == "console":
        return asyncio.run(run_console(config_path, args.input_command))
    if command == "parity-harness":
        report = asyncio.run(run_harness(args.workspace_root))
        print(report.render_text())
        return 0 if report.all_passed() else 1
    if command == "compare-parity":
        result = compare_snapshots(args.baseline, args.candidate)
        print(result.render_text())
        return 0 if result.equal else 1

    parser.error(f"unknown command: {command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
