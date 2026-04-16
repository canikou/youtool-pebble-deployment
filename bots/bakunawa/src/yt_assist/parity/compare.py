from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .snapshot_normalize import canonical_json_text, normalize_json_value


@dataclass(slots=True)
class CompareResult:
    baseline_path: Path
    candidate_path: Path
    equal: bool
    mismatches: list[str]

    def render_text(self) -> str:
        lines = [
            f"baseline: {self.baseline_path}",
            f"candidate: {self.candidate_path}",
            f"equal: {'yes' if self.equal else 'no'}",
        ]
        if self.mismatches:
            lines.append("mismatches:")
            lines.extend(f"- {mismatch}" for mismatch in self.mismatches)
        return "\n".join(lines)


def _diff_values(baseline: Any, candidate: Any, path: str = "$") -> list[str]:
    if type(baseline) is not type(candidate):
        return [f"{path}: type mismatch {type(baseline).__name__} != {type(candidate).__name__}"]

    if isinstance(baseline, dict):
        mismatches: list[str] = []
        baseline_keys = set(baseline)
        candidate_keys = set(candidate)
        missing = sorted(baseline_keys - candidate_keys)
        extra = sorted(candidate_keys - baseline_keys)
        if missing:
            mismatches.append(f"{path}: missing keys {missing}")
        if extra:
            mismatches.append(f"{path}: extra keys {extra}")
        for key in sorted(baseline_keys & candidate_keys):
            mismatches.extend(_diff_values(baseline[key], candidate[key], f"{path}.{key}"))
        return mismatches

    if isinstance(baseline, list):
        mismatches = []
        if len(baseline) != len(candidate):
            mismatches.append(f"{path}: length mismatch {len(baseline)} != {len(candidate)}")
        for index, (left, right) in enumerate(zip(baseline, candidate, strict=False)):
            mismatches.extend(_diff_values(left, right, f"{path}[{index}]"))
        return mismatches

    if baseline != candidate:
        return [f"{path}: value mismatch {baseline!r} != {candidate!r}"]
    return []


def compare_snapshots(baseline_path: Path | str, candidate_path: Path | str) -> CompareResult:
    baseline_path = Path(baseline_path)
    candidate_path = Path(candidate_path)
    baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
    candidate = json.loads(candidate_path.read_text(encoding="utf-8"))

    normalized_baseline = normalize_json_value(baseline)
    normalized_candidate = normalize_json_value(candidate)
    mismatches = _diff_values(normalized_baseline, normalized_candidate)
    return CompareResult(
        baseline_path=baseline_path,
        candidate_path=candidate_path,
        equal=not mismatches,
        mismatches=mismatches,
    )


def compare_snapshot_text(baseline_path: Path | str, candidate_path: Path | str) -> str:
    result = compare_snapshots(baseline_path, candidate_path)
    return result.render_text()


def snapshot_text_for_value(value: Any) -> str:
    return canonical_json_text(value)
