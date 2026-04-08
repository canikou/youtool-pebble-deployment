from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_STABLE_LIST_KEYS = ("id", "name", "item_name", "user_id", "receipt_id", "action")


def _sort_key(value: Any) -> tuple[str, str]:
    return (type(value).__name__, repr(value))


def _list_sort_key(item: Any) -> tuple[Any, ...] | tuple[str, str]:
    if isinstance(item, dict):
        for key in _STABLE_LIST_KEYS:
            if key in item:
                return (key, repr(item.get(key)))
    if isinstance(item, str):
        return ("str", item)
    return _sort_key(item)


def normalize_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: normalize_json_value(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        normalized = [normalize_json_value(item) for item in value]
        if not normalized:
            return []
        if all(isinstance(item, str) for item in normalized):
            return sorted(normalized)
        if all(isinstance(item, (str, int, float, bool)) or item is None for item in normalized):
            return sorted(normalized, key=_sort_key)
        if all(isinstance(item, dict) for item in normalized):
            return sorted(normalized, key=_list_sort_key)
        return normalized
    return value


def canonical_json_text(value: Any) -> str:
    normalized = normalize_json_value(value)
    return json.dumps(normalized, indent=2, ensure_ascii=False, sort_keys=True) + "\n"


def load_json_file(path: Path | str) -> Any:
    path = Path(path)
    return json.loads(path.read_text(encoding="utf-8"))
