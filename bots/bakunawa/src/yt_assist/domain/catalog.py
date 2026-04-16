from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .serialization import to_primitive


def normalize_name(input: str) -> str:
    return input.strip().lower()


@dataclass(slots=True)
class CatalogItem:
    name: str
    aliases: list[str] = field(default_factory=list)
    unit_price: int = 0
    bulk_price: int | None = None
    bulk_min_qty: int | None = None
    unit_cost: int | None = None
    category: str | None = None
    price_pending: bool = False
    active: bool = True

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> CatalogItem:
        raw_unit_price = data.get("unit_price")
        unit_price = 0 if raw_unit_price in {None, ""} else int(raw_unit_price)
        return cls(
            name=str(data["name"]),
            aliases=[str(alias) for alias in data.get("aliases", [])],
            unit_price=unit_price,
            bulk_price=None if data.get("bulk_price") is None else int(data["bulk_price"]),
            bulk_min_qty=None if data.get("bulk_min_qty") is None else int(data["bulk_min_qty"]),
            unit_cost=None if data.get("unit_cost") is None else int(data["unit_cost"]),
            category=None if data.get("category") is None else str(data["category"]),
            price_pending=bool(data.get("price_pending", raw_unit_price in {None, ""})),
            active=bool(data.get("active", True)),
        )


@dataclass(slots=True)
class CatalogFile:
    items: list[CatalogItem]

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> CatalogFile:
        return cls(items=[CatalogItem.from_mapping(item) for item in data.get("items", [])])

    def to_mapping(self) -> dict[str, Any]:
        return to_primitive(self)


@dataclass(slots=True)
class Catalog:
    items: list[CatalogItem]
    _lookup: dict[str, int] = field(default_factory=dict, repr=False)

    @classmethod
    def load_from(cls, path: Path | str) -> Catalog:
        path = Path(path)
        raw = path.read_text(encoding="utf-8")
        file = CatalogFile.from_mapping(tomllib.loads(raw))
        return cls.from_items(file.items)

    @classmethod
    def from_items(cls, items: list[CatalogItem]) -> Catalog:
        lookup: dict[str, int] = {}
        for index, item in enumerate(items):
            if not item.name.strip():
                raise ValueError(f"catalog item at index {index} has a blank name")

            lookup[normalize_name(item.name)] = index
            for alias in item.aliases:
                if alias.strip():
                    lookup[normalize_name(alias)] = index

        return cls(items=list(items), _lookup=lookup)

    def items_view(self) -> list[CatalogItem]:
        return list(self.items)

    def find_item(self, input: str) -> CatalogItem | None:
        index = self._lookup.get(normalize_name(input))
        if index is None:
            return None
        item = self.items[index]
        return item if item.active else None

    def snapshot(self) -> CatalogFile:
        return CatalogFile(items=list(self.items))

    def save_to(self, path: Path | str) -> None:
        path = Path(path)
        lines: list[str] = []
        for item in self.items:
            lines.append("[[items]]")
            lines.append(f'name = "{item.name}"')
            if item.aliases:
                aliases = ", ".join(f'"{alias}"' for alias in item.aliases)
                lines.append(f"aliases = [{aliases}]")
            lines.append(f"unit_price = {item.unit_price}")
            if item.bulk_price is not None:
                lines.append(f"bulk_price = {item.bulk_price}")
            if item.bulk_min_qty is not None:
                lines.append(f"bulk_min_qty = {item.bulk_min_qty}")
            if item.unit_cost is not None:
                lines.append(f"unit_cost = {item.unit_cost}")
            if item.category is not None:
                lines.append(f'category = "{item.category}"')
            if item.price_pending:
                lines.append("price_pending = true")
            lines.append(f"active = {'true' if item.active else 'false'}")
            lines.append("")
        path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")

