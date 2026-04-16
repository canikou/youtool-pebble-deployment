from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .catalog import Catalog
from .models import DraftItem


def normalize_key(value: str) -> str:
    return value.strip().lower().replace("-", "_").replace(" ", "_")


@dataclass(slots=True)
class PackageDefinition:
    key: str
    label: str
    price_item: str | None = None
    price_items: dict[str, str] = field(default_factory=dict)
    price_choice: str | None = None
    variant_choice: str | None = None
    required_choices: list[str] = field(default_factory=list)
    base_materials: list[str] = field(default_factory=list)
    extra_materials: list[str] = field(default_factory=list)
    choice_material_groups: list[str] = field(default_factory=list)
    variant_materials: dict[str, list[str]] = field(default_factory=dict)
    count_materials: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, key: str, data: dict[str, Any]) -> PackageDefinition:
        return cls(
            key=key,
            label=str(data["label"]),
            price_item=None if data.get("price_item") is None else str(data["price_item"]),
            price_items={str(k): str(v) for k, v in data.get("price_items", {}).items()},
            price_choice=None if data.get("price_choice") is None else str(data["price_choice"]),
            variant_choice=None if data.get("variant_choice") is None else str(data["variant_choice"]),
            required_choices=[str(item) for item in data.get("required_choices", [])],
            base_materials=[str(item) for item in data.get("base_materials", [])],
            extra_materials=[str(item) for item in data.get("extra_materials", [])],
            choice_material_groups=[str(item) for item in data.get("choice_material_groups", [])],
            variant_materials={
                str(variant): [str(item) for item in materials]
                for variant, materials in data.get("materials", {}).items()
            },
            count_materials={str(k): str(v) for k, v in data.get("count_materials", {}).items()},
        )


@dataclass(slots=True)
class PackageCatalog:
    choices: dict[str, dict[str, str]]
    materials: dict[str, dict[str, str]]
    packages: dict[str, PackageDefinition]

    @classmethod
    def load_from(cls, path: Path | str) -> PackageCatalog:
        raw = Path(path).read_text(encoding="utf-8")
        data = tomllib.loads(raw)
        return cls.from_mapping(data)

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> PackageCatalog:
        choices = {
            str(group): {normalize_key(str(key)): str(value) for key, value in values.items()}
            for group, values in data.get("choices", {}).items()
        }
        materials = {
            str(group): {normalize_key(str(key)): str(value) for key, value in values.items()}
            for group, values in data.get("materials", {}).items()
        }
        packages = {
            normalize_key(str(key)): PackageDefinition.from_mapping(normalize_key(str(key)), value)
            for key, value in data.get("packages", {}).items()
        }
        return cls(choices=choices, materials=materials, packages=packages)

    def package_options(self) -> list[PackageDefinition]:
        preferred_order = {
            "full_performance_upgrade": 0,
            "full_cosmetics": 1,
            "full_tuning": 2,
            "full_upgrades": 3,
            "full_maintenance": 4,
            "repair": 5,
        }
        return sorted(
            self.packages.values(),
            key=lambda definition: (preferred_order.get(definition.key, 100), definition.label),
        )

    def find_package(self, key: str) -> PackageDefinition | None:
        return self.packages.get(normalize_key(key))


@dataclass(slots=True)
class PackageSelection:
    package_key: str
    choices: dict[str, str] = field(default_factory=dict)
    counts: dict[str, int] = field(default_factory=dict)


@dataclass(slots=True)
class PackageExpansion:
    label: str
    package_key: str
    choices: dict[str, str]
    counts: dict[str, int]
    price_item: str
    display_name: str
    materials: list[str]
    draft_items: list[DraftItem]
    price_pending: bool


def expand_package(
    package_catalog: PackageCatalog,
    catalog: Catalog,
    selection: PackageSelection,
) -> PackageExpansion:
    definition = package_catalog.find_package(selection.package_key)
    if definition is None:
        raise ValueError(f"unknown package: {selection.package_key}")

    normalized_choices = {
        normalize_key(group): normalize_key(value) for group, value in selection.choices.items()
    }
    normalized_counts = {normalize_key(key): value for key, value in selection.counts.items()}
    _validate_required_choices(definition, normalized_choices)
    _validate_counts(definition, normalized_counts)

    price_item = _package_price_item(definition, normalized_choices)
    price_catalog_item = catalog.find_item(price_item)
    if price_catalog_item is None:
        raise ValueError(f"package price item is missing from catalog: {price_item}")

    material_names = _package_materials(package_catalog, definition, normalized_choices, normalized_counts)
    display_name = _package_display_name(package_catalog, definition, normalized_choices, normalized_counts)
    draft_items = [
        DraftItem(
            item_name=price_catalog_item.name,
            quantity=1,
            override_unit_price=None,
            contract_name=None,
            display_name=display_name,
            package_key=definition.key,
            package_choices=dict(normalized_choices),
            package_counts=dict(normalized_counts),
        )
    ]
    draft_items.extend(
        DraftItem(
            item_name=_material_catalog_name(catalog, material),
            quantity=1,
            override_unit_price=0,
            contract_name=None,
            display_name=material,
            override_unit_cost=0,
            package_key=definition.key,
            package_choices=dict(normalized_choices),
            package_counts=dict(normalized_counts),
        )
        for material in material_names
    )
    return PackageExpansion(
        label=definition.label,
        package_key=definition.key,
        choices=normalized_choices,
        counts=normalized_counts,
        price_item=price_catalog_item.name,
        display_name=display_name,
        materials=material_names,
        draft_items=draft_items,
        price_pending=price_catalog_item.price_pending,
    )


def append_unique_items(target: list[DraftItem], items: list[DraftItem]) -> None:
    seen = {normalize_key(item.display_name or item.item_name) for item in target}
    for item in items:
        key = normalize_key(item.display_name or item.item_name)
        if key in seen:
            continue
        target.append(item)
        seen.add(key)


def _validate_required_choices(definition: PackageDefinition, choices: dict[str, str]) -> None:
    missing = [choice for choice in definition.required_choices if normalize_key(choice) not in choices]
    if missing:
        raise ValueError(f"{definition.label} requires: {', '.join(missing)}")


def _validate_counts(definition: PackageDefinition, counts: dict[str, int]) -> None:
    for count_key in definition.count_materials:
        if count_key not in counts:
            raise ValueError(f"{definition.label} requires a {count_key} count")
        if counts[count_key] < 0:
            raise ValueError(f"{count_key} count cannot be negative")


def _package_price_item(definition: PackageDefinition, choices: dict[str, str]) -> str:
    if definition.price_item is not None:
        return definition.price_item
    choice_group = definition.price_choice or definition.variant_choice
    if choice_group is None:
        raise ValueError(f"{definition.label} has no price item configured")
    choice = choices.get(normalize_key(choice_group))
    if choice is None:
        raise ValueError(f"{definition.label} requires {choice_group} for pricing")
    try:
        return definition.price_items[choice]
    except KeyError as error:
        raise ValueError(f"{definition.label} has no price item for {choice}") from error


def _package_materials(
    package_catalog: PackageCatalog,
    definition: PackageDefinition,
    choices: dict[str, str],
    counts: dict[str, int],
) -> list[str]:
    materials = list(definition.base_materials)
    if definition.variant_choice is not None:
        variant = choices[normalize_key(definition.variant_choice)]
        materials.extend(definition.variant_materials.get(variant, []))
    for group in definition.choice_material_groups:
        choice = choices[normalize_key(group)]
        material = _choice_material(package_catalog, group, choice)
        if material:
            materials.append(material)
    materials.extend(definition.extra_materials)
    materials = [_material_with_inherited_count(material, counts) for material in materials]
    for count_key, material_name in definition.count_materials.items():
        count = counts[normalize_key(count_key)]
        materials.append(f"{count}x {material_name}" if count != 1 else material_name)
    return materials


def _package_display_name(
    package_catalog: PackageCatalog,
    definition: PackageDefinition,
    choices: dict[str, str],
    counts: dict[str, int],
) -> str:
    details: list[str] = []
    for group in definition.required_choices:
        normalized_group = normalize_key(group)
        choice = choices.get(normalized_group)
        if choice is None:
            continue
        details.append(package_catalog.choices[normalized_group][choice])
    for count_key in definition.count_materials:
        count = counts[normalize_key(count_key)]
        details.append(f"{count}x {definition.count_materials[count_key]}")
    return definition.label if not details else f"{definition.label} ({' / '.join(details)})"


def _material_catalog_name(catalog: Catalog, material: str) -> str:
    if catalog.find_item(material) is not None:
        return material
    normalized = material.lower()
    if normalized.endswith("x cosmetic parts"):
        return "COSMETIC PARTS"
    if normalized.endswith("x extras kit"):
        return "EXTRAS KIT"
    if normalized.endswith("x lighting controller"):
        return "2x LIGHTING CONTROLLER"
    return material


def _material_with_inherited_count(material: str, counts: dict[str, int]) -> str:
    normalized = material.strip().lower()
    replacement_keys = {
        "??x cosmetic parts": "cosmetic_parts",
        "??x extras kit": "extras_kit",
    }
    count_key = replacement_keys.get(normalized)
    if count_key is None or count_key not in counts:
        return material
    count = counts[count_key]
    item_name = material.strip()[4:].strip()
    return f"{count}x {item_name}" if count != 1 else item_name


def _choice_material(package_catalog: PackageCatalog, group: str, choice: str) -> str:
    normalized_group = normalize_key(group)
    normalized_choice = normalize_key(choice)
    configured = package_catalog.materials.get(normalized_group, {}).get(normalized_choice)
    if configured is not None:
        return configured.strip()
    return package_catalog.choices[normalized_group][normalized_choice].strip()
