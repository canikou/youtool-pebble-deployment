from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from .catalog import Catalog, normalize_name
from .models import DraftItem
from .serialization import to_primitive


@dataclass(slots=True)
class ContractPriceEntry:
    item_name: str
    unit_price: int


@dataclass(slots=True)
class ContractEntry:
    name: str
    aliases: list[str] = field(default_factory=list)
    prices: list[ContractPriceEntry] = field(default_factory=list)


@dataclass(slots=True)
class Contract:
    name: str
    aliases: list[str]
    prices: list[ContractPriceEntry]
    price_lookup: dict[str, int] = field(default_factory=dict, repr=False)


@dataclass(slots=True)
class ContractsFile:
    contracts: list[ContractEntry] = field(default_factory=list)

    @classmethod
    def from_mapping(cls, data: dict) -> "ContractsFile":
        return cls(
            contracts=[
                ContractEntry(
                    name=str(contract["name"]),
                    aliases=[str(alias) for alias in contract.get("aliases", [])],
                    prices=[
                        ContractPriceEntry(item_name=str(price["item_name"]), unit_price=int(price["unit_price"]))
                        for price in contract.get("prices", [])
                    ],
                )
                for contract in data.get("contracts", [])
            ]
        )

    def to_mapping(self) -> dict:
        return to_primitive(self)


@dataclass(slots=True)
class Contracts:
    entries: list[Contract] = field(default_factory=list)
    _lookup: dict[str, int] = field(default_factory=dict, repr=False)

    @classmethod
    def load_from(cls, path: Path | str, catalog: Catalog) -> "Contracts":
        path = Path(path)
        if not path.exists():
            return cls()
        file = ContractsFile.from_mapping(json.loads(path.read_text(encoding="utf-8")))
        return cls.from_entries(file.contracts, catalog)

    @classmethod
    def from_entries(cls, entries: list[ContractEntry], catalog: Catalog) -> "Contracts":
        lookup: dict[str, int] = {}
        normalized_entries: list[Contract] = []

        for index, entry in enumerate(entries):
            if not entry.name.strip():
                raise ValueError(f"contract at index {index} has a blank name")

            price_lookup: dict[str, int] = {}
            prices: list[ContractPriceEntry] = []
            for price in entry.prices:
                if not price.item_name.strip():
                    raise ValueError(f"contract `{entry.name}` contains a blank item name")
                if price.unit_price <= 0:
                    raise ValueError(
                        f"contract `{entry.name}` item `{price.item_name}` has a non-positive unit price"
                    )

                catalog_item = catalog.find_item(price.item_name)
                if catalog_item is None:
                    raise ValueError(
                        f"contract `{entry.name}` references unknown catalog item `{price.item_name}`"
                    )

                canonical_name = catalog_item.name
                price_lookup[normalize_name(canonical_name)] = price.unit_price
                for alias in catalog_item.aliases:
                    if alias.strip():
                        price_lookup[normalize_name(alias)] = price.unit_price

                prices.append(ContractPriceEntry(item_name=canonical_name, unit_price=price.unit_price))

            lookup[normalize_name(entry.name)] = index
            for alias in entry.aliases:
                if alias.strip():
                    lookup[normalize_name(alias)] = index

            normalized_entries.append(
                Contract(
                    name=entry.name,
                    aliases=list(entry.aliases),
                    prices=prices,
                    price_lookup=price_lookup,
                )
            )

        return cls(entries=normalized_entries, _lookup=lookup)

    def snapshot_entries(self) -> list[ContractEntry]:
        return [
            ContractEntry(name=contract.name, aliases=list(contract.aliases), prices=list(contract.prices))
            for contract in self.entries
        ]

    def find_contract(self, input: str) -> Contract | None:
        index = self._lookup.get(normalize_name(input))
        if index is None:
            return None
        return self.entries[index]

    def add_contract(self, entry: ContractEntry, catalog: Catalog) -> None:
        entries = [
            contract
            for contract in self.snapshot_entries()
            if normalize_name(contract.name) != normalize_name(entry.name)
            and all(normalize_name(alias) != normalize_name(entry.name) for alias in contract.aliases)
        ]
        entries.append(entry)
        rebuilt = self.from_entries(entries, catalog)
        self.entries = rebuilt.entries
        self._lookup = rebuilt._lookup

    def save_to(self, path: Path | str) -> None:
        path = Path(path)
        payload = ContractsFile(contracts=self.snapshot_entries()).to_mapping()
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    def contract_price(self, contract_name: str, item_name: str) -> int | None:
        contract = self.find_contract(contract_name)
        if contract is None:
            return None
        return contract.price_lookup.get(normalize_name(item_name))


def apply_contract_to_items(
    contracts: Contracts,
    selected_contract: str | None,
    items: list[DraftItem],
) -> None:
    for item in items:
        if selected_contract is not None:
            if item.contract_name is not None or item.override_unit_price is None:
                contract_price = contracts.contract_price(selected_contract, item.item_name)
                if contract_price is not None:
                    item.override_unit_price = contract_price
                    contract = contracts.find_contract(selected_contract)
                    item.contract_name = contract.name if contract is not None else None
                elif item.contract_name is not None:
                    item.override_unit_price = None
                    item.contract_name = None
        elif item.contract_name is not None:
            item.override_unit_price = None
            item.contract_name = None

