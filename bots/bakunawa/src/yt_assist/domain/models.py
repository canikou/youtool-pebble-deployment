from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Self

from .serialization import parse_datetime, to_primitive

I64_MIN = -(2**63)
I64_MAX = 2**63 - 1


def utcnow() -> datetime:
    return datetime.now(UTC)


def saturating_add(left: int, right: int) -> int:
    return max(I64_MIN, min(I64_MAX, left + right))


def saturating_mul(left: int, right: int) -> int:
    return max(I64_MIN, min(I64_MAX, left * right))


class PricingSource(str, Enum):
    DEFAULT = "default"
    BULK = "bulk"
    OVERRIDE = "override"

    def as_str(self) -> str:
        return self.value

    @classmethod
    def from_db(cls, value: str) -> Self:
        if value == "bulk":
            return cls.BULK
        if value == "override":
            return cls.OVERRIDE
        return cls.DEFAULT


class ReceiptStatus(str, Enum):
    ACTIVE = "active"
    PAID = "paid"
    INVALIDATED = "invalidated"

    def as_str(self) -> str:
        return self.value

    @classmethod
    def from_db(cls, value: str) -> Self:
        if value == "paid":
            return cls.PAID
        if value in {"invalidated", "deleted"}:
            return cls.INVALIDATED
        return cls.ACTIVE

    def counts_for_historical_stats(self) -> bool:
        return self in {self.ACTIVE, self.PAID}

    def counts_for_payouts(self) -> bool:
        return self is self.ACTIVE


class AccountingPolicy(str, Enum):
    LEGACY_REIMBURSEMENT = "legacy_reimbursement"
    PROCUREMENT_FUNDS = "procurement_funds"

    def as_str(self) -> str:
        return self.value

    @classmethod
    def from_db(cls, value: str) -> Self:
        if value == "procurement_funds":
            return cls.PROCUREMENT_FUNDS
        return cls.LEGACY_REIMBURSEMENT

    def is_legacy(self) -> bool:
        return self is self.LEGACY_REIMBURSEMENT


class StatsSort(str, Enum):
    SALES = "sales"
    PROCUREMENT = "procurement"
    COUNT = "count"

    @classmethod
    def parse(cls, value: str | None) -> Self:
        token = (value or "").strip().lower()
        if token in {"procurement", "cost", "procurement_cost"}:
            return cls.PROCUREMENT
        if token in {"count", "receipts"}:
            return cls.COUNT
        return cls.SALES

    def as_str(self) -> str:
        return self.value


@dataclass(slots=True)
class ReceiptAccountingRecord:
    receipt_id: str
    policy: AccountingPolicy
    recorded_by_user_id: str
    recorded_by_display_name: str
    recorded_for_user_id: str
    recorded_for_display_name: str
    created_at: datetime
    updated_at: datetime


@dataclass(slots=True)
class ProcurementLedgerEntry:
    id: int
    user_id: str
    amount: int
    reason: str
    receipt_id: str | None
    actor_user_id: str
    actor_display_name: str
    created_at: datetime


@dataclass(slots=True)
class ProcurementCutoverState:
    cutover_at: datetime | None
    actor_user_id: str | None
    actor_display_name: str | None
    updated_at: datetime


@dataclass(slots=True)
class ProcurementBalance:
    user_id: str
    withdrawn_total: int = 0
    returned_total: int = 0
    spent_total: int = 0
    ledger_total: int = 0
    available_total: int = 0


@dataclass(slots=True)
class DraftItem:
    item_name: str
    quantity: int
    override_unit_price: int | None
    contract_name: str | None = None
    display_name: str | None = None
    override_unit_cost: int | None = None
    package_key: str | None = None
    package_choices: dict[str, str] = field(default_factory=dict)
    package_counts: dict[str, int] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> Self:
        return cls(
            item_name=str(data["item_name"]),
            quantity=int(data["quantity"]),
            override_unit_price=(
                None if data.get("override_unit_price") is None else int(data["override_unit_price"])
            ),
            contract_name=data.get("contract_name"),
            display_name=data.get("display_name"),
            override_unit_cost=(
                None if data.get("override_unit_cost") is None else int(data["override_unit_cost"])
            ),
            package_key=data.get("package_key"),
            package_choices={
                str(key): str(value) for key, value in data.get("package_choices", {}).items()
            },
            package_counts={
                str(key): int(value) for key, value in data.get("package_counts", {}).items()
            },
        )


@dataclass(slots=True)
class PricedItem:
    item_name: str
    quantity: int
    unit_sale_price: int
    unit_cost: int
    pricing_source: PricingSource
    line_sale_total: int
    line_cost_total: int

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> Self:
        raw_source = data.get("pricing_source")
        legacy_flags = [str(flag) for flag in data.get("flags", [])]
        if raw_source is not None:
            pricing_source = PricingSource.from_db(str(raw_source))
        elif any(flag.lower() in {"bulk", "bulk_discount"} for flag in legacy_flags) or bool(
            data.get("bulk")
        ) or bool(data.get("bulk_discount")):
            pricing_source = PricingSource.BULK
        elif any(
            flag.lower() in {"special", "special_pricing", "override"} for flag in legacy_flags
        ) or bool(data.get("special")) or bool(data.get("special_pricing")) or bool(data.get("override")):
            pricing_source = PricingSource.OVERRIDE
        else:
            pricing_source = PricingSource.DEFAULT

        return cls(
            item_name=str(data["item_name"]),
            quantity=int(data["quantity"]),
            unit_sale_price=int(data["unit_sale_price"]),
            unit_cost=int(data["unit_cost"]),
            pricing_source=pricing_source,
            line_sale_total=int(data["line_sale_total"]),
            line_cost_total=int(data["line_cost_total"]),
        )


@dataclass(slots=True)
class ExportedPaymentProof:
    file_name: str | None = None
    content_type: str | None = None
    data_base64: str = ""

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> Self:
        return cls(
            file_name=data.get("file_name"),
            content_type=data.get("content_type"),
            data_base64=str(data.get("data_base64") or data.get("base64") or data.get("data") or ""),
        )


@dataclass(slots=True)
class PricedReceipt:
    items: list[PricedItem]
    total_sale: int
    procurement_cost: int
    profit: int
    used_override: bool
    used_bulk: bool


@dataclass(slots=True)
class NewReceipt:
    id: str
    creator_user_id: str
    creator_username: str
    creator_display_name: str
    guild_id: str | None
    channel_id: str
    total_sale: int
    procurement_cost: int
    profit: int
    status: ReceiptStatus
    payment_proof_path: str | None
    payment_proof_source_url: str | None
    payment_proof: ExportedPaymentProof | None = None
    payment_proofs: list[ExportedPaymentProof] | None = None
    admin_note: str | None = None
    finalized_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    items: list[PricedItem] = field(default_factory=list)


@dataclass(slots=True)
class PersistedReceipt:
    id: str
    creator_user_id: str
    creator_username: str
    creator_display_name: str
    guild_id: str | None
    channel_id: str
    total_sale: int
    procurement_cost: int
    profit: int
    status: ReceiptStatus
    payment_proof_path: str | None
    payment_proof_source_url: str | None
    payment_proof: ExportedPaymentProof | None = None
    payment_proofs: list[ExportedPaymentProof] | None = None
    admin_note: str | None = None
    finalized_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    items: list[PricedItem] = field(default_factory=list)


@dataclass(slots=True)
class ReceiptSummary:
    id: str
    creator_display_name: str
    creator_user_id: str
    total_sale: int
    procurement_cost: int
    status: ReceiptStatus
    finalized_at: datetime


@dataclass(slots=True)
class LeaderboardEntry:
    user_id: str
    display_name: str
    total_sales: int
    procurement_cost: int
    receipt_count: int


@dataclass(slots=True)
class PayoutEntry:
    user_id: str
    display_name: str
    reimbursement: int
    profit: int
    total_payout_half_units: int
    company_balance: int
    adjusted_total_payout_half_units: int
    receipt_count: int


@dataclass(slots=True)
class AuditEvent:
    id: int
    actor_user_id: str
    actor_display_name: str
    action: str
    target_receipt_id: str | None
    detail_json: dict[str, Any]
    created_at: datetime

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> Self:
        return cls(
            id=int(data["id"]),
            actor_user_id=str(data["actor_user_id"]),
            actor_display_name=str(data["actor_display_name"]),
            action=str(data["action"]),
            target_receipt_id=data.get("target_receipt_id"),
            detail_json=dict(data["detail_json"]),
            created_at=parse_datetime(str(data["created_at"])),
        )


@dataclass(slots=True)
class AuditEventInput:
    actor_user_id: str
    actor_display_name: str
    action: str
    target_receipt_id: str | None
    detail_json: dict[str, Any]


@dataclass(slots=True)
class ExportBundle:
    schema_version: int
    exported_at: datetime
    catalog: Any
    receipts: list[PersistedReceipt]
    audit_events: list[AuditEvent]

    def to_mapping(self) -> dict[str, Any]:
        return to_primitive(self)

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> Self:
        from .catalog import CatalogFile

        catalog_data = data.get("catalog", {})
        catalog = CatalogFile.from_mapping(catalog_data) if isinstance(catalog_data, dict) else catalog_data
        return cls(
            schema_version=int(data["schema_version"]),
            exported_at=parse_datetime(str(data["exported_at"])),
            catalog=catalog,
            receipts=[PersistedReceipt.from_mapping(item) for item in data.get("receipts", [])],
            audit_events=[AuditEvent.from_mapping(item) for item in data.get("audit_events", [])],
        )

    @classmethod
    def from_mapping_or_json(cls, data: dict[str, Any] | str) -> Self:
        import json

        if isinstance(data, str):
            return cls.from_mapping(json.loads(data))
        return cls.from_mapping(data)


def _persisted_receipt_from_mapping(cls, data: dict[str, Any]) -> PersistedReceipt:
    return PersistedReceipt(
        id=str(data["id"]),
        creator_user_id=str(data["creator_user_id"]),
        creator_username=str(data["creator_username"]),
        creator_display_name=str(data["creator_display_name"]),
        guild_id=data.get("guild_id"),
        channel_id=str(data["channel_id"]),
        total_sale=int(data["total_sale"]),
        procurement_cost=int(data["procurement_cost"]),
        profit=int(data["profit"]),
        status=ReceiptStatus.from_db(str(data["status"])),
        payment_proof_path=data.get("payment_proof_path"),
        payment_proof_source_url=data.get("payment_proof_source_url"),
        payment_proof=(
            None
            if data.get("payment_proof") is None
            else ExportedPaymentProof.from_mapping(data["payment_proof"])
        ),
        payment_proofs=(
            None
            if data.get("payment_proofs") is None
            else [ExportedPaymentProof.from_mapping(item) for item in data["payment_proofs"]]
        ),
        admin_note=data.get("admin_note"),
        finalized_at=parse_datetime(str(data["finalized_at"])),
        items=[PricedItem.from_mapping(item) for item in data.get("items", [])],
    )


PersistedReceipt.from_mapping = classmethod(_persisted_receipt_from_mapping)


def model_to_dict(value: Any) -> Any:
    return to_primitive(value)
