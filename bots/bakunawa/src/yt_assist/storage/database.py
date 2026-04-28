"""SQLite persistence layer with Rust-parity semantics."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import aiosqlite

from yt_assist.domain.catalog import Catalog
from yt_assist.domain.models import (
    AccountingPolicy,
    AuditEvent,
    AuditEventInput,
    ExportBundle,
    LeaderboardEntry,
    NewReceipt,
    PayoutEntry,
    PersistedReceipt,
    PricedItem,
    PricingSource,
    ProcurementBalance,
    ProcurementCutoverState,
    ReceiptAccountingRecord,
    ReceiptStatus,
    ReceiptSummary,
    StatsSort,
    utcnow,
)
from yt_assist.domain.proof import (
    join_proof_values,
    load_embedded_payment_proofs,
    materialize_imported_payment_proofs,
)
from yt_assist.domain.serialization import parse_datetime
from yt_assist.storage.migrations import run_migrations

LOGGER = logging.getLogger(__name__)
EXPORT_SCHEMA_VERSION = 4
PROCUREMENT_CUTOVER_ROW_ID = 1


@dataclass(slots=True)
class ImportReport:
    total_receipts_in_file: int = 0
    imported_receipts: int = 0
    skipped_existing_receipts: int = 0
    skipped_duplicate_ids_in_file: int = 0
    imported_audit_events: int = 0
    skipped_existing_audit_events: int = 0
    duplicate_receipt_ids: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ImportStatusCounts:
    active: int = 0
    paid: int = 0
    invalidated: int = 0

    def increment(self, status: ReceiptStatus) -> None:
        if status is ReceiptStatus.ACTIVE:
            self.active += 1
        elif status is ReceiptStatus.PAID:
            self.paid += 1
        else:
            self.invalidated += 1


@dataclass(slots=True)
class ImportPreview:
    total_receipts_in_file: int = 0
    importable_receipts: int = 0
    skipped_existing_receipts: int = 0
    skipped_duplicate_ids_in_file: int = 0
    duplicate_receipt_ids: list[str] = field(default_factory=list)
    affected_user_ids: list[str] = field(default_factory=list)
    resulting_status_counts: ImportStatusCounts = field(default_factory=ImportStatusCounts)


@dataclass(slots=True)
class DocumentedReceiptSources:
    proof_message_ids: set[int] = field(default_factory=set)
    proof_urls: set[str] = field(default_factory=set)


class Database:
    def __init__(self, connection: aiosqlite.Connection, database_path: Path) -> None:
        self._connection = connection
        self._database_path = database_path

    @classmethod
    async def connect(cls, database_path: Path) -> Database:
        database_path.parent.mkdir(parents=True, exist_ok=True)
        connection = await aiosqlite.connect(database_path)
        connection.row_factory = aiosqlite.Row
        await connection.execute("PRAGMA foreign_keys = ON")
        await connection.execute("PRAGMA journal_mode = WAL")
        migrations_dir = Path(__file__).resolve().parents[3] / "migrations"
        await run_migrations(connection, migrations_dir)
        return cls(connection, database_path)

    async def close(self) -> None:
        await self._connection.close()

    async def save_receipt(
        self,
        receipt: NewReceipt,
        audit_event: AuditEventInput | None,
    ) -> None:
        await self.save_receipt_with_accounting(receipt, audit_event, None)

    async def save_receipt_with_accounting(
        self,
        receipt: NewReceipt,
        audit_event: AuditEventInput | None,
        accounting_override: ReceiptAccountingRecord | None,
    ) -> None:
        policy = await self._effective_accounting_policy_for_receipt(
            receipt.finalized_at, accounting_override
        )
        accounting = accounting_override or ReceiptAccountingRecord(
            receipt_id=receipt.id,
            policy=policy,
            recorded_by_user_id=receipt.creator_user_id,
            recorded_by_display_name=receipt.creator_display_name,
            recorded_for_user_id=receipt.creator_user_id,
            recorded_for_display_name=receipt.creator_display_name,
            created_at=receipt.finalized_at,
            updated_at=receipt.finalized_at,
        )

        await self._connection.execute("BEGIN")
        try:
            await self._connection.execute(
                """
                INSERT INTO receipts (
                    id,
                    creator_user_id,
                    creator_username,
                    creator_display_name,
                    guild_id,
                    channel_id,
                    total_sale,
                    procurement_cost,
                    profit,
                    status,
                    payment_proof_path,
                    payment_proof_source_url,
                    admin_note,
                    finalized_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    receipt.id,
                    receipt.creator_user_id,
                    receipt.creator_username,
                    receipt.creator_display_name,
                    receipt.guild_id,
                    receipt.channel_id,
                    receipt.total_sale,
                    receipt.procurement_cost,
                    receipt.profit,
                    receipt.status.as_str(),
                    receipt.payment_proof_path,
                    receipt.payment_proof_source_url,
                    receipt.admin_note,
                    receipt.finalized_at.isoformat(),
                ),
            )
            await self._insert_or_replace_receipt_accounting(accounting)
            for item in receipt.items:
                await self._connection.execute(
                    """
                    INSERT INTO receipt_items (
                        receipt_id,
                        item_name,
                        quantity,
                        unit_sale_price,
                        unit_cost,
                        pricing_source,
                        line_sale_total,
                        line_cost_total
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        receipt.id,
                        item.item_name,
                        item.quantity,
                        item.unit_sale_price,
                        item.unit_cost,
                        item.pricing_source.as_str(),
                        item.line_sale_total,
                        item.line_cost_total,
                    ),
                )
            if audit_event is not None:
                await self._insert_audit_event(
                    self._enrich_receipt_created_audit(audit_event, receipt, accounting, policy)
                )
            await self._connection.commit()
        except Exception:
            await self._connection.rollback()
            raise

    async def insert_audit_event(self, audit_event: AuditEventInput) -> None:
        await self._insert_audit_event(audit_event)
        await self._connection.commit()

    async def leaderboard(self, sort: StatsSort) -> list[LeaderboardEntry]:
        order_by = {
            StatsSort.SALES: "total_sales DESC, procurement_cost DESC, receipt_count DESC",
            StatsSort.PROCUREMENT: "procurement_cost DESC, total_sales DESC, receipt_count DESC",
            StatsSort.COUNT: "receipt_count DESC, total_sales DESC, procurement_cost DESC",
        }[sort]
        query = f"""
            SELECT
                r.creator_user_id,
                (
                    SELECT latest.creator_display_name
                    FROM receipts latest
                    WHERE latest.status IN ('active', 'paid')
                      AND latest.creator_user_id = r.creator_user_id
                    ORDER BY latest.finalized_at DESC, latest.id DESC
                    LIMIT 1
                ) AS creator_display_name,
                SUM(r.total_sale) AS total_sales,
                SUM(r.procurement_cost) AS procurement_cost,
                COUNT(*) AS receipt_count
            FROM receipts r
            WHERE r.status IN ('active', 'paid')
            GROUP BY r.creator_user_id
            ORDER BY {order_by}
        """
        async with self._connection.execute(query) as cursor:
            rows = await cursor.fetchall()
        return [
            LeaderboardEntry(
                user_id=str(row["creator_user_id"]),
                display_name=str(row["creator_display_name"]),
                total_sales=int(row["total_sales"]),
                procurement_cost=int(row["procurement_cost"]),
                receipt_count=int(row["receipt_count"]),
            )
            for row in rows
        ]

    async def payouts(self, creator_user_id: str | None) -> list[PayoutEntry]:
        raw_entries: dict[str, PayoutEntry] = {}
        if creator_user_id is None:
            query = """
                SELECT
                    COALESCE(accounting.recorded_for_user_id, r.creator_user_id) AS payout_user_id,
                    MAX(COALESCE(accounting.recorded_for_display_name, r.creator_display_name)) AS payout_display_name,
                    SUM(r.procurement_cost) AS reimbursement,
                    SUM(r.profit) AS profit,
                    COUNT(*) AS receipt_count,
                    SUM(r.profit * 60) AS total_payout_cents
                FROM receipts r
                LEFT JOIN receipt_accounting accounting ON accounting.receipt_id = r.id
                WHERE r.status = 'active'
                GROUP BY COALESCE(accounting.recorded_for_user_id, r.creator_user_id)
            """
            params: tuple[Any, ...] = ()
        else:
            query = """
                SELECT
                    COALESCE(accounting.recorded_for_user_id, r.creator_user_id) AS payout_user_id,
                    MAX(COALESCE(accounting.recorded_for_display_name, r.creator_display_name)) AS payout_display_name,
                    SUM(r.procurement_cost) AS reimbursement,
                    SUM(r.profit) AS profit,
                    COUNT(*) AS receipt_count,
                    SUM(r.profit * 60) AS total_payout_cents
                FROM receipts r
                LEFT JOIN receipt_accounting accounting ON accounting.receipt_id = r.id
                WHERE r.status = 'active'
                  AND COALESCE(accounting.recorded_for_user_id, r.creator_user_id) = ?
                GROUP BY COALESCE(accounting.recorded_for_user_id, r.creator_user_id)
            """
            params = (creator_user_id,)

        async with self._connection.execute(query, params) as cursor:
            rows = await cursor.fetchall()

        for row in rows:
            reimbursement = int(row["reimbursement"])
            profit = int(row["profit"])
            total_payout_cents = int(row["total_payout_cents"])
            user_id = str(row["payout_user_id"])
            raw_entries[user_id] = PayoutEntry(
                user_id=user_id,
                display_name=str(row["payout_display_name"]),
                reimbursement=reimbursement,
                profit=profit,
                total_payout_cents=total_payout_cents,
                company_balance=0,
                adjustment_cents=0,
                adjusted_total_payout_cents=total_payout_cents,
                receipt_count=int(row["receipt_count"]),
            )

        if creator_user_id is None:
            adjustment_query = """
                SELECT
                    user_id,
                    MAX(display_name) AS display_name,
                    SUM(amount_cents) AS adjustment_cents
                FROM payout_adjustments
                WHERE settled_at IS NULL
                GROUP BY user_id
            """
            adjustment_params: tuple[Any, ...] = ()
        else:
            adjustment_query = """
                SELECT
                    user_id,
                    MAX(display_name) AS display_name,
                    SUM(amount_cents) AS adjustment_cents
                FROM payout_adjustments
                WHERE settled_at IS NULL
                  AND user_id = ?
                GROUP BY user_id
            """
            adjustment_params = (creator_user_id,)

        async with self._connection.execute(adjustment_query, adjustment_params) as cursor:
            adjustment_rows = await cursor.fetchall()

        for row in adjustment_rows:
            user_id = str(row["user_id"])
            adjustment_cents = int(row["adjustment_cents"])
            existing = raw_entries.get(user_id)
            if existing is None:
                raw_entries[user_id] = PayoutEntry(
                    user_id=user_id,
                    display_name=str(row["display_name"]),
                    reimbursement=0,
                    profit=0,
                    total_payout_cents=0,
                    company_balance=0,
                    adjustment_cents=adjustment_cents,
                    adjusted_total_payout_cents=adjustment_cents,
                    receipt_count=0,
                )
                continue
            existing.adjustment_cents = adjustment_cents
            existing.adjusted_total_payout_cents = existing.total_payout_cents + adjustment_cents

        entries = list(raw_entries.values())
        entries.sort(
            key=lambda entry: (
                -entry.adjusted_total_payout_cents,
                -entry.total_payout_cents,
                -entry.profit,
                entry.display_name,
            )
        )
        return entries

    async def list_open_payout_adjustment_user_ids(self) -> list[str]:
        async with self._connection.execute(
            """
            SELECT DISTINCT user_id
            FROM payout_adjustments
            WHERE settled_at IS NULL
            ORDER BY user_id ASC
            """
        ) as cursor:
            rows = await cursor.fetchall()
        return [str(row["user_id"]) for row in rows]

    async def apply_payout_adjustments(
        self,
        adjustments: list[tuple[str, str, int, str, str | None, str | None]],
        actor_user_id: str,
        actor_display_name: str,
    ) -> int:
        if not adjustments:
            return 0

        created_at = utcnow()
        inserted = 0
        await self._connection.execute("BEGIN")
        try:
            for (
                user_id,
                display_name,
                amount_cents,
                reason,
                source_user_id,
                source_display_name,
            ) in adjustments:
                cursor = await self._connection.execute(
                    """
                    INSERT INTO payout_adjustments (
                        user_id,
                        display_name,
                        amount_cents,
                        reason,
                        source_user_id,
                        source_display_name,
                        actor_user_id,
                        actor_display_name,
                        created_at,
                        settled_at,
                        settled_by_user_id,
                        settled_by_display_name
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL)
                    """,
                    (
                        user_id,
                        display_name,
                        amount_cents,
                        reason,
                        source_user_id,
                        source_display_name,
                        actor_user_id,
                        actor_display_name,
                        created_at.isoformat(),
                    ),
                )
                inserted_id = int(cursor.lastrowid)
                await self._insert_audit_event(
                    AuditEventInput(
                        actor_user_id=actor_user_id,
                        actor_display_name=actor_display_name,
                        action="payout_adjustment_created",
                        target_receipt_id=None,
                        detail_json={
                            "adjustment_id": inserted_id,
                            "user_id": user_id,
                            "display_name": display_name,
                            "amount_cents": amount_cents,
                            "reason": reason,
                            "source_user_id": source_user_id,
                            "source_display_name": source_display_name,
                            "created_at": created_at.isoformat(),
                        },
                    )
                )
                inserted += 1
            await self._connection.commit()
        except Exception:
            await self._connection.rollback()
            raise
        return inserted

    async def settle_payout_adjustments(
        self,
        user_ids: list[str] | None,
        actor_user_id: str,
        actor_display_name: str,
        reason: str,
    ) -> int:
        settled_at = utcnow()
        params: list[Any] = [settled_at.isoformat(), actor_user_id, actor_display_name]
        where_clause = "settled_at IS NULL"
        if user_ids is not None:
            if not user_ids:
                return 0
            placeholders = ", ".join("?" for _ in user_ids)
            where_clause += f" AND user_id IN ({placeholders})"
            params.extend(user_ids)

        await self._connection.execute("BEGIN")
        try:
            cursor = await self._connection.execute(
                f"""
                UPDATE payout_adjustments
                SET settled_at = ?, settled_by_user_id = ?, settled_by_display_name = ?
                WHERE {where_clause}
                """,
                tuple(params),
            )
            updated = cursor.rowcount
            if updated > 0:
                await self._insert_audit_event(
                    AuditEventInput(
                        actor_user_id=actor_user_id,
                        actor_display_name=actor_display_name,
                        action="payout_adjustments_settled",
                        target_receipt_id=None,
                        detail_json={
                            "reason": reason,
                            "user_ids": list(user_ids) if user_ids is not None else None,
                            "settled_count": updated,
                            "settled_at": settled_at.isoformat(),
                        },
                    )
                )
            await self._connection.commit()
        except Exception:
            await self._connection.rollback()
            raise
        return updated

    async def documented_receipt_sources(self) -> DocumentedReceiptSources:
        async with self._connection.execute(
            "SELECT detail_json FROM audit_events WHERE action = 'receipt_created'"
        ) as cursor:
            rows = await cursor.fetchall()

        documented = DocumentedReceiptSources()
        for row in rows:
            try:
                detail = json.loads(str(row["detail_json"]))
            except json.JSONDecodeError:
                continue
            proof_message_id = detail.get("proof_message_id")
            if isinstance(proof_message_id, int):
                documented.proof_message_ids.add(proof_message_id)
            proof_url = detail.get("proof_url")
            if isinstance(proof_url, str):
                documented.proof_urls.add(proof_url)
            proof_urls = detail.get("proof_urls")
            if isinstance(proof_urls, list):
                documented.proof_urls.update(str(value) for value in proof_urls if isinstance(value, str))
        return documented

    async def list_receipts(self, page: int, per_page: int) -> list[ReceiptSummary]:
        offset = page * per_page
        async with self._connection.execute(
            """
            SELECT
                id,
                creator_display_name,
                creator_user_id,
                total_sale,
                procurement_cost,
                status,
                finalized_at
            FROM receipts
            ORDER BY finalized_at DESC
            LIMIT ? OFFSET ?
            """,
            (per_page, offset),
        ) as cursor:
            rows = await cursor.fetchall()
        return [self._parse_receipt_summary(row) for row in rows]

    async def list_all_receipts(self) -> list[PersistedReceipt]:
        async with self._connection.execute(
            "SELECT id FROM receipts ORDER BY finalized_at ASC, id ASC"
        ) as cursor:
            rows = await cursor.fetchall()
        receipts: list[PersistedReceipt] = []
        for row in rows:
            receipt = await self.get_receipt(str(row["id"]))
            if receipt is not None:
                receipts.append(receipt)
        return receipts

    async def get_receipt(self, receipt_id: str) -> PersistedReceipt | None:
        async with self._connection.execute(
            """
            SELECT
                id,
                creator_user_id,
                creator_username,
                creator_display_name,
                guild_id,
                channel_id,
                total_sale,
                procurement_cost,
                profit,
                status,
                payment_proof_path,
                payment_proof_source_url,
                admin_note,
                finalized_at
            FROM receipts
            WHERE id = ?
            """,
            (receipt_id,),
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            return None

        async with self._connection.execute(
            """
            SELECT
                item_name,
                quantity,
                unit_sale_price,
                unit_cost,
                pricing_source,
                line_sale_total,
                line_cost_total
            FROM receipt_items
            WHERE receipt_id = ?
            ORDER BY id ASC
            """,
            (receipt_id,),
        ) as cursor:
            item_rows = await cursor.fetchall()

        items = [self._parse_priced_item(item_row) for item_row in item_rows]
        return PersistedReceipt(
            id=str(row["id"]),
            creator_user_id=str(row["creator_user_id"]),
            creator_username=str(row["creator_username"]),
            creator_display_name=str(row["creator_display_name"]),
            guild_id=row["guild_id"],
            channel_id=str(row["channel_id"]),
            total_sale=int(row["total_sale"]),
            procurement_cost=int(row["procurement_cost"]),
            profit=int(row["profit"]),
            status=ReceiptStatus.from_db(str(row["status"])),
            payment_proof_path=row["payment_proof_path"],
            payment_proof_source_url=row["payment_proof_source_url"],
            payment_proof=None,
            payment_proofs=None,
            admin_note=row["admin_note"],
            finalized_at=parse_datetime(str(row["finalized_at"])),
            items=items,
        )

    async def receipt_accounting_record(
        self,
        receipt_id: str,
    ) -> ReceiptAccountingRecord | None:
        async with self._connection.execute(
            """
            SELECT
                receipt_id,
                policy,
                recorded_by_user_id,
                recorded_by_display_name,
                recorded_for_user_id,
                recorded_for_display_name,
                created_at,
                updated_at
            FROM receipt_accounting
            WHERE receipt_id = ?
            """,
            (receipt_id,),
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            return None
        return ReceiptAccountingRecord(
            receipt_id=str(row["receipt_id"]),
            policy=AccountingPolicy.from_db(str(row["policy"])),
            recorded_by_user_id=str(row["recorded_by_user_id"]),
            recorded_by_display_name=str(row["recorded_by_display_name"]),
            recorded_for_user_id=str(row["recorded_for_user_id"]),
            recorded_for_display_name=str(row["recorded_for_display_name"]),
            created_at=parse_datetime(str(row["created_at"])),
            updated_at=parse_datetime(str(row["updated_at"])),
        )

    async def procurement_cutover_state(self) -> ProcurementCutoverState | None:
        async with self._connection.execute(
            """
            SELECT cutover_at, actor_user_id, actor_display_name, updated_at
            FROM procurement_settings
            WHERE id = ?
            """,
            (PROCUREMENT_CUTOVER_ROW_ID,),
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            return None
        cutover_raw = row["cutover_at"]
        return ProcurementCutoverState(
            cutover_at=parse_datetime(str(cutover_raw)) if cutover_raw is not None else None,
            actor_user_id=row["actor_user_id"],
            actor_display_name=row["actor_display_name"],
            updated_at=parse_datetime(str(row["updated_at"])),
        )

    async def set_procurement_cutover(
        self,
        cutover_at,
        actor_user_id: str,
        actor_display_name: str,
    ) -> None:
        await self._connection.execute(
            """
            INSERT INTO procurement_settings (
                id, cutover_at, actor_user_id, actor_display_name, updated_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                cutover_at = excluded.cutover_at,
                actor_user_id = excluded.actor_user_id,
                actor_display_name = excluded.actor_display_name,
                updated_at = excluded.updated_at
            """,
            (
                PROCUREMENT_CUTOVER_ROW_ID,
                cutover_at.isoformat(),
                actor_user_id,
                actor_display_name,
                utcnow().isoformat(),
            ),
        )
        await self._connection.commit()

    async def clear_procurement_cutover(self) -> None:
        await self._connection.execute(
            "DELETE FROM procurement_settings WHERE id = ?",
            (PROCUREMENT_CUTOVER_ROW_ID,),
        )
        await self._connection.commit()

    async def record_procurement_ledger_entry(
        self,
        user_id: str,
        amount: int,
        reason: str,
        receipt_id: str | None,
        actor_user_id: str,
        actor_display_name: str,
    ) -> int:
        cursor = await self._connection.execute(
            """
            INSERT INTO procurement_ledger (
                user_id, amount, reason, receipt_id, actor_user_id, actor_display_name, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                amount,
                reason,
                receipt_id,
                actor_user_id,
                actor_display_name,
                utcnow().isoformat(),
            ),
        )
        await self._connection.commit()
        return int(cursor.lastrowid)

    async def procurement_balance(self, user_id: str) -> ProcurementBalance:
        async with self._connection.execute(
            """
            SELECT
                COALESCE(SUM(amount), 0) AS ledger_total,
                COALESCE(SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END), 0) AS withdrawn_total,
                COALESCE(SUM(CASE WHEN amount < 0 THEN -amount ELSE 0 END), 0) AS returned_total
            FROM procurement_ledger
            WHERE user_id = ?
            """,
            (user_id,),
        ) as cursor:
            ledger_row = await cursor.fetchone()
        async with self._connection.execute(
            """
            SELECT COALESCE(SUM(r.procurement_cost), 0) AS spent_total
            FROM receipts r
            LEFT JOIN receipt_accounting accounting ON accounting.receipt_id = r.id
            WHERE COALESCE(accounting.policy, 'legacy_reimbursement') = 'procurement_funds'
              AND r.status = 'active'
              AND COALESCE(accounting.recorded_for_user_id, r.creator_user_id) = ?
            """,
            (user_id,),
        ) as cursor:
            spent_row = await cursor.fetchone()

        ledger_total = int(ledger_row["ledger_total"])
        spent_total = int(spent_row["spent_total"])
        return ProcurementBalance(
            user_id=user_id,
            withdrawn_total=int(ledger_row["withdrawn_total"]),
            returned_total=int(ledger_row["returned_total"]),
            spent_total=spent_total,
            ledger_total=ledger_total,
            available_total=ledger_total - spent_total,
        )

    async def settle_procurement_balance(
        self,
        user_id: str,
        outstanding_company_balance: int,
        actor_user_id: str,
        actor_display_name: str,
    ) -> int | None:
        if outstanding_company_balance == 0:
            return None
        amount = -outstanding_company_balance
        reason = (
            "mark_paid_settlement_deduction"
            if outstanding_company_balance > 0
            else "mark_paid_settlement_reimbursement"
        )
        return await self.record_procurement_ledger_entry(
            user_id,
            amount,
            reason,
            None,
            actor_user_id,
            actor_display_name,
        )

    async def update_receipt_status(
        self,
        receipt_id: str,
        status: ReceiptStatus,
        actor: AuditEventInput,
        admin_note: str | None,
    ) -> bool:
        cursor = await self._connection.execute(
            """
            UPDATE receipts
            SET status = ?, admin_note = COALESCE(?, admin_note)
            WHERE id = ?
            """,
            (status.as_str(), admin_note, receipt_id),
        )
        if cursor.rowcount == 0:
            await self._connection.rollback()
            return False
        await self._insert_audit_event(actor)
        await self._connection.commit()
        return True

    async def update_receipt_note(
        self,
        receipt_id: str,
        admin_note: str | None,
        actor: AuditEventInput,
    ) -> bool:
        cursor = await self._connection.execute(
            """
            UPDATE receipts
            SET admin_note = ?
            WHERE id = ?
            """,
            (admin_note, receipt_id),
        )
        if cursor.rowcount == 0:
            await self._connection.rollback()
            return False
        await self._insert_audit_event(actor)
        await self._connection.commit()
        return True

    async def update_receipt_statuses(
        self,
        receipt_ids: list[str],
        status: ReceiptStatus,
        current_status: ReceiptStatus | None,
        actor_user_id: str,
        actor_display_name: str,
        admin_note: str | None,
    ) -> int:
        if not receipt_ids:
            return 0

        updated = 0
        await self._connection.execute("BEGIN")
        try:
            for receipt_id in receipt_ids:
                if current_status is None:
                    cursor = await self._connection.execute(
                        """
                        UPDATE receipts
                        SET status = ?, admin_note = COALESCE(?, admin_note)
                        WHERE id = ?
                        """,
                        (status.as_str(), admin_note, receipt_id),
                    )
                else:
                    cursor = await self._connection.execute(
                        """
                        UPDATE receipts
                        SET status = ?, admin_note = COALESCE(?, admin_note)
                        WHERE id = ?
                          AND status = ?
                        """,
                        (status.as_str(), admin_note, receipt_id, current_status.as_str()),
                    )
                if cursor.rowcount == 0:
                    continue
                await self._insert_audit_event(
                    AuditEventInput(
                        actor_user_id=actor_user_id,
                        actor_display_name=actor_display_name,
                        action=f"receipt_status_{status.as_str()}",
                        target_receipt_id=receipt_id,
                        detail_json={"status": status.as_str()},
                    )
                )
                updated += cursor.rowcount
            await self._connection.commit()
        except Exception:
            await self._connection.rollback()
            raise
        return updated

    async def update_receipt_proof(
        self,
        receipt_id: str,
        payment_proof_path: str,
        payment_proof_source_url: str,
        actor: AuditEventInput,
    ) -> bool:
        cursor = await self._connection.execute(
            """
            UPDATE receipts
            SET payment_proof_path = ?, payment_proof_source_url = ?
            WHERE id = ?
            """,
            (payment_proof_path, payment_proof_source_url, receipt_id),
        )
        if cursor.rowcount == 0:
            await self._connection.rollback()
            return False
        await self._insert_audit_event(actor)
        await self._connection.commit()
        return True

    async def export_bundle(self, catalog: Catalog) -> ExportBundle:
        async with self._connection.execute(
            "SELECT id FROM receipts ORDER BY finalized_at ASC"
        ) as cursor:
            receipt_rows = await cursor.fetchall()

        receipts: list[PersistedReceipt] = []
        for row in receipt_rows:
            receipt = await self.get_receipt(str(row["id"]))
            if receipt is None:
                continue
            if receipt.status is ReceiptStatus.ACTIVE:
                payment_proofs = load_embedded_payment_proofs(receipt.payment_proof_path)
                receipt.payment_proof = payment_proofs[0] if payment_proofs else None
                receipt.payment_proofs = payment_proofs or None
            else:
                receipt.payment_proof = None
                receipt.payment_proofs = None
            receipts.append(receipt)

        async with self._connection.execute(
            """
            SELECT
                receipt_id,
                policy,
                recorded_by_user_id,
                recorded_by_display_name,
                recorded_for_user_id,
                recorded_for_display_name,
                created_at,
                updated_at
            FROM receipt_accounting
            ORDER BY created_at ASC, receipt_id ASC
            """
        ) as cursor:
            accounting_rows = await cursor.fetchall()

        synthetic_audit_events: list[AuditEvent] = []
        synthetic_id = -1
        for row in accounting_rows:
            detail_json = {
                "receipt_id": str(row["receipt_id"]),
                "accounting_policy": str(row["policy"]),
                "recorded_by_user_id": str(row["recorded_by_user_id"]),
                "recorded_by_display_name": str(row["recorded_by_display_name"]),
                "recorded_for_user_id": str(row["recorded_for_user_id"]),
                "recorded_for_display_name": str(row["recorded_for_display_name"]),
                "created_at": str(row["created_at"]),
                "updated_at": str(row["updated_at"]),
            }
            synthetic_audit_events.append(
                AuditEvent(
                    id=synthetic_id,
                    actor_user_id=str(row["recorded_by_user_id"]),
                    actor_display_name=str(row["recorded_by_display_name"]),
                    action="receipt_accounting_snapshot",
                    target_receipt_id=str(row["receipt_id"]),
                    detail_json=detail_json,
                    created_at=parse_datetime(str(row["created_at"])),
                )
            )
            synthetic_id -= 1

        cutover = await self.procurement_cutover_state()
        if cutover is not None and cutover.cutover_at is not None:
            synthetic_audit_events.append(
                AuditEvent(
                    id=synthetic_id,
                    actor_user_id=cutover.actor_user_id or "",
                    actor_display_name=cutover.actor_display_name or "",
                    action="procurement_cutover_set",
                    target_receipt_id=None,
                    detail_json={
                        "cutover_at": cutover.cutover_at.isoformat(),
                        "actor_user_id": cutover.actor_user_id,
                        "actor_display_name": cutover.actor_display_name,
                        "updated_at": cutover.updated_at.isoformat(),
                    },
                    created_at=cutover.updated_at,
                )
            )
            synthetic_id -= 1

        async with self._connection.execute(
            """
            SELECT
                id,
                user_id,
                amount,
                reason,
                receipt_id,
                actor_user_id,
                actor_display_name,
                created_at
            FROM procurement_ledger
            ORDER BY created_at ASC, id ASC
            """
        ) as cursor:
            ledger_rows = await cursor.fetchall()
        for row in ledger_rows:
            created_at = parse_datetime(str(row["created_at"]))
            synthetic_audit_events.append(
                AuditEvent(
                    id=synthetic_id,
                    actor_user_id=str(row["actor_user_id"]),
                    actor_display_name=str(row["actor_display_name"]),
                    action="procurement_ledger_entry",
                    target_receipt_id=row["receipt_id"],
                    detail_json={
                        "ledger_id": int(row["id"]),
                        "user_id": str(row["user_id"]),
                        "amount": int(row["amount"]),
                        "reason": str(row["reason"]),
                        "receipt_id": row["receipt_id"],
                        "actor_user_id": str(row["actor_user_id"]),
                        "actor_display_name": str(row["actor_display_name"]),
                        "created_at": str(row["created_at"]),
                    },
                    created_at=created_at,
                )
            )
            synthetic_id -= 1

        async with self._connection.execute(
            """
            SELECT
                id,
                user_id,
                display_name,
                amount_cents,
                reason,
                source_user_id,
                source_display_name,
                actor_user_id,
                actor_display_name,
                created_at,
                settled_at,
                settled_by_user_id,
                settled_by_display_name
            FROM payout_adjustments
            ORDER BY created_at ASC, id ASC
            """
        ) as cursor:
            payout_adjustment_rows = await cursor.fetchall()
        for row in payout_adjustment_rows:
            created_at = parse_datetime(str(row["created_at"]))
            detail_json = {
                "adjustment_id": int(row["id"]),
                "user_id": str(row["user_id"]),
                "display_name": str(row["display_name"]),
                "amount_cents": int(row["amount_cents"]),
                "reason": str(row["reason"]),
                "source_user_id": row["source_user_id"],
                "source_display_name": row["source_display_name"],
                "actor_user_id": str(row["actor_user_id"]),
                "actor_display_name": str(row["actor_display_name"]),
                "created_at": str(row["created_at"]),
                "settled_at": row["settled_at"],
                "settled_by_user_id": row["settled_by_user_id"],
                "settled_by_display_name": row["settled_by_display_name"],
            }
            synthetic_audit_events.append(
                AuditEvent(
                    id=synthetic_id,
                    actor_user_id=str(row["actor_user_id"]),
                    actor_display_name=str(row["actor_display_name"]),
                    action="payout_adjustment_snapshot",
                    target_receipt_id=None,
                    detail_json=detail_json,
                    created_at=created_at,
                )
            )
            synthetic_id -= 1

        async with self._connection.execute(
            """
            SELECT
                id,
                actor_user_id,
                actor_display_name,
                action,
                target_receipt_id,
                detail_json,
                created_at
            FROM audit_events
            ORDER BY id ASC
            """
        ) as cursor:
            audit_rows = await cursor.fetchall()

        audit_events = [self._parse_audit_event(row) for row in audit_rows]
        audit_events.extend(synthetic_audit_events)
        return ExportBundle(
            schema_version=EXPORT_SCHEMA_VERSION,
            exported_at=utcnow(),
            catalog=catalog.snapshot(),
            receipts=receipts,
            audit_events=audit_events,
        )

    async def clear_receipts(self) -> None:
        await self._connection.execute("BEGIN")
        try:
            await self._connection.execute("DELETE FROM receipt_accounting")
            await self._connection.execute("DELETE FROM procurement_ledger")
            await self._connection.execute("DELETE FROM payout_adjustments")
            await self._connection.execute(
                "DELETE FROM procurement_settings WHERE id = ?",
                (PROCUREMENT_CUTOVER_ROW_ID,),
            )
            await self._connection.execute("DELETE FROM receipt_items")
            await self._connection.execute("DELETE FROM receipts")
            await self._connection.execute("DELETE FROM audit_events")
            await self._connection.commit()
        except Exception:
            await self._connection.rollback()
            raise

    async def delete_receipts_by_ids(self, receipt_ids: list[str]) -> int:
        if not receipt_ids:
            return 0
        deleted = 0
        await self._connection.execute("BEGIN")
        try:
            for receipt_id in receipt_ids:
                await self._connection.execute(
                    "DELETE FROM receipt_accounting WHERE receipt_id = ?",
                    (receipt_id,),
                )
                await self._connection.execute(
                    "DELETE FROM procurement_ledger WHERE receipt_id = ?",
                    (receipt_id,),
                )
                await self._connection.execute(
                    "DELETE FROM receipt_items WHERE receipt_id = ?",
                    (receipt_id,),
                )
                await self._connection.execute(
                    "DELETE FROM audit_events WHERE target_receipt_id = ?",
                    (receipt_id,),
                )
                cursor = await self._connection.execute(
                    "DELETE FROM receipts WHERE id = ?",
                    (receipt_id,),
                )
                deleted += cursor.rowcount
            await self._connection.commit()
        except Exception:
            await self._connection.rollback()
            raise
        return deleted

    async def inspect_import_bundle(
        self,
        bundle: ExportBundle,
        status_override: ReceiptStatus | None,
    ) -> ImportPreview:
        preview = ImportPreview(total_receipts_in_file=len(bundle.receipts))
        seen_receipt_ids: set[str] = set()
        affected_user_ids: set[str] = set()
        for receipt in bundle.receipts:
            if receipt.id in seen_receipt_ids:
                preview.skipped_duplicate_ids_in_file += 1
                if len(preview.duplicate_receipt_ids) < 10:
                    preview.duplicate_receipt_ids.append(receipt.id)
                continue
            seen_receipt_ids.add(receipt.id)

            if await self._receipt_exists(receipt.id):
                preview.skipped_existing_receipts += 1
                continue

            final_status = status_override or receipt.status
            preview.importable_receipts += 1
            preview.resulting_status_counts.increment(final_status)
            affected_user_ids.add(receipt.creator_user_id)
        preview.affected_user_ids = sorted(affected_user_ids)
        return preview

    async def import_bundle(
        self,
        bundle: ExportBundle,
        attachment_dir: Path,
        status_override: ReceiptStatus | None,
        override_actor: tuple[str, str] | None,
    ) -> ImportReport:
        report = ImportReport(total_receipts_in_file=len(bundle.receipts))
        seen_receipt_ids: set[str] = set()

        for receipt in bundle.receipts:
            if receipt.id in seen_receipt_ids:
                report.skipped_duplicate_ids_in_file += 1
                if len(report.duplicate_receipt_ids) < 10:
                    report.duplicate_receipt_ids.append(receipt.id)
                continue
            seen_receipt_ids.add(receipt.id)

            if await self._receipt_exists(receipt.id):
                report.skipped_existing_receipts += 1
                continue

            final_status = status_override or receipt.status
            embedded_proofs = (
                list(receipt.payment_proofs)
                if receipt.payment_proofs
                else ([receipt.payment_proof] if receipt.payment_proof is not None else [])
            )

            try:
                proof_paths = materialize_imported_payment_proofs(
                    attachment_dir,
                    receipt.id,
                    embedded_proofs,
                    receipt.payment_proof_path,
                    receipt.payment_proof_source_url,
                )
                payment_proof_path = join_proof_values([str(path) for path in proof_paths])
            except Exception as error:  # noqa: BLE001
                LOGGER.warning(
                    "failed to materialize imported payment proof for receipt %s: %s",
                    receipt.id,
                    error,
                )
                payment_proof_path = None

            accounting_override = None
            if bundle.schema_version < EXPORT_SCHEMA_VERSION:
                accounting_override = ReceiptAccountingRecord(
                    receipt_id=receipt.id,
                    policy=AccountingPolicy.LEGACY_REIMBURSEMENT,
                    recorded_by_user_id=receipt.creator_user_id,
                    recorded_by_display_name=receipt.creator_display_name,
                    recorded_for_user_id=receipt.creator_user_id,
                    recorded_for_display_name=receipt.creator_display_name,
                    created_at=receipt.finalized_at,
                    updated_at=receipt.finalized_at,
                )

            try:
                await self.save_receipt_with_accounting(
                    NewReceipt(
                        id=receipt.id,
                        creator_user_id=receipt.creator_user_id,
                        creator_username=receipt.creator_username,
                        creator_display_name=receipt.creator_display_name,
                        guild_id=receipt.guild_id,
                        channel_id=receipt.channel_id,
                        total_sale=receipt.total_sale,
                        procurement_cost=receipt.procurement_cost,
                        profit=receipt.profit,
                        status=final_status,
                        payment_proof_path=payment_proof_path,
                        payment_proof_source_url=receipt.payment_proof_source_url,
                        payment_proof=None,
                        payment_proofs=None,
                        admin_note=receipt.admin_note,
                        finalized_at=receipt.finalized_at,
                        items=list(receipt.items),
                    ),
                    None,
                    accounting_override,
                )
            except aiosqlite.IntegrityError as error:
                if "receipts.id" in str(error).lower() or "unique constraint failed" in str(error).lower():
                    report.skipped_existing_receipts += 1
                    continue
                raise

            if override_actor is not None and status_override is not None and final_status is not receipt.status:
                actor_user_id, actor_display_name = override_actor
                await self.insert_audit_event(
                    AuditEventInput(
                        actor_user_id=actor_user_id,
                        actor_display_name=actor_display_name,
                        action="receipt_status_import_override",
                        target_receipt_id=receipt.id,
                        detail_json={
                            "from": receipt.status.as_str(),
                            "to": final_status.as_str(),
                        },
                    )
                )

            report.imported_receipts += 1

        for audit in bundle.audit_events:
            if await self._restore_snapshot_metadata_from_audit_event(audit):
                continue

            async with self._connection.execute(
                "SELECT 1 FROM audit_events WHERE id = ?",
                (audit.id,),
            ) as cursor:
                exists = await cursor.fetchone()
            if exists is not None:
                report.skipped_existing_audit_events += 1
                continue

            await self._connection.execute(
                """
                INSERT INTO audit_events (
                    id,
                    actor_user_id,
                    actor_display_name,
                    action,
                    target_receipt_id,
                    detail_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    audit.id,
                    audit.actor_user_id,
                    audit.actor_display_name,
                    audit.action,
                    audit.target_receipt_id,
                    json.dumps(audit.detail_json, ensure_ascii=False),
                    audit.created_at.isoformat(),
                ),
            )
            report.imported_audit_events += 1

        await self._connection.commit()
        return report

    async def _restore_snapshot_metadata_from_audit_event(self, audit: AuditEvent) -> bool:
        if audit.action == "receipt_accounting_snapshot":
            await self._restore_receipt_accounting_snapshot(audit)
            return True
        if audit.action == "procurement_cutover_set":
            await self._restore_procurement_cutover_from_audit(audit)
            return True
        if audit.action == "procurement_ledger_entry":
            await self._restore_procurement_ledger_from_audit(audit)
            return True
        if audit.action == "payout_adjustment_snapshot":
            await self._restore_payout_adjustment_from_audit(audit)
            return True
        return False

    async def _restore_receipt_accounting_snapshot(self, audit: AuditEvent) -> None:
        receipt_id = audit.target_receipt_id
        if receipt_id is None:
            return

        detail = dict(audit.detail_json)
        created_at = (
            parse_datetime(str(detail["created_at"]))
            if detail.get("created_at") is not None
            else audit.created_at
        )
        updated_at = (
            parse_datetime(str(detail["updated_at"]))
            if detail.get("updated_at") is not None
            else created_at
        )
        accounting = ReceiptAccountingRecord(
            receipt_id=receipt_id,
            policy=AccountingPolicy.from_db(str(detail.get("accounting_policy") or "")),
            recorded_by_user_id=str(detail.get("recorded_by_user_id") or audit.actor_user_id),
            recorded_by_display_name=str(
                detail.get("recorded_by_display_name") or audit.actor_display_name
            ),
            recorded_for_user_id=str(detail.get("recorded_for_user_id") or receipt_id),
            recorded_for_display_name=str(
                detail.get("recorded_for_display_name") or audit.actor_display_name
            ),
            created_at=created_at,
            updated_at=updated_at,
        )
        await self._insert_or_replace_receipt_accounting(accounting)

    async def _restore_procurement_cutover_from_audit(self, audit: AuditEvent) -> None:
        detail = dict(audit.detail_json)
        cutover_at_raw = detail.get("cutover_at")
        if cutover_at_raw is None:
            return
        cutover_at = parse_datetime(str(cutover_at_raw))
        updated_at = str(detail.get("updated_at") or audit.created_at.isoformat())
        await self._connection.execute(
            """
            INSERT INTO procurement_settings (
                id,
                cutover_at,
                actor_user_id,
                actor_display_name,
                updated_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                cutover_at = excluded.cutover_at,
                actor_user_id = excluded.actor_user_id,
                actor_display_name = excluded.actor_display_name,
                updated_at = excluded.updated_at
            """,
            (
                PROCUREMENT_CUTOVER_ROW_ID,
                cutover_at.isoformat(),
                detail.get("actor_user_id"),
                detail.get("actor_display_name"),
                updated_at,
            ),
        )

    async def _restore_procurement_ledger_from_audit(self, audit: AuditEvent) -> None:
        detail = dict(audit.detail_json)
        ledger_id = detail.get("ledger_id")
        if ledger_id is None:
            return

        async with self._connection.execute(
            "SELECT 1 FROM procurement_ledger WHERE id = ?",
            (int(ledger_id),),
        ) as cursor:
            exists = await cursor.fetchone()
        if exists is not None:
            return

        created_at = (
            parse_datetime(str(detail["created_at"]))
            if detail.get("created_at") is not None
            else audit.created_at
        )
        await self._connection.execute(
            """
            INSERT INTO procurement_ledger (
                id,
                user_id,
                amount,
                reason,
                receipt_id,
                actor_user_id,
                actor_display_name,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(ledger_id),
                str(detail.get("user_id") or ""),
                int(detail.get("amount") or 0),
                str(detail.get("reason") or ""),
                detail.get("receipt_id"),
                str(detail.get("actor_user_id") or audit.actor_user_id),
                str(detail.get("actor_display_name") or audit.actor_display_name),
                created_at.isoformat(),
            ),
        )

    async def _restore_payout_adjustment_from_audit(self, audit: AuditEvent) -> None:
        detail = dict(audit.detail_json)
        adjustment_id = detail.get("adjustment_id")
        if adjustment_id is None:
            return

        async with self._connection.execute(
            "SELECT 1 FROM payout_adjustments WHERE id = ?",
            (int(adjustment_id),),
        ) as cursor:
            exists = await cursor.fetchone()
        if exists is not None:
            return

        await self._connection.execute(
            """
            INSERT INTO payout_adjustments (
                id,
                user_id,
                display_name,
                amount_cents,
                reason,
                source_user_id,
                source_display_name,
                actor_user_id,
                actor_display_name,
                created_at,
                settled_at,
                settled_by_user_id,
                settled_by_display_name
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(adjustment_id),
                str(detail.get("user_id") or ""),
                str(detail.get("display_name") or detail.get("user_id") or ""),
                int(detail.get("amount_cents") or 0),
                str(detail.get("reason") or ""),
                detail.get("source_user_id"),
                detail.get("source_display_name"),
                str(detail.get("actor_user_id") or audit.actor_user_id),
                str(detail.get("actor_display_name") or audit.actor_display_name),
                str(detail.get("created_at") or audit.created_at.isoformat()),
                detail.get("settled_at"),
                detail.get("settled_by_user_id"),
                detail.get("settled_by_display_name"),
            ),
        )

    async def _receipt_exists(self, receipt_id: str) -> bool:
        async with self._connection.execute(
            "SELECT 1 FROM receipts WHERE id = ?",
            (receipt_id,),
        ) as cursor:
            row = await cursor.fetchone()
        return row is not None

    async def _effective_accounting_policy_for_receipt(
        self,
        finalized_at,
        accounting_override: ReceiptAccountingRecord | None,
    ) -> AccountingPolicy:
        if accounting_override is not None:
            return accounting_override.policy

        cutover = await self.procurement_cutover_state()
        if cutover is None or cutover.cutover_at is None:
            return AccountingPolicy.LEGACY_REIMBURSEMENT
        if finalized_at >= cutover.cutover_at:
            return AccountingPolicy.PROCUREMENT_FUNDS
        return AccountingPolicy.LEGACY_REIMBURSEMENT

    async def _insert_or_replace_receipt_accounting(
        self,
        accounting: ReceiptAccountingRecord,
    ) -> None:
        await self._connection.execute(
            """
            INSERT INTO receipt_accounting (
                receipt_id,
                policy,
                recorded_by_user_id,
                recorded_by_display_name,
                recorded_for_user_id,
                recorded_for_display_name,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(receipt_id) DO UPDATE SET
                policy = excluded.policy,
                recorded_by_user_id = excluded.recorded_by_user_id,
                recorded_by_display_name = excluded.recorded_by_display_name,
                recorded_for_user_id = excluded.recorded_for_user_id,
                recorded_for_display_name = excluded.recorded_for_display_name,
                updated_at = excluded.updated_at
            """,
            (
                accounting.receipt_id,
                accounting.policy.as_str(),
                accounting.recorded_by_user_id,
                accounting.recorded_by_display_name,
                accounting.recorded_for_user_id,
                accounting.recorded_for_display_name,
                accounting.created_at.isoformat(),
                accounting.updated_at.isoformat(),
            ),
        )

    def _enrich_receipt_created_audit(
        self,
        audit_event: AuditEventInput,
        receipt: NewReceipt,
        accounting: ReceiptAccountingRecord,
        policy: AccountingPolicy,
    ) -> AuditEventInput:
        if audit_event.action != "receipt_created":
            return audit_event

        detail = dict(audit_event.detail_json)
        detail["accounting_policy"] = policy.as_str()
        detail["recorded_by_user_id"] = accounting.recorded_by_user_id
        detail["recorded_by_display_name"] = accounting.recorded_by_display_name
        detail["recorded_for_user_id"] = accounting.recorded_for_user_id
        detail["recorded_for_display_name"] = accounting.recorded_for_display_name
        detail["receipt_id"] = receipt.id
        return AuditEventInput(
            actor_user_id=audit_event.actor_user_id,
            actor_display_name=audit_event.actor_display_name,
            action=audit_event.action,
            target_receipt_id=audit_event.target_receipt_id,
            detail_json=detail,
        )

    async def _insert_audit_event(self, audit_event: AuditEventInput) -> None:
        await self._connection.execute(
            """
            INSERT INTO audit_events (
                actor_user_id,
                actor_display_name,
                action,
                target_receipt_id,
                detail_json,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                audit_event.actor_user_id,
                audit_event.actor_display_name,
                audit_event.action,
                audit_event.target_receipt_id,
                json.dumps(audit_event.detail_json, ensure_ascii=False),
                utcnow().isoformat(),
            ),
        )

    def _parse_receipt_summary(self, row: aiosqlite.Row) -> ReceiptSummary:
        return ReceiptSummary(
            id=str(row["id"]),
            creator_display_name=str(row["creator_display_name"]),
            creator_user_id=str(row["creator_user_id"]),
            total_sale=int(row["total_sale"]),
            procurement_cost=int(row["procurement_cost"]),
            status=ReceiptStatus.from_db(str(row["status"])),
            finalized_at=parse_datetime(str(row["finalized_at"])),
        )

    def _parse_priced_item(self, row: aiosqlite.Row) -> PricedItem:
        return PricedItem(
            item_name=str(row["item_name"]),
            quantity=int(row["quantity"]),
            unit_sale_price=int(row["unit_sale_price"]),
            unit_cost=int(row["unit_cost"]),
            pricing_source=PricingSource.from_db(str(row["pricing_source"])),
            line_sale_total=int(row["line_sale_total"]),
            line_cost_total=int(row["line_cost_total"]),
        )

    def _parse_audit_event(self, row: aiosqlite.Row) -> AuditEvent:
        detail_json = json.loads(str(row["detail_json"]))
        return AuditEvent(
            id=int(row["id"]),
            actor_user_id=str(row["actor_user_id"]),
            actor_display_name=str(row["actor_display_name"]),
            action=str(row["action"]),
            target_receipt_id=row["target_receipt_id"],
            detail_json=detail_json,
            created_at=parse_datetime(str(row["created_at"])),
        )
