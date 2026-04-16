"""Shared in-memory bot state for calculator and workflow sessions."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from uuid import uuid4

from yt_assist.domain.models import DraftItem, utcnow


@dataclass(slots=True)
class SessionCreditTarget:
    user_id: str
    username: str
    display_name: str

    @classmethod
    def from_user_id(cls, user_id: int) -> SessionCreditTarget:
        raw = str(user_id)
        return cls(user_id=raw, username=raw, display_name=raw)


@dataclass(slots=True)
class CalculatorSession:
    user_id: int
    channel_id: int
    credited_user_id: str
    credited_username: str
    credited_display_name: str
    workflow_notice: str | None = None
    rescan_active: bool = False
    items: list[DraftItem] = field(default_factory=list)
    selected_contract_name: str | None = None
    payment_proof_path: str | None = None
    payment_proof_source_url: str | None = None
    last_selected_item_index: int | None = None
    awaiting_proof: bool = False
    proof_processing: bool = False
    timeout_warning_sent: bool = False
    panel_channel_id: int | None = None
    panel_message_id: int | None = None
    last_updated_at: datetime = field(default_factory=utcnow)


class BotState:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._sessions: dict[int, CalculatorSession] = {}

    async def upsert_session_with_credit(
        self,
        user_id: int,
        channel_id: int,
        credited: SessionCreditTarget,
    ) -> CalculatorSession:
        async with self._lock:
            existing = self._sessions.get(user_id)
            if existing is not None:
                existing.channel_id = channel_id
                existing.credited_user_id = credited.user_id
                existing.credited_username = credited.username
                existing.credited_display_name = credited.display_name
                existing.workflow_notice = None
                existing.rescan_active = False
                existing.payment_proof_path = None
                existing.payment_proof_source_url = None
                existing.last_selected_item_index = None
                existing.awaiting_proof = False
                existing.proof_processing = False
                existing.selected_contract_name = None
                existing.timeout_warning_sent = False
                existing.panel_channel_id = None
                existing.panel_message_id = None
                existing.last_updated_at = utcnow()
                return _clone_session(existing)

            session = CalculatorSession(
                user_id=user_id,
                channel_id=channel_id,
                credited_user_id=credited.user_id,
                credited_username=credited.username,
                credited_display_name=credited.display_name,
            )
            self._sessions[user_id] = session
            return _clone_session(session)

    async def current_session(self, user_id: int) -> CalculatorSession | None:
        async with self._lock:
            session = self._sessions.get(user_id)
            return _clone_session(session) if session is not None else None

    async def touch_session(self, user_id: int) -> CalculatorSession | None:
        async with self._lock:
            session = self._sessions.get(user_id)
            if session is None:
                return None
            session.last_updated_at = utcnow()
            session.timeout_warning_sent = False
            return _clone_session(session)

    async def replace_session(self, session: CalculatorSession) -> CalculatorSession:
        async with self._lock:
            session.last_updated_at = utcnow()
            self._sessions[session.user_id] = _clone_session(session)
            return _clone_session(session)

    async def update_session(
        self,
        user_id: int,
        updater,
    ) -> CalculatorSession | None:
        async with self._lock:
            session = self._sessions.get(user_id)
            if session is None:
                return None
            updater(session)
            session.last_updated_at = utcnow()
            session.timeout_warning_sent = False
            return _clone_session(session)

    async def set_panel_message(
        self,
        user_id: int,
        channel_id: int | None,
        message_id: int | None,
    ) -> CalculatorSession | None:
        async with self._lock:
            session = self._sessions.get(user_id)
            if session is None:
                return None
            session.panel_channel_id = channel_id
            session.panel_message_id = message_id
            return _clone_session(session)

    async def mark_waiting_for_proof(self, user_id: int) -> CalculatorSession:
        async with self._lock:
            session = self._sessions.get(user_id)
            if session is None:
                raise ValueError("no active calculator session")
            if not session.items:
                raise ValueError("add at least one item before printing the receipt")
            session.awaiting_proof = True
            session.proof_processing = False
            session.timeout_warning_sent = False
            session.last_updated_at = utcnow()
            return _clone_session(session)

    async def mark_proof_processing(self, user_id: int) -> CalculatorSession:
        async with self._lock:
            session = self._sessions.get(user_id)
            if session is None:
                raise ValueError("no active calculator session")
            if session.proof_processing:
                raise ValueError("receipt proof is already being processed")
            session.proof_processing = True
            session.timeout_warning_sent = False
            session.last_updated_at = utcnow()
            return _clone_session(session)

    async def restore_after_finalize_failure(
        self,
        user_id: int,
        *,
        awaiting_proof: bool,
    ) -> None:
        async with self._lock:
            session = self._sessions.get(user_id)
            if session is None:
                return
            session.awaiting_proof = awaiting_proof
            session.proof_processing = False
            session.timeout_warning_sent = False
            session.last_updated_at = utcnow()

    async def remove_session(self, user_id: int) -> CalculatorSession | None:
        async with self._lock:
            session = self._sessions.pop(user_id, None)
            return _clone_session(session) if session is not None else None

    async def sessions_snapshot(self) -> list[CalculatorSession]:
        async with self._lock:
            return [_clone_session(session) for session in self._sessions.values()]

    async def find_waiting_proof_session(
        self,
        user_id: int,
        channel_id: int,
        timeout_seconds: int,
    ) -> CalculatorSession | None:
        async with self._lock:
            session = self._sessions.get(user_id)
            if session is None:
                return None
            if not session.awaiting_proof or session.channel_id != channel_id:
                return None
            if session.last_updated_at + timedelta(seconds=timeout_seconds) < utcnow():
                self._sessions.pop(user_id, None)
                return None
            return _clone_session(session)


def new_receipt_id() -> str:
    suffix = uuid4().hex[:6].upper()
    return f"BM-{utcnow().strftime('%Y%m%d-%H%M%S')}-{suffix}"


def _clone_session(session: CalculatorSession | None) -> CalculatorSession:
    assert session is not None
    return CalculatorSession(
        user_id=session.user_id,
        channel_id=session.channel_id,
        credited_user_id=session.credited_user_id,
        credited_username=session.credited_username,
        credited_display_name=session.credited_display_name,
        workflow_notice=session.workflow_notice,
        rescan_active=session.rescan_active,
        items=[
                DraftItem(
                    item_name=item.item_name,
                    quantity=item.quantity,
                    override_unit_price=item.override_unit_price,
                    contract_name=item.contract_name,
                    display_name=item.display_name,
                    override_unit_cost=item.override_unit_cost,
                    package_key=item.package_key,
                    package_choices=dict(item.package_choices),
                    package_counts=dict(item.package_counts),
                )
            for item in session.items
        ],
        selected_contract_name=session.selected_contract_name,
        payment_proof_path=session.payment_proof_path,
        payment_proof_source_url=session.payment_proof_source_url,
        last_selected_item_index=session.last_selected_item_index,
        awaiting_proof=session.awaiting_proof,
        proof_processing=session.proof_processing,
        timeout_warning_sent=session.timeout_warning_sent,
        panel_channel_id=session.panel_channel_id,
        panel_message_id=session.panel_message_id,
        last_updated_at=session.last_updated_at,
    )
