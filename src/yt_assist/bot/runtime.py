"""Discord and local runtimes for the parity port."""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import shlex
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any

import discord
from discord import app_commands

from yt_assist.app import RuntimeContext, build_runtime_context_with_options
from yt_assist.config import load_runtime_config
from yt_assist.domain.backup import load_export_async, save_export_async
from yt_assist.domain.catalog import Catalog, CatalogItem
from yt_assist.domain.contracts import ContractEntry, ContractPriceEntry, Contracts, apply_contract_to_items
from yt_assist.domain.models import (
    AccountingPolicy,
    AuditEventInput,
    DraftItem,
    ExportBundle,
    LeaderboardEntry,
    NewReceipt,
    PricedItem,
    PricingSource,
    ReceiptAccountingRecord,
    ReceiptStatus,
    utcnow,
)
from yt_assist.domain.proof import join_proof_values, save_proof_attachment, split_proof_values
from yt_assist.domain.pricing import price_items
from yt_assist.single_instance import SingleInstanceError, SingleInstanceGuard
from yt_assist.storage.database import (
    ImportPreview,
    ImportReport,
    SanitizePreview,
)

from .commands import (
    CommandContext,
    CommandEvent,
    CommandResult,
    ConsoleActor,
    ConsoleChannel,
    execute_input,
    handle_component_click,
)
from .command_support import (
    parse_add_item_inputs,
    parse_calc_prefix_input,
    parse_prefill_items,
    parse_receipt_item_editor_input,
    parse_user_token,
)
from .render import (
    ActionRowPayload,
    BUTTON_STYLE_DANGER,
    BUTTON_STYLE_PRIMARY,
    BUTTON_STYLE_SECONDARY,
    BUTTON_STYLE_SUCCESS,
    ButtonPayload,
    EmbedPayload,
    ReplyPayload,
    ReceiptDisplayContext,
    admin_receipt_status_label,
    calc_action_rows,
    calc_completed_embed,
    calc_embeds,
    calc_timeout_action_rows,
    calc_timeout_warning_embed,
    help_action_rows,
    help_page_embed,
    calc_failure_embed,
    calc_processing_embed,
    calc_reply_payload,
    lifecycle_status_embed,
    manage_page_parts,
    render_stats_description,
    receipt_detail_payload,
    receipt_log_payload,
    receipt_main_payload,
    task_error_embed,
    task_status_embed,
    task_warning_embed,
)
from .sanitize_support import (
    sanitize_completed_message,
    sanitize_confirmation_message,
    sanitize_noop_message,
    sanitize_preview_has_actions,
)
from .state import CalculatorSession, SessionCreditTarget, new_receipt_id

LOGGER = logging.getLogger(__name__)
STATUS_SCAN_LIMIT = 1_000
STATUS_REPOST_DEBOUNCE_SECONDS = 5
STATUS_REPOST_FALLBACK_INTERVAL_SECONDS = 60
TOPIC_HEARTBEAT_INTERVAL_SECONDS = 600
LOCAL_STOP_SIGNAL_FILE = "yt-assist.stop"
MANILA_OFFSET = timedelta(hours=8)


def _default_config_path(path: Path | None) -> Path:
    if path is not None and path.exists():
        return path
    if path is not None and not path.exists():
        example = path.with_name(path.name + ".example")
        if example.exists():
            return example
    config_path = Path("config") / "app.toml"
    if config_path.exists():
        return config_path
    example = Path("config") / "app.toml.example"
    if example.exists():
        return example
    raise FileNotFoundError("No runtime config file found.")


@dataclass(slots=True)
class ConsoleState:
    actor: ConsoleActor
    channel: ConsoleChannel
    interaction_mode: bool = False


@dataclass(slots=True)
class DiscordReplySpec:
    content: str | None
    embeds: list[discord.Embed]
    view: discord.ui.View | None
    ephemeral: bool
    allowed_mentions: discord.AllowedMentions | None

    def send_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        if self.content is not None:
            kwargs["content"] = self.content
        if self.embeds:
            kwargs["embeds"] = self.embeds
        if self.view is not None:
            kwargs["view"] = self.view
        if self.allowed_mentions is not None:
            kwargs["allowed_mentions"] = self.allowed_mentions
        return kwargs

    def edit_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "content": self.content,
            "embeds": self.embeds,
            "view": self.view,
        }
        if self.allowed_mentions is not None:
            kwargs["allowed_mentions"] = self.allowed_mentions
        return kwargs


@dataclass(slots=True)
class DiscordDispatchContext:
    actor: ConsoleActor
    channel: ConsoleChannel
    channel_object: Any
    is_interaction: bool
    invocation_message: discord.Message | None = None
    interaction: discord.Interaction[Any] | None = None
    reply_message: discord.Message | None = None


class ResetMode(str, Enum):
    ALL = "all"
    ONLY = "only"
    EXCEPT = "except"


class ResetAction(str, Enum):
    MARK_PAID = "paid"
    INVALIDATE = "invalidate"

    def target_status(self) -> ReceiptStatus:
        if self is ResetAction.MARK_PAID:
            return ReceiptStatus.PAID
        return ReceiptStatus.INVALIDATED

    def progress_prefix(self) -> str:
        if self is ResetAction.MARK_PAID:
            return "Marking active receipts as paid for"
        return "Invalidating active receipts for"

    def completed_prefix(self) -> str:
        if self is ResetAction.MARK_PAID:
            return "Marked active receipts as paid for"
        return "Invalidated active receipts for"

    def export_suffix(self) -> str:
        if self is ResetAction.MARK_PAID:
            return "paid"
        return "invalidated"

    def empty_result_verb(self) -> str:
        if self is ResetAction.MARK_PAID:
            return "marked paid"
        return "invalidated"


@dataclass(slots=True)
class PendingHelpPanel:
    prompt_message: discord.Message
    expires_at: Any


@dataclass(slots=True)
class PendingImport:
    channel_id: int
    expires_at: Any
    prompt_message: discord.Message
    status_override: ReceiptStatus | None


@dataclass(slots=True)
class PendingImportReview:
    channel_id: int
    expires_at: Any
    prompt_message: discord.Message
    status_override: ReceiptStatus | None
    temp_path: Path
    delete_after_use: bool
    attachment_filename: str
    preview: ImportPreview


@dataclass(slots=True)
class PendingProofUpdate:
    receipt_id: str
    channel_id: int
    expires_at: Any
    prompt_message: discord.Message


@dataclass(slots=True)
class PendingRescanCandidate:
    credited: SessionCreditTarget
    workflow_notice: str
    items: list[DraftItem]
    payment_proof_path: str | None
    payment_proof_source_url: str | None


@dataclass(slots=True)
class PendingReset:
    mode: ResetMode
    user_ids: list[int]


@dataclass(slots=True)
class PendingSanitizeReview:
    channel_id: int
    expires_at: Any
    prompt_message: discord.Message
    preview: SanitizePreview


class LifecycleStatusKind(str, Enum):
    STARTING = "starting"
    ONLINE = "online"
    STOPPING = "stopping"
    STOPPED = "stopped"


@dataclass(slots=True)
class LifecycleStatusState:
    kind: LifecycleStatusKind
    description: str


class ConsoleRuntime:
    def __init__(self, runtime: RuntimeContext, config_path: Path) -> None:
        self.runtime = runtime
        self.config_path = config_path
        admin_user_ids = [value for value in runtime.config.discord.admin_user_ids if value != 0]
        allowed_channels = [value for value in runtime.config.discord.allowed_channel_ids if value != 0]
        admin_channels = [value for value in runtime.config.discord.admin_channel_ids if value != 0]
        receipt_log_channel_id = runtime.config.discord.receipt_log_channel_id

        default_user_id = admin_user_ids[0] if admin_user_ids else 1
        default_channel_id = allowed_channels[0] if allowed_channels else 100
        default_label = "main"
        if not allowed_channels and admin_channels:
            default_channel_id = admin_channels[0]
            default_label = "admin"
        elif not allowed_channels and not admin_channels and receipt_log_channel_id:
            default_channel_id = receipt_log_channel_id
            default_label = "log"

        self.state = ConsoleState(
            actor=ConsoleActor(
                user_id=default_user_id,
                username=str(default_user_id),
                display_name=str(default_user_id),
            ),
            channel=ConsoleChannel(channel_id=default_channel_id, label=default_label),
        )

    async def run(self) -> int:
        print("YTAssist local parity console")
        print(f"config: {self.config_path}")
        print("type :help for console controls")
        while True:
            line = await asyncio.to_thread(input, self._prompt())
            if not line.strip():
                continue
            if line.startswith(":"):
                should_continue = await self._handle_meta(line)
                if not should_continue:
                    return 0
                continue
            await self._execute_command(line)

    async def run_once(self, command: str) -> int:
        await self._execute_command(command)
        return 0

    def _prompt(self) -> str:
        mode = "slash" if self.state.interaction_mode else "prefix"
        return (
            f"[user={self.state.actor.user_id} channel={self.state.channel.label}:{self.state.channel.channel_id} "
            f"mode={mode}] > "
        )

    async def _handle_meta(self, line: str) -> bool:
        parts = line[1:].strip().split()
        if not parts:
            return True
        command = parts[0].lower()
        args = parts[1:]

        if command in {"quit", "exit"}:
            return False
        if command == "help":
            print(
                "\n".join(
                    [
                        ":help",
                        ":context",
                        ":user <id> [display_name]",
                        ":channel main|admin|log|<id>",
                        ":interaction on|off",
                        ":click <custom_id>",
                        ":seed demo",
                        ":clear-db",
                        ":quit",
                    ]
                )
            )
            return True
        if command == "context":
            print(
                json.dumps(
                    {
                        "actor": {
                            "user_id": self.state.actor.user_id,
                            "username": self.state.actor.username,
                            "display_name": self.state.actor.display_name,
                        },
                        "channel": {
                            "channel_id": self.state.channel.channel_id,
                            "label": self.state.channel.label,
                        },
                        "interaction_mode": self.state.interaction_mode,
                    },
                    indent=2,
                    ensure_ascii=False,
                )
            )
            return True
        if command == "user" and args:
            user_id = int(args[0])
            display_name = " ".join(args[1:]) if len(args) > 1 else str(user_id)
            self.state.actor = ConsoleActor(
                user_id=user_id,
                username=str(user_id),
                display_name=display_name,
            )
            return True
        if command == "channel" and args:
            await self._set_channel(args[0])
            return True
        if command == "interaction" and args:
            self.state.interaction_mode = args[0].lower() in {"on", "true", "1", "yes"}
            return True
        if command == "click" and args:
            await self._click(args[0])
            return True
        if command == "seed" and args and args[0].lower() == "demo":
            await self._seed_demo_data()
            print("seeded demo data")
            return True
        if command == "clear-db":
            await self.runtime.database.clear_receipts()
            print("cleared receipt database")
            return True

        print(f"unknown console command: {line}")
        return True

    async def _set_channel(self, raw: str) -> None:
        token = raw.lower()
        config = self.runtime.config.discord
        if token == "main":
            channel_id = next((value for value in config.allowed_channel_ids if value != 0), 100)
            self.state.channel = ConsoleChannel(channel_id=channel_id, label="main")
            return
        if token == "admin":
            channel_id = next((value for value in config.admin_channel_ids if value != 0), 200)
            self.state.channel = ConsoleChannel(channel_id=channel_id, label="admin")
            return
        if token == "log":
            channel_id = config.receipt_log_channel_id or 300
            self.state.channel = ConsoleChannel(channel_id=channel_id, label="log")
            return
        self.state.channel = ConsoleChannel(channel_id=int(raw), label="custom")

    async def _click(self, custom_id: str) -> None:
        ctx = CommandContext(
            runtime=self.runtime,
            actor=self.state.actor,
            channel=self.state.channel,
            is_interaction=True,
            raw_input=custom_id,
        )
        result = await handle_component_click(ctx, custom_id)
        self._print_result(result)

    async def _execute_command(self, command: str) -> None:
        ctx = CommandContext(
            runtime=self.runtime,
            actor=self.state.actor,
            channel=self.state.channel,
            is_interaction=self.state.interaction_mode or command.strip().startswith("/"),
            raw_input=command,
        )
        result = await execute_input(ctx, command)
        if not result.handled:
            print(json.dumps({"type": "ignored", "input": command}, indent=2, ensure_ascii=False))
            return
        self._print_result(result)

    def _print_result(self, result: CommandResult) -> None:
        for event in result.events:
            print(json.dumps(event.to_dict(), indent=2, ensure_ascii=False))

    async def _seed_demo_data(self) -> None:
        now = utcnow()
        suffix = now.strftime("%Y%m%d%H%M%S")
        active = NewReceipt(
            id=f"YT-DEMO-ACTIVE-{suffix}",
            creator_user_id="42",
            creator_username="42",
            creator_display_name="Tester",
            guild_id="demo",
            channel_id=str(self.state.channel.channel_id),
            total_sale=15000,
            procurement_cost=10000,
            profit=5000,
            status=ReceiptStatus.ACTIVE,
            payment_proof_path=None,
            payment_proof_source_url=None,
            finalized_at=now,
            items=[
                PricedItem(
                    item_name="Shovel",
                    quantity=1,
                    unit_sale_price=15000,
                    unit_cost=10000,
                    pricing_source=PricingSource.DEFAULT,
                    line_sale_total=15000,
                    line_cost_total=10000,
                )
            ],
        )
        paid = NewReceipt(
            id=f"YT-DEMO-PAID-{suffix}",
            creator_user_id="42",
            creator_username="42",
            creator_display_name="Tester",
            guild_id="demo",
            channel_id=str(self.state.channel.channel_id),
            total_sale=8001,
            procurement_cost=5000,
            profit=3001,
            status=ReceiptStatus.PAID,
            payment_proof_path=None,
            payment_proof_source_url=None,
            finalized_at=now,
            items=[
                PricedItem(
                    item_name="Bucket",
                    quantity=1,
                    unit_sale_price=8001,
                    unit_cost=5000,
                    pricing_source=PricingSource.DEFAULT,
                    line_sale_total=8001,
                    line_cost_total=5000,
                )
            ],
        )
        invalidated = NewReceipt(
            id=f"YT-DEMO-VOID-{suffix}",
            creator_user_id="7",
            creator_username="7",
            creator_display_name="Another",
            guild_id="demo",
            channel_id=str(self.state.channel.channel_id),
            total_sale=5000,
            procurement_cost=2000,
            profit=3000,
            status=ReceiptStatus.INVALIDATED,
            payment_proof_path=None,
            payment_proof_source_url=None,
            finalized_at=now,
            items=[
                PricedItem(
                    item_name="Gloves",
                    quantity=1,
                    unit_sale_price=5000,
                    unit_cost=2000,
                    pricing_source=PricingSource.DEFAULT,
                    line_sale_total=5000,
                    line_cost_total=2000,
                )
            ],
        )

        await self.runtime.database.save_receipt(active, None)
        await self.runtime.database.save_receipt(paid, None)
        await self.runtime.database.save_receipt(invalidated, None)

        cutover_at = now
        await self.runtime.database.set_procurement_cutover(
            cutover_at,
            str(self.state.actor.user_id),
            self.state.actor.display_name,
        )
        await self.runtime.database.record_procurement_ledger_entry(
            "42",
            15000,
            "withdrawal",
            None,
            str(self.state.actor.user_id),
            self.state.actor.display_name,
        )
        procurement_receipt = NewReceipt(
            id=f"YT-DEMO-PROC-{suffix}",
            creator_user_id="42",
            creator_username="42",
            creator_display_name="Tester",
            guild_id="demo",
            channel_id=str(self.state.channel.channel_id),
            total_sale=12000,
            procurement_cost=6000,
            profit=6000,
            status=ReceiptStatus.ACTIVE,
            payment_proof_path=None,
            payment_proof_source_url=None,
            finalized_at=now,
            items=[
                PricedItem(
                    item_name="Lockpick",
                    quantity=1,
                    unit_sale_price=12000,
                    unit_cost=6000,
                    pricing_source=PricingSource.DEFAULT,
                    line_sale_total=12000,
                    line_cost_total=6000,
                )
            ],
        )
        await self.runtime.database.save_receipt_with_accounting(
            procurement_receipt,
            None,
            ReceiptAccountingRecord(
                receipt_id=procurement_receipt.id,
                policy=AccountingPolicy.PROCUREMENT_FUNDS,
                recorded_by_user_id="42",
                recorded_by_display_name="Tester",
                recorded_for_user_id="42",
                recorded_for_display_name="Tester",
                created_at=now,
                updated_at=now,
            ),
        )


class DiscordRuntimeFacade:
    def __init__(self, runtime: RuntimeContext, client: "YTAssistDiscordClient") -> None:
        self._runtime = runtime
        self._client = client

    @property
    def config(self):
        return self._runtime.config

    @property
    def logging_guards(self):
        return self._runtime.logging_guards

    @property
    def database(self):
        return self._runtime.database

    @property
    def bot_state(self):
        return self._runtime.bot_state

    @property
    def catalog(self):
        return self._runtime.catalog

    @catalog.setter
    def catalog(self, value) -> None:
        self._runtime.catalog = value

    @property
    def contracts(self):
        return self._runtime.contracts

    @contracts.setter
    def contracts(self, value) -> None:
        self._runtime.contracts = value

    async def build_health_report(self) -> tuple[list[str], list[str]]:
        return await self._client.build_health_report()


class PayloadView(discord.ui.View):
    def __init__(
        self,
        client: "YTAssistDiscordClient",
        runtime: DiscordRuntimeFacade,
        payload: ReplyPayload,
    ) -> None:
        timeout_seconds = runtime.config.discord.transient_message_timeout_seconds
        super().__init__(timeout=float(timeout_seconds) if timeout_seconds > 0 else None)
        self._client = client
        for row_index, row in enumerate(payload.components):
            for button_payload in row.components:
                self.add_item(
                    DispatchButton(
                        client=client,
                        label=button_payload.label,
                        style=_button_style(button_payload.style),
                        custom_id=button_payload.custom_id,
                        disabled=button_payload.disabled,
                        row=row_index,
                    )
                )

    async def on_error(
        self,
        interaction: discord.Interaction[Any],
        error: Exception,
        item: discord.ui.Item[Any],
    ) -> None:
        LOGGER.exception("custom_id=%s component callback failed", getattr(item, "custom_id", None))
        await _safe_interaction_warning(
            interaction,
            f"Component `{getattr(item, 'custom_id', 'unknown')}` failed: {error}",
        )


class DispatchButton(discord.ui.Button[Any]):
    def __init__(
        self,
        *,
        client: "YTAssistDiscordClient",
        label: str,
        style: discord.ButtonStyle,
        custom_id: str,
        disabled: bool = False,
        row: int | None = None,
    ) -> None:
        super().__init__(
            label=label,
            style=style,
            custom_id=custom_id,
            disabled=disabled,
            row=row,
        )
        self._client = client

    async def callback(self, interaction: discord.Interaction[Any]) -> None:
        return None


class DispatchSelect(discord.ui.Select[Any]):
    def __init__(
        self,
        *,
        client: "YTAssistDiscordClient",
        custom_id: str,
        placeholder: str,
        options: list[discord.SelectOption],
        row: int | None = None,
    ) -> None:
        super().__init__(
            custom_id=custom_id,
            placeholder=placeholder,
            options=options,
            min_values=1,
            max_values=1,
            row=row,
        )
        self._client = client

    async def callback(self, interaction: discord.Interaction[Any]) -> None:
        return None


class DispatchView(discord.ui.View):
    def __init__(self, client: "YTAssistDiscordClient", timeout_seconds: int) -> None:
        super().__init__(timeout=float(timeout_seconds) if timeout_seconds > 0 else None)
        self._client = client

    async def on_error(
        self,
        interaction: discord.Interaction[Any],
        error: Exception,
        item: discord.ui.Item[Any],
    ) -> None:
        LOGGER.exception("custom_id=%s component callback failed", getattr(item, "custom_id", None))
        await _safe_interaction_warning(
            interaction,
            f"Component `{getattr(item, 'custom_id', 'unknown')}` failed: {error}",
        )


def embed_from_payload(payload: EmbedPayload) -> discord.Embed:
    embed = discord.Embed(
        title=payload.title,
        description=payload.description,
        color=payload.color,
    )
    for field in payload.fields:
        embed.add_field(name=field.name, value=field.value, inline=field.inline)
    if payload.footer is not None:
        embed.set_footer(text=payload.footer.text)
    if payload.image_url is not None:
        embed.set_image(url=payload.image_url)
    return embed


def view_from_payload(
    client: "YTAssistDiscordClient",
    runtime: DiscordRuntimeFacade,
    payload: ReplyPayload,
) -> discord.ui.View | None:
    if not payload.components:
        return None
    return PayloadView(client, runtime, payload)


def reply_payload_to_spec(
    client: "YTAssistDiscordClient",
    runtime: DiscordRuntimeFacade,
    payload: ReplyPayload,
) -> DiscordReplySpec:
    return DiscordReplySpec(
        content=payload.content,
        embeds=[embed_from_payload(embed) for embed in payload.embeds],
        view=view_from_payload(client, runtime, payload),
        ephemeral=payload.ephemeral,
        allowed_mentions=discord.AllowedMentions.none() if payload.suppress_mentions else None,
    )


def _button_style(style: int) -> discord.ButtonStyle:
    if style == BUTTON_STYLE_SECONDARY:
        return discord.ButtonStyle.secondary
    if style == BUTTON_STYLE_SUCCESS:
        return discord.ButtonStyle.success
    if style == BUTTON_STYLE_DANGER:
        return discord.ButtonStyle.danger
    if style == BUTTON_STYLE_PRIMARY:
        return discord.ButtonStyle.primary
    return discord.ButtonStyle.secondary


def _actor_from_user(user: discord.abc.User) -> ConsoleActor:
    display_name = getattr(user, "display_name", user.name)
    return ConsoleActor(
        user_id=user.id,
        username=user.name,
        display_name=display_name,
    )


def _channel_label(runtime: RuntimeContext, channel_id: int) -> str:
    config = runtime.config.discord
    if channel_id in config.admin_channel_ids:
        return "admin"
    if channel_id in config.allowed_channel_ids:
        return "main"
    if config.receipt_log_channel_id == channel_id:
        return "log"
    return "discord"


def _channel_from_id(runtime: RuntimeContext, channel_id: int) -> ConsoleChannel:
    return ConsoleChannel(channel_id=channel_id, label=_channel_label(runtime, channel_id))


def _member_token(member: discord.Member | None) -> str | None:
    if member is None:
        return None
    return f"<@{member.id}>"


def _build_slash_input(command_name: str, *parts: str | None) -> str:
    tokens = [f"/{command_name}"]
    tokens.extend(part for part in parts if part is not None and part.strip())
    return " ".join(tokens)


def _interaction_custom_id(interaction: discord.Interaction[Any]) -> str:
    data = interaction.data or {}
    custom_id = data.get("custom_id")
    return str(custom_id) if custom_id is not None else ""


def _interaction_values(interaction: discord.Interaction[Any]) -> list[str]:
    data = interaction.data or {}
    values = data.get("values")
    if not isinstance(values, list):
        return []
    return [str(value) for value in values]


class YTAssistDiscordClient(discord.Client):
    def __init__(self, runtime: RuntimeContext) -> None:
        intents = discord.Intents.default()
        intents.guilds = True
        intents.message_content = True
        intents.messages = True
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self.base_runtime = runtime
        self.shared_runtime = DiscordRuntimeFacade(runtime, self)
        self._background_tasks: set[asyncio.Task[Any]] = set()
        self._commands_registered = False
        self._presence_applied = False
        self._monitors_started = False
        self._pending_lock = asyncio.Lock()
        self._pending_help_panels: dict[int, PendingHelpPanel] = {}
        self._pending_imports: dict[int, PendingImport] = {}
        self._pending_import_reviews: dict[int, PendingImportReview] = {}
        self._pending_proof_updates: dict[int, PendingProofUpdate] = {}
        self._pending_rescans: dict[int, list[PendingRescanCandidate]] = {}
        self._pending_resets: dict[int, PendingReset] = {}
        self._pending_sanitizes: dict[int, PendingSanitizeReview] = {}
        self._shutdown_requested = False
        self._restart_requested = False
        self._ready_announced = False
        self._host_shutdown_signal_name: str | None = None
        self._main_status: LifecycleStatusState | None = None
        self._admin_status: LifecycleStatusState | None = None
        self._status_message_ids: dict[int, int] = {}
        self._status_refresh_deadlines: dict[int, float] = {}
        self._status_refresh_tasks: dict[int, asyncio.Task[Any]] = {}
        self._channel_topics: dict[int, str] = {}
        self._channel_topic_next_allowed_at: dict[int, float] = {}
        self._lifecycle_refresh_lock = asyncio.Lock()

    async def setup_hook(self) -> None:
        await self._clear_local_stop_signal()
        self._register_app_commands()
        await self._sync_app_commands()
        self._track_task(self._announce_starting_up())

    async def on_ready(self) -> None:
        user = self.user
        if user is None:
            return
        if not self._presence_applied:
            await self.change_presence(activity=discord.Game(name=self.base_runtime.config.discord.status_text))
            self._presence_applied = True
        if not self._monitors_started:
            self._monitors_started = True
            self._track_task(self._monitor_calculator_sessions())
            self._track_task(self._monitor_help_panels())
            self._track_task(self._monitor_lifecycle_status_messages())
            self._track_task(self._monitor_channel_heartbeat())
            self._track_task(self._monitor_local_stop_signal())
        LOGGER.info("connected to Discord as %s (%s)", user.name, user.id)
        if not self._ready_announced:
            self._ready_announced = True
            self._track_task(self._announce_ready())

    async def on_interaction(self, interaction: discord.Interaction[Any]) -> None:
        if interaction.type is discord.InteractionType.component:
            LOGGER.debug(
                "user_id=%s channel_id=%s custom_id=%s component received",
                getattr(interaction.user, "id", None),
                interaction.channel_id,
                _interaction_custom_id(interaction),
            )
            try:
                await self.dispatch_component_interaction(interaction)
            except Exception as error:
                LOGGER.exception(
                    "user_id=%s channel_id=%s custom_id=%s component dispatch failed",
                    getattr(interaction.user, "id", None),
                    interaction.channel_id,
                    _interaction_custom_id(interaction),
                )
                await _safe_interaction_warning(
                    interaction,
                    (
                        f"Component `{_interaction_custom_id(interaction) or 'unknown'}` failed: {error}\n"
                        "The bot logged the stack trace to `logs/yt-assist.log`."
                    ),
                )

    async def on_message(self, message: discord.Message) -> None:
        self._note_channel_activity(message)
        if message.author.bot:
            return

        channel_id = message.channel.id
        is_allowed_channel = channel_id in self.base_runtime.config.discord.allowed_channel_ids
        is_admin_channel = channel_id in self.base_runtime.config.discord.admin_channel_ids

        if self.base_runtime.config.discord.log_seen_messages and (is_allowed_channel or is_admin_channel):
            LOGGER.debug(
                "user_id=%s channel_id=%s attachments=%s content=%s message seen",
                message.author.id,
                channel_id,
                len(message.attachments),
                message.content,
            )

        if message.attachments and (is_allowed_channel or is_admin_channel):
            if await self._handle_pending_import(message):
                return
            if await self._handle_pending_proof_update(message):
                return
            if await self._handle_waiting_receipt_proof(message):
                return

        prefix_match = self._parse_prefix_command(message.content)
        if prefix_match is not None:
            command_name, remainder = prefix_match
            if command_name == "calc":
                await self._run_calc_prefix(message, remainder)
                return
            if command_name == "manage":
                await self._run_manage_prefix(message)
                return
            if command_name == "adjustprices":
                await self._run_adjustprices_prefix(message)
                return
            if command_name == "reset":
                await self._run_reset_prefix(message, remainder, ResetMode.ALL)
                return
            if command_name == "resetexcept":
                await self._run_reset_prefix(message, remainder, ResetMode.EXCEPT)
                return
            if command_name == "import":
                await self._run_import_prefix(message, remainder)
                return
            if command_name == "sanitize":
                await self._run_sanitize_prefix(message)
                return
            if command_name == "rescan":
                await self._run_rescan_prefix(message, remainder)
                return
            if command_name == "rebuildlogs":
                await self._run_rebuild_logs_prefix(message)
                return
            if command_name == "restartbot":
                await self._run_restart_prefix(message)
                return
            if command_name == "stop":
                await self._run_stop_prefix(message)
                return

        ctx = CommandContext(
            runtime=self.shared_runtime,
            actor=_actor_from_user(message.author),
            channel=_channel_from_id(self.base_runtime, channel_id),
            is_interaction=False,
            raw_input=message.content,
        )
        result = await execute_input(ctx, message.content)
        if not result.handled:
            return

        dispatch = DiscordDispatchContext(
            actor=ctx.actor,
            channel=ctx.channel,
            channel_object=message.channel,
            is_interaction=False,
            invocation_message=message,
        )
        await self.apply_result(dispatch, result)

    async def _announce_starting_up(self) -> None:
        if self._ready_announced:
            return
        self._admin_status = LifecycleStatusState(
            kind=LifecycleStatusKind.STARTING,
            description="Bot is starting up. Command registration and readiness checks are in progress...",
        )
        self._main_status = LifecycleStatusState(
            kind=LifecycleStatusKind.STARTING,
            description="The calculator bot is starting up. Commands will be available in a moment.",
        )
        await self._refresh_lifecycle_status_messages_now(force=True, respect_active_session=False)

    async def _announce_ready(self) -> None:
        self._admin_status = LifecycleStatusState(
            kind=LifecycleStatusKind.ONLINE,
            description=(
                "Bot is fully online, ready to calculate!\n\n"
                "yt!payouts to see current staff payouts. yt!reset to mark active receipts paid or invalidate them."
            ),
        )
        self._main_status = LifecycleStatusState(
            kind=LifecycleStatusKind.ONLINE,
            description="Bot is fully online, ready to calculate!\n\ntype yt!calc or /ytcalc to start!",
        )
        await self._refresh_lifecycle_status_messages_now(force=True, respect_active_session=False)
        await self._refresh_channel_heartbeats_now()

    def request_host_shutdown(self, signal_name: str) -> None:
        normalized = signal_name.strip().upper() or "SIGTERM"
        self._host_shutdown_signal_name = normalized
        LOGGER.warning("signal=%s graceful shutdown requested from host control plane", normalized)
        if self.is_closed() or self._shutdown_requested:
            return
        self._track_task(
            self._request_shutdown(
                None,
                None,
                restart=False,
                source="host_signal",
            )
        )

    async def _announce_shutdown_status(
        self,
        *,
        restart: bool,
        source: str,
        actor_user_id: int | None,
    ) -> None:
        contact_user_id = self._shutdown_contact_user_id()
        if source == "user" and actor_user_id is not None:
            admin_reason = (
                f"Bot {'restart' if restart else 'shutdown'} requested by <@{actor_user_id}>. Shutting down now."
            )
        elif source == "host_signal":
            signal_name = self._host_shutdown_signal_name or "SIGTERM"
            admin_reason = (
                f"Bot shutdown requested by the hosting panel ({signal_name}). Shutting down now."
                if not restart
                else f"Bot restart requested by the hosting panel ({signal_name}). Shutting down now."
            )
        elif source == "local_cli":
            admin_reason = (
                "Bot shutdown requested from `stop.bat` / `stop-linux.sh`. Shutting down now."
                if not restart
                else "Bot restart requested from a local runner. Shutting down now."
            )
        else:
            admin_reason = (
                f"Bot {'restart' if restart else 'shutdown'} requested from the console. Shutting down now."
            )
        if contact_user_id is not None:
            admin_reason = f"<@{contact_user_id}> {admin_reason}"

        if restart:
            if contact_user_id is not None:
                main_reason = (
                    f"The calculator bot is shutting down for a restart. "
                    f"Please let <@{contact_user_id}> know if you need help while it is offline."
                )
            else:
                main_reason = (
                    "The calculator bot is shutting down for a restart. "
                    "Please let an admin know if you need help while it is offline."
                )
        else:
            if contact_user_id is not None:
                main_reason = (
                    f"The calculator bot is shutting down. "
                    f"Please let <@{contact_user_id}> know if you need help while it is offline."
                )
            else:
                main_reason = (
                    "The calculator bot is shutting down. "
                    "Please let an admin know if you need help while it is offline."
                )

        self._admin_status = LifecycleStatusState(
            kind=LifecycleStatusKind.STOPPING,
            description=admin_reason,
        )
        self._main_status = LifecycleStatusState(
            kind=LifecycleStatusKind.STOPPING,
            description=main_reason,
        )
        await self._refresh_lifecycle_status_messages_now(force=True, respect_active_session=False)

    async def _announce_stopped_status(self) -> None:
        contact_user_id = self._shutdown_contact_user_id()
        admin_reason = (
            f"<@{contact_user_id}> Bot is offline. Start the runner again to bring it back online."
            if contact_user_id is not None
            else "Bot is offline. Start the runner again to bring it back online."
        )
        main_reason = (
            f"The calculator bot is offline. Please let <@{contact_user_id}> know if you need help."
            if contact_user_id is not None
            else "The calculator bot is offline. Please let an admin know if you need help."
        )
        self._admin_status = LifecycleStatusState(
            kind=LifecycleStatusKind.STOPPED,
            description=admin_reason,
        )
        self._main_status = LifecycleStatusState(
            kind=LifecycleStatusKind.STOPPED,
            description=main_reason,
        )
        await self._refresh_lifecycle_status_messages_now(force=True, respect_active_session=False)

    def _configured_main_channel_id(self) -> int | None:
        for channel_id in self.base_runtime.config.discord.allowed_channel_ids:
            if channel_id > 0:
                return channel_id
        return None

    def _configured_admin_channel_id(self) -> int | None:
        for channel_id in self.base_runtime.config.discord.admin_channel_ids:
            if channel_id > 0:
                return channel_id
        return None

    def _shutdown_contact_user_id(self) -> int | None:
        for user_id in self.base_runtime.config.discord.admin_user_ids:
            if user_id > 0:
                return user_id
        return None

    def _is_lifecycle_channel_id(self, channel_id: int) -> bool:
        return channel_id in {
            channel
            for channel in (
                self._configured_main_channel_id(),
                self._configured_admin_channel_id(),
            )
            if channel is not None
        }

    async def _refresh_lifecycle_status_messages_now(
        self,
        *,
        force: bool = False,
        respect_active_session: bool = True,
    ) -> None:
        async with self._lifecycle_refresh_lock:
            admin_channel_id = self._configured_admin_channel_id()
            main_channel_id = self._configured_main_channel_id()
            if admin_channel_id is not None:
                await self._refresh_lifecycle_status_message(
                    admin_channel_id,
                    admin=True,
                    force=force,
                    respect_active_session=respect_active_session,
                )
            if main_channel_id is not None and main_channel_id != admin_channel_id:
                await self._refresh_lifecycle_status_message(
                    main_channel_id,
                    admin=False,
                    force=force,
                    respect_active_session=respect_active_session,
                )

    def _note_channel_activity(self, message: discord.Message) -> None:
        channel_id = message.channel.id
        if not self._is_lifecycle_channel_id(channel_id):
            return
        if _is_lifecycle_status_message(message):
            return
        self._schedule_lifecycle_status_refresh(channel_id)

    def _schedule_lifecycle_status_refresh(
        self,
        channel_id: int,
        *,
        delay_seconds: float = STATUS_REPOST_DEBOUNCE_SECONDS,
    ) -> None:
        self._status_refresh_deadlines[channel_id] = time.monotonic() + max(0.0, delay_seconds)
        existing = self._status_refresh_tasks.get(channel_id)
        if existing is None or existing.done():
            task = self._track_task(self._debounced_lifecycle_status_refresh(channel_id))
            self._status_refresh_tasks[channel_id] = task

    async def _debounced_lifecycle_status_refresh(self, channel_id: int) -> None:
        try:
            while not self.is_closed():
                deadline = self._status_refresh_deadlines.get(channel_id)
                if deadline is None:
                    return
                delay = max(0.0, deadline - time.monotonic())
                if delay > 0:
                    await asyncio.sleep(delay)
                    continue
                self._status_refresh_deadlines.pop(channel_id, None)
                await self._refresh_lifecycle_status_message(
                    channel_id,
                    admin=self._is_admin_channel_id(channel_id),
                )
                return
        finally:
            current = self._status_refresh_tasks.get(channel_id)
            if current is asyncio.current_task():
                self._status_refresh_tasks.pop(channel_id, None)

    async def _refresh_lifecycle_status_message(
        self,
        channel_id: int,
        *,
        admin: bool,
        force: bool = False,
        respect_active_session: bool = True,
    ) -> None:
        status = self._admin_status if admin else self._main_status
        if status is None:
            self._status_message_ids.pop(channel_id, None)
            return

        if respect_active_session and await self._has_active_session_in_channel(channel_id):
            message_id = self._status_message_ids.pop(channel_id, None)
            if message_id is not None:
                channel = self.get_channel(channel_id)
                if isinstance(channel, discord.TextChannel):
                    try:
                        message = await channel.fetch_message(message_id)
                    except (discord.HTTPException, discord.NotFound):
                        message = None
                    if message is not None:
                        await _safe_delete_message(message)
            return

        latest_message_id = await self._latest_channel_message_id(channel_id)
        cached_message_id = self._status_message_ids.get(channel_id)
        if not force and latest_message_id is not None and latest_message_id == cached_message_id:
            return

        channel = self.get_channel(channel_id)
        if not isinstance(channel, discord.TextChannel):
            try:
                fetched_channel = await self.fetch_channel(channel_id)
            except discord.HTTPException as error:
                LOGGER.warning("channel_id=%s failed to refresh lifecycle status: %s", channel_id, error)
                return
            if not isinstance(fetched_channel, discord.TextChannel):
                return
            channel = fetched_channel

        existing = await self._collect_lifecycle_status_messages(channel)
        for message in existing:
            await _safe_delete_message(message)

        sent = await channel.send(
            embed=embed_from_payload(lifecycle_status_embed(status.kind.value, status.description)),
            allowed_mentions=discord.AllowedMentions.none(),
        )
        self._status_message_ids[channel_id] = sent.id

    async def _latest_channel_message_id(self, channel_id: int) -> int | None:
        channel = self.get_channel(channel_id)
        if not isinstance(channel, discord.TextChannel):
            try:
                fetched_channel = await self.fetch_channel(channel_id)
            except discord.HTTPException:
                return None
            if not isinstance(fetched_channel, discord.TextChannel):
                return None
            channel = fetched_channel
        try:
            messages = [message async for message in channel.history(limit=1)]
        except discord.HTTPException:
            return None
        return messages[0].id if messages else None

    async def _collect_lifecycle_status_messages(
        self,
        channel: discord.TextChannel,
    ) -> list[discord.Message]:
        bot_user = self.user
        if bot_user is None:
            return []
        matches: list[discord.Message] = []
        inspected = 0
        async for message in channel.history(limit=STATUS_SCAN_LIMIT):
            inspected += 1
            if inspected > STATUS_SCAN_LIMIT:
                break
            if message.author.id != bot_user.id:
                continue
            if _is_lifecycle_status_message(message):
                matches.append(message)
        return matches

    async def _has_active_session_in_channel(self, channel_id: int) -> bool:
        snapshot = await self.base_runtime.bot_state.sessions_snapshot()
        return any(session.channel_id == channel_id for session in snapshot)

    async def _monitor_lifecycle_status_messages(self) -> None:
        while not self.is_closed():
            await asyncio.sleep(STATUS_REPOST_FALLBACK_INTERVAL_SECONDS)
            await self._refresh_lifecycle_status_messages_now()

    async def _refresh_channel_heartbeats_now(self) -> None:
        channels: list[int] = []
        main_channel_id = self._configured_main_channel_id()
        admin_channel_id = self._configured_admin_channel_id()
        if main_channel_id is not None:
            channels.append(main_channel_id)
        if admin_channel_id is not None and admin_channel_id not in channels:
            channels.append(admin_channel_id)
        for channel_id in channels:
            await self._update_channel_heartbeat(channel_id)

    async def _monitor_channel_heartbeat(self) -> None:
        while not self.is_closed():
            await asyncio.sleep(TOPIC_HEARTBEAT_INTERVAL_SECONDS)
            await self._refresh_channel_heartbeats_now()

    async def _update_channel_heartbeat(self, channel_id: int) -> None:
        channel = self.get_channel(channel_id)
        if not isinstance(channel, discord.TextChannel):
            try:
                fetched_channel = await self.fetch_channel(channel_id)
            except discord.HTTPException as error:
                LOGGER.warning("channel_id=%s failed to refresh channel heartbeat: %s", channel_id, error)
                return
            if not isinstance(fetched_channel, discord.TextChannel):
                return
            channel = fetched_channel

        base_topic = self._channel_topics.get(channel_id)
        if base_topic is None:
            base_topic = _strip_heartbeat_suffix(channel.topic or "")
            self._channel_topics[channel_id] = base_topic
            self._remember_recent_channel_heartbeat(channel_id, channel.topic or "")

        next_allowed_at = self._channel_topic_next_allowed_at.get(channel_id, 0.0)
        now_monotonic = time.monotonic()
        if now_monotonic < next_allowed_at:
            LOGGER.debug(
                "channel_id=%s skipped channel heartbeat update; next eligible in %.2f seconds",
                channel_id,
                next_allowed_at - now_monotonic,
            )
            return

        heartbeat = f"Status: Online! as of {_manila_now_local().strftime('%I:%M:%S %p %m/%d/%y')}."
        topic = heartbeat if not base_topic.strip() else f"{base_topic.strip()} | {heartbeat}"
        if topic == (channel.topic or ""):
            self._channel_topic_next_allowed_at[channel_id] = time.monotonic() + TOPIC_HEARTBEAT_INTERVAL_SECONDS
            return
        try:
            await channel.edit(topic=topic)
            self._channel_topic_next_allowed_at[channel_id] = time.monotonic() + TOPIC_HEARTBEAT_INTERVAL_SECONDS
        except discord.HTTPException as error:
            LOGGER.warning("channel_id=%s failed to update channel topic heartbeat: %s", channel_id, error)

    def _remember_recent_channel_heartbeat(self, channel_id: int, topic: str) -> None:
        timestamp = _extract_heartbeat_timestamp(topic)
        if timestamp is None:
            return
        elapsed = (_manila_now_local() - timestamp).total_seconds()
        if elapsed < 0 or elapsed >= TOPIC_HEARTBEAT_INTERVAL_SECONDS:
            return
        remaining = TOPIC_HEARTBEAT_INTERVAL_SECONDS - elapsed
        self._channel_topic_next_allowed_at[channel_id] = time.monotonic() + remaining

    async def _monitor_local_stop_signal(self) -> None:
        while not self.is_closed():
            await asyncio.sleep(STATUS_REPOST_DEBOUNCE_SECONDS)
            signal_path = self._local_stop_signal_path()
            if not signal_path.exists():
                continue
            LOGGER.warning("path=%s graceful shutdown requested from local stop signal", signal_path)
            await self._clear_local_stop_signal()
            if self._shutdown_requested:
                return
            await self._request_shutdown(
                None,
                None,
                restart=False,
                source="local_cli",
            )
            return

    def _local_stop_signal_path(self) -> Path:
        return Path.cwd() / "data" / LOCAL_STOP_SIGNAL_FILE

    async def _clear_local_stop_signal(self) -> None:
        signal_path = self._local_stop_signal_path()
        if not signal_path.exists():
            return
        try:
            signal_path.unlink()
        except OSError as error:
            LOGGER.warning("path=%s failed to clear local stop signal: %s", signal_path, error)

    def _parse_prefix_command(self, content: str) -> tuple[str, str] | None:
        prefix = self.base_runtime.config.discord.prefix
        stripped = content.strip()
        if not stripped.startswith(prefix):
            return None
        body = stripped[len(prefix) :].strip()
        if not body:
            return None
        parts = body.split(maxsplit=1)
        command = parts[0].lower()
        remainder = parts[1] if len(parts) > 1 else ""
        return command, remainder

    def _is_allowed_channel_id(self, channel_id: int) -> bool:
        config = self.base_runtime.config.discord
        return channel_id in config.allowed_channel_ids or channel_id in config.admin_channel_ids

    def _is_admin_channel_id(self, channel_id: int) -> bool:
        return channel_id in self.base_runtime.config.discord.admin_channel_ids

    def _is_admin_user(self, user_id: int) -> bool:
        return user_id in self.base_runtime.config.discord.admin_user_ids

    async def _run_calc_prefix(self, message: discord.Message, remainder: str) -> None:
        if not self._is_allowed_channel_id(message.channel.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("YouTool Access", "This command is not enabled in this channel."))]
            )
            return
        target_user_id, prefills = parse_calc_prefix_input(self.base_runtime.catalog, remainder)
        LOGGER.info(
            "user_id=%s channel_id=%s calc prefix parsed target_user_id=%s prefilled_items=%s raw=%r",
            message.author.id,
            message.channel.id,
            target_user_id,
            [(item.item_name, item.quantity) for item in prefills],
            remainder,
        )
        await self._start_calc_session(
            actor=message.author,
            channel=message.channel,
            target_user_id=target_user_id,
            prefilled_items=prefills,
            prefix_message=message,
        )

    async def _run_manage_prefix(self, message: discord.Message) -> None:
        if not self._is_admin_user(message.author.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("YouTool Access", "You are not an admin for this bot."))]
            )
            return
        if not self._is_admin_channel_id(message.channel.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("YouTool Access", "This admin command is not enabled in this channel."))]
                )
            return
        await self._send_manage_panel_message(message.author, message.channel)

    async def _run_adjustprices_prefix(self, message: discord.Message) -> None:
        if not self._is_admin_user(message.author.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("YouTool Access", "You are not an admin for this bot."))]
            )
            return
        if not self._is_admin_channel_id(message.channel.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("YouTool Access", "This admin command is not enabled in this channel."))]
            )
            return
        await self._send_adjustprices_panel(message.author, message.channel, prefix_message=message)

    async def _run_reset_prefix(
        self,
        message: discord.Message,
        remainder: str,
        mode: ResetMode,
    ) -> None:
        if not self._is_admin_user(message.author.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("YouTool Access", "You are not an admin for this bot."))]
            )
            return
        if not self._is_admin_channel_id(message.channel.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("YouTool Access", "This admin command is not enabled in this channel."))]
            )
            return
        user_ids = _parse_reset_target_user_ids(remainder)
        if mode is ResetMode.ALL and user_ids:
            mode = ResetMode.ONLY
        if mode is ResetMode.EXCEPT and not user_ids:
            await message.channel.send(
                embeds=[
                    embed_from_payload(
                        task_error_embed(
                            "YouTool Reset",
                            "Mention at least one user to exclude from the reset.",
                        )
                    )
                ]
            )
            return
        await self._send_reset_confirmation(
            actor=message.author,
            channel=message.channel,
            mode=mode,
            user_ids=user_ids,
            prefix_message=message,
        )

    async def _run_import_prefix(self, message: discord.Message, remainder: str) -> None:
        if not self._is_admin_user(message.author.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("YouTool Access", "You are not an admin for this bot."))]
            )
            return
        if not self._is_admin_channel_id(message.channel.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("YouTool Access", "This admin command is not enabled in this channel."))]
            )
            return
        try:
            file_name, status_override = _parse_import_request(remainder)
        except ValueError as error:
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("YouTool Import", str(error)))]
            )
            return
        if file_name is not None:
            await self._start_import_from_file(
                actor=message.author,
                channel=message.channel,
                file_name=file_name,
                status_override=status_override,
                prefix_message=message,
            )
            return
        await self._start_import_prompt(
            actor=message.author,
            channel=message.channel,
            status_override=status_override,
            prefix_message=message,
        )

    async def _run_sanitize_prefix(self, message: discord.Message) -> None:
        if not self._is_admin_user(message.author.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("YouTool Access", "You are not an admin for this bot."))]
            )
            return
        if not self._is_admin_channel_id(message.channel.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("YouTool Access", "This admin command is not enabled in this channel."))]
            )
            return
        await self._send_sanitize_confirmation(
            actor=message.author,
            channel=message.channel,
            prefix_message=message,
        )

    async def _run_rescan_prefix(self, message: discord.Message, remainder: str) -> None:
        if not self._is_admin_user(message.author.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("YouTool Access", "You are not an admin for this bot."))]
            )
            return
        if not (self._is_allowed_channel_id(message.channel.id) or self._is_admin_channel_id(message.channel.id)):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("YouTool Access", "This command is not enabled in this channel."))]
            )
            return
        await self._start_rescan(
            actor=message.author,
            invocation_channel=message.channel,
            boundary=remainder,
            prefix_message=message,
        )

    async def _run_rebuild_logs_prefix(self, message: discord.Message) -> None:
        if not self._is_admin_user(message.author.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("YouTool Access", "You are not an admin for this bot."))]
            )
            return
        if not self._is_admin_channel_id(message.channel.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("YouTool Access", "This admin command is not enabled in this channel."))]
            )
            return
        await self._rebuild_receipt_logs(message.author, message.channel, prefix_message=message)

    async def _run_restart_prefix(self, message: discord.Message) -> None:
        if not self._is_admin_user(message.author.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("YouTool Access", "You are not an admin for this bot."))]
            )
            return
        if not self._is_admin_channel_id(message.channel.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("YouTool Access", "This admin command is not enabled in this channel."))]
            )
            return
        await self._request_shutdown(message.author, message.channel, restart=True, prefix_message=message)

    async def _run_stop_prefix(self, message: discord.Message) -> None:
        if not self._is_admin_user(message.author.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("YouTool Access", "You are not an admin for this bot."))]
            )
            return
        if not self._is_admin_channel_id(message.channel.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("YouTool Access", "This admin command is not enabled in this channel."))]
            )
            return
        await self._request_shutdown(message.author, message.channel, restart=False, prefix_message=message)

    async def _start_calc_session(
        self,
        *,
        actor: discord.abc.User,
        channel: discord.abc.MessageableChannel,
        target_user_id: int | None,
        prefilled_items: list[DraftItem],
        interaction: discord.Interaction[Any] | None = None,
        prefix_message: discord.Message | None = None,
    ) -> None:
        try:
            credited = self._resolve_calc_credit_target(actor, target_user_id)
        except ValueError as error:
            if interaction is not None:
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("YouTool Access", str(error))),
                    ephemeral=True,
                )
            elif prefix_message is not None:
                await prefix_message.channel.send(
                    embeds=[embed_from_payload(task_error_embed("YouTool Access", str(error)))]
                )
            return
        session = await self.base_runtime.bot_state.upsert_session_with_credit(
            actor.id,
            channel.id,
            credited,
        )
        if prefilled_items:
            session.items = [
                DraftItem(
                    item_name=item.item_name,
                    quantity=item.quantity,
                    override_unit_price=item.override_unit_price,
                    contract_name=item.contract_name,
                )
                for item in prefilled_items
            ]
            session = await self.base_runtime.bot_state.replace_session(session)
        if prefix_message is not None and prefix_message.attachments:
            await self._preload_calc_proof_from_message(actor.id, prefix_message.attachments)
            refreshed = await self.base_runtime.bot_state.current_session(actor.id)
            if refreshed is not None:
                session = refreshed
        await self.base_runtime.bot_state.replace_session(session)
        payload = calc_reply_payload(self.base_runtime.catalog.items, session)

        dispatch = DiscordDispatchContext(
            actor=_actor_from_user(actor),
            channel=_channel_from_id(self.base_runtime, channel.id),
            channel_object=channel,
            is_interaction=interaction is not None,
            invocation_message=prefix_message,
            interaction=interaction,
        )
        await self._send_reply(dispatch, payload)
        if dispatch.reply_message is not None:
            await self.base_runtime.bot_state.set_panel_message(
                actor.id,
                dispatch.reply_message.channel.id,
                dispatch.reply_message.id,
            )
        _schedule_prefix_invocation_cleanup(self, prefix_message)

    def _resolve_calc_credit_target(
        self,
        actor: discord.abc.User,
        target_user_id: int | None,
    ) -> SessionCreditTarget:
        if target_user_id is None or target_user_id == actor.id:
            return SessionCreditTarget(
                user_id=str(actor.id),
                username=actor.name,
                display_name=getattr(actor, "display_name", actor.name),
            )
        if not self._is_admin_user(actor.id):
            raise ValueError("Only admins can credit a receipt to another user.")
        return SessionCreditTarget.from_user_id(target_user_id)

    async def _preload_calc_proof_from_message(
        self,
        user_id: int,
        attachments: list[discord.Attachment],
    ) -> None:
        proof_attachments = [
            attachment
            for attachment in attachments
            if (attachment.content_type or "").startswith("image/")
        ]
        if not proof_attachments:
            return
        session_receipt_id = f"YT-CALC-{user_id}-{utcnow().strftime('%Y%m%d-%H%M%S')}"
        proof_paths: list[str] = []
        proof_urls: list[str] = []
        for attachment in proof_attachments:
            saved = save_proof_attachment(
                self.base_runtime.config.storage.attachment_dir,
                session_receipt_id,
                attachment.filename,
                await attachment.read(),
            )
            proof_paths.append(str(saved.path))
            proof_urls.append(attachment.url)

        joined_paths = join_proof_values(proof_paths)
        joined_urls = join_proof_values(proof_urls)
        if joined_paths is None or joined_urls is None:
            return
        await self.base_runtime.bot_state.update_session(
            user_id,
            lambda session: _set_session_proof(session, joined_paths, joined_urls),
        )

    async def _handle_waiting_receipt_proof(self, message: discord.Message) -> bool:
        session = await self.base_runtime.bot_state.find_waiting_proof_session(
            message.author.id,
            message.channel.id,
            self.base_runtime.config.discord.pending_upload_timeout_seconds,
        )
        if session is None:
            return False
        try:
            processing = await self.base_runtime.bot_state.mark_proof_processing(message.author.id)
        except ValueError:
            return True

        try:
            saved_paths: list[str] = []
            source_urls: list[str] = []
            for attachment in message.attachments:
                saved = save_proof_attachment(
                    self.base_runtime.config.storage.attachment_dir,
                    f"YT-CALC-{processing.user_id}-{utcnow().strftime('%Y%m%d-%H%M%S')}",
                    attachment.filename,
                    await attachment.read(),
                )
                saved_paths.append(str(saved.path))
                source_urls.append(attachment.url)
            joined_paths = join_proof_values(saved_paths)
            joined_urls = join_proof_values(source_urls)
            if joined_paths is None or joined_urls is None:
                raise ValueError("No proof attachments were saved.")
            processing.payment_proof_path = joined_paths
            processing.payment_proof_source_url = joined_urls
            receipt_id = await self._finalize_session_receipt(
                processing,
                actor_user_id=str(message.author.id),
                actor_display_name=getattr(message.author, "display_name", message.author.name),
                guild_id=str(message.guild.id) if message.guild is not None else None,
                channel_id=str(message.channel.id),
                proof_message_id=message.id,
            )
            if processing.panel_channel_id is not None and processing.panel_message_id is not None:
                panel_channel = self.get_channel(processing.panel_channel_id)
                if isinstance(panel_channel, discord.abc.Messageable):
                    try:
                        panel_message = await panel_channel.fetch_message(processing.panel_message_id)
                    except (discord.HTTPException, discord.NotFound, AttributeError):
                        panel_message = None
                    if panel_message is not None:
                        if processing.rescan_active:
                            next_session = await self._advance_rescan_after_success(processing)
                            if next_session is not None:
                                await panel_message.edit(
                                    **reply_payload_to_spec(
                                        self,
                                        self.shared_runtime,
                                        calc_reply_payload(self.base_runtime.catalog.items, next_session),
                                    ).edit_kwargs()
                                )
                                return True
                            await panel_message.edit(
                                content="",
                                embeds=[embed_from_payload(task_status_embed("YouTool Rescan", "Receipt saved. No more undocumented receipt candidates remain in this queue."))],
                                view=None,
                            )
                        else:
                            await panel_message.edit(
                                content="",
                                embeds=[embed_from_payload(calc_completed_embed(receipt_id))],
                                view=None,
                            )
                        self._track_task(_safe_delete_message_later(panel_message, 10))
            return True
        except Exception:
            await self.base_runtime.bot_state.restore_after_finalize_failure(
                message.author.id,
                awaiting_proof=True,
            )
            raise

    async def build_health_report(self) -> tuple[list[str], list[str]]:
        current_user = self.user
        if current_user is None:
            checks = ["The Discord client is still connecting and has not authenticated yet."]
            issues = ["Wait for the bot to become ready, then run the health command again."]
            return checks, issues

        checks = [
            f"Authenticated as `{current_user.name}` (`{current_user.id}`) using the configured bot token."
        ]
        issues: list[str] = []
        config = self.base_runtime.config.discord

        if not config.admin_user_ids:
            issues.append("`admin_user_ids` is empty. Add at least one Discord user ID.")
        else:
            checks.append(f"Configured admin users: {len(config.admin_user_ids)}")

        if not config.allowed_channel_ids:
            issues.append("`allowed_channel_ids` is empty. Add at least one main channel ID.")

        if not config.admin_channel_ids:
            issues.append("`admin_channel_ids` is empty. Admin commands will not be usable anywhere.")

        if config.receipt_log_channel_id is None:
            issues.append("`receipt_log_channel_id` is not set. Admin log cards are currently disabled.")

        main_required = [
            ("view_channel", "View Channel"),
            ("send_messages", "Send Messages"),
            ("embed_links", "Embed Links"),
            ("attach_files", "Attach Files"),
            ("read_message_history", "Read Message History"),
            ("manage_messages", "Manage Messages"),
            ("manage_channels", "Manage Channels"),
        ]
        admin_required = [
            ("view_channel", "View Channel"),
            ("send_messages", "Send Messages"),
            ("embed_links", "Embed Links"),
            ("attach_files", "Attach Files"),
            ("read_message_history", "Read Message History"),
            ("manage_channels", "Manage Channels"),
        ]
        log_required = [
            ("view_channel", "View Channel"),
            ("send_messages", "Send Messages"),
            ("embed_links", "Embed Links"),
            ("attach_files", "Attach Files"),
            ("read_message_history", "Read Message History"),
        ]

        for channel_id in config.allowed_channel_ids:
            await self._inspect_channel(channel_id, "Main channel", main_required, checks, issues)

        for channel_id in config.admin_channel_ids:
            await self._inspect_channel(channel_id, "Admin channel", admin_required, checks, issues)

        if config.receipt_log_channel_id is not None:
            await self._inspect_channel(
                config.receipt_log_channel_id,
                "Receipt log channel",
                log_required,
                checks,
                issues,
            )

        if config.test_guild_id is None:
            checks.append(
                "`test_guild_id` is not set, so slash command updates register globally and may take longer to appear."
            )
        else:
            checks.append(f"Slash commands register instantly in test guild `{config.test_guild_id}`.")

        return checks, issues

    async def _handle_calc_component(
        self,
        interaction: discord.Interaction[Any],
        custom_id: str,
    ) -> None:
        parts = custom_id.split("|")
        if len(parts) < 3:
            await interaction.response.send_message("Malformed calculator component.", ephemeral=True)
            return
        owner_id = int(parts[2])
        if owner_id != interaction.user.id:
            await interaction.response.send_message(
                "This calculator panel belongs to another user.",
                ephemeral=True,
            )
            return

        action = parts[1]
        if action == "add":
            await self._open_add_item_panel(interaction)
            return
        if action == "pick":
            values = _interaction_values(interaction)
            if not values:
                await interaction.response.send_message("No item was selected.", ephemeral=True)
                return
            item_index = int(values[0])
            await interaction.response.send_modal(
                AddItemModal(self, interaction.user.id, item_index, self.base_runtime.catalog.items[item_index].name)
            )
            return
        if action == "add_last" and len(parts) >= 4:
            item_index = int(parts[3])
            item = self.base_runtime.catalog.items[item_index]
            await interaction.response.send_modal(
                AddItemModal(self, interaction.user.id, item_index, item.name)
            )
            return
        if action == "remove":
            await self._open_remove_item_panel(interaction)
            return
        if action == "remove_pick":
            values = _interaction_values(interaction)
            if not values:
                await interaction.response.send_message("No receipt line was selected.", ephemeral=True)
                return
            selected_index = int(values[0])
            session = await self.base_runtime.bot_state.update_session(
                interaction.user.id,
                lambda current: current.items.pop(selected_index) if selected_index < len(current.items) else None,
            )
            if session is None:
                await interaction.response.send_message("Calculator session missing.", ephemeral=True)
                return
            await self._edit_calc_panel(interaction, session)
            return
        if action == "contract":
            await self._open_contract_panel(interaction)
            return
        if action == "contract_pick":
            values = _interaction_values(interaction)
            if not values:
                await interaction.response.send_message("No contract was selected.", ephemeral=True)
                return
            selected_contract = values[0]
            session = await self.base_runtime.bot_state.update_session(
                interaction.user.id,
                lambda current: _apply_selected_contract(self.base_runtime.contracts, current, selected_contract),
            )
            if session is None:
                await interaction.response.send_message("Calculator session missing.", ephemeral=True)
                return
            await self._edit_calc_panel(interaction, session)
            return
        if action == "contract_clear":
            session = await self.base_runtime.bot_state.update_session(
                interaction.user.id,
                lambda current: _clear_selected_contract(self.base_runtime.contracts, current),
            )
            if session is None:
                await interaction.response.send_message("Calculator session missing.", ephemeral=True)
                return
            await self._edit_calc_panel(interaction, session)
            return
        if action == "rescan_skip":
            next_session = await self._activate_next_rescan_candidate(interaction.user.id)
            if next_session is None:
                await self.base_runtime.bot_state.remove_session(interaction.user.id)
                await interaction.response.edit_message(
                    content="",
                    embeds=[embed_from_payload(task_status_embed("YouTool Rescan", "No more undocumented receipt candidates remain in this queue."))],
                    view=None,
                )
                if interaction.message is not None:
                    self._track_task(_safe_delete_message_later(interaction.message, 10))
                return
            await self._edit_calc_panel(interaction, next_session)
            return
        if action in {"panel", "keepalive"}:
            session = await self.base_runtime.bot_state.touch_session(interaction.user.id)
            if session is None:
                await interaction.response.send_message("Calculator session missing.", ephemeral=True)
                return
            await self._edit_calc_panel(interaction, session)
            return
        if action in {"close", "cancel"}:
            session = await self.base_runtime.bot_state.remove_session(interaction.user.id)
            async with self._pending_lock:
                self._pending_rescans.pop(interaction.user.id, None)
            restored_channel_id = (
                session.channel_id
                if session is not None
                else getattr(getattr(interaction, "channel", None), "id", None)
            )
            if not interaction.response.is_done():
                await interaction.response.defer()
            deleted = False
            if interaction.message is not None:
                deleted = await _safe_delete_component_message(interaction, interaction.message)
            elif session is not None and session.panel_channel_id is not None and session.panel_message_id is not None:
                panel_channel = self.get_channel(session.panel_channel_id)
                if panel_channel is not None:
                    try:
                        panel_message = await panel_channel.fetch_message(session.panel_message_id)
                    except (discord.HTTPException, discord.NotFound, AttributeError):
                        panel_message = None
                    if panel_message is not None:
                        deleted = await _safe_delete_component_message(interaction, panel_message)
            if not deleted:
                await _safe_interaction_warning(
                    interaction,
                    "Calculator closed, but the panel message could not be deleted cleanly. "
                    "If it is still visible, you can ignore it.",
                )
            if restored_channel_id is not None:
                self._schedule_lifecycle_status_refresh(restored_channel_id, delay_seconds=0.25)
            return
        if action == "print":
            session = await self.base_runtime.bot_state.current_session(interaction.user.id)
            if session is None:
                await interaction.response.send_message("Calculator session missing.", ephemeral=True)
                return
            if session.payment_proof_path and session.payment_proof_source_url:
                try:
                    processing = await self.base_runtime.bot_state.mark_proof_processing(interaction.user.id)
                except ValueError:
                    return
                await interaction.response.edit_message(
                    content="",
                    embeds=[embed_from_payload(calc_processing_embed(self.base_runtime.catalog.items, processing, "Receipt proof already attached. Finalizing receipt now..."))],
                    view=None,
                )
                try:
                    receipt_id = await self._finalize_session_receipt(
                        processing,
                        actor_user_id=str(interaction.user.id),
                        actor_display_name=getattr(interaction.user, "display_name", interaction.user.name),
                        guild_id=str(interaction.guild.id) if interaction.guild is not None else None,
                        channel_id=str(interaction.channel_id),
                        proof_message_id=None,
                    )
                except Exception as error:
                    await self.base_runtime.bot_state.restore_after_finalize_failure(
                        interaction.user.id,
                        awaiting_proof=False,
                    )
                    await interaction.edit_original_response(
                        content="",
                        embeds=[embed_from_payload(calc_failure_embed(self.base_runtime.catalog.items, session, f"Receipt finalization failed for <@{interaction.user.id}>: {error}"))],
                        view=PayloadView(self, self.shared_runtime, ReplyPayload(components=calc_action_rows(session.user_id, False, session.rescan_active))),
                    )
                    raise
                await interaction.edit_original_response(
                    content="",
                    embeds=[embed_from_payload(calc_completed_embed(receipt_id))],
                    view=None,
                )
                original = await self._try_original_response(interaction)
                if processing.rescan_active and original is not None:
                    next_session = await self._advance_rescan_after_success(processing)
                    if next_session is not None:
                        await original.edit(
                            **reply_payload_to_spec(
                                self,
                                self.shared_runtime,
                                calc_reply_payload(self.base_runtime.catalog.items, next_session),
                            ).edit_kwargs()
                        )
                        return
                    await original.edit(
                        content="",
                        embeds=[embed_from_payload(task_status_embed("YouTool Rescan", "Receipt saved. No more undocumented receipt candidates remain in this queue."))],
                        view=None,
                    )
                if original is not None:
                    self._track_task(_safe_delete_message_later(original, 10))
                return
            try:
                waiting = await self.base_runtime.bot_state.mark_waiting_for_proof(interaction.user.id)
            except ValueError as error:
                await interaction.response.send_message(str(error), ephemeral=True)
                return
            await self._edit_calc_panel(interaction, waiting)
            return

    async def _handle_manage_component(
        self,
        interaction: discord.Interaction[Any],
        custom_id: str,
    ) -> None:
        parts = custom_id.split("|")
        if len(parts) < 4:
            await interaction.response.send_message("Malformed receipt manager component.", ephemeral=True)
            return
        owner_id = int(parts[2])
        if owner_id != interaction.user.id:
            await interaction.response.send_message("This receipt manager belongs to another user.", ephemeral=True)
            return
        if not self._is_admin_user(interaction.user.id):
            await interaction.response.send_message("You are not an admin for this bot.", ephemeral=True)
            return
        action = parts[1]
        if action == "page":
            page = int(parts[3])
            receipts = await self.base_runtime.database.list_receipts(page, 10)
            embed, rows = manage_page_parts(interaction.user.id, page, receipts)
            await interaction.response.edit_message(
                content=None,
                embeds=[embed_from_payload(embed)],
                view=PayloadView(self, self.shared_runtime, ReplyPayload(components=rows)),
            )
            return
        if action == "view" and len(parts) >= 5:
            page = int(parts[3])
            slot = int(parts[4])
            receipts = await self.base_runtime.database.list_receipts(page, 10)
            if slot >= len(receipts):
                await interaction.response.send_message("Receipt was not found.", ephemeral=True)
                return
            receipt = await self.base_runtime.database.get_receipt(receipts[slot].id)
            if receipt is None:
                await interaction.response.send_message("Receipt disappeared.", ephemeral=True)
                return
            payload = await self._receipt_detail_reply(receipt, interaction.user.id)
            spec = reply_payload_to_spec(self, self.shared_runtime, payload)
            await interaction.response.send_message(**spec.send_kwargs(), ephemeral=True)

    async def _handle_receipt_component(
        self,
        interaction: discord.Interaction[Any],
        custom_id: str,
    ) -> None:
        parts = custom_id.split("|")
        if len(parts) < 3:
            await interaction.response.send_message("Malformed receipt component.", ephemeral=True)
            return
        action = parts[1]
        if action == "delete" and len(parts) >= 4:
            creator_user_id = parts[2]
            receipt_id = parts[3]
            if str(interaction.user.id) != creator_user_id and not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message(
                    "Only the receipt creator or an admin can do that.",
                    ephemeral=True,
                )
                return
            confirm_payload = ReplyPayload(
                content=f"Are you sure you want to invalidate receipt `{receipt_id}`?",
                components=[
                    ActionRowPayload(
                        components=[
                            ButtonPayload(
                                custom_id=f"receipt|delete_confirm|{creator_user_id}|{receipt_id}",
                                label="Confirm Invalidate",
                                style=BUTTON_STYLE_DANGER,
                            ),
                            ButtonPayload(
                                custom_id=f"receipt|delete_cancel|{creator_user_id}|{receipt_id}",
                                label="Cancel",
                                style=BUTTON_STYLE_SECONDARY,
                            ),
                        ]
                    )
                ],
                ephemeral=True,
            )
            await interaction.response.send_message(
                **reply_payload_to_spec(self, self.shared_runtime, confirm_payload).send_kwargs(),
                ephemeral=True,
            )
            return
        if action == "edit":
            creator_user_id = parts[2]
            receipt_id = parts[3]
            if str(interaction.user.id) != creator_user_id and not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message(
                    "Only the receipt creator or an admin can do that.",
                    ephemeral=True,
                )
                return
            receipt = await self.base_runtime.database.get_receipt(receipt_id)
            if receipt is None:
                await interaction.response.send_message(f"Receipt `{receipt_id}` was not found.", ephemeral=True)
                return
            payload = await self._receipt_detail_reply(receipt, interaction.user.id)
            spec = reply_payload_to_spec(self, self.shared_runtime, payload)
            await interaction.response.send_message(**spec.send_kwargs(), ephemeral=True)
            return
        if action == "items" and len(parts) >= 4:
            owner_id = int(parts[2])
            receipt_id = parts[3]
            if owner_id != interaction.user.id:
                await interaction.response.send_message("This receipt panel belongs to another user.", ephemeral=True)
                return
            receipt = await self.base_runtime.database.get_receipt(receipt_id)
            if receipt is None:
                await interaction.response.send_message(f"Receipt `{receipt_id}` was not found.", ephemeral=True)
                return
            if str(interaction.user.id) != receipt.creator_user_id and not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message(
                    "Only the receipt creator or an admin can do that.",
                    ephemeral=True,
                )
                return
            await interaction.response.send_modal(
                EditReceiptItemsModal(
                    self,
                    owner_id,
                    receipt,
                )
            )
            return
        if action == "delete_confirm" and len(parts) >= 4:
            creator_user_id = parts[2]
            receipt_id = parts[3]
            if str(interaction.user.id) != creator_user_id and not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message(
                    "Only the receipt creator or an admin can do that.",
                    ephemeral=True,
                )
                return
            actor = AuditEventInput(
                actor_user_id=str(interaction.user.id),
                actor_display_name=getattr(interaction.user, "display_name", interaction.user.name),
                action="receipt_status_invalidated",
                target_receipt_id=receipt_id,
                detail_json={"status": "invalidated"},
            )
            updated = await self.base_runtime.database.update_receipt_status(
                receipt_id,
                ReceiptStatus.INVALIDATED,
                actor,
                None,
            )
            if not updated:
                await interaction.response.edit_message(content=f"Receipt `{receipt_id}` was not found.", embeds=[], view=None)
                return
            receipt = await self.base_runtime.database.get_receipt(receipt_id)
            if receipt is not None:
                await self._refresh_posted_receipt_messages(receipt)
            await interaction.response.edit_message(
                content=f"Receipt `{receipt_id}` invalidated. Matching receipt cards were refreshed.",
                embeds=[],
                view=None,
            )
            return
        if action == "delete_cancel":
            await interaction.response.edit_message(
                content="Invalidation cancelled.",
                embeds=[],
                view=None,
            )
            return
        if action == "log_refresh" and len(parts) >= 4:
            if not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message("You are not an admin for this bot.", ephemeral=True)
                return
            receipt_id = parts[3]
            receipt = await self.base_runtime.database.get_receipt(receipt_id)
            if receipt is None:
                await interaction.response.edit_message(content=f"Receipt `{receipt_id}` was not found.", embeds=[], view=None)
                return
            payload = await self._receipt_log_reply(receipt)
            await interaction.response.edit_message(**reply_payload_to_spec(self, self.shared_runtime, payload).edit_kwargs())
            return
        if action == "log_status" and len(parts) >= 4:
            if not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message("You are not an admin for this bot.", ephemeral=True)
                return
            receipt_id = parts[2]
            status = _admin_receipt_target_status(parts[3])
            if status is None:
                await interaction.response.send_message("Invalid receipt status target.", ephemeral=True)
                return
            await self._update_receipt_status(interaction, receipt_id, status, update_message=True)
            return
        if action == "detail_status" and len(parts) >= 5:
            owner_id = int(parts[2])
            if owner_id != interaction.user.id:
                await interaction.response.send_message("This receipt panel belongs to another user.", ephemeral=True)
                return
            if not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message("You are not an admin for this bot.", ephemeral=True)
                return
            receipt_id = parts[3]
            status = _admin_receipt_target_status(parts[4])
            if status is None:
                await interaction.response.send_message("Invalid receipt status target.", ephemeral=True)
                return
            await self._update_receipt_status(interaction, receipt_id, status, update_message=True, detail_owner_id=owner_id)
            return
        if action == "proof":
            owner_id = int(parts[2])
            receipt_id = parts[3]
            if owner_id != interaction.user.id:
                await interaction.response.send_message("This receipt panel belongs to another user.", ephemeral=True)
                return
            expires_at = utcnow() + _seconds_delta(self.base_runtime.config.discord.pending_upload_timeout_seconds)
            prompt = await _send_ephemeral_prompt(
                interaction,
                (
                    f"Upload a new proof image in this channel within "
                    f"{self.base_runtime.config.discord.pending_upload_timeout_seconds} seconds."
                ),
            )
            if prompt is not None:
                async with self._pending_lock:
                    self._pending_proof_updates[interaction.user.id] = PendingProofUpdate(
                        receipt_id=receipt_id,
                        channel_id=interaction.channel_id,
                        expires_at=expires_at,
                        prompt_message=prompt,
                    )
            return

    async def _handle_contracts_component(
        self,
        interaction: discord.Interaction[Any],
        custom_id: str,
    ) -> None:
        parts = custom_id.split("|")
        if len(parts) < 3 or parts[1] != "add":
            await interaction.response.send_message("Malformed contracts component.", ephemeral=True)
            return
        owner_id = int(parts[2])
        if owner_id != interaction.user.id:
            await interaction.response.send_message("This contracts panel belongs to another user.", ephemeral=True)
            return
        if not self._is_admin_user(interaction.user.id):
            await interaction.response.send_message("You are not an admin for this bot.", ephemeral=True)
            return
        await interaction.response.send_modal(ContractAddModal(self, owner_id))

    async def _handle_adjustprices_component(
        self,
        interaction: discord.Interaction[Any],
        custom_id: str,
    ) -> None:
        parts = custom_id.split("|")
        if len(parts) < 3 or parts[1] != "pick":
            await interaction.response.send_message("Malformed adjust prices component.", ephemeral=True)
            return
        owner_id = int(parts[2])
        if owner_id != interaction.user.id:
            await interaction.response.send_message("This price panel belongs to another user.", ephemeral=True)
            return
        if not self._is_admin_user(interaction.user.id):
            await interaction.response.send_message("You are not an admin for this bot.", ephemeral=True)
            return
        values = _interaction_values(interaction)
        if not values:
            await interaction.response.send_message("No catalog item was selected.", ephemeral=True)
            return
        item_index = int(values[0])
        if item_index >= len(self.base_runtime.catalog.items):
            await interaction.response.send_message("Selected catalog item no longer exists.", ephemeral=True)
            return
        await interaction.response.send_modal(
            AdjustPriceModal(self, owner_id, item_index, self.base_runtime.catalog.items[item_index])
        )

    async def _handle_import_component(
        self,
        interaction: discord.Interaction[Any],
        custom_id: str,
    ) -> None:
        parts = custom_id.split("|")
        if len(parts) < 3:
            await interaction.response.send_message("Malformed import component.", ephemeral=True)
            return
        owner_id = int(parts[2])
        if owner_id != interaction.user.id:
            await interaction.response.send_message("This import prompt belongs to another user.", ephemeral=True)
            return
        if not self._is_admin_user(interaction.user.id):
            await interaction.response.send_message("You are not an admin for this bot.", ephemeral=True)
            return
        action = parts[1]
        if action == "confirm":
            await self._confirm_import(interaction)
            return
        if action == "cancel":
            await self._cancel_import(interaction)
            return

    async def _handle_sanitize_component(
        self,
        interaction: discord.Interaction[Any],
        custom_id: str,
    ) -> None:
        parts = custom_id.split("|")
        if len(parts) < 3:
            await interaction.response.send_message("Malformed sanitize component.", ephemeral=True)
            return
        owner_id = int(parts[2])
        if owner_id != interaction.user.id:
            await interaction.response.send_message("This sanitize prompt belongs to another user.", ephemeral=True)
            return
        if not self._is_admin_user(interaction.user.id):
            await interaction.response.send_message("You are not an admin for this bot.", ephemeral=True)
            return
        action = parts[1]
        if action == "confirm":
            await self._confirm_sanitize(interaction)
            return
        if action == "cancel":
            await self._cancel_sanitize(interaction)
            return

    async def _handle_reset_component(
        self,
        interaction: discord.Interaction[Any],
        custom_id: str,
    ) -> None:
        parts = custom_id.split("|")
        if len(parts) < 3:
            await interaction.response.send_message("Malformed reset component.", ephemeral=True)
            return
        owner_id = int(parts[2])
        if owner_id != interaction.user.id:
            await interaction.response.send_message("This reset prompt belongs to another user.", ephemeral=True)
            return
        if not self._is_admin_user(interaction.user.id):
            await interaction.response.send_message("You are not an admin for this bot.", ephemeral=True)
            return
        action = parts[1]
        if action == "confirm":
            async with self._pending_lock:
                pending = self._pending_resets.get(interaction.user.id)
            if pending is None:
                await interaction.response.edit_message(
                    embeds=[embed_from_payload(task_error_embed("YouTool Reset", "This reset prompt expired. Run `yt!reset` again."))],
                    view=None,
                )
                return
            buttons = [
                ButtonPayload(
                    custom_id=f"reset|paid|{interaction.user.id}",
                    label="Mark Paid",
                    style=BUTTON_STYLE_SUCCESS,
                ),
                ButtonPayload(
                    custom_id=f"reset|invalidate|{interaction.user.id}",
                    label="Invalidate",
                    style=BUTTON_STYLE_DANGER,
                ),
                ButtonPayload(
                    custom_id=f"reset|cancel|{interaction.user.id}",
                    label="Cancel",
                    style=BUTTON_STYLE_SECONDARY,
                ),
            ]
            await interaction.response.edit_message(
                content=None,
                embeds=[
                    embed_from_payload(
                        task_warning_embed(
                            "YouTool Reset",
                            _reset_action_prompt_message(pending.mode, pending.user_ids),
                        )
                    )
                ],
                view=PayloadView(
                    self,
                    self.shared_runtime,
                    ReplyPayload(components=[ActionRowPayload(components=buttons)]),
                ),
            )
            return
        if action == "paid":
            await self._perform_reset(interaction, ResetAction.MARK_PAID)
            return
        if action == "invalidate":
            await self._perform_reset(interaction, ResetAction.INVALIDATE)
            return
        if action == "cancel":
            async with self._pending_lock:
                self._pending_resets.pop(interaction.user.id, None)
            await interaction.response.edit_message(content="Reset cancelled.", embeds=[], view=None)
            return

    async def _handle_help_component(
        self,
        interaction: discord.Interaction[Any],
        custom_id: str,
    ) -> None:
        parts = custom_id.split("|")
        if len(parts) < 4 or parts[:2] != ["help", "page"]:
            await interaction.response.send_message(f"Unknown component `{custom_id}`.", ephemeral=True)
            return
        owner_user_id = int(parts[2])
        page = int(parts[3])
        if owner_user_id != interaction.user.id:
            await interaction.response.send_message("This help panel belongs to another user.", ephemeral=True)
            return
        async with self._pending_lock:
            panel = self._pending_help_panels.get(interaction.user.id)
            if panel is None or panel.expires_at < utcnow():
                self._pending_help_panels.pop(interaction.user.id, None)
                await interaction.response.send_message(
                    embed=embed_from_payload(
                        task_warning_embed(
                            "Help Panel Expired",
                            "Run `yt!help` or `/ythelp` again to reopen the help panel.",
                        )
                    ),
                    ephemeral=True,
                )
                return
            panel.expires_at = utcnow() + _seconds_delta(self.base_runtime.config.discord.transient_message_timeout_seconds)
        payload = ReplyPayload(
            embeds=[help_page_embed(self.base_runtime.config.discord.prefix, page)],
            components=help_action_rows(interaction.user.id, page),
            ephemeral=True,
        )
        await interaction.response.edit_message(**reply_payload_to_spec(self, self.shared_runtime, payload).edit_kwargs())

    async def _send_manage_panel_message(
        self,
        actor: discord.abc.User,
        channel: discord.abc.MessageableChannel,
        interaction: discord.Interaction[Any] | None = None,
    ) -> None:
        receipts = await self.base_runtime.database.list_receipts(0, 10)
        embed, rows = manage_page_parts(actor.id, 0, receipts)
        payload = ReplyPayload(embeds=[embed], components=rows, ephemeral=interaction is not None)
        dispatch = DiscordDispatchContext(
            actor=_actor_from_user(actor),
            channel=_channel_from_id(self.base_runtime, channel.id),
            channel_object=channel,
            is_interaction=interaction is not None,
            interaction=interaction,
        )
        await self._send_reply(dispatch, payload)

    async def _send_adjustprices_panel(
        self,
        actor: discord.abc.User,
        channel: discord.abc.MessageableChannel,
        interaction: discord.Interaction[Any] | None = None,
        prefix_message: discord.Message | None = None,
        notice: str | None = None,
    ) -> None:
        payload = ReplyPayload(
            embeds=[_adjustprices_embed(self.base_runtime.catalog.items, notice)],
            ephemeral=interaction is not None,
        )
        dispatch = DiscordDispatchContext(
            actor=_actor_from_user(actor),
            channel=_channel_from_id(self.base_runtime, channel.id),
            channel_object=channel,
            is_interaction=interaction is not None,
            interaction=interaction,
            invocation_message=prefix_message,
        )
        await self._send_reply(dispatch, payload)
        if dispatch.reply_message is not None:
            await dispatch.reply_message.edit(
                view=self._build_adjustprices_view(actor.id, self.base_runtime.catalog.items)
            )
        _schedule_prefix_invocation_cleanup(self, prefix_message)

    async def _send_reset_confirmation(
        self,
        *,
        actor: discord.abc.User,
        channel: discord.abc.MessageableChannel,
        mode: ResetMode,
        user_ids: list[int],
        interaction: discord.Interaction[Any] | None = None,
        prefix_message: discord.Message | None = None,
    ) -> None:
        dispatch = DiscordDispatchContext(
            actor=_actor_from_user(actor),
            channel=_channel_from_id(self.base_runtime, channel.id),
            channel_object=channel,
            is_interaction=interaction is not None,
            interaction=interaction,
            invocation_message=prefix_message,
        )
        fixed_payload = ReplyPayload(
            embeds=[task_warning_embed("YouTool Reset", _reset_warning_message(mode, user_ids))],
            components=[
                ActionRowPayload(
                    components=[
                        ButtonPayload(
                            custom_id=f"reset|confirm|{actor.id}",
                            label="Continue",
                            style=BUTTON_STYLE_DANGER,
                        ),
                        ButtonPayload(
                            custom_id=f"reset|cancel|{actor.id}",
                            label="Cancel",
                            style=BUTTON_STYLE_SECONDARY,
                        ),
                    ]
                )
            ],
            ephemeral=interaction is not None,
        )
        await self._send_reply(dispatch, fixed_payload)
        if dispatch.reply_message is not None:
            async with self._pending_lock:
                self._pending_resets[actor.id] = PendingReset(mode=mode, user_ids=list(user_ids))
        _schedule_prefix_invocation_cleanup(self, prefix_message)

    async def _start_import_prompt(
        self,
        *,
        actor: discord.abc.User,
        channel: discord.abc.MessageableChannel,
        status_override: ReceiptStatus | None,
        interaction: discord.Interaction[Any] | None = None,
        prefix_message: discord.Message | None = None,
    ) -> None:
        async with self._pending_lock:
            prior_review = self._pending_import_reviews.pop(actor.id, None)
            if prior_review is not None:
                _cleanup_import_review_file(prior_review)
            self._pending_imports.pop(actor.id, None)
        prompt_payload = ReplyPayload(
            embeds=[
                task_status_embed(
                    "YouTool Import",
                    (
                        f"Upload the exported `.json` file in this channel within "
                        f"{self.base_runtime.config.discord.pending_upload_timeout_seconds} seconds.\n"
                        f"{_import_mode_prompt_line(status_override)}"
                    ),
                )
            ],
            ephemeral=interaction is not None,
        )
        dispatch = DiscordDispatchContext(
            actor=_actor_from_user(actor),
            channel=_channel_from_id(self.base_runtime, channel.id),
            channel_object=channel,
            is_interaction=interaction is not None,
            interaction=interaction,
            invocation_message=prefix_message,
        )
        await self._send_reply(dispatch, prompt_payload)
        if dispatch.reply_message is not None:
            async with self._pending_lock:
                self._pending_imports[actor.id] = PendingImport(
                    channel_id=channel.id,
                    expires_at=utcnow() + _seconds_delta(self.base_runtime.config.discord.pending_upload_timeout_seconds),
                    prompt_message=dispatch.reply_message,
                    status_override=status_override,
                )
        _schedule_prefix_invocation_cleanup(self, prefix_message)

    async def _start_import_from_file(
        self,
        *,
        actor: discord.abc.User,
        channel: discord.abc.MessageableChannel,
        file_name: str,
        status_override: ReceiptStatus | None,
        prefix_message: discord.Message | None = None,
    ) -> None:
        try:
            source_path = _resolve_import_file_path(
                self.base_runtime.config.storage.import_dir,
                file_name,
            )
        except ValueError as error:
            await channel.send(
                embeds=[embed_from_payload(task_error_embed("YouTool Import", str(error)))]
            )
            return

        async with self._pending_lock:
            prior_review = self._pending_import_reviews.pop(actor.id, None)
            if prior_review is not None:
                _cleanup_import_review_file(prior_review)
            self._pending_imports.pop(actor.id, None)

        prompt_message = await channel.send(
            embed=embed_from_payload(
                task_status_embed(
                    "YouTool Import",
                    (
                        f"Import file found in `/import`: `{source_path.name}` "
                        f"({source_path.stat().st_size} bytes).\n"
                        f"Validating file and preparing import preview...\n"
                        f"{_import_mode_prompt_line(status_override)}"
                    ),
                )
            )
        )
        await self._stage_import_review(
            actor=actor,
            channel_id=channel.id,
            prompt_message=prompt_message,
            status_override=status_override,
            bundle_path=source_path,
            attachment_filename=source_path.name,
            delete_after_use=False,
        )
        _schedule_prefix_invocation_cleanup(self, prefix_message)

    async def _stage_import_review(
        self,
        *,
        actor: discord.abc.User,
        channel_id: int,
        prompt_message: discord.Message,
        status_override: ReceiptStatus | None,
        bundle_path: Path,
        attachment_filename: str,
        delete_after_use: bool,
    ) -> None:
        try:
            bundle = await load_export_async(bundle_path)
            preview = await self.base_runtime.database.inspect_import_bundle(bundle, status_override)
        except Exception as error:
            if delete_after_use:
                bundle_path.unlink(missing_ok=True)
            await _safe_edit_message(
                prompt_message,
                content=f"Import failed for <@{actor.id}> while processing `{attachment_filename}`: {error}",
                embeds=[],
                view=None,
            )
            return

        if preview.importable_receipts == 0:
            if delete_after_use:
                bundle_path.unlink(missing_ok=True)
            await _safe_edit_message(
                prompt_message,
                content="",
                embeds=[
                    embed_from_payload(
                        task_status_embed(
                            "YouTool Import",
                            _import_noop_message(status_override, preview),
                        )
                    )
                ],
                view=None,
            )
            return

        async with self._pending_lock:
            self._pending_import_reviews[actor.id] = PendingImportReview(
                channel_id=channel_id,
                expires_at=utcnow()
                + _seconds_delta(self.base_runtime.config.discord.pending_upload_timeout_seconds),
                prompt_message=prompt_message,
                status_override=status_override,
                temp_path=bundle_path,
                delete_after_use=delete_after_use,
                attachment_filename=attachment_filename,
                preview=preview,
            )

        confirm_payload = ReplyPayload(
            embeds=[task_warning_embed("YouTool Import", _import_confirmation_message(status_override, preview))],
            components=[
                ActionRowPayload(
                    components=[
                        ButtonPayload(
                            custom_id=f"import|confirm|{actor.id}",
                            label="Confirm Import",
                            style=BUTTON_STYLE_SUCCESS,
                        ),
                        ButtonPayload(
                            custom_id=f"import|cancel|{actor.id}",
                            label="Cancel",
                            style=BUTTON_STYLE_SECONDARY,
                        ),
                    ]
                )
            ],
            ephemeral=True,
        )
        await _safe_edit_message(
            prompt_message,
            content="",
            embeds=[embed_from_payload(embed) for embed in confirm_payload.embeds],
            view=PayloadView(self, self.shared_runtime, confirm_payload),
        )
        LOGGER.info(
            "file=%s importable=%s skipped_existing=%s skipped_duplicate_ids_in_file=%s import staged for confirmation",
            attachment_filename,
            preview.importable_receipts,
            preview.skipped_existing_receipts,
            preview.skipped_duplicate_ids_in_file,
        )

    async def _send_sanitize_confirmation(
        self,
        *,
        actor: discord.abc.User,
        channel: discord.abc.MessageableChannel,
        interaction: discord.Interaction[Any] | None = None,
        prefix_message: discord.Message | None = None,
    ) -> None:
        async with self._pending_lock:
            self._pending_sanitizes.pop(actor.id, None)
            protected_import_paths = {
                review.temp_path
                for review in self._pending_import_reviews.values()
                if review.temp_path.exists()
            }

        try:
            preview = await self.base_runtime.database.preview_sanitize(
                self.base_runtime.config.storage.attachment_dir,
                self.base_runtime.config.storage.export_dir,
                self.base_runtime.config.storage.import_dir,
                protected_import_paths=protected_import_paths,
            )
        except Exception as error:
            if interaction is not None:
                await interaction.response.send_message(
                    embed=embed_from_payload(
                        task_error_embed("YouTool Sanitize", f"Sanitize preview failed: {error}")
                    ),
                    ephemeral=True,
                )
            else:
                await channel.send(
                    embeds=[
                        embed_from_payload(
                            task_error_embed("YouTool Sanitize", f"Sanitize preview failed: {error}")
                        )
                    ]
                )
            return

        dispatch = DiscordDispatchContext(
            actor=_actor_from_user(actor),
            channel=_channel_from_id(self.base_runtime, channel.id),
            channel_object=channel,
            is_interaction=interaction is not None,
            interaction=interaction,
            invocation_message=prefix_message,
        )

        if not sanitize_preview_has_actions(preview):
            await self._send_reply(
                dispatch,
                ReplyPayload(
                    embeds=[
                        task_status_embed(
                            "YouTool Sanitize",
                            sanitize_noop_message(preview),
                        )
                    ],
                    ephemeral=interaction is not None,
                ),
            )
            _schedule_prefix_invocation_cleanup(self, prefix_message)
            return

        payload = ReplyPayload(
            embeds=[task_warning_embed("YouTool Sanitize", sanitize_confirmation_message(preview))],
            components=[
                ActionRowPayload(
                    components=[
                        ButtonPayload(
                            custom_id=f"sanitize|confirm|{actor.id}",
                            label="Apply Safe Cleanup",
                            style=BUTTON_STYLE_DANGER,
                        ),
                        ButtonPayload(
                            custom_id=f"sanitize|cancel|{actor.id}",
                            label="Cancel",
                            style=BUTTON_STYLE_SECONDARY,
                        ),
                    ]
                )
            ],
            ephemeral=interaction is not None,
        )
        await self._send_reply(dispatch, payload)
        if dispatch.reply_message is not None:
            async with self._pending_lock:
                self._pending_sanitizes[actor.id] = PendingSanitizeReview(
                    channel_id=channel.id,
                    expires_at=utcnow()
                    + _seconds_delta(self.base_runtime.config.discord.pending_upload_timeout_seconds),
                    prompt_message=dispatch.reply_message,
                    preview=preview,
                )
        _schedule_prefix_invocation_cleanup(self, prefix_message)

    async def _start_rescan(
        self,
        *,
        actor: discord.abc.User,
        invocation_channel: discord.abc.MessageableChannel,
        boundary: str,
        interaction: discord.Interaction[Any] | None = None,
        prefix_message: discord.Message | None = None,
    ) -> None:
        boundary = boundary.strip()
        if not boundary:
            error_text = (
                f"Missing message ID or message link. Example: "
                f"`{self.base_runtime.config.discord.prefix}rescan 123456789012345678`."
            )
            if interaction is not None:
                await interaction.response.send_message(embed=embed_from_payload(task_error_embed("YouTool Rescan", error_text)), ephemeral=True)
            else:
                await invocation_channel.send(embeds=[embed_from_payload(task_error_embed("YouTool Rescan", error_text))])
            return

        try:
            scan_channel_id, after_message_id = _resolve_rescan_boundary(
                self.base_runtime.config.discord.allowed_channel_ids,
                invocation_channel.id,
                boundary,
                self._is_allowed_channel_id(invocation_channel.id),
            )
        except ValueError as error:
            if interaction is not None:
                await interaction.response.send_message(embed=embed_from_payload(task_error_embed("YouTool Rescan", str(error))), ephemeral=True)
            else:
                await invocation_channel.send(embeds=[embed_from_payload(task_error_embed("YouTool Rescan", str(error)))])
            return
        progress_payload = ReplyPayload(
            embeds=[
                task_status_embed(
                    "YouTool Rescan",
                    (
                        f"Scanning <#{scan_channel_id}> for undocumented receipts after message "
                        f"`{after_message_id}`..."
                    ),
                )
            ],
            ephemeral=interaction is not None,
        )
        dispatch = DiscordDispatchContext(
            actor=_actor_from_user(actor),
            channel=_channel_from_id(self.base_runtime, invocation_channel.id),
            channel_object=invocation_channel,
            is_interaction=interaction is not None,
            interaction=interaction,
            invocation_message=prefix_message,
        )
        await self._send_reply(dispatch, progress_payload)
        _schedule_prefix_invocation_cleanup(self, prefix_message)

        scan_channel = self.get_channel(scan_channel_id)
        if scan_channel is None:
            scan_channel = await self.fetch_channel(scan_channel_id)
        if not isinstance(scan_channel, discord.TextChannel):
            raise RuntimeError(f"Rescan channel {scan_channel_id} is not a text channel.")

        documented = await self.base_runtime.database.documented_receipt_sources()
        candidates: list[PendingRescanCandidate] = []
        async for candidate_message in scan_channel.history(
            limit=None,
            after=discord.Object(id=after_message_id),
            oldest_first=True,
        ):
            if candidate_message.author.bot:
                continue
            attachments = _receipt_like_attachments(candidate_message)
            if not attachments:
                continue
            if candidate_message.id in documented.proof_message_ids or any(
                attachment.url in documented.proof_urls for attachment in attachments
            ):
                continue
            proof_urls = [attachment.url for attachment in attachments]
            proof_paths: list[str] = []
            for attachment in attachments:
                saved = save_proof_attachment(
                    self.base_runtime.config.storage.attachment_dir,
                    f"YT-RESCAN-{candidate_message.id}",
                    attachment.filename,
                    await attachment.read(),
                )
                proof_paths.append(str(saved.path))
            credited = _resolve_rescan_credit_target(candidate_message)
            parsed_items = parse_prefill_items(self.base_runtime.catalog, candidate_message.content)
            summary = (
                "No caption items were confidently matched. Review and add items manually."
                if not parsed_items
                else "Prefilled from caption: "
                + ", ".join(f"{item.quantity} {item.item_name}" for item in parsed_items)
            )
            caption = candidate_message.content.strip() or "[no text caption]"
            candidates.append(
                PendingRescanCandidate(
                    credited=credited,
                    workflow_notice=(
                        f"Source: {candidate_message.jump_url}\n"
                        f"Sent by: {getattr(candidate_message.author, 'display_name', candidate_message.author.name)} ({candidate_message.author.id})\n"
                        f"Crediting: {credited.display_name} ({credited.user_id})\n"
                        f"{summary}\n"
                        f"Caption: {caption}"
                    ),
                    items=[
                        DraftItem(
                            item_name=item.item_name,
                            quantity=item.quantity,
                            override_unit_price=item.override_unit_price,
                            contract_name=item.contract_name,
                        )
                        for item in parsed_items
                    ],
                    payment_proof_path=join_proof_values(proof_paths),
                    payment_proof_source_url=join_proof_values(proof_urls),
                )
            )

        if not candidates:
            if dispatch.reply_message is not None:
                await dispatch.reply_message.edit(
                    embeds=[
                        embed_from_payload(
                            task_warning_embed(
                                "YouTool Rescan",
                                (
                                    f"No undocumented receipt candidates were found in "
                                    f"<#{scan_channel_id}> after message `{after_message_id}`."
                                ),
                            )
                        )
                    ],
                    view=None,
                )
            return

        first_candidate = candidates[0]
        remaining = candidates[1:]
        async with self._pending_lock:
            if remaining:
                self._pending_rescans[actor.id] = remaining
            else:
                self._pending_rescans.pop(actor.id, None)
        session = await self.base_runtime.bot_state.upsert_session_with_credit(
            actor.id,
            invocation_channel.id,
            first_candidate.credited,
        )
        session.workflow_notice = first_candidate.workflow_notice
        session.rescan_active = True
        session.items = [
            DraftItem(
                item_name=item.item_name,
                quantity=item.quantity,
                override_unit_price=item.override_unit_price,
                contract_name=item.contract_name,
            )
            for item in first_candidate.items
        ]
        session.payment_proof_path = first_candidate.payment_proof_path
        session.payment_proof_source_url = first_candidate.payment_proof_source_url
        session.awaiting_proof = False
        session.proof_processing = False
        session = await self.base_runtime.bot_state.replace_session(session)

        calc_dispatch = DiscordDispatchContext(
            actor=_actor_from_user(actor),
            channel=_channel_from_id(self.base_runtime, invocation_channel.id),
            channel_object=invocation_channel,
            is_interaction=interaction is not None,
            interaction=interaction,
        )
        await self._send_reply(calc_dispatch, calc_reply_payload(self.base_runtime.catalog.items, session))
        if calc_dispatch.reply_message is not None:
            await self.base_runtime.bot_state.set_panel_message(
                actor.id,
                calc_dispatch.reply_message.channel.id,
                calc_dispatch.reply_message.id,
            )
        if dispatch.reply_message is not None:
            await dispatch.reply_message.edit(
                embeds=[
                    embed_from_payload(
                        task_status_embed(
                            "YouTool Rescan",
                            f"Found {len(candidates)} undocumented receipt candidate(s). Reviewing them one by one now.",
                        )
                    )
                ],
                view=None,
            )

    async def _activate_next_rescan_candidate(
        self,
        user_id: int,
        *,
        channel_id: int | None = None,
        panel_channel_id: int | None = None,
        panel_message_id: int | None = None,
    ) -> CalculatorSession | None:
        async with self._pending_lock:
            candidates = self._pending_rescans.get(user_id)
            if not candidates:
                self._pending_rescans.pop(user_id, None)
                return None
            candidate = candidates.pop(0)
            if not candidates:
                self._pending_rescans.pop(user_id, None)

        existing = await self.base_runtime.bot_state.current_session(user_id)
        resolved_channel_id = channel_id or (existing.channel_id if existing is not None else None)
        resolved_panel_channel_id = panel_channel_id if panel_channel_id is not None else (
            existing.panel_channel_id if existing is not None else None
        )
        resolved_panel_message_id = panel_message_id if panel_message_id is not None else (
            existing.panel_message_id if existing is not None else None
        )
        if resolved_channel_id is None:
            return None
        session = await self.base_runtime.bot_state.upsert_session_with_credit(
            user_id,
            resolved_channel_id,
            candidate.credited,
        )
        session.workflow_notice = candidate.workflow_notice
        session.rescan_active = True
        session.items = [
            DraftItem(
                item_name=item.item_name,
                quantity=item.quantity,
                override_unit_price=item.override_unit_price,
                contract_name=item.contract_name,
            )
            for item in candidate.items
        ]
        session.payment_proof_path = candidate.payment_proof_path
        session.payment_proof_source_url = candidate.payment_proof_source_url
        session.panel_channel_id = resolved_panel_channel_id
        session.panel_message_id = resolved_panel_message_id
        return await self.base_runtime.bot_state.replace_session(session)

    async def _advance_rescan_after_success(
        self,
        session: CalculatorSession,
    ) -> CalculatorSession | None:
        return await self._activate_next_rescan_candidate(
            session.user_id,
            channel_id=session.channel_id,
            panel_channel_id=session.panel_channel_id,
            panel_message_id=session.panel_message_id,
        )

    async def _rebuild_receipt_logs(
        self,
        actor: discord.abc.User,
        channel: discord.abc.MessageableChannel,
        interaction: discord.Interaction[Any] | None = None,
        prefix_message: discord.Message | None = None,
    ) -> None:
        log_channel_id = self.base_runtime.config.discord.receipt_log_channel_id or channel.id
        log_channel = self.get_channel(log_channel_id)
        if log_channel is None:
            log_channel = await self.fetch_channel(log_channel_id)
        if not isinstance(log_channel, discord.abc.Messageable):
            raise RuntimeError(f"Receipt log channel {log_channel_id} is not messageable.")

        progress_payload = ReplyPayload(
            embeds=[
                task_status_embed(
                    "YouTool Rebuild Logs",
                    (
                        f"Preparing receipt log rebuild for <#{log_channel_id}>. "
                        "Loading receipts from the database..."
                    ),
                )
            ],
            ephemeral=interaction is not None,
        )
        dispatch = DiscordDispatchContext(
            actor=_actor_from_user(actor),
            channel=_channel_from_id(self.base_runtime, channel.id),
            channel_object=channel,
            is_interaction=interaction is not None,
            interaction=interaction,
            invocation_message=prefix_message,
        )
        await self._send_reply(dispatch, progress_payload)
        _schedule_prefix_invocation_cleanup(self, prefix_message)

        receipts = await self.base_runtime.database.list_all_receipts()
        bot_user = self.user
        deleted_messages = 0
        delete_failures = 0
        inspected_messages = 0
        if isinstance(log_channel, discord.TextChannel) and bot_user is not None:
            async for existing in log_channel.history(limit=None):
                inspected_messages += 1
                if existing.author.id != bot_user.id:
                    continue
                try:
                    await existing.delete()
                    deleted_messages += 1
                except discord.HTTPException:
                    delete_failures += 1
        if dispatch.reply_message is not None:
            await dispatch.reply_message.edit(
                embeds=[
                    embed_from_payload(
                        task_status_embed(
                            "YouTool Rebuild Logs",
                            (
                                f"Rebuilding receipt log channel <#{log_channel_id}> from the database. "
                                f"Reposting {len(receipts)} receipt(s) now..."
                            ),
                        )
                    )
                ]
            )
        reposted_receipts = 0
        repost_failures = 0
        for receipt in receipts:
            display = await self._load_receipt_display_context(receipt.id, receipt.creator_user_id)
            payload = receipt_log_payload(receipt, display)
            spec = reply_payload_to_spec(self, self.shared_runtime, payload)
            try:
                await log_channel.send(**spec.send_kwargs())
                reposted_receipts += 1
            except discord.HTTPException:
                repost_failures += 1
        if dispatch.reply_message is not None:
            await dispatch.reply_message.edit(
                embeds=[
                    embed_from_payload(
                        task_status_embed(
                            "YouTool Rebuild Logs",
                            (
                                f"Receipt log rebuild finished for <#{log_channel_id}>.\n"
                                f"Receipts replayed: {reposted_receipts}\n"
                                f"Existing bot log messages deleted: {deleted_messages}\n"
                                f"Delete failures: {delete_failures}\n"
                                f"Repost failures: {repost_failures}\n"
                                f"Messages inspected: {inspected_messages}"
                            ),
                        )
                    )
                ],
                view=None,
            )
        LOGGER.info(
            "channel_id=%s receipt_count=%s reposted_receipts=%s deleted_messages=%s delete_failures=%s repost_failures=%s inspected_messages=%s receipt log channel rebuilt",
            log_channel_id,
            len(receipts),
            reposted_receipts,
            deleted_messages,
            delete_failures,
            repost_failures,
            inspected_messages,
        )

    async def _request_shutdown(
        self,
        actor: discord.abc.User | None,
        channel: discord.abc.MessageableChannel | None,
        *,
        restart: bool,
        interaction: discord.Interaction[Any] | None = None,
        prefix_message: discord.Message | None = None,
        source: str = "user",
    ) -> None:
        title = "YouTool Restart" if restart else "YouTool Stop"
        if self._shutdown_requested and restart == self._restart_requested:
            description = "A bot restart is already in progress." if restart else "A bot shutdown is already in progress."
            if interaction is not None:
                await interaction.response.send_message(embed=embed_from_payload(task_warning_embed(title, description)), ephemeral=True)
            elif channel is not None:
                await channel.send(embeds=[embed_from_payload(task_warning_embed(title, description))])
            return
        self._shutdown_requested = True
        self._restart_requested = restart
        if interaction is not None:
            await interaction.response.send_message(
                embed=embed_from_payload(
                    task_status_embed(
                        title,
                        "Restart accepted. Sending shutdown notices now..."
                        if restart
                        else "Shutdown accepted. Sending shutdown notices now...",
                    )
                ),
                ephemeral=True,
            )
        elif channel is not None:
            await channel.send(
                embeds=[
                    embed_from_payload(
                        task_status_embed(
                            title,
                            "Restart accepted. Sending shutdown notices now..."
                            if restart
                            else "Shutdown accepted. Sending shutdown notices now...",
                        )
                    )
                ]
            )
            _schedule_prefix_invocation_cleanup(self, prefix_message)
        await self._announce_shutdown_status(
            restart=restart,
            source=source,
            actor_user_id=actor.id if actor is not None else None,
        )
        self._track_task(self._finish_shutdown())

    async def _finish_shutdown(self) -> None:
        if not self._restart_requested:
            await asyncio.sleep(0.5)
            await self._announce_stopped_status()
        await asyncio.sleep(0.5)
        await self.close()

    async def _open_add_item_panel(self, interaction: discord.Interaction[Any]) -> None:
        session = await self.base_runtime.bot_state.touch_session(interaction.user.id)
        if session is None:
            await interaction.response.send_message("Calculator session missing.", ephemeral=True)
            return
        embeds = [embed_from_payload(embed) for embed in calc_embeds(self.base_runtime.catalog.items, session)]
        embeds[0].add_field(name="Next Step", value="Choose an item from the catalog.", inline=False)
        view = self._build_add_item_view(interaction.user.id, session)
        await interaction.response.edit_message(content="", embeds=embeds, view=view)

    async def _open_remove_item_panel(self, interaction: discord.Interaction[Any]) -> None:
        session = await self.base_runtime.bot_state.touch_session(interaction.user.id)
        if session is None:
            await interaction.response.send_message("Calculator session missing.", ephemeral=True)
            return
        if not session.items:
            await self._edit_calc_panel(interaction, session, content="Nothing to remove yet.")
            return
        embeds = [embed_from_payload(embed) for embed in calc_embeds(self.base_runtime.catalog.items, session)]
        embeds[0].add_field(
            name="Next Step",
            value="Choose an item from the running list to remove.",
            inline=False,
        )
        view = self._build_remove_item_view(interaction.user.id, session)
        await interaction.response.edit_message(content="", embeds=embeds, view=view)

    async def _open_contract_panel(self, interaction: discord.Interaction[Any]) -> None:
        session = await self.base_runtime.bot_state.touch_session(interaction.user.id)
        if session is None:
            await interaction.response.send_message("Calculator session missing.", ephemeral=True)
            return
        embed = discord.Embed(
            title="YouTool Contract Picker",
            description=(
                f"**Selected Contract:** {session.selected_contract_name or 'None'}\n\n"
                "Choose a contract to auto-apply its preset prices to matching items."
            ),
            color=0x2BA17E,
        )
        embed.add_field(
            name="Next Step",
            value="Select a contract from the list below, or clear the current contract.",
            inline=False,
        )
        view = self._build_contract_view(interaction.user.id)
        await interaction.response.edit_message(content="", embeds=[embed], view=view)

    async def _edit_calc_panel(
        self,
        interaction: discord.Interaction[Any],
        session: CalculatorSession,
        *,
        content: str | None = None,
    ) -> None:
        payload = calc_reply_payload(self.base_runtime.catalog.items, session)
        spec = reply_payload_to_spec(self, self.shared_runtime, payload)
        kwargs = spec.edit_kwargs()
        if content is not None:
            kwargs["content"] = content
        await interaction.response.edit_message(**kwargs)

    def _build_add_item_view(self, owner_user_id: int, session: CalculatorSession) -> discord.ui.View:
        timeout = self.base_runtime.config.discord.transient_message_timeout_seconds
        view = DispatchView(self, timeout)
        options = [
            discord.SelectOption(
                label=item.name,
                value=str(index),
                description=_catalog_item_option_description(item),
            )
            for index, item in enumerate(self.base_runtime.catalog.items)
            if item.active
        ]
        view.add_item(
            DispatchSelect(
                client=self,
                custom_id=f"calc|pick|{owner_user_id}",
                placeholder="Select an item",
                options=options,
                row=0,
            )
        )
        if session.last_selected_item_index is not None:
            item_index = session.last_selected_item_index
            if 0 <= item_index < len(self.base_runtime.catalog.items):
                item = self.base_runtime.catalog.items[item_index]
                if item.active:
                    view.add_item(
                        DispatchButton(
                            client=self,
                            label=f"Reopen {item.name}",
                            style=discord.ButtonStyle.primary,
                            custom_id=f"calc|add_last|{owner_user_id}|{item_index}",
                            row=1,
                        )
                    )
        view.add_item(
            DispatchButton(
                client=self,
                label="Back",
                style=discord.ButtonStyle.secondary,
                custom_id=f"calc|panel|{owner_user_id}",
                row=1,
            )
        )
        view.add_item(
            DispatchButton(
                client=self,
                label="Cancel",
                style=discord.ButtonStyle.danger,
                custom_id=f"calc|cancel|{owner_user_id}",
                row=1,
            )
        )
        return view

    def _build_remove_item_view(self, owner_user_id: int, session: CalculatorSession) -> discord.ui.View:
        timeout = self.base_runtime.config.discord.transient_message_timeout_seconds
        view = DispatchView(self, timeout)
        priced = price_items(self.base_runtime.catalog, session.items)
        options = [
            discord.SelectOption(
                label=f"{item.quantity}x {item.item_name}",
                value=str(index),
                description=f"{_pricing_source_label(item.pricing_source)} | ${item.line_sale_total:,}",
            )
            for index, item in enumerate(priced.items)
        ]
        view.add_item(
            DispatchSelect(
                client=self,
                custom_id=f"calc|remove_pick|{owner_user_id}",
                placeholder="Select an item to remove",
                options=options,
                row=0,
            )
        )
        view.add_item(
            DispatchButton(
                client=self,
                label="Back",
                style=discord.ButtonStyle.secondary,
                custom_id=f"calc|panel|{owner_user_id}",
                row=1,
            )
        )
        view.add_item(
            DispatchButton(
                client=self,
                label="Cancel",
                style=discord.ButtonStyle.danger,
                custom_id=f"calc|cancel|{owner_user_id}",
                row=1,
            )
        )
        return view

    def _build_contract_view(self, owner_user_id: int) -> discord.ui.View:
        timeout = self.base_runtime.config.discord.transient_message_timeout_seconds
        view = DispatchView(self, timeout)
        if self.base_runtime.contracts.entries:
            options = [
                discord.SelectOption(
                    label=contract.name,
                    value=contract.name,
                    description=" | ".join(
                        f"{price.item_name} ${price.unit_price:,}" for price in contract.prices
                    )[:100],
                )
                for contract in self.base_runtime.contracts.entries
            ]
            view.add_item(
                DispatchSelect(
                    client=self,
                    custom_id=f"calc|contract_pick|{owner_user_id}",
                    placeholder="Select a contract",
                    options=options,
                    row=0,
                )
            )
        view.add_item(
            DispatchButton(
                client=self,
                label="Clear Contract",
                style=discord.ButtonStyle.secondary,
                custom_id=f"calc|contract_clear|{owner_user_id}",
                row=1,
            )
        )
        view.add_item(
            DispatchButton(
                client=self,
                label="Back",
                style=discord.ButtonStyle.secondary,
                custom_id=f"calc|panel|{owner_user_id}",
                row=1,
            )
        )
        view.add_item(
            DispatchButton(
                client=self,
                label="Cancel",
                style=discord.ButtonStyle.danger,
                custom_id=f"calc|cancel|{owner_user_id}",
                row=1,
            )
        )
        return view

    def _build_adjustprices_view(
        self,
        owner_user_id: int,
        catalog_items: list[CatalogItem],
    ) -> discord.ui.View:
        timeout = self.base_runtime.config.discord.transient_message_timeout_seconds
        view = DispatchView(self, timeout)
        if catalog_items:
            options = [
                discord.SelectOption(
                    label=item.name,
                    value=str(index),
                    description=f"Unit ${item.unit_price:,} | {'Active' if item.active else 'Inactive'}",
                )
                for index, item in enumerate(catalog_items)
            ]
            view.add_item(
                DispatchSelect(
                    client=self,
                    custom_id=f"adjustprices|pick|{owner_user_id}",
                    placeholder="Select a catalog item",
                    options=options,
                    row=0,
                )
            )
        return view

    async def _handle_pending_import(self, message: discord.Message) -> bool:
        async with self._pending_lock:
            pending = self._pending_imports.get(message.author.id)
            if pending is None:
                return False
            if pending.channel_id != message.channel.id or pending.expires_at < utcnow():
                self._pending_imports.pop(message.author.id, None)
                return False
            self._pending_imports.pop(message.author.id, None)

        attachment = message.attachments[0] if message.attachments else None
        if attachment is None:
            await _safe_edit_message(
                pending.prompt_message,
                content=f"Upload one JSON export file in <#{pending.channel_id}> to continue this import preview.",
                embeds=[],
                view=None,
            )
            return True

        await _safe_edit_message(
            pending.prompt_message,
            content=(
                f"Import file received from <@{message.author.id}>: `{attachment.filename}` "
                f"({attachment.size} bytes). Validating file and preparing import preview..."
            ),
            embeds=[],
            view=None,
        )
        try:
            bytes_value = await attachment.read()
            temp_path = self.base_runtime.config.storage.export_dir / f"upload-import-{new_receipt_id()}.json"
            temp_path.write_bytes(bytes_value)
        except Exception as error:
            temp_path = locals().get("temp_path")
            if isinstance(temp_path, Path):
                temp_path.unlink(missing_ok=True)
            await _safe_edit_message(
                pending.prompt_message,
                content=f"Import failed for <@{message.author.id}> while processing `{attachment.filename}`: {error}",
                embeds=[],
                view=None,
            )
            return True

        await self._stage_import_review(
            actor=message.author,
            channel_id=pending.channel_id,
            prompt_message=pending.prompt_message,
            status_override=pending.status_override,
            bundle_path=temp_path,
            attachment_filename=attachment.filename,
            delete_after_use=True,
        )
        return True

    async def _confirm_import(self, interaction: discord.Interaction[Any]) -> None:
        async with self._pending_lock:
            review = self._pending_import_reviews.pop(interaction.user.id, None)
        if review is None or review.channel_id != interaction.channel_id or review.expires_at < utcnow():
            if review is not None:
                _cleanup_import_review_file(review)
            await interaction.response.edit_message(
                embeds=[embed_from_payload(task_error_embed("YouTool Import", "This import prompt expired. Run `yt!import` again."))],
                view=None,
            )
            return
        await interaction.response.defer()
        await _safe_edit_message(
            review.prompt_message,
            content="",
            embeds=[
                embed_from_payload(
                    task_status_embed(
                        "YouTool Import",
                        (
                            f"Importing `{review.attachment_filename}` now.\n"
                            f"{_import_confirmation_message(review.status_override, review.preview)}"
                        ),
                    )
                )
            ],
            view=None,
        )
        try:
            bundle = await load_export_async(review.temp_path)
            report = await self.base_runtime.database.import_bundle(
                bundle,
                self.base_runtime.config.storage.attachment_dir,
                review.status_override,
                (str(interaction.user.id), getattr(interaction.user, "display_name", interaction.user.name)),
            )
        except Exception as error:
            _cleanup_import_review_file(review)
            await _safe_edit_message(
                review.prompt_message,
                content="",
                embeds=[embed_from_payload(task_error_embed("YouTool Import", f"Import failed for `{review.attachment_filename}`: {error}"))],
                view=None,
            )
            raise
        _cleanup_import_review_file(review)
        await _safe_edit_message(
            review.prompt_message,
            content="",
            embeds=[
                embed_from_payload(
                    task_status_embed(
                        "YouTool Import",
                        _import_completed_message(
                            interaction.user.id,
                            review.attachment_filename,
                            bundle.schema_version,
                            review.status_override,
                            report,
                        ),
                    )
                )
            ],
            view=None,
        )
        await interaction.edit_original_response(content="Import completed.", view=None)

    async def _cancel_import(self, interaction: discord.Interaction[Any]) -> None:
        async with self._pending_lock:
            review = self._pending_import_reviews.pop(interaction.user.id, None)
            self._pending_imports.pop(interaction.user.id, None)
        if review is not None:
            _cleanup_import_review_file(review)
        await interaction.response.edit_message(content="Import cancelled.", embeds=[], view=None)

    async def _confirm_sanitize(self, interaction: discord.Interaction[Any]) -> None:
        async with self._pending_lock:
            review = self._pending_sanitizes.pop(interaction.user.id, None)
        if review is None or review.channel_id != interaction.channel_id or review.expires_at < utcnow():
            await interaction.response.edit_message(
                embeds=[
                    embed_from_payload(
                        task_error_embed(
                            "YouTool Sanitize",
                            "This sanitize prompt expired. Run `yt!sanitize` again.",
                        )
                    )
                ],
                view=None,
            )
            return

        await interaction.response.defer()
        await _safe_edit_message(
            review.prompt_message,
            content="",
            embeds=[
                embed_from_payload(
                    task_status_embed(
                        "YouTool Sanitize",
                        (
                            "Creating a backup export and applying the safe cleanup now...\n"
                            f"{sanitize_confirmation_message(review.preview)}"
                        ),
                    )
                )
            ],
            view=None,
        )
        try:
            backup_path = await save_export_async(
                await self.base_runtime.database.export_bundle(self.base_runtime.catalog),
                self.base_runtime.config.storage.export_dir,
                "yt-assist-sanitize-backup",
            )
            async with self._pending_lock:
                protected_import_paths = {
                    pending_review.temp_path
                    for pending_review in self._pending_import_reviews.values()
                    if pending_review.temp_path.exists()
                }
            report = await self.base_runtime.database.sanitize_storage(
                self.base_runtime.config.storage.attachment_dir,
                self.base_runtime.config.storage.export_dir,
                self.base_runtime.config.storage.import_dir,
                protected_import_paths=protected_import_paths,
            )
            await self.base_runtime.database.insert_audit_event(
                AuditEventInput(
                    actor_user_id=str(interaction.user.id),
                    actor_display_name=getattr(interaction.user, "display_name", interaction.user.name),
                    action="sanitize_storage",
                    target_receipt_id=None,
                    detail_json={
                        "backup_file_name": backup_path.name,
                        "receipt_paths_updated": report.receipt_paths_updated,
                        "proof_files_renamed": report.proof_files_renamed,
                        "proof_files_deleted": report.proof_files_deleted,
                        "orphaned_files_deleted": report.orphaned_files_deleted,
                        "stale_import_files_deleted": report.stale_import_files_deleted,
                        "rename_collisions": report.preview.rename_collisions,
                        "non_active_proof_paths_retained": report.preview.non_active_proof_paths_retained,
                    },
                )
            )
        except Exception as error:
            await _safe_edit_message(
                review.prompt_message,
                content="",
                embeds=[
                    embed_from_payload(
                        task_error_embed("YouTool Sanitize", f"Sanitize failed: {error}")
                    )
                ],
                view=None,
            )
            raise
        await _safe_edit_message(
            review.prompt_message,
            content="",
            embeds=[
                embed_from_payload(
                    task_status_embed(
                        "YouTool Sanitize",
                        sanitize_completed_message(
                            interaction.user.id,
                            backup_path.name,
                            report,
                        ),
                    )
                )
            ],
            view=None,
        )
        await interaction.edit_original_response(content="Sanitize completed.", view=None)

    async def _cancel_sanitize(self, interaction: discord.Interaction[Any]) -> None:
        async with self._pending_lock:
            self._pending_sanitizes.pop(interaction.user.id, None)
        await interaction.response.edit_message(content="Sanitize cancelled.", embeds=[], view=None)

    async def _handle_pending_proof_update(self, message: discord.Message) -> bool:
        async with self._pending_lock:
            pending = self._pending_proof_updates.get(message.author.id)
            if pending is None:
                return False
            if pending.channel_id != message.channel.id or pending.expires_at < utcnow():
                self._pending_proof_updates.pop(message.author.id, None)
                return False
            self._pending_proof_updates.pop(message.author.id, None)

        proof_attachments = [
            attachment
            for attachment in message.attachments
            if (attachment.content_type or "").startswith("image/")
        ]
        if not proof_attachments:
            return False

        await _safe_edit_message(
            pending.prompt_message,
            content=f"Replacement proof received for receipt `{pending.receipt_id}`. Updating now...",
            embeds=[],
            view=None,
        )
        try:
            proof_paths: list[str] = []
            proof_urls: list[str] = []
            for attachment in proof_attachments:
                local_path = save_proof_attachment(
                    self.base_runtime.config.storage.attachment_dir,
                    pending.receipt_id,
                    attachment.filename,
                    await attachment.read(),
                )
                proof_paths.append(str(local_path.path))
                proof_urls.append(attachment.url)
            joined_proof_paths = join_proof_values(proof_paths)
            joined_proof_urls = join_proof_values(proof_urls)
            if joined_proof_paths is None or joined_proof_urls is None:
                raise ValueError("missing replacement proof paths")
            updated = await self.base_runtime.database.update_receipt_proof(
                pending.receipt_id,
                joined_proof_paths,
                joined_proof_urls,
                AuditEventInput(
                    actor_user_id=str(message.author.id),
                    actor_display_name=getattr(message.author, "display_name", message.author.name),
                    action="receipt_proof_replaced",
                    target_receipt_id=pending.receipt_id,
                    detail_json={
                        "proof_message_id": message.id,
                        "proof_url": proof_urls[0] if proof_urls else None,
                        "proof_urls": proof_urls,
                    },
                ),
            )
        except Exception as error:
            await _safe_edit_message(
                pending.prompt_message,
                content=f"Proof update failed for receipt `{pending.receipt_id}`: {error}",
                embeds=[],
                view=None,
            )
            raise
        await _safe_delete_message(message)
        if updated:
            refreshed_receipt = await self.base_runtime.database.get_receipt(pending.receipt_id)
            if refreshed_receipt is not None:
                await self._refresh_posted_receipt_messages(refreshed_receipt)
        await _safe_edit_message(
            pending.prompt_message,
            content=(
                f"Updated payment proof for receipt `{pending.receipt_id}`.\nYou may dismiss this message."
                if updated
                else f"Receipt `{pending.receipt_id}` was not found.\nYou may dismiss this message."
            ),
            embeds=[],
            view=None,
        )
        return True

    async def _perform_reset(
        self,
        interaction: discord.Interaction[Any],
        action: ResetAction,
    ) -> None:
        async with self._pending_lock:
            scope = self._pending_resets.pop(interaction.user.id, None)
        if scope is None:
            await interaction.response.edit_message(
                embeds=[embed_from_payload(task_error_embed("YouTool Reset", "This reset prompt expired. Run `yt!reset` again."))],
                view=None,
            )
            return

        await interaction.response.defer()
        await interaction.edit_original_response(
            content=_reset_progress_message(scope.mode, scope.user_ids, action),
            embeds=[],
            view=None,
        )

        bundle = await self.base_runtime.database.export_bundle(self.base_runtime.catalog)
        affected_receipts = [
            receipt
            for receipt in bundle.receipts
            if receipt.status.counts_for_payouts()
            and _reset_scope_matches(scope.mode, scope.user_ids, receipt.creator_user_id)
        ]
        if not affected_receipts:
            await interaction.edit_original_response(
                content=_reset_empty_result_message(action, scope.mode, scope.user_ids),
                embeds=[],
                view=None,
            )
            return

        affected_receipt_ids = {receipt.id for receipt in affected_receipts}
        filtered_bundle = ExportBundle(
            schema_version=bundle.schema_version,
            exported_at=bundle.exported_at,
            catalog=bundle.catalog,
            receipts=affected_receipts,
            audit_events=[
                event
                for event in bundle.audit_events
                if event.target_receipt_id is not None and event.target_receipt_id in affected_receipt_ids
            ],
        )
        summary = render_stats_description(_leaderboard_from_receipts(affected_receipts))
        receipt_ids = [receipt.id for receipt in affected_receipts]
        settlement_targets: list[tuple[str, int]] = []
        if action is ResetAction.MARK_PAID:
            seen_users: set[str] = set()
            for receipt in affected_receipts:
                if receipt.creator_user_id in seen_users:
                    continue
                seen_users.add(receipt.creator_user_id)
                outstanding = (await self.base_runtime.database.procurement_balance(receipt.creator_user_id)).available_total
                if outstanding != 0:
                    settlement_targets.append((receipt.creator_user_id, outstanding))
        path = await save_export_async(
            filtered_bundle,
            self.base_runtime.config.storage.export_dir,
            _reset_export_label(scope.mode, action),
        )
        updated = await self.base_runtime.database.update_receipt_statuses(
            receipt_ids,
            action.target_status(),
            ReceiptStatus.ACTIVE,
            str(interaction.user.id),
            getattr(interaction.user, "display_name", interaction.user.name),
            None,
        )
        if action is ResetAction.MARK_PAID:
            for user_id, outstanding in settlement_targets:
                await self.base_runtime.database.settle_procurement_balance(
                    user_id,
                    outstanding,
                    str(interaction.user.id),
                    getattr(interaction.user, "display_name", interaction.user.name),
                )
        await self._clear_reset_sessions(scope.mode, scope.user_ids)
        completion = (
            f"{_reset_scope_action(scope.mode, scope.user_ids, action)}.\n"
            f"{updated} active receipt(s) updated.\n"
            f"Backup saved to `{path}`\n"
            f"Previous summary:\n{summary}"
        )
        await interaction.edit_original_response(content=completion, embeds=[], view=None)
        main_channel_id = next(iter(self.base_runtime.config.discord.allowed_channel_ids), interaction.channel_id)
        main_channel = self.get_channel(main_channel_id)
        notice = (
            f"{_reset_scope_action(scope.mode, scope.user_ids, action)} by <@{interaction.user.id}>.\n"
            f"Backup saved to `{path}`\n"
            f"Previous summary:\n{summary}"
        )
        if interaction.channel is not None:
            await interaction.channel.send(notice)
        if main_channel is not None and getattr(main_channel, "id", None) != getattr(interaction.channel, "id", None):
            await main_channel.send(notice)

    async def _clear_reset_sessions(self, mode: ResetMode, user_ids: list[int]) -> None:
        if mode is ResetMode.ALL:
            for session in await self.base_runtime.bot_state.sessions_snapshot():
                await self.base_runtime.bot_state.remove_session(session.user_id)
            async with self._pending_lock:
                self._pending_proof_updates.clear()
            return

        targeted_users = set(user_ids)
        for session in await self.base_runtime.bot_state.sessions_snapshot():
            should_remove = (mode is ResetMode.ONLY and session.user_id in targeted_users) or (
                mode is ResetMode.EXCEPT and session.user_id not in targeted_users
            )
            if should_remove:
                await self.base_runtime.bot_state.remove_session(session.user_id)
        async with self._pending_lock:
            kept: dict[int, PendingProofUpdate] = {}
            for user_id, pending in self._pending_proof_updates.items():
                keep = (mode is ResetMode.EXCEPT and user_id in targeted_users) or (
                    mode is ResetMode.ONLY and user_id not in targeted_users
                )
                if keep:
                    kept[user_id] = pending
            self._pending_proof_updates = kept

    async def _monitor_calculator_sessions(self) -> None:
        while not self.is_closed():
            await asyncio.sleep(15)
            snapshot = await self.base_runtime.bot_state.sessions_snapshot()
            if not snapshot:
                continue
            now = utcnow()
            idle_timeout = self.base_runtime.config.discord.receipt_idle_timeout_seconds
            warning_seconds = self.base_runtime.config.discord.receipt_idle_warning_seconds
            for session in snapshot:
                idle_for = (now - session.last_updated_at).total_seconds()
                if idle_for >= idle_timeout:
                    expired = await self.base_runtime.bot_state.remove_session(session.user_id)
                    if expired is None:
                        continue
                    if expired.panel_channel_id is not None and expired.panel_message_id is not None:
                        panel_channel = self.get_channel(expired.panel_channel_id)
                        if panel_channel is not None:
                            try:
                                panel_message = await panel_channel.fetch_message(expired.panel_message_id)
                            except (discord.HTTPException, discord.NotFound, AttributeError):
                                panel_message = None
                            if panel_message is not None:
                                await _safe_delete_message(panel_message)
                    self._schedule_lifecycle_status_refresh(expired.channel_id, delay_seconds=0.25)
                    continue
                if session.timeout_warning_sent or idle_for + warning_seconds < idle_timeout:
                    continue
                warned = await self.base_runtime.bot_state.update_session(
                    session.user_id,
                    lambda current: setattr(current, "timeout_warning_sent", True),
                )
                if warned is None or warned.panel_channel_id is None or warned.panel_message_id is None:
                    continue
                panel_channel = self.get_channel(warned.panel_channel_id)
                if panel_channel is None:
                    continue
                try:
                    panel_message = await panel_channel.fetch_message(warned.panel_message_id)
                except (discord.HTTPException, discord.NotFound, AttributeError):
                    continue
                warning_payload = ReplyPayload(
                    embeds=[calc_timeout_warning_embed(self.base_runtime.catalog.items, warned)],
                    components=calc_timeout_action_rows(warned.user_id),
                    ephemeral=True,
                )
                await panel_message.edit(**reply_payload_to_spec(self, self.shared_runtime, warning_payload).edit_kwargs())

    async def _monitor_help_panels(self) -> None:
        while not self.is_closed():
            await asyncio.sleep(15)
            async with self._pending_lock:
                expired = [
                    (user_id, panel)
                    for user_id, panel in self._pending_help_panels.items()
                    if panel.expires_at <= utcnow()
                ]
                for user_id, _ in expired:
                    self._pending_help_panels.pop(user_id, None)
            for _, panel in expired:
                await _safe_delete_message(panel.prompt_message)

    async def _maybe_remember_help_panel(
        self,
        reply_message: discord.Message,
        payload: ReplyPayload,
    ) -> None:
        if not payload.components:
            return
        first_row = payload.components[0]
        if not first_row.components:
            return
        first_id = first_row.components[0].custom_id
        if not first_id.startswith("help|page|"):
            return
        parts = first_id.split("|")
        if len(parts) < 4:
            return
        owner_user_id = int(parts[2])
        async with self._pending_lock:
            previous = self._pending_help_panels.get(owner_user_id)
            self._pending_help_panels[owner_user_id] = PendingHelpPanel(
                prompt_message=reply_message,
                expires_at=utcnow() + _seconds_delta(self.base_runtime.config.discord.transient_message_timeout_seconds),
            )
        if previous is not None and previous.prompt_message.id != reply_message.id:
            await _safe_delete_message(previous.prompt_message)

    async def _receipt_detail_reply(
        self,
        receipt,
        owner_user_id: int,
    ) -> ReplyPayload:
        display = await self._load_receipt_display_context(receipt.id, receipt.creator_user_id)
        return receipt_detail_payload(receipt, owner_user_id, display)

    async def _receipt_log_reply(self, receipt) -> ReplyPayload:
        display = await self._load_receipt_display_context(receipt.id, receipt.creator_user_id)
        return receipt_log_payload(receipt, display)

    async def _load_receipt_display_context(
        self,
        receipt_id: str,
        creator_user_id: str,
    ) -> ReceiptDisplayContext | None:
        accounting = await self.base_runtime.database.receipt_accounting_record(receipt_id)
        if accounting is None:
            return None
        return ReceiptDisplayContext(
            recorded_by_label=(
                f"{accounting.recorded_by_display_name} ({accounting.recorded_by_user_id})"
                if accounting.recorded_by_user_id != creator_user_id
                else None
            ),
            accounting_policy=accounting.policy,
        )

    async def _refresh_posted_receipt_messages(self, receipt) -> None:
        display = await self._load_receipt_display_context(receipt.id, receipt.creator_user_id)
        main_channel_id = int(receipt.channel_id)
        main_channel = self.get_channel(main_channel_id)
        if not isinstance(main_channel, discord.TextChannel):
            try:
                fetched_main = await self.fetch_channel(main_channel_id)
            except discord.HTTPException:
                fetched_main = None
            if isinstance(fetched_main, discord.TextChannel):
                main_channel = fetched_main
        if isinstance(main_channel, discord.TextChannel):
            main_payload = receipt_main_payload(receipt, display)
            await self._refresh_matching_receipt_messages_in_channel(
                main_channel,
                receipt.id,
                main_payload,
            )

        log_channel_id = self.base_runtime.config.discord.receipt_log_channel_id
        if log_channel_id is None or log_channel_id == main_channel_id:
            return
        log_channel = self.get_channel(log_channel_id)
        if not isinstance(log_channel, discord.TextChannel):
            try:
                fetched_log = await self.fetch_channel(log_channel_id)
            except discord.HTTPException:
                fetched_log = None
            if isinstance(fetched_log, discord.TextChannel):
                log_channel = fetched_log
        if isinstance(log_channel, discord.TextChannel):
            log_payload = receipt_log_payload(receipt, display)
            await self._refresh_matching_receipt_messages_in_channel(
                log_channel,
                receipt.id,
                log_payload,
            )

    async def _refresh_matching_receipt_messages_in_channel(
        self,
        channel: discord.TextChannel,
        receipt_id: str,
        payload: ReplyPayload,
    ) -> None:
        spec = reply_payload_to_spec(self, self.shared_runtime, payload)
        refreshed = 0
        async for message in channel.history(limit=500):
            if not _is_matching_receipt_message(message, receipt_id, self.user.id if self.user is not None else None):
                continue
            try:
                await message.edit(**spec.edit_kwargs())
                refreshed += 1
            except (discord.HTTPException, discord.NotFound) as error:
                LOGGER.warning(
                    "channel_id=%s message_id=%s failed to refresh posted receipt `%s`: %s",
                    channel.id,
                    message.id,
                    receipt_id,
                    error,
                )
        if refreshed:
            LOGGER.info(
                "receipt_id=%s channel_id=%s refreshed_posted_receipt_messages=%s",
                receipt_id,
                channel.id,
                refreshed,
            )

    async def _update_receipt_status(
        self,
        interaction: discord.Interaction[Any],
        receipt_id: str,
        new_status: ReceiptStatus,
        *,
        update_message: bool,
        detail_owner_id: int | None = None,
    ) -> None:
        actor = AuditEventInput(
            actor_user_id=str(interaction.user.id),
            actor_display_name=getattr(interaction.user, "display_name", interaction.user.name),
            action=f"receipt_status_{new_status.as_str()}",
            target_receipt_id=receipt_id,
            detail_json={"status": new_status.as_str()},
        )
        updated = await self.base_runtime.database.update_receipt_status(receipt_id, new_status, actor, None)
        if not updated:
            if update_message:
                await interaction.response.edit_message(content=f"Receipt `{receipt_id}` was not found.", embeds=[], view=None)
            else:
                await interaction.response.send_message(f"Receipt `{receipt_id}` was not found.", ephemeral=True)
            return
        receipt = await self.base_runtime.database.get_receipt(receipt_id)
        if receipt is None:
            await interaction.response.send_message(f"Receipt `{receipt_id}` was not found.", ephemeral=True)
            return
        if detail_owner_id is not None:
            payload = await self._receipt_detail_reply(receipt, detail_owner_id)
        else:
            payload = await self._receipt_log_reply(receipt)
        await interaction.response.edit_message(**reply_payload_to_spec(self, self.shared_runtime, payload).edit_kwargs())
        await self._refresh_posted_receipt_messages(receipt)

    async def _finalize_session_receipt(
        self,
        session: CalculatorSession,
        *,
        actor_user_id: str,
        actor_display_name: str,
        guild_id: str | None,
        channel_id: str,
        proof_message_id: int | None,
    ) -> str:
        priced = price_items(self.base_runtime.catalog, session.items)
        receipt_id = new_receipt_id()
        proof_urls = split_proof_values(session.payment_proof_source_url)
        receipt = NewReceipt(
            id=receipt_id,
            creator_user_id=session.credited_user_id,
            creator_username=session.credited_username,
            creator_display_name=session.credited_display_name,
            guild_id=guild_id,
            channel_id=channel_id,
            total_sale=priced.total_sale,
            procurement_cost=priced.procurement_cost,
            profit=priced.profit,
            status=ReceiptStatus.ACTIVE,
            payment_proof_path=session.payment_proof_path,
            payment_proof_source_url=session.payment_proof_source_url,
            finalized_at=utcnow(),
            items=priced.items,
        )
        accounting_policy = await self._receipt_accounting_policy_for_time(receipt.finalized_at)
        accounting = ReceiptAccountingRecord(
            receipt_id=receipt_id,
            policy=accounting_policy,
            recorded_by_user_id=actor_user_id,
            recorded_by_display_name=actor_display_name,
            recorded_for_user_id=receipt.creator_user_id,
            recorded_for_display_name=receipt.creator_display_name,
            created_at=receipt.finalized_at,
            updated_at=receipt.finalized_at,
        )
        audit = AuditEventInput(
            actor_user_id=actor_user_id,
            actor_display_name=actor_display_name,
            action="receipt_created",
            target_receipt_id=receipt_id,
            detail_json={
                "total_sale": receipt.total_sale,
                "procurement_cost": receipt.procurement_cost,
                "credited_user_id": receipt.creator_user_id,
                "credited_display_name": receipt.creator_display_name,
                "proof_message_id": proof_message_id,
                "proof_url": proof_urls[0] if proof_urls else None,
                "proof_urls": proof_urls,
            },
        )
        await self.base_runtime.database.save_receipt_with_accounting(receipt, audit, accounting)
        await self.base_runtime.bot_state.remove_session(session.user_id)
        self._schedule_lifecycle_status_refresh(session.channel_id, delay_seconds=0.25)
        await self._post_receipt_messages(receipt, accounting)
        return receipt_id

    async def _receipt_accounting_policy_for_time(self, finalized_at) -> AccountingPolicy:
        cutover = await self.base_runtime.database.procurement_cutover_state()
        if cutover is not None and cutover.cutover_at is not None and finalized_at >= cutover.cutover_at:
            return AccountingPolicy.PROCUREMENT_FUNDS
        return AccountingPolicy.LEGACY_REIMBURSEMENT

    async def _post_receipt_messages(
        self,
        receipt: NewReceipt,
        accounting: ReceiptAccountingRecord,
    ) -> None:
        display = ReceiptDisplayContext(
            recorded_by_label=(
                f"{accounting.recorded_by_display_name} ({accounting.recorded_by_user_id})"
                if accounting.recorded_by_user_id != receipt.creator_user_id
                else None
            ),
            accounting_policy=accounting.policy,
        )
        main_channel_id = int(receipt.channel_id)
        main_channel = self.get_channel(main_channel_id) or await self.fetch_channel(main_channel_id)
        main_payload = receipt_main_payload(receipt, display)
        main_spec = reply_payload_to_spec(self, self.shared_runtime, main_payload)
        await main_channel.send(**main_spec.send_kwargs())
        log_channel_id = self.base_runtime.config.discord.receipt_log_channel_id
        if log_channel_id is None or log_channel_id == main_channel_id:
            return
        log_channel = self.get_channel(log_channel_id) or await self.fetch_channel(log_channel_id)
        log_payload = receipt_log_payload(receipt, display)
        log_spec = reply_payload_to_spec(self, self.shared_runtime, log_payload)
        await log_channel.send(**log_spec.send_kwargs())

    async def dispatch_component_interaction(
        self,
        interaction: discord.Interaction[Any],
    ) -> None:
        channel = interaction.channel
        if channel is None:
            if interaction.response.is_done():
                await interaction.followup.send("Component is missing a channel context.", ephemeral=True)
            else:
                await interaction.response.send_message(
                    "Component is missing a channel context.",
                    ephemeral=True,
                )
            return

        custom_id = _interaction_custom_id(interaction)
        if not custom_id:
            await interaction.response.send_message("Component is missing a custom ID.", ephemeral=True)
            return

        if custom_id.startswith("calc|"):
            await self._handle_calc_component(interaction, custom_id)
            return
        if custom_id.startswith("manage|"):
            await self._handle_manage_component(interaction, custom_id)
            return
        if custom_id.startswith("receipt|"):
            await self._handle_receipt_component(interaction, custom_id)
            return
        if custom_id.startswith("contracts|"):
            await self._handle_contracts_component(interaction, custom_id)
            return
        if custom_id.startswith("adjustprices|"):
            await self._handle_adjustprices_component(interaction, custom_id)
            return
        if custom_id.startswith("import|"):
            await self._handle_import_component(interaction, custom_id)
            return
        if custom_id.startswith("sanitize|"):
            await self._handle_sanitize_component(interaction, custom_id)
            return
        if custom_id.startswith("reset|"):
            await self._handle_reset_component(interaction, custom_id)
            return
        if custom_id.startswith("help|"):
            await self._handle_help_component(interaction, custom_id)
            return

        ctx = CommandContext(
            runtime=self.shared_runtime,
            actor=_actor_from_user(interaction.user),
            channel=_channel_from_id(self.base_runtime, channel.id),
            is_interaction=True,
            raw_input=custom_id,
        )
        result = await handle_component_click(ctx, custom_id)
        dispatch = DiscordDispatchContext(
            actor=ctx.actor,
            channel=ctx.channel,
            channel_object=channel,
            is_interaction=True,
            invocation_message=interaction.message,
            interaction=interaction,
            reply_message=interaction.message,
        )
        await self.apply_result(dispatch, result)

    async def apply_result(self, dispatch: DiscordDispatchContext, result: CommandResult) -> None:
        for event in result.events:
            await self._apply_event(dispatch, event)

    async def _apply_event(self, dispatch: DiscordDispatchContext, event: CommandEvent) -> None:
        if event.type == "send":
            if event.reply is not None:
                await self._send_reply(dispatch, event.reply)
            return
        if event.type == "edit":
            if event.reply is not None:
                await self._edit_reply(dispatch, event.reply)
            return
        if event.type == "delete":
            await self._delete_target(dispatch, event.target)
            return
        if event.type == "schedule_delete" and event.after_seconds is not None:
            self._track_task(self._delete_later(dispatch, event.target, event.after_seconds))

    async def _send_reply(self, dispatch: DiscordDispatchContext, payload: ReplyPayload) -> None:
        spec = reply_payload_to_spec(self, self.shared_runtime, payload)
        interaction = dispatch.interaction
        if interaction is None:
            dispatch.reply_message = await dispatch.channel_object.send(**spec.send_kwargs())
            await self._maybe_remember_help_panel(dispatch.reply_message, payload)
            return

        if not interaction.response.is_done():
            await interaction.response.send_message(**spec.send_kwargs(), ephemeral=spec.ephemeral)
            dispatch.reply_message = await self._try_original_response(interaction)
            if dispatch.reply_message is not None:
                await self._maybe_remember_help_panel(dispatch.reply_message, payload)
            return

        followup = await interaction.followup.send(
            **spec.send_kwargs(),
            ephemeral=spec.ephemeral,
            wait=True,
        )
        if isinstance(followup, discord.Message):
            dispatch.reply_message = followup
            await self._maybe_remember_help_panel(dispatch.reply_message, payload)

    async def _edit_reply(self, dispatch: DiscordDispatchContext, payload: ReplyPayload) -> None:
        spec = reply_payload_to_spec(self, self.shared_runtime, payload)
        interaction = dispatch.interaction
        if (
            interaction is not None
            and interaction.type is discord.InteractionType.component
            and not interaction.response.is_done()
        ):
            await interaction.response.edit_message(**spec.edit_kwargs())
            dispatch.reply_message = interaction.message
            return

        if dispatch.reply_message is not None:
            await dispatch.reply_message.edit(**spec.edit_kwargs())
            return

        if interaction is not None:
            await interaction.edit_original_response(**spec.edit_kwargs())
            dispatch.reply_message = await self._try_original_response(interaction)

    async def _delete_target(self, dispatch: DiscordDispatchContext, target: str | None) -> None:
        if target == "invocation" and dispatch.invocation_message is not None:
            await _safe_delete_message(dispatch.invocation_message)
            return
        if target == "reply":
            if dispatch.reply_message is not None:
                await _safe_delete_message(dispatch.reply_message)
                return
            if dispatch.interaction is not None:
                try:
                    await dispatch.interaction.delete_original_response()
                except (discord.HTTPException, discord.NotFound):
                    LOGGER.debug("reply delete skipped because the original interaction response no longer exists")

    async def _delete_later(
        self,
        dispatch: DiscordDispatchContext,
        target: str | None,
        after_seconds: int,
    ) -> None:
        await asyncio.sleep(after_seconds)
        await self._delete_target(dispatch, target)

    def _track_task(self, coroutine: Any) -> asyncio.Task[Any]:
        task = asyncio.create_task(coroutine)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    async def _try_original_response(
        self,
        interaction: discord.Interaction[Any],
    ) -> discord.Message | None:
        try:
            original = await interaction.original_response()
        except (discord.HTTPException, discord.NotFound):
            return None
        return original

    async def _inspect_channel(
        self,
        channel_id: int,
        label: str,
        required: list[tuple[str, str]],
        checks: list[str],
        issues: list[str],
    ) -> None:
        try:
            channel = self.get_channel(channel_id) or await self.fetch_channel(channel_id)
        except discord.HTTPException as error:
            issues.append(
                f"{label} `{channel_id}` could not be found or accessed: {error}. Check the channel ID and bot access."
            )
            return

        if not isinstance(channel, discord.TextChannel):
            if isinstance(channel, discord.abc.GuildChannel):
                issues.append(
                    f"{label} <#{channel_id}> exists but is not text-based. Pick a text channel."
                )
            else:
                issues.append(
                    f"{label} `{channel_id}` exists but is not a guild text channel. Use a normal server text channel."
                )
            return

        bot_user = self.user
        if bot_user is None:
            issues.append(f"{label} <#{channel_id}> was found, but the bot user is not ready yet.")
            return

        try:
            member = channel.guild.me or await channel.guild.fetch_member(bot_user.id)
        except discord.HTTPException as error:
            issues.append(
                f"{label} <#{channel_id}> was found, but the bot member could not be loaded: {error}"
            )
            return

        permissions = channel.permissions_for(member)
        missing = [name for attr, name in required if not getattr(permissions, attr)]
        if not missing:
            checks.append(f"{label} <#{channel_id}> found with required permissions.")
            return

        issues.append(
            f"{label} <#{channel_id}> is missing bot permissions: {', '.join(missing)}. "
            "Fix the channel overrides or the bot role."
        )

    def _register_app_commands(self) -> None:
        if self._commands_registered:
            return
        self._commands_registered = True
        guild = self._test_guild_object()
        command_kwargs: dict[str, Any] = {}
        if guild is not None:
            command_kwargs["guild"] = guild

        @self.tree.command(
            name="ytcalc",
            description="Open the calculator panel.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        @app_commands.describe(
            user="Optional user to credit the receipt to",
            items="Optional items like `3 shovel 2 gloves 1 bucket`",
        )
        async def ytcalc(
            interaction: discord.Interaction[Any],
            user: discord.Member | None = None,
            items: str | None = None,
        ) -> None:
            channel = interaction.channel
            if channel is None:
                await interaction.response.send_message("This interaction is missing a channel context.", ephemeral=True)
                return
            if not self._is_allowed_channel_id(channel.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("YouTool Access", "This command is not enabled in this channel.")),
                    ephemeral=True,
                )
                return
            prefilled_items = parse_calc_prefix_input(self.base_runtime.catalog, items or "")[1]
            await self._start_calc_session(
                actor=interaction.user,
                channel=channel,
                target_user_id=user.id if user is not None else None,
                prefilled_items=prefilled_items,
                interaction=interaction,
            )

        @self.tree.command(
            name="ytmanage",
            description="Open the receipt manager.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        async def ytmanage(interaction: discord.Interaction[Any]) -> None:
            channel = interaction.channel
            if channel is None:
                await interaction.response.send_message("This interaction is missing a channel context.", ephemeral=True)
                return
            if not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("YouTool Access", "You are not an admin for this bot.")),
                    ephemeral=True,
                )
                return
            if not self._is_admin_channel_id(channel.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("YouTool Access", "This admin command is not enabled in this channel.")),
                    ephemeral=True,
                )
                return
            await self._send_manage_panel_message(interaction.user, channel, interaction)

        @self.tree.command(name="ythelp", description="Open the paged help panel.", **command_kwargs)
        @app_commands.guild_only()
        async def ythelp(interaction: discord.Interaction[Any]) -> None:
            await self._run_slash_command(interaction, _build_slash_input("ythelp"))

        @self.tree.command(name="ythealth", description="Check bot setup and permissions.", **command_kwargs)
        @app_commands.guild_only()
        async def ythealth(interaction: discord.Interaction[Any]) -> None:
            await self._run_slash_command(interaction, _build_slash_input("ythealth"))

        @self.tree.command(name="ytstats", description="Show the leaderboard.", **command_kwargs)
        @app_commands.guild_only()
        @app_commands.describe(sort="Optional sort: sales, procurement, or count.")
        @app_commands.choices(
            sort=[
                app_commands.Choice(name="Sales", value="sales"),
                app_commands.Choice(name="Procurement", value="procurement"),
                app_commands.Choice(name="Receipt Count", value="count"),
            ]
        )
        async def ytstats(
            interaction: discord.Interaction[Any],
            sort: app_commands.Choice[str] | None = None,
        ) -> None:
            await self._run_slash_command(
                interaction,
                _build_slash_input("ytstats", sort.value if sort is not None else None),
            )

        @self.tree.command(
            name="ytpricesheet",
            description="Show the current catalog price sheet.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        async def ytpricesheet(interaction: discord.Interaction[Any]) -> None:
            await self._run_slash_command(interaction, _build_slash_input("ytpricesheet"))

        @self.tree.command(
            name="ytcontracts",
            description="Show contract pricing or open the add flow.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        @app_commands.describe(action="Use add to open the contract add/update flow.")
        @app_commands.choices(action=[app_commands.Choice(name="Add", value="add")])
        async def ytcontracts(
            interaction: discord.Interaction[Any],
            action: app_commands.Choice[str] | None = None,
        ) -> None:
            await self._run_slash_command(
                interaction,
                _build_slash_input("ytcontracts", action.value if action is not None else None),
            )

        @self.tree.command(
            name="ytpayouts",
            description="Show current employee payout totals.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        @app_commands.describe(user="Optional user to filter the payout list.")
        async def ytpayouts(
            interaction: discord.Interaction[Any],
            user: discord.Member | None = None,
        ) -> None:
            await self._run_slash_command(
                interaction,
                _build_slash_input("ytpayouts", _member_token(user)),
            )

        @self.tree.command(
            name="ytrefresh",
            description="Reload catalog and contract files from disk.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        async def ytrefresh(interaction: discord.Interaction[Any]) -> None:
            await self._run_slash_command(interaction, _build_slash_input("ytrefresh"))

        @self.tree.command(
            name="ytprocurementcutover",
            description="Switch new receipts to procurement-funds accounting.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        async def ytprocurementcutover(interaction: discord.Interaction[Any]) -> None:
            await self._run_slash_command(interaction, _build_slash_input("ytprocurementcutover"))

        @self.tree.command(
            name="ytprocurementfunds",
            description="Record a procurement-funds withdrawal.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        @app_commands.describe(amount="Withdrawal amount.", user="Optional user to credit.")
        async def ytprocurementfunds(
            interaction: discord.Interaction[Any],
            amount: int,
            user: discord.Member | None = None,
        ) -> None:
            await self._run_slash_command(
                interaction,
                _build_slash_input("ytprocurementfunds", str(amount), _member_token(user)),
            )

        @self.tree.command(
            name="ytprocurementreturn",
            description="Record unused company funds returned.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        @app_commands.describe(amount="Return amount.", user="Optional user to credit.")
        async def ytprocurementreturn(
            interaction: discord.Interaction[Any],
            amount: int,
            user: discord.Member | None = None,
        ) -> None:
            await self._run_slash_command(
                interaction,
                _build_slash_input("ytprocurementreturn", str(amount), _member_token(user)),
            )

        @self.tree.command(
            name="ytprocurementbalance",
            description="Show remaining company procurement funds.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        @app_commands.describe(user="Optional user to inspect.")
        async def ytprocurementbalance(
            interaction: discord.Interaction[Any],
            user: discord.Member | None = None,
        ) -> None:
            await self._run_slash_command(
                interaction,
                _build_slash_input("ytprocurementbalance", _member_token(user)),
            )

        @self.tree.command(
            name="ytadjustprices",
            description="Edit the live catalog and refresh it.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        async def ytadjustprices(interaction: discord.Interaction[Any]) -> None:
            channel = interaction.channel
            if channel is None:
                await interaction.response.send_message("This interaction is missing a channel context.", ephemeral=True)
                return
            if not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("YouTool Access", "You are not an admin for this bot.")),
                    ephemeral=True,
                )
                return
            if not self._is_admin_channel_id(channel.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("YouTool Access", "This admin command is not enabled in this channel.")),
                    ephemeral=True,
                )
                return
            await self._send_adjustprices_panel(interaction.user, channel, interaction)

        @self.tree.command(
            name="yttemplates",
            description="Show or reload the live templates file.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        @app_commands.describe(action="Use reload to refresh the templates JSON from disk.")
        @app_commands.choices(action=[app_commands.Choice(name="Reload", value="reload")])
        async def yttemplates(
            interaction: discord.Interaction[Any],
            action: app_commands.Choice[str] | None = None,
        ) -> None:
            await self._run_slash_command(
                interaction,
                _build_slash_input("yttemplates", action.value if action is not None else None),
            )

        @self.tree.command(
            name="ytreset",
            description="Backup active receipts, then mark them paid or invalidate them.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        async def ytreset(interaction: discord.Interaction[Any]) -> None:
            channel = interaction.channel
            if channel is None:
                await interaction.response.send_message("This interaction is missing a channel context.", ephemeral=True)
                return
            if not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("YouTool Access", "You are not an admin for this bot.")),
                    ephemeral=True,
                )
                return
            if not self._is_admin_channel_id(channel.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("YouTool Access", "This admin command is not enabled in this channel.")),
                    ephemeral=True,
                )
                return
            await self._send_reset_confirmation(
                actor=interaction.user,
                channel=channel,
                mode=ResetMode.ALL,
                user_ids=[],
                interaction=interaction,
            )

        @self.tree.command(
            name="ytimport",
            description="Upload an export, review the preview, then confirm import.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        @app_commands.describe(mode="preserve, active, paid, or invalidated")
        @app_commands.choices(
            mode=[
                app_commands.Choice(name="Preserve", value="preserve"),
                app_commands.Choice(name="Active", value="active"),
                app_commands.Choice(name="Paid", value="paid"),
                app_commands.Choice(name="Invalidated", value="invalidated"),
            ]
        )
        async def ytimport(
            interaction: discord.Interaction[Any],
            mode: app_commands.Choice[str] | None = None,
        ) -> None:
            channel = interaction.channel
            if channel is None:
                await interaction.response.send_message("This interaction is missing a channel context.", ephemeral=True)
                return
            if not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("YouTool Access", "You are not an admin for this bot.")),
                    ephemeral=True,
                )
                return
            if not self._is_admin_channel_id(channel.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("YouTool Access", "This admin command is not enabled in this channel.")),
                    ephemeral=True,
                )
                return
            await self._start_import_prompt(
                actor=interaction.user,
                channel=channel,
                status_override=_parse_import_status_override(mode.value if mode is not None else None),
                interaction=interaction,
            )

        @self.tree.command(
            name="ytsanitize",
            description="Preview safe proof/import cleanup and confirm it before applying.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        async def ytsanitize(interaction: discord.Interaction[Any]) -> None:
            channel = interaction.channel
            if channel is None:
                await interaction.response.send_message("This interaction is missing a channel context.", ephemeral=True)
                return
            if not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("YouTool Access", "You are not an admin for this bot.")),
                    ephemeral=True,
                )
                return
            if not self._is_admin_channel_id(channel.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("YouTool Access", "This admin command is not enabled in this channel.")),
                    ephemeral=True,
                )
                return
            await self._send_sanitize_confirmation(
                actor=interaction.user,
                channel=channel,
                interaction=interaction,
            )

        @self.tree.command(
            name="ytrescan",
            description="Scan the main channel for undocumented receipts after a message and review them one by one.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        @app_commands.describe(boundary="Message link or message ID to scan after")
        async def ytrescan(
            interaction: discord.Interaction[Any],
            boundary: str,
        ) -> None:
            channel = interaction.channel
            if channel is None:
                await interaction.response.send_message("This interaction is missing a channel context.", ephemeral=True)
                return
            if not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("YouTool Access", "You are not an admin for this bot.")),
                    ephemeral=True,
                )
                return
            if not (self._is_allowed_channel_id(channel.id) or self._is_admin_channel_id(channel.id)):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("YouTool Access", "This command is not enabled in this channel.")),
                    ephemeral=True,
                )
                return
            await self._start_rescan(
                actor=interaction.user,
                invocation_channel=channel,
                boundary=boundary,
                interaction=interaction,
            )

        @self.tree.command(
            name="ytrebuildlogs",
            description="Rebuild the receipt log channel from the database.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        async def ytrebuildlogs(interaction: discord.Interaction[Any]) -> None:
            channel = interaction.channel
            if channel is None:
                await interaction.response.send_message("This interaction is missing a channel context.", ephemeral=True)
                return
            if not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("YouTool Access", "You are not an admin for this bot.")),
                    ephemeral=True,
                )
                return
            if not self._is_admin_channel_id(channel.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("YouTool Access", "This admin command is not enabled in this channel.")),
                    ephemeral=True,
                )
                return
            await self._rebuild_receipt_logs(interaction.user, channel, interaction)

        @self.tree.command(
            name="ytrestartbot",
            description="Restart the bot remotely.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        async def ytrestartbot(interaction: discord.Interaction[Any]) -> None:
            channel = interaction.channel
            if channel is None:
                await interaction.response.send_message("This interaction is missing a channel context.", ephemeral=True)
                return
            if not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("YouTool Access", "You are not an admin for this bot.")),
                    ephemeral=True,
                )
                return
            if not self._is_admin_channel_id(channel.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("YouTool Access", "This admin command is not enabled in this channel.")),
                    ephemeral=True,
                )
                return
            await self._request_shutdown(interaction.user, channel, restart=True, interaction=interaction)

        @self.tree.command(
            name="ytstop",
            description="Shut the bot down gracefully.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        async def ytstop(interaction: discord.Interaction[Any]) -> None:
            channel = interaction.channel
            if channel is None:
                await interaction.response.send_message("This interaction is missing a channel context.", ephemeral=True)
                return
            if not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("YouTool Access", "You are not an admin for this bot.")),
                    ephemeral=True,
                )
                return
            if not self._is_admin_channel_id(channel.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("YouTool Access", "This admin command is not enabled in this channel.")),
                    ephemeral=True,
                )
                return
            await self._request_shutdown(interaction.user, channel, restart=False, interaction=interaction)

        @self.tree.command(
            name="ytexport",
            description="Export the current database as JSON.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        async def ytexport(interaction: discord.Interaction[Any]) -> None:
            await self._run_slash_command(interaction, _build_slash_input("ytexport"))

    async def _sync_app_commands(self) -> None:
        guild = self._test_guild_object()
        if guild is not None:
            synced = await self.tree.sync(guild=guild)
            LOGGER.info("synced %s slash commands to test guild %s", len(synced), guild.id)
            return
        synced = await self.tree.sync()
        LOGGER.info("synced %s global slash commands", len(synced))

    def _test_guild_object(self) -> discord.Object | None:
        guild_id = self.base_runtime.config.discord.test_guild_id
        if guild_id is None or guild_id <= 0:
            return None
        return discord.Object(id=guild_id)

    async def _run_slash_command(
        self,
        interaction: discord.Interaction[Any],
        input_text: str,
    ) -> None:
        channel = interaction.channel
        if channel is None:
            await interaction.response.send_message(
                "This interaction is missing a channel context.",
                ephemeral=True,
            )
            return

        ctx = CommandContext(
            runtime=self.shared_runtime,
            actor=_actor_from_user(interaction.user),
            channel=_channel_from_id(self.base_runtime, channel.id),
            is_interaction=True,
            raw_input=input_text,
        )
        result = await execute_input(ctx, input_text)
        dispatch = DiscordDispatchContext(
            actor=ctx.actor,
            channel=ctx.channel,
            channel_object=channel,
            is_interaction=True,
            interaction=interaction,
        )
        await self.apply_result(dispatch, result)


class AddItemModal(discord.ui.Modal, title="Add Item"):
    quantity = discord.ui.TextInput(
        label="Quantity Ordered",
        custom_id="quantity",
        required=True,
        default="1",
    )
    override_unit_price = discord.ui.TextInput(
        label="Special Pricing (unit price, optional)",
        custom_id="override_unit_price",
        required=False,
        default="",
    )

    def __init__(
        self,
        client: YTAssistDiscordClient,
        owner_user_id: int,
        item_index: int,
        item_name: str,
    ) -> None:
        super().__init__(timeout=300)
        self._client = client
        self._owner_user_id = owner_user_id
        self._item_index = item_index
        self.title = f"Add {item_name}"

    async def on_submit(self, interaction: discord.Interaction[Any]) -> None:
        if interaction.user.id != self._owner_user_id:
            await interaction.response.send_message(
                "This calculator panel belongs to another user.",
                ephemeral=True,
            )
            return
        try:
            quantity, manual_override_unit_price = parse_add_item_inputs(
                str(self.quantity),
                str(self.override_unit_price),
            )
        except ValueError as error:
            await interaction.response.send_message(str(error), ephemeral=True)
            return

        if self._item_index >= len(self._client.base_runtime.catalog.items):
            await interaction.response.send_message(
                "Selected catalog item no longer exists.",
                ephemeral=True,
            )
            return
        catalog_item = self._client.base_runtime.catalog.items[self._item_index]
        if not catalog_item.active:
            await interaction.response.send_message(
                "Selected catalog item no longer exists.",
                ephemeral=True,
            )
            return

        session = await self._client.base_runtime.bot_state.update_session(
            interaction.user.id,
            lambda current: _insert_item_into_session(
                current,
                self._client.base_runtime.contracts,
                catalog_item.name,
                quantity,
                manual_override_unit_price,
            ),
        )
        if session is None:
            await interaction.response.send_message("Calculator session expired.", ephemeral=True)
            return

        payload = calc_reply_payload(self._client.base_runtime.catalog.items, session)
        spec = reply_payload_to_spec(self._client, self._client.shared_runtime, payload)
        await interaction.response.edit_message(**spec.edit_kwargs())


class EditReceiptItemsModal(discord.ui.Modal, title="Edit Receipt Items"):
    items = discord.ui.TextInput(
        label="Items (one per line: Qty Item or Qty Item = Unit Price)",
        custom_id="items",
        required=True,
        style=discord.TextStyle.paragraph,
    )

    def __init__(
        self,
        client: YTAssistDiscordClient,
        owner_user_id: int,
        receipt,
    ) -> None:
        super().__init__(timeout=300)
        self._client = client
        self._owner_user_id = owner_user_id
        self._receipt_id = receipt.id
        self.items.default = _receipt_items_editor_default_text(client.base_runtime.catalog, receipt)

    async def on_submit(self, interaction: discord.Interaction[Any]) -> None:
        if interaction.user.id != self._owner_user_id:
            await interaction.response.send_message(
                "This receipt panel belongs to another user.",
                ephemeral=True,
            )
            return

        receipt = await self._client.base_runtime.database.get_receipt(self._receipt_id)
        if receipt is None:
            await interaction.response.send_message(
                f"Receipt `{self._receipt_id}` was not found.",
                ephemeral=True,
            )
            return
        if str(interaction.user.id) != receipt.creator_user_id and not self._client._is_admin_user(interaction.user.id):
            await interaction.response.send_message(
                "Only the receipt creator or an admin can do that.",
                ephemeral=True,
            )
            return

        try:
            draft_items = parse_receipt_item_editor_input(
                self._client.base_runtime.catalog,
                str(self.items),
            )
            priced = price_items(self._client.base_runtime.catalog, draft_items)
        except ValueError as error:
            await interaction.response.send_message(str(error), ephemeral=True)
            return

        actor_display_name = getattr(interaction.user, "display_name", interaction.user.name)
        updated = await self._client.base_runtime.database.update_receipt_items(
            receipt.id,
            priced.items,
            total_sale=priced.total_sale,
            procurement_cost=priced.procurement_cost,
            profit=priced.profit,
            actor=AuditEventInput(
                actor_user_id=str(interaction.user.id),
                actor_display_name=actor_display_name,
                action="receipt_items_updated",
                target_receipt_id=receipt.id,
                detail_json={
                    "item_count": len(priced.items),
                    "total_sale": priced.total_sale,
                    "procurement_cost": priced.procurement_cost,
                    "profit": priced.profit,
                    "items": [
                        {
                            "item_name": item.item_name,
                            "quantity": item.quantity,
                            "unit_sale_price": item.unit_sale_price,
                            "unit_cost": item.unit_cost,
                            "pricing_source": item.pricing_source.as_str(),
                            "line_sale_total": item.line_sale_total,
                            "line_cost_total": item.line_cost_total,
                        }
                        for item in priced.items
                    ],
                },
            ),
        )
        if not updated:
            await interaction.response.send_message(
                f"Receipt `{receipt.id}` was not found.",
                ephemeral=True,
            )
            return

        refreshed = await self._client.base_runtime.database.get_receipt(receipt.id)
        if refreshed is None:
            await interaction.response.send_message(
                f"Receipt `{receipt.id}` disappeared after saving.",
                ephemeral=True,
            )
            return

        await self._client._refresh_posted_receipt_messages(refreshed)
        payload = await self._client._receipt_detail_reply(refreshed, interaction.user.id)
        spec = reply_payload_to_spec(self._client, self._client.shared_runtime, payload)
        await interaction.response.send_message(**spec.send_kwargs(), ephemeral=True)


class ContractAddModal(discord.ui.Modal, title="Add or Update Contract"):
    contract_name = discord.ui.TextInput(
        label="Person / Gang",
        custom_id="contract_name",
        required=True,
    )
    contract_aliases = discord.ui.TextInput(
        label="Aliases (optional, comma-separated)",
        custom_id="contract_aliases",
        required=False,
        default="",
    )
    contract_prices = discord.ui.TextInput(
        label="Item Prices (one per line: Item=Price)",
        custom_id="contract_prices",
        required=True,
        style=discord.TextStyle.paragraph,
    )

    def __init__(self, client: YTAssistDiscordClient, owner_user_id: int) -> None:
        super().__init__(timeout=300)
        self._client = client
        self._owner_user_id = owner_user_id

    async def on_submit(self, interaction: discord.Interaction[Any]) -> None:
        if interaction.user.id != self._owner_user_id:
            await interaction.response.send_message("This contracts panel belongs to another user.", ephemeral=True)
            return
        try:
            entry = ContractEntry(
                name=str(self.contract_name).strip(),
                aliases=[
                    alias.strip()
                    for alias in str(self.contract_aliases).split(",")
                    if alias.strip()
                ],
                prices=_parse_contract_prices(str(self.contract_prices)),
            )
            contracts = Contracts.from_entries(
                [
                    contract
                    for contract in self._client.base_runtime.contracts.snapshot_entries()
                    if not _contract_matches_name(contract, entry.name)
                ]
                + [entry],
                self._client.base_runtime.catalog,
            )
            contracts.save_to(self._client.base_runtime.config.storage.contracts_path)
        except Exception as error:
            await interaction.response.send_message(
                embed=embed_from_payload(task_error_embed("YouTool Contracts", str(error))),
                ephemeral=True,
            )
            return

        self._client.base_runtime.contracts = contracts
        await interaction.response.send_message(
            embed=embed_from_payload(
                task_status_embed(
                    "YouTool Contracts",
                    f"Saved contract `{entry.name}` with {len(entry.prices)} configured item price(s).",
                )
            ),
            ephemeral=True,
        )


class AdjustPriceModal(discord.ui.Modal):
    item_name = discord.ui.TextInput(
        label="Item Name",
        custom_id="item_name",
        required=True,
    )
    aliases = discord.ui.TextInput(
        label="Aliases (comma-separated)",
        custom_id="aliases",
        required=False,
        default="",
    )
    unit_price = discord.ui.TextInput(
        label="Unit Price",
        custom_id="unit_price",
        required=True,
    )
    bulk_config = discord.ui.TextInput(
        label="Bulk Price,Min Qty (blank to clear)",
        custom_id="bulk_config",
        required=False,
        default="",
    )
    unit_cost_active = discord.ui.TextInput(
        label="Unit Cost,Active (example: 4500,true)",
        custom_id="unit_cost_active",
        required=True,
    )

    def __init__(
        self,
        client: YTAssistDiscordClient,
        owner_user_id: int,
        item_index: int,
        item: CatalogItem,
    ) -> None:
        super().__init__(timeout=300, title=f"Edit {item.name}")
        self._client = client
        self._owner_user_id = owner_user_id
        self._item_index = item_index
        self.item_name.default = item.name
        self.aliases.default = ", ".join(item.aliases)
        self.unit_price.default = str(item.unit_price)
        self.bulk_config.default = (
            f"{item.bulk_price},{item.bulk_min_qty}"
            if item.bulk_price is not None and item.bulk_min_qty is not None
            else ""
        )
        self.unit_cost_active.default = f"{item.unit_cost or ''},{str(item.active).lower()}"

    async def on_submit(self, interaction: discord.Interaction[Any]) -> None:
        if interaction.user.id != self._owner_user_id:
            await interaction.response.send_message("This price panel belongs to another user.", ephemeral=True)
            return
        try:
            updated_item = _parse_adjusted_catalog_item(
                {
                    "item_name": str(self.item_name),
                    "aliases": str(self.aliases),
                    "unit_price": str(self.unit_price),
                    "bulk_config": str(self.bulk_config),
                    "unit_cost_active": str(self.unit_cost_active),
                }
            )
            summary_name = updated_item.name
            catalog_items = await _save_adjusted_catalog_item(
                self._client.base_runtime,
                self._item_index,
                updated_item,
            )
        except Exception as error:
            await interaction.response.send_message(
                embed=embed_from_payload(task_warning_embed("YouTool Adjust Prices", f"Price update failed. Nothing live was changed.\n{error}")),
                ephemeral=True,
            )
            return

        await interaction.response.edit_message(
            content="",
            embeds=[embed_from_payload(_adjustprices_embed(catalog_items, f"Saved `{summary_name}` and refreshed the live catalog."))],
            view=self._client._build_adjustprices_view(interaction.user.id, catalog_items),
        )


async def _safe_delete_message(message: discord.Message) -> None:
    try:
        await message.delete()
    except (discord.NotFound, discord.HTTPException):
        LOGGER.debug("message_id=%s delete skipped because it no longer exists", message.id)


async def _safe_delete_message_later(message: discord.Message, after_seconds: int) -> None:
    if after_seconds > 0:
        await asyncio.sleep(after_seconds)
    await _safe_delete_message(message)


def _schedule_prefix_invocation_cleanup(
    client: "YTAssistDiscordClient",
    message: discord.Message | None,
    *,
    skip_in_admin_channel: bool = True,
) -> None:
    if message is None:
        return
    if skip_in_admin_channel and client._is_admin_channel_id(message.channel.id):
        return
    timeout = client.base_runtime.config.discord.transient_message_timeout_seconds
    client._track_task(_safe_delete_message_later(message, timeout))


async def _safe_delete_component_message(
    interaction: discord.Interaction[Any],
    message: discord.Message,
) -> bool:
    try:
        await message.delete()
        return True
    except (discord.NotFound, discord.HTTPException):
        try:
            await interaction.delete_original_response()
            return True
        except (discord.NotFound, discord.HTTPException):
            LOGGER.warning(
                "message_id=%s custom_id=%s failed to delete component message cleanly",
                getattr(message, "id", None),
                _interaction_custom_id(interaction),
            )
            return False


async def _safe_interaction_warning(
    interaction: discord.Interaction[Any],
    content: str,
) -> None:
    try:
        if interaction.response.is_done():
            await interaction.followup.send(content=content, ephemeral=True)
            return
        await interaction.response.send_message(content=content, ephemeral=True)
    except (discord.NotFound, discord.HTTPException) as error:
        LOGGER.warning(
            "custom_id=%s failed to send interaction warning `%s`: %s",
            _interaction_custom_id(interaction),
            content,
            error,
        )


def _catalog_item_option_description(item) -> str:
    details = [f"Price ${item.unit_price:,}"]
    if item.bulk_price is not None and item.bulk_min_qty is not None:
        details.append(f"Bulk ${item.bulk_price:,} @ {item.bulk_min_qty:,}+")
    return " | ".join(details)


def _pricing_source_label(pricing_source: PricingSource) -> str:
    if pricing_source is PricingSource.BULK:
        return "Bulk"
    if pricing_source is PricingSource.OVERRIDE:
        return "Special"
    return "Default"


def _set_session_proof(session: CalculatorSession, proof_path: str, proof_url: str) -> None:
    session.payment_proof_path = proof_path
    session.payment_proof_source_url = proof_url


def _apply_selected_contract(contracts, session: CalculatorSession, selected_contract: str) -> None:
    session.selected_contract_name = selected_contract
    apply_contract_to_items(contracts, selected_contract, session.items)


def _clear_selected_contract(contracts, session: CalculatorSession) -> None:
    session.selected_contract_name = None
    apply_contract_to_items(contracts, None, session.items)


def _insert_item_into_session(
    session: CalculatorSession,
    contracts,
    item_name: str,
    quantity: int,
    manual_override_unit_price: int | None,
) -> None:
    contract_override_unit_price = None
    contract_name = None
    if manual_override_unit_price is None and session.selected_contract_name is not None:
        contract_override_unit_price = contracts.contract_price(session.selected_contract_name, item_name)
        if contract_override_unit_price is not None:
            contract = contracts.find_contract(session.selected_contract_name)
            contract_name = contract.name if contract is not None else None

    from yt_assist.domain.calculator import insert_draft_item

    insert_draft_item(
        session.items,
        DraftItem(
            item_name=item_name,
            quantity=quantity,
            override_unit_price=manual_override_unit_price or contract_override_unit_price,
            contract_name=contract_name,
        ),
        False,
    )
    session.last_selected_item_index = next(
        (index for index, item in enumerate(session.items) if item.item_name == item_name),
        session.last_selected_item_index,
    )


def _admin_receipt_target_status(status: str) -> ReceiptStatus | None:
    if status == "active":
        return ReceiptStatus.ACTIVE
    if status == "paid":
        return ReceiptStatus.PAID
    if status in {"deleted", "invalidated"}:
        return ReceiptStatus.INVALIDATED
    return None


def _seconds_delta(seconds: int):
    from datetime import timedelta

    return timedelta(seconds=seconds)


async def _safe_edit_message(
    message: discord.Message,
    *,
    content: str | None,
    embeds: list[discord.Embed],
    view: discord.ui.View | None,
) -> None:
    try:
        await message.edit(content=content, embeds=embeds, view=view)
    except (discord.HTTPException, discord.NotFound):
        LOGGER.debug("message_id=%s edit skipped because it no longer exists", message.id)


async def _send_ephemeral_prompt(
    interaction: discord.Interaction[Any],
    content: str,
) -> discord.Message | None:
    await interaction.response.send_message(content, ephemeral=True)
    return await interaction.original_response()


def _is_lifecycle_status_message(message: discord.Message) -> bool:
    return any(embed.title == "YouTool Status" for embed in message.embeds)


def _strip_heartbeat_suffix(topic: str) -> str:
    trimmed = topic.strip()
    heartbeat_label = "Status: Online! as of "
    if trimmed.startswith(heartbeat_label):
        return ""
    marker = " | Status: Online! as of "
    if marker in trimmed:
        return trimmed.split(marker, 1)[0].strip()
    return trimmed


def _extract_heartbeat_timestamp(topic: str) -> datetime | None:
    trimmed = topic.strip()
    heartbeat_label = "Status: Online! as of "
    if trimmed.startswith(heartbeat_label):
        raw_value = trimmed[len(heartbeat_label) :]
    else:
        marker = " | Status: Online! as of "
        if marker not in trimmed:
            return None
        raw_value = trimmed.rsplit(marker, 1)[1]
    normalized = raw_value.rstrip(".").strip()
    if not normalized:
        return None
    try:
        return datetime.strptime(normalized, "%I:%M:%S %p %m/%d/%y")
    except ValueError:
        return None


def _manila_now_local() -> datetime:
    return (datetime.now(UTC) + MANILA_OFFSET).replace(tzinfo=None)


def _is_matching_receipt_message(
    message: discord.Message,
    receipt_id: str,
    bot_user_id: int | None,
) -> bool:
    if bot_user_id is not None and message.author.id != bot_user_id:
        return False
    if f"Receipt `{receipt_id}`" in (message.content or ""):
        return True
    expected_title = f"YouTool Receipt {receipt_id}"
    return any(embed.title == expected_title for embed in message.embeds)


def _parse_reset_target_user_ids(scope: str | None) -> list[int]:
    if not scope:
        return []
    user_ids: list[int] = []
    for token in scope.split():
        parsed = parse_user_token(token)
        if parsed is not None and parsed not in user_ids:
            user_ids.append(parsed)
            continue
        normalized = "".join(ch for ch in token if ch.isdigit())
        if normalized:
            value = int(normalized)
            if value not in user_ids:
                user_ids.append(value)
    return user_ids


def _reset_scope_subject(mode: ResetMode, user_ids: list[int]) -> str:
    if mode is ResetMode.ALL:
        return "everyone"
    if mode is ResetMode.ONLY:
        if len(user_ids) == 1:
            return f"<@{user_ids[0]}>"
        return ", ".join(f"<@{user_id}>" for user_id in user_ids)
    if len(user_ids) == 1:
        return f"everyone except <@{user_ids[0]}>"
    return "everyone except " + ", ".join(f"<@{user_id}>" for user_id in user_ids)


def _reset_warning_message(mode: ResetMode, user_ids: list[int]) -> str:
    return (
        f"Are you sure you want to reset {_reset_scope_subject(mode, user_ids)}?\n"
        "Only active receipts are affected, and a backup export will be saved before any status changes."
    )


def _reset_action_prompt_message(mode: ResetMode, user_ids: list[int]) -> str:
    return (
        f"How should we reset {_reset_scope_subject(mode, user_ids)}?\n"
        "Choose `Mark Paid` if those active receipts were already paid out. Choose `Invalidate` for a general reset or bad receipts."
    )


def _reset_scope_matches(mode: ResetMode, user_ids: list[int], creator_user_id: str) -> bool:
    try:
        creator_id = int(creator_user_id)
    except ValueError:
        creator_id = None
    if mode is ResetMode.ALL:
        return True
    if mode is ResetMode.ONLY:
        return creator_id in user_ids
    return creator_id is None or creator_id not in user_ids


def _reset_progress_message(mode: ResetMode, user_ids: list[int], action: ResetAction) -> str:
    return f"{action.progress_prefix()} {_reset_scope_subject(mode, user_ids)} now..."


def _reset_export_label(mode: ResetMode, action: ResetAction) -> str:
    if mode is ResetMode.ALL:
        return f"yt-assist-reset-all-{action.export_suffix()}"
    if mode is ResetMode.ONLY:
        return f"yt-assist-reset-selected-{action.export_suffix()}"
    return f"yt-assist-reset-except-{action.export_suffix()}"


def _reset_scope_action(mode: ResetMode, user_ids: list[int], action: ResetAction) -> str:
    return f"{action.completed_prefix()} {_reset_scope_subject(mode, user_ids)}"


def _reset_empty_result_message(action: ResetAction, mode: ResetMode, user_ids: list[int]) -> str:
    return (
        f"No active receipts matched {_reset_scope_subject(mode, user_ids)}.\n"
        f"Nothing was {action.empty_result_verb()}."
    )


def _parse_import_status_override(mode: str | None) -> ReceiptStatus | None:
    normalized = (mode or "").strip().lower()
    if not normalized or normalized in {"preserve", "asis", "as-is", "as_is", "file", "default"}:
        return None
    if normalized == "active":
        return ReceiptStatus.ACTIVE
    if normalized == "paid":
        return ReceiptStatus.PAID
    if normalized in {"invalid", "invalidate", "invalidated"}:
        return ReceiptStatus.INVALIDATED
    raise ValueError(
        f"Invalid import mode `{mode}`. Use `preserve`, `active`, `paid`, or `invalidated`."
    )


def _parse_import_request(remainder: str | None) -> tuple[str | None, ReceiptStatus | None]:
    raw = (remainder or "").strip()
    if not raw:
        return None, None
    try:
        tokens = shlex.split(raw)
    except ValueError as error:
        raise ValueError(f"Invalid import syntax: {error}") from error
    if not tokens:
        return None, None
    if len(tokens) == 1:
        try:
            return None, _parse_import_status_override(tokens[0])
        except ValueError:
            return tokens[0], None

    try:
        status_override = _parse_import_status_override(tokens[-1])
        file_name = " ".join(tokens[:-1]).strip()
        if file_name:
            return file_name, status_override
    except ValueError:
        status_override = None

    return " ".join(tokens).strip(), status_override


def _resolve_import_file_path(import_dir: Path, file_name: str) -> Path:
    requested = file_name.strip()
    if not requested:
        raise ValueError("Provide a file name from the `/import` folder.")

    root = import_dir.resolve()
    if not root.exists():
        raise ValueError("The `/import` folder does not exist yet.")
    candidate = (root / requested).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as error:
        raise ValueError("Import file paths must stay inside the `/import` folder.") from error

    if candidate.is_file():
        return candidate

    for path in root.iterdir():
        if path.is_file() and path.name.lower() == requested.lower():
            return path

    available = sorted(path.name for path in root.iterdir() if path.is_file())
    detail = ""
    if available:
        preview = ", ".join(f"`{name}`" for name in available[:5])
        remaining = len(available) - min(len(available), 5)
        suffix = f" (+{remaining} more)" if remaining > 0 else ""
        detail = f" Available files: {preview}{suffix}"
    raise ValueError(f"Import file `{requested}` was not found in `/import`.{detail}")


def _cleanup_import_review_file(review: PendingImportReview) -> None:
    if review.delete_after_use:
        review.temp_path.unlink(missing_ok=True)


def _import_mode_label(status_override: ReceiptStatus | None) -> str:
    if status_override is None:
        return "Preserve File Statuses"
    if status_override is ReceiptStatus.ACTIVE:
        return "Active"
    if status_override is ReceiptStatus.PAID:
        return "Paid"
    return "Invalidated"


def _import_mode_prompt_line(status_override: ReceiptStatus | None) -> str:
    if status_override is None:
        return "Imported receipts will preserve the status stored in the file."
    return f"Imported receipts will be overridden to `{_import_mode_label(status_override)}` during confirmation."


def _import_status_handling_description(status_override: ReceiptStatus | None) -> str:
    if status_override is None:
        return "preserve each new receipt's status from the uploaded file"
    return f"override every new receipt to {_import_mode_label(status_override)}"


def _format_import_affected_users(user_ids: list[str]) -> str:
    if not user_ids:
        return "nobody"
    shown = ", ".join(f"<@{user_id}>" for user_id in user_ids[:10])
    remaining = len(user_ids) - min(len(user_ids), 10)
    if remaining <= 0:
        return shown
    return f"{shown} (+{remaining} more)"


def _format_import_status_counts(counts) -> str | None:
    parts: list[str] = []
    if counts.active > 0:
        parts.append(f"Active {counts.active}")
    if counts.paid > 0:
        parts.append(f"Paid {counts.paid}")
    if counts.invalidated > 0:
        parts.append(f"Invalidated {counts.invalidated}")
    return ", ".join(parts) if parts else None


def _import_confirmation_message(
    status_override: ReceiptStatus | None,
    preview: ImportPreview,
) -> str:
    lines = ["Review this import before continuing:"]
    lines.append(f"New receipts to add: {preview.importable_receipts}")
    lines.append(f"Status handling: {_import_status_handling_description(status_override)}")
    lines.append(f"Affected staff: {_format_import_affected_users(preview.affected_user_ids)}")
    lines.append(f"Existing receipts with matching IDs: {preview.skipped_existing_receipts} skipped and left unchanged")
    lines.append(f"Duplicate IDs inside this upload: {preview.skipped_duplicate_ids_in_file} skipped")
    lines.append(f"Receipts in uploaded file: {preview.total_receipts_in_file}")
    status_summary = _format_import_status_counts(preview.resulting_status_counts)
    if status_summary is not None:
        lines.append(f"New receipts by final status: {status_summary}")
    if preview.duplicate_receipt_ids:
        lines.append(f"Duplicate ID sample: {', '.join(preview.duplicate_receipt_ids)}")
    lines.append("Nothing will overwrite or modify existing receipts.")
    lines.append("Press Confirm Import to add only the new receipts above, or Cancel to abort.")
    return "\n".join(lines)


def _import_noop_message(
    status_override: ReceiptStatus | None,
    preview: ImportPreview,
) -> str:
    lines = ["Nothing new will be imported."]
    lines.append(f"Status handling requested: {_import_status_handling_description(status_override)}")
    lines.append(f"Receipts in uploaded file: {preview.total_receipts_in_file}")
    lines.append(f"Existing receipts with matching IDs: {preview.skipped_existing_receipts} skipped and left unchanged")
    lines.append(f"Duplicate IDs inside this upload: {preview.skipped_duplicate_ids_in_file} skipped")
    if preview.duplicate_receipt_ids:
        lines.append(f"Duplicate ID sample: {', '.join(preview.duplicate_receipt_ids)}")
    lines.append("No current receipt data was modified.")
    return "\n".join(lines)


def _import_completed_message(
    actor_user_id: int,
    attachment_filename: str,
    schema_version: int,
    status_override: ReceiptStatus | None,
    report: ImportReport,
) -> str:
    lines = [f"Import complete for <@{actor_user_id}> from `{attachment_filename}`."]
    lines.append(f"New receipts added: {report.imported_receipts}")
    lines.append(f"Status handling used: {_import_status_handling_description(status_override)}")
    lines.append(f"Receipts in uploaded file: {report.total_receipts_in_file}")
    lines.append(f"Existing receipts with matching IDs: {report.skipped_existing_receipts} skipped and left unchanged")
    lines.append(f"Duplicate IDs inside this upload: {report.skipped_duplicate_ids_in_file} skipped")
    lines.append(f"Imported audit events: {report.imported_audit_events}")
    lines.append(f"Skipped existing audit events: {report.skipped_existing_audit_events}")
    lines.append(f"Schema version: {schema_version}")
    lines.append("No existing receipts were overwritten.")
    if report.duplicate_receipt_ids:
        lines.append(f"Duplicate ID sample: {', '.join(report.duplicate_receipt_ids)}")
    return "\n".join(lines)


def _receipt_items_editor_default_text(catalog: Catalog, receipt) -> str:
    lines: list[str] = []
    for item in receipt.items:
        line = f"{item.quantity} {item.item_name}"
        expected_unit_price = _expected_catalog_unit_price(catalog, item.item_name, item.quantity)
        if expected_unit_price is None or expected_unit_price != item.unit_sale_price:
            line += f" = {item.unit_sale_price}"
        lines.append(line)
    return "\n".join(lines)


def _expected_catalog_unit_price(catalog: Catalog, item_name: str, quantity: int) -> int | None:
    catalog_item = catalog.find_item(item_name)
    if catalog_item is None:
        return None
    if (
        catalog_item.bulk_price is not None
        and catalog_item.bulk_min_qty is not None
        and quantity >= catalog_item.bulk_min_qty
    ):
        return catalog_item.bulk_price
    return catalog_item.unit_price


def _adjustprices_embed(catalog_items: list[CatalogItem], notice: str | None = None) -> EmbedPayload:
    if not catalog_items:
        description = "No catalog items are configured yet."
    else:
        description = "\n".join(
            (
                f"{index + 1}. {item.name} | Unit ${item.unit_price:,} | "
                f"{(f'Bulk ${item.bulk_price:,} @ {item.bulk_min_qty:,}+' if item.bulk_price is not None and item.bulk_min_qty is not None else 'Bulk n/a')} | "
                f"{(f'Cost ${item.unit_cost:,}' if item.unit_cost is not None else 'Cost n/a')} | "
                f"{'Active' if item.active else 'Inactive'}"
            )
            for index, item in enumerate(catalog_items)
        )
    return task_status_embed("YouTool Adjust Prices", description).field(
        "Next Step",
        notice or "Select a catalog item to edit its live pricing values.",
        False,
    )


def _parse_contract_prices(input_text: str) -> list[ContractPriceEntry]:
    prices: list[ContractPriceEntry] = []
    for line in input_text.splitlines():
        line = line.strip()
        if not line:
            continue
        if "=" not in line:
            raise ValueError("contract prices must use `Item=Price`, one per line")
        item_name, unit_price_raw = line.split("=", 1)
        item_name = item_name.strip()
        if not item_name:
            raise ValueError("contract prices cannot contain a blank item name")
        try:
            unit_price = int(unit_price_raw.strip())
        except ValueError as error:
            raise ValueError(f"contract price for `{item_name}` must be a whole number") from error
        if unit_price <= 0:
            raise ValueError(f"contract price for `{item_name}` must be greater than 0")
        prices.append(ContractPriceEntry(item_name=item_name, unit_price=unit_price))
    if not prices:
        raise ValueError("add at least one contract price line")
    return prices


def _contract_matches_name(contract: ContractEntry, name: str) -> bool:
    normalized = name.strip().lower()
    if contract.name.strip().lower() == normalized:
        return True
    return any(alias.strip().lower() == normalized for alias in contract.aliases)


def _parse_adjusted_catalog_item(values: dict[str, str]) -> CatalogItem:
    name = values.get("item_name", "").strip()
    if not name:
        raise ValueError("item name cannot be blank")
    aliases = [alias.strip() for alias in values.get("aliases", "").split(",") if alias.strip()]
    try:
        unit_price = int(values.get("unit_price", "").strip())
    except ValueError as error:
        raise ValueError("unit price must be a whole number") from error
    if unit_price <= 0:
        raise ValueError("unit price must be greater than 0")

    bulk_raw = values.get("bulk_config", "").strip()
    if not bulk_raw:
        bulk_price = None
        bulk_min_qty = None
    else:
        if "," not in bulk_raw:
            raise ValueError("bulk config must use `price,min_qty` or stay blank")
        bulk_price_raw, bulk_min_qty_raw = bulk_raw.split(",", 1)
        try:
            bulk_price = int(bulk_price_raw.strip()) if bulk_price_raw.strip() else None
            bulk_min_qty = int(bulk_min_qty_raw.strip()) if bulk_min_qty_raw.strip() else None
        except ValueError as error:
            raise ValueError("bulk price and minimum quantity must be whole numbers") from error
        if (bulk_price is None) != (bulk_min_qty is None):
            raise ValueError("bulk config must provide both price and minimum quantity, or stay blank")
        if bulk_price is not None and bulk_price <= 0:
            raise ValueError("bulk price must be greater than 0")
        if bulk_min_qty is not None and bulk_min_qty <= 0:
            raise ValueError("bulk minimum quantity must be greater than 0")

    unit_cost_active = values.get("unit_cost_active", "")
    if "," not in unit_cost_active:
        raise ValueError("unit cost and active must use `cost,true` or `cost,false`")
    unit_cost_raw, active_raw = unit_cost_active.split(",", 1)
    unit_cost_raw = unit_cost_raw.strip()
    unit_cost = int(unit_cost_raw) if unit_cost_raw else None
    if unit_cost is not None and unit_cost <= 0:
        raise ValueError("unit cost must be greater than 0")
    active_token = active_raw.strip().lower()
    if active_token in {"true", "yes", "1", "active"}:
        active = True
    elif active_token in {"false", "no", "0", "inactive"}:
        active = False
    else:
        raise ValueError("active must be true/false, yes/no, 1/0, or active/inactive")

    return CatalogItem(
        name=name,
        aliases=aliases,
        unit_price=unit_price,
        bulk_price=bulk_price,
        bulk_min_qty=bulk_min_qty,
        unit_cost=unit_cost,
        active=active,
    )


async def _save_adjusted_catalog_item(
    runtime,
    item_index: int,
    updated_item: CatalogItem,
) -> list[CatalogItem]:
    snapshot = runtime.catalog.snapshot()
    if item_index >= len(snapshot.items):
        raise ValueError("selected catalog item no longer exists")
    old_name = snapshot.items[item_index].name
    snapshot.items[item_index] = updated_item
    updated_catalog = Catalog.from_items(snapshot.items)

    updated_contract_entries = runtime.contracts.snapshot_entries()
    if old_name.strip().lower() != updated_item.name.strip().lower():
        for contract in updated_contract_entries:
            for price in contract.prices:
                if price.item_name.strip().lower() == old_name.strip().lower():
                    price.item_name = updated_item.name

    validated_contracts = Contracts.from_entries(updated_contract_entries, updated_catalog)
    updated_catalog.save_to(runtime.config.storage.catalog_path)
    validated_contracts.save_to(runtime.config.storage.contracts_path)
    runtime.catalog = Catalog.load_from(runtime.config.storage.catalog_path)
    runtime.contracts = Contracts.load_from(runtime.config.storage.contracts_path, runtime.catalog)
    return runtime.catalog.items


def _leaderboard_from_receipts(receipts) -> list[LeaderboardEntry]:
    buckets: dict[str, LeaderboardEntry] = {}
    for receipt in receipts:
        current = buckets.get(receipt.creator_user_id)
        if current is None:
            current = LeaderboardEntry(
                user_id=receipt.creator_user_id,
                display_name=receipt.creator_display_name,
                total_sales=0,
                procurement_cost=0,
                receipt_count=0,
            )
            buckets[receipt.creator_user_id] = current
        current.total_sales += receipt.total_sale
        current.procurement_cost += receipt.procurement_cost
        current.receipt_count += 1
    return sorted(
        buckets.values(),
        key=lambda entry: (-entry.total_sales, -entry.procurement_cost, -entry.receipt_count, entry.display_name),
    )


def _parse_discord_message_link(input_text: str) -> tuple[int, int] | None:
    parts = input_text.strip().rstrip("/").split("/")
    if len(parts) < 3:
        return None
    try:
        message_id = int(parts[-1])
        channel_id = int(parts[-2])
    except ValueError:
        return None
    return channel_id, message_id


def _resolve_rescan_boundary(
    allowed_channel_ids: list[int],
    invocation_channel_id: int,
    boundary: str,
    invocation_channel_allowed: bool,
) -> tuple[int, int]:
    parsed_link = _parse_discord_message_link(boundary)
    if parsed_link is not None:
        channel_id, message_id = parsed_link
    else:
        message_id = int(boundary)
        channel_id = invocation_channel_id if invocation_channel_allowed else next(iter(allowed_channel_ids), invocation_channel_id)
    if allowed_channel_ids and channel_id not in allowed_channel_ids:
        raise ValueError("rescan only supports configured main channels")
    return channel_id, message_id


def _receipt_like_attachments(message: discord.Message) -> list[discord.Attachment]:
    attachments: list[discord.Attachment] = []
    for attachment in message.attachments:
        content_type = (attachment.content_type or "").lower()
        extension = attachment.filename.rsplit(".", 1)[-1].lower() if "." in attachment.filename else ""
        if (
            content_type.startswith("image/")
            or attachment.width is not None
            or extension in {"png", "jpg", "jpeg", "webp", "gif", "bmp"}
        ):
            attachments.append(attachment)
    return attachments


def _resolve_rescan_credit_target(message: discord.Message) -> SessionCreditTarget:
    credited_user = next(
        (user for user in message.mentions if user.id != message.author.id and not user.bot),
        message.author,
    )
    return SessionCreditTarget(
        user_id=str(credited_user.id),
        username=credited_user.name,
        display_name=getattr(credited_user, "display_name", getattr(credited_user, "global_name", credited_user.name)),
    )


async def run_console(config_path: Path | None = None, command: str | None = None) -> int:
    resolved_config_path = _default_config_path(config_path)
    runtime = await build_runtime_context_with_options(resolved_config_path, require_token=False)
    try:
        console = ConsoleRuntime(runtime, resolved_config_path)
        if command is not None:
            return await console.run_once(command)
        return await console.run()
    finally:
        await runtime.database.close()
        runtime.logging_guards.stop()


async def run_discord(config_path: Path | None = None) -> int:
    resolved_config_path = _default_config_path(config_path)
    preflight_config = load_runtime_config(resolved_config_path, require_token=True)
    preflight_config.ensure_directories()
    lock_path = preflight_config.storage.database_path.parent / "yt-assist.lock"
    pid_path = preflight_config.storage.database_path.parent / "yt-assist.pid"
    try:
        instance_guard = SingleInstanceGuard.acquire(lock_path, pid_path)
    except SingleInstanceError as error:
        LOGGER.error("%s", error)
        return 1

    try:
        runtime = await build_runtime_context_with_options(resolved_config_path, require_token=True)
    except Exception:
        instance_guard.release()
        raise
    client = YTAssistDiscordClient(runtime)
    loop = asyncio.get_running_loop()
    stop_signal_path = preflight_config.storage.database_path.parent / LOCAL_STOP_SIGNAL_FILE
    previous_signal_handlers: dict[signal.Signals, Any] = {}
    host_shutdown_noted = False

    def _persist_host_stop_signal(signal_name: str) -> None:
        timestamp = datetime.now(UTC).isoformat()
        payload = f"{timestamp} {signal_name}\n"
        try:
            stop_signal_path.parent.mkdir(parents=True, exist_ok=True)
            stop_signal_path.write_text(payload, encoding="utf-8")
        except OSError as error:
            LOGGER.warning("path=%s failed to persist host stop signal: %s", stop_signal_path, error)

    def _handle_host_signal(signum: int, _frame: Any) -> None:
        nonlocal host_shutdown_noted
        try:
            signal_name = signal.Signals(signum).name
        except ValueError:
            signal_name = f"SIGNAL_{signum}"
        LOGGER.warning("signal=%s received from host", signal_name)
        _persist_host_stop_signal(signal_name)
        if host_shutdown_noted:
            return
        host_shutdown_noted = True
        try:
            loop.call_soon_threadsafe(client.request_host_shutdown, signal_name)
        except RuntimeError:
            LOGGER.warning("signal=%s host shutdown callback could not be scheduled because the event loop is closing", signal_name)

    for handled_signal in (signal.SIGTERM, signal.SIGINT):
        try:
            previous_signal_handlers[handled_signal] = signal.getsignal(handled_signal)
            signal.signal(handled_signal, _handle_host_signal)
        except (AttributeError, OSError, ValueError):
            continue
    try:
        LOGGER.info("starting Discord runtime")
        await client.start(runtime.config.discord.token)
        return 0
    finally:
        try:
            for handled_signal, previous_handler in previous_signal_handlers.items():
                try:
                    signal.signal(handled_signal, previous_handler)
                except (AttributeError, OSError, ValueError):
                    continue
            if not client.is_closed():
                await client.close()
            for task in list(client._background_tasks):
                task.cancel()
            if client._background_tasks:
                await asyncio.gather(*client._background_tasks, return_exceptions=True)
            await runtime.database.close()
            runtime.logging_guards.stop()
        finally:
            instance_guard.release()
