"""Discord and local runtimes for the parity port."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import shlex
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

import discord
from discord import app_commands

from yt_assist.app import RuntimeContext, build_runtime_context_with_options
from yt_assist.config import load_runtime_config
from yt_assist.domain.backup import load_export_async, save_export_async
from yt_assist.domain.catalog import Catalog, CatalogItem
from yt_assist.domain.contracts import (
    ContractEntry,
    ContractPriceEntry,
    Contracts,
    apply_contract_to_items,
)
from yt_assist.domain.models import (
    AccountingPolicy,
    AuditEvent,
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
from yt_assist.domain.packages import PackageExpansion, PackageSelection, append_unique_items, expand_package
from yt_assist.domain.pricing import price_items
from yt_assist.domain.proof import join_proof_values, save_proof_attachment, split_proof_values
from yt_assist.single_instance import SingleInstanceError, SingleInstanceGuard
from yt_assist.storage.database import ImportPreview, ImportReport

from .command_support import (
    parse_add_item_inputs,
    parse_calc_prefix_input,
    parse_prefill_items,
    parse_user_token,
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
from .render import (
    BUTTON_STYLE_DANGER,
    BUTTON_STYLE_PRIMARY,
    BUTTON_STYLE_SECONDARY,
    BUTTON_STYLE_SUCCESS,
    ActionRowPayload,
    ButtonPayload,
    EmbedPayload,
    ReceiptDisplayContext,
    ReplyPayload,
    calc_action_rows,
    calc_completed_embed,
    calc_embeds,
    calc_failure_embed,
    calc_processing_embed,
    calc_reply_payload,
    calc_timeout_action_rows,
    calc_timeout_warning_embed,
    help_action_rows,
    help_page_embed,
    lifecycle_status_embed,
    manage_page_parts,
    receipt_detail_payload,
    receipt_log_payload,
    receipt_main_payload,
    render_stats_description,
    task_error_embed,
    task_status_embed,
    task_warning_embed,
)
from .state import CalculatorSession, SessionCreditTarget, new_receipt_id

LOGGER = logging.getLogger(__name__)
STATUS_SCAN_LIMIT = 1_000
STATUS_REPOST_DEBOUNCE_SECONDS = 5
STATUS_REPOST_FALLBACK_INTERVAL_SECONDS = 60
TOPIC_HEARTBEAT_INTERVAL_SECONDS = 600
LOCAL_STOP_SIGNAL_FILE = "bakunawa-mech.stop"
CALCULATOR_THREAD_PREFIX = "BM Calc - "
RECEIPT_LOG_THREAD_PREFIX = "BM Logs - "
MAINTENANCE_WRITE_DELAY_SECONDS = 0.35


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
    attachment_paths: list[str]
    allowed_mentions: discord.AllowedMentions | None

    def send_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        if self.content is not None:
            kwargs["content"] = self.content
        if self.embeds:
            kwargs["embeds"] = self.embeds
        if self.view is not None:
            kwargs["view"] = self.view
        files = _discord_files_from_paths(self.attachment_paths)
        if files:
            kwargs["files"] = files
        if self.allowed_mentions is not None:
            kwargs["allowed_mentions"] = self.allowed_mentions
        return kwargs

    def edit_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "content": self.content,
            "embeds": self.embeds,
            "view": self.view,
        }
        files = _discord_files_from_paths(self.attachment_paths)
        if files:
            kwargs["attachments"] = files
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


@dataclass(slots=True)
class CleanResult:
    inspected: int = 0
    deleted: int = 0
    preserved: int = 0
    failed: int = 0


@dataclass(slots=True)
class ProofPreviewRepairResult:
    receipts_checked: int = 0
    receipts_with_proofs: int = 0
    receipts_with_local_proofs: int = 0
    receipts_missing_local_proofs: int = 0
    channels_scanned: int = 0
    messages_inspected: int = 0
    messages_refreshed: int = 0
    refresh_failures: int = 0
    channel_failures: int = 0


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
        print("BakunawaMech local parity console")
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
            id=f"BM-DEMO-ACTIVE-{suffix}",
            creator_user_id="42",
            creator_username="42",
            creator_display_name="Tester",
            guild_id="demo",
            channel_id=str(self.state.channel.channel_id),
            total_sale=50000,
            procurement_cost=25000,
            profit=25000,
            status=ReceiptStatus.ACTIVE,
            payment_proof_path=None,
            payment_proof_source_url=None,
            finalized_at=now,
            items=[
                PricedItem(
                    item_name="TURBO CHARGER",
                    quantity=1,
                    unit_sale_price=50000,
                    unit_cost=25000,
                    pricing_source=PricingSource.DEFAULT,
                    line_sale_total=50000,
                    line_cost_total=25000,
                )
            ],
        )
        paid = NewReceipt(
            id=f"BM-DEMO-PAID-{suffix}",
            creator_user_id="42",
            creator_username="42",
            creator_display_name="Tester",
            guild_id="demo",
            channel_id=str(self.state.channel.channel_id),
            total_sale=50000,
            procurement_cost=10000,
            profit=40000,
            status=ReceiptStatus.PAID,
            payment_proof_path=None,
            payment_proof_source_url=None,
            finalized_at=now,
            items=[
                PricedItem(
                    item_name="ENGINE OIL",
                    quantity=1,
                    unit_sale_price=50000,
                    unit_cost=10000,
                    pricing_source=PricingSource.DEFAULT,
                    line_sale_total=50000,
                    line_cost_total=10000,
                )
            ],
        )
        invalidated = NewReceipt(
            id=f"BM-DEMO-VOID-{suffix}",
            creator_user_id="7",
            creator_username="7",
            creator_display_name="Another",
            guild_id="demo",
            channel_id=str(self.state.channel.channel_id),
            total_sale=500,
            procurement_cost=100,
            profit=400,
            status=ReceiptStatus.INVALIDATED,
            payment_proof_path=None,
            payment_proof_source_url=None,
            finalized_at=now,
            items=[
                PricedItem(
                    item_name="RESPRAY KIT",
                    quantity=1,
                    unit_sale_price=500,
                    unit_cost=100,
                    pricing_source=PricingSource.DEFAULT,
                    line_sale_total=500,
                    line_cost_total=100,
                )
            ],
        )

        await self.runtime.database.save_receipt(active, None)
        await self.runtime.database.save_receipt(paid, None)
        await self.runtime.database.save_receipt(invalidated, None)


class DiscordRuntimeFacade:
    def __init__(self, runtime: RuntimeContext, client: BakunawaMechDiscordClient) -> None:
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
    def packages(self):
        return self._runtime.packages

    @packages.setter
    def packages(self, value) -> None:
        self._runtime.packages = value

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
        client: BakunawaMechDiscordClient,
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
        client: BakunawaMechDiscordClient,
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
        client: BakunawaMechDiscordClient,
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
    def __init__(self, client: BakunawaMechDiscordClient, timeout_seconds: int) -> None:
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
    client: BakunawaMechDiscordClient,
    runtime: DiscordRuntimeFacade,
    payload: ReplyPayload,
) -> discord.ui.View | None:
    if not payload.components:
        return None
    return PayloadView(client, runtime, payload)


def _discord_files_from_paths(paths: list[str]) -> list[discord.File]:
    files: list[discord.File] = []
    for raw_path in paths[:10]:
        path = Path(raw_path)
        if not path.is_file():
            continue
        files.append(discord.File(path, filename=path.name))
    return files


def reply_payload_to_spec(
    client: BakunawaMechDiscordClient,
    runtime: DiscordRuntimeFacade,
    payload: ReplyPayload,
) -> DiscordReplySpec:
    return DiscordReplySpec(
        content=payload.content,
        embeds=[embed_from_payload(embed) for embed in payload.embeds],
        view=view_from_payload(client, runtime, payload),
        ephemeral=payload.ephemeral,
        attachment_paths=list(payload.attachment_paths),
        allowed_mentions=discord.AllowedMentions.none() if payload.silent_mentions else None,
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


class BakunawaMechDiscordClient(discord.Client):
    def __init__(self, runtime: RuntimeContext) -> None:
        intents = discord.Intents.default()
        intents.guilds = True
        intents.message_content = runtime.config.discord.message_content_intent
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
        self._shutdown_requested = False
        self._restart_requested = False
        self._ready_announced = False
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
        if interaction.channel is not None and not self._is_scoped_messageable_channel(interaction.channel):
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "Bakunawa Mech is only enabled in its configured shop channels.",
                    ephemeral=True,
                )
            return
        if (
            interaction.channel is None
            and interaction.channel_id is not None
            and not self._is_scoped_channel_id(interaction.channel_id)
        ):
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "Bakunawa Mech is only enabled in its configured shop channels.",
                    ephemeral=True,
                )
            return
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
                        "The bot logged the stack trace to `logs/bakunawa-mech.log`."
                    ),
                )

    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot:
            return

        channel_id = message.channel.id
        if not self._is_scoped_messageable_channel(message.channel):
            return
        self._note_channel_activity(message)
        is_allowed_channel = self._is_allowed_messageable_channel(message.channel)
        is_admin_channel = self._is_admin_messageable_channel(message.channel)

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
            if command_name == "rebuildlogs":
                await self._run_rebuild_logs_prefix(message)
                return
            if command_name in {"fixpreviews", "repairpreviews"}:
                await self._run_fix_previews_prefix(message)
                return
            if command_name == "clean":
                await self._run_clean_prefix(message)
                return
            if command_name == "note":
                await self._run_note_prefix(message, remainder)
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
        prefix = self.base_runtime.config.discord.prefix
        self._admin_status = LifecycleStatusState(
            kind=LifecycleStatusKind.ONLINE,
            description=(
                "Bot is fully online, ready to calculate!\n\n"
                f"{prefix}payouts to see current staff payouts. "
                f"{prefix}reset to mark active receipts paid or invalidate them."
            ),
        )
        self._main_status = LifecycleStatusState(
            kind=LifecycleStatusKind.ONLINE,
            description=f"Bot is fully online, ready to calculate!\n\ntype {prefix}calc or /mechcalc to start!",
        )
        await self._refresh_lifecycle_status_messages_now(force=True, respect_active_session=False)
        await self._refresh_channel_heartbeats_now()

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

        heartbeat = f"Status: Online! as of {datetime.now().astimezone().strftime('%H:%M:%S %m/%d/%y')}."
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
        elapsed = (datetime.now() - timestamp).total_seconds()
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

    def _is_allowed_messageable_channel(self, channel: discord.abc.MessageableChannel) -> bool:
        channel_id = _configured_parent_channel_id(channel)
        return self._is_allowed_channel_id(channel_id)

    def _is_scoped_channel_id(self, channel_id: int) -> bool:
        config = self.base_runtime.config.discord
        return (
            channel_id in config.allowed_channel_ids
            or channel_id in config.admin_channel_ids
            or channel_id == config.receipt_log_channel_id
        )

    def _is_scoped_messageable_channel(self, channel: discord.abc.MessageableChannel) -> bool:
        return self._is_scoped_channel_id(_configured_parent_channel_id(channel))

    def _is_admin_channel_id(self, channel_id: int) -> bool:
        return channel_id in self.base_runtime.config.discord.admin_channel_ids

    def _is_admin_messageable_channel(self, channel: discord.abc.MessageableChannel) -> bool:
        return self._is_admin_channel_id(_configured_parent_channel_id(channel))

    def _is_admin_user(self, user_id: int) -> bool:
        return user_id in self.base_runtime.config.discord.admin_user_ids

    async def _run_calc_prefix(self, message: discord.Message, remainder: str) -> None:
        if not self._is_allowed_messageable_channel(message.channel):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Access", "This command is not enabled in this channel."))]
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
        target_channel = await self._resolve_calculator_thread(message.channel, message.author)
        if target_channel.id != message.channel.id:
            referral = await message.channel.send(
                f"<@{message.author.id}> I opened your Bakunawa Mech calculator in {target_channel.mention}."
            )
            self._track_task(
                _safe_delete_message_later(
                    referral,
                    self.base_runtime.config.discord.transient_message_timeout_seconds,
                )
            )
        await self._start_calc_session(
            actor=message.author,
            channel=target_channel,
            target_user_id=target_user_id,
            prefilled_items=prefills,
            prefix_message=message,
        )

    async def _run_manage_prefix(self, message: discord.Message) -> None:
        if not self._is_admin_user(message.author.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Access", "You are not an admin for this bot."))]
            )
            return
        if not self._is_admin_channel_id(message.channel.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Access", "This admin command is not enabled in this channel."))]
                )
            return
        await self._send_manage_panel_message(message.author, message.channel)

    async def _run_adjustprices_prefix(self, message: discord.Message) -> None:
        if not self._is_admin_user(message.author.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Access", "You are not an admin for this bot."))]
            )
            return
        if not self._is_admin_channel_id(message.channel.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Access", "This admin command is not enabled in this channel."))]
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
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Access", "You are not an admin for this bot."))]
            )
            return
        if not self._is_admin_channel_id(message.channel.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Access", "This admin command is not enabled in this channel."))]
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
                            "Bakunawa Mech Reset",
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
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Access", "You are not an admin for this bot."))]
            )
            return
        if not self._is_admin_channel_id(message.channel.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Access", "This admin command is not enabled in this channel."))]
            )
            return
        try:
            file_name, status_override = _parse_import_request(remainder)
        except ValueError as error:
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Import", str(error)))]
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

    async def _run_rescan_prefix(self, message: discord.Message, remainder: str) -> None:
        if not self._is_admin_user(message.author.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Access", "You are not an admin for this bot."))]
            )
            return
        if not (self._is_allowed_channel_id(message.channel.id) or self._is_admin_channel_id(message.channel.id)):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Access", "This command is not enabled in this channel."))]
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
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Access", "You are not an admin for this bot."))]
            )
            return
        if not self._is_admin_messageable_channel(message.channel):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Access", "This admin command is not enabled in this channel."))]
            )
            return
        await self._rebuild_receipt_logs(message.author, message.channel, prefix_message=message)

    async def _run_fix_previews_prefix(self, message: discord.Message) -> None:
        if not self._is_admin_user(message.author.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Access", "You are not an admin for this bot."))]
            )
            return
        if not self._is_admin_messageable_channel(message.channel):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Access", "This admin command is not enabled in this channel."))]
            )
            return
        await self._repair_proof_previews(message.author, message.channel, prefix_message=message)

    async def _run_clean_prefix(self, message: discord.Message) -> None:
        if not self._is_admin_user(message.author.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Access", "You are not an admin for this bot."))]
            )
            return
        if not self._is_scoped_messageable_channel(message.channel):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Access", "This command is not enabled in this channel."))]
            )
            return
        progress = await message.channel.send(
            embeds=[embed_from_payload(task_status_embed("Bakunawa Mech Clean", "Cleaning non-log messages in this channel..."))]
        )
        result = await self._clean_non_log_messages(message.channel, preserve_message_ids={progress.id})
        await progress.edit(
            embeds=[
                embed_from_payload(
                    task_status_embed(
                        "Bakunawa Mech Clean",
                        _clean_result_description(result),
                    )
                )
            ]
        )
        self._track_task(
            _safe_delete_message_later(
                progress,
                self.base_runtime.config.discord.transient_message_timeout_seconds,
            )
        )

    async def _run_note_prefix(self, message: discord.Message, remainder: str) -> None:
        if not self._is_scoped_messageable_channel(message.channel):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Access", "This command is not enabled in this channel."))]
            )
            return
        try:
            receipt_id, note = _parse_note_command(remainder)
        except ValueError as error:
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Receipt Note", str(error)))]
            )
            return
        receipt = await self.base_runtime.database.get_receipt(receipt_id)
        if receipt is None:
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Receipt Note", f"Receipt `{receipt_id}` was not found."))]
            )
            return
        if str(message.author.id) != receipt.creator_user_id and not self._is_admin_user(message.author.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Receipt Note", "Only the receipt creator or an admin can edit that note."))]
            )
            return
        updated = await self._set_receipt_note(
            receipt_id,
            note,
            actor_user_id=str(message.author.id),
            actor_display_name=getattr(message.author, "display_name", message.author.name),
        )
        if not updated:
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Receipt Note", f"Receipt `{receipt_id}` was not found."))]
            )
            return
        await _safe_delete_message(message)
        reply = await message.channel.send(
            embeds=[
                embed_from_payload(
                    task_status_embed(
                        "Bakunawa Mech Receipt Note",
                        f"Updated note for receipt `{receipt_id}`.",
                    )
                )
            ]
        )
        self._track_task(
            _safe_delete_message_later(
                reply,
                self.base_runtime.config.discord.transient_message_timeout_seconds,
            )
        )

    async def _run_restart_prefix(self, message: discord.Message) -> None:
        if not self._is_admin_user(message.author.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Access", "You are not an admin for this bot."))]
            )
            return
        if not self._is_admin_channel_id(message.channel.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Access", "This admin command is not enabled in this channel."))]
            )
            return
        await self._request_shutdown(message.author, message.channel, restart=True, prefix_message=message)

    async def _run_stop_prefix(self, message: discord.Message) -> None:
        if not self._is_admin_user(message.author.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Access", "You are not an admin for this bot."))]
            )
            return
        if not self._is_admin_channel_id(message.channel.id):
            await message.channel.send(
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Access", "This admin command is not enabled in this channel."))]
            )
            return
        await self._request_shutdown(message.author, message.channel, restart=False, prefix_message=message)

    async def _resolve_calculator_thread(
        self,
        channel: discord.abc.MessageableChannel,
        actor: discord.abc.User,
    ) -> discord.abc.MessageableChannel:
        if isinstance(channel, discord.Thread):
            if _thread_matches_employee(channel, CALCULATOR_THREAD_PREFIX, actor.id):
                await _ensure_thread_open(channel)
                return channel
            parent = channel.parent
            if isinstance(parent, discord.TextChannel):
                return await self._get_or_create_employee_thread(
                    parent,
                    CALCULATOR_THREAD_PREFIX,
                    actor.id,
                    getattr(actor, "display_name", actor.name),
                )
            return channel
        if isinstance(channel, discord.TextChannel):
            return await self._get_or_create_employee_thread(
                channel,
                CALCULATOR_THREAD_PREFIX,
                actor.id,
                getattr(actor, "display_name", actor.name),
            )
        return channel

    async def _get_or_create_employee_thread(
        self,
        parent: discord.TextChannel,
        prefix: str,
        user_id: int | str,
        display_name: str,
    ) -> discord.Thread:
        user_id_text = str(user_id)
        for thread in parent.threads:
            if _thread_matches_employee(thread, prefix, user_id_text):
                await _ensure_thread_open(thread)
                return thread

        try:
            async for thread in parent.archived_threads(limit=None):
                if _thread_matches_employee(thread, prefix, user_id_text):
                    await _ensure_thread_open(thread)
                    return thread
        except (discord.Forbidden, discord.HTTPException):
            LOGGER.debug("channel_id=%s archived thread lookup failed for prefix=%s user_id=%s", parent.id, prefix, user_id_text)

        return await parent.create_thread(
            name=_employee_thread_name(prefix, display_name, user_id_text),
            type=discord.ChannelType.public_thread,
            auto_archive_duration=getattr(parent, "default_auto_archive_duration", 1440) or 1440,
            reason="Bakunawa Mech employee thread",
        )

    async def _resolve_receipt_log_thread(self, receipt) -> discord.abc.MessageableChannel | None:
        log_channel_id = self.base_runtime.config.discord.receipt_log_channel_id
        if log_channel_id is None:
            return None
        log_channel = self.get_channel(log_channel_id) or await self.fetch_channel(log_channel_id)
        if isinstance(log_channel, discord.TextChannel):
            return await self._get_or_create_employee_thread(
                log_channel,
                RECEIPT_LOG_THREAD_PREFIX,
                receipt.creator_user_id,
                receipt.creator_display_name or receipt.creator_username,
            )
        if isinstance(log_channel, discord.abc.Messageable):
            return log_channel
        return None

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
                    embed=embed_from_payload(task_error_embed("Bakunawa Mech Access", str(error))),
                    ephemeral=True,
                )
            elif prefix_message is not None:
                await prefix_message.channel.send(
                    embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Access", str(error)))]
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
                    display_name=item.display_name,
                    override_unit_cost=item.override_unit_cost,
                    package_key=item.package_key,
                    package_choices=dict(item.package_choices),
                    package_counts=dict(item.package_counts),
                )
                for item in prefilled_items
            ]
        await self.base_runtime.bot_state.replace_session(session)
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
            interaction=interaction if interaction is None or interaction.channel_id == channel.id else None,
        )
        await self._send_reply(dispatch, payload)
        if interaction is not None and interaction.channel_id != channel.id:
            await interaction.response.send_message(
                f"I opened your Bakunawa Mech calculator in {channel.mention}.",
                ephemeral=True,
            )
        if dispatch.reply_message is not None:
            await self.base_runtime.bot_state.set_panel_message(
                actor.id,
                dispatch.reply_message.channel.id,
                dispatch.reply_message.id,
            )
        if prefix_message is not None and not self._is_admin_messageable_channel(prefix_message.channel):
            await _safe_delete_message(prefix_message)

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
        session_receipt_id = f"BM-CALC-{user_id}-{utcnow().strftime('%Y%m%d-%H%M%S')}"
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
                    f"BM-CALC-{processing.user_id}-{utcnow().strftime('%Y%m%d-%H%M%S')}",
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
                                embeds=[embed_from_payload(task_status_embed("Bakunawa Mech Rescan", "Receipt saved. No more undocumented receipt candidates remain in this queue."))],
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
            ("send_messages_in_threads", "Send Messages in Threads"),
            ("create_public_threads", "Create Public Threads"),
            ("embed_links", "Embed Links"),
            ("attach_files", "Attach Files"),
            ("read_message_history", "Read Message History"),
            ("manage_messages", "Manage Messages"),
            ("manage_channels", "Manage Channels"),
            ("manage_threads", "Manage Threads"),
        ]
        admin_required = [
            ("view_channel", "View Channel"),
            ("send_messages", "Send Messages"),
            ("send_messages_in_threads", "Send Messages in Threads"),
            ("create_public_threads", "Create Public Threads"),
            ("embed_links", "Embed Links"),
            ("attach_files", "Attach Files"),
            ("read_message_history", "Read Message History"),
            ("manage_messages", "Manage Messages"),
            ("manage_channels", "Manage Channels"),
            ("manage_threads", "Manage Threads"),
        ]
        log_required = [
            ("view_channel", "View Channel"),
            ("send_messages", "Send Messages"),
            ("send_messages_in_threads", "Send Messages in Threads"),
            ("create_public_threads", "Create Public Threads"),
            ("embed_links", "Embed Links"),
            ("attach_files", "Attach Files"),
            ("read_message_history", "Read Message History"),
            ("manage_messages", "Manage Messages"),
            ("manage_threads", "Manage Threads"),
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

        if config.message_content_intent:
            checks.append(f"Direct `{config.prefix}` commands are enabled through Message Content Intent.")
        else:
            checks.append(f"Direct `{config.prefix}` commands are disabled; use slash commands in Discord.")

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
        if action == "package":
            await self._open_add_package_panel(interaction, owner_id)
            return
        if action == "package_pick":
            values = _interaction_values(interaction)
            if not values:
                await interaction.response.send_message("No package was selected.", ephemeral=True)
                return
            await self._handle_package_selection(interaction, owner_id, values[0])
            return
        if action == "package_choice" and len(parts) >= 5:
            values = _interaction_values(interaction)
            if not values:
                await interaction.response.send_message("No option was selected.", ephemeral=True)
                return
            package_key = parts[3]
            choices = _decode_package_choices(parts[4])
            counts = _decode_package_counts(parts[5]) if len(parts) >= 6 else {}
            group = _next_package_choice_group(self.base_runtime.packages, package_key, choices)
            if group is None:
                await interaction.response.send_message("Package selection is already complete.", ephemeral=True)
                return
            choices[group] = values[0]
            await self._continue_or_add_package(interaction, owner_id, package_key, choices, counts)
            return
        if action == "add":
            await self._open_add_item_panel(interaction, owner_id)
            return
        if action == "item_category":
            values = _interaction_values(interaction)
            if not values:
                await interaction.response.send_message("No item category was selected.", ephemeral=True)
                return
            await self._open_add_item_panel(interaction, owner_id, values[0])
            return
        if action == "pick":
            values = _interaction_values(interaction)
            if not values:
                await interaction.response.send_message("No item was selected.", ephemeral=True)
                return
            item_index = int(values[0])
            await interaction.response.send_modal(
                AddItemModal(self, owner_id, item_index, self.base_runtime.catalog.items[item_index].name)
            )
            return
        if action == "add_last" and len(parts) >= 4:
            item_index = int(parts[3])
            item = self.base_runtime.catalog.items[item_index]
            await interaction.response.send_modal(
                AddItemModal(self, owner_id, item_index, item.name)
            )
            return
        if action == "remove":
            await self._open_remove_item_panel(interaction, owner_id)
            return
        if action == "remove_pick":
            values = _interaction_values(interaction)
            if not values:
                await interaction.response.send_message("No receipt line was selected.", ephemeral=True)
                return
            selected_index = int(values[0])
            session = await self.base_runtime.bot_state.update_session(
                owner_id,
                lambda current: current.items.pop(selected_index) if selected_index < len(current.items) else None,
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
                    embeds=[embed_from_payload(task_status_embed("Bakunawa Mech Rescan", "No more undocumented receipt candidates remain in this queue."))],
                    view=None,
                )
                if interaction.message is not None:
                    self._track_task(_safe_delete_message_later(interaction.message, 10))
                return
            await self._edit_calc_panel(interaction, next_session)
            return
        if action in {"panel", "keepalive"}:
            session = await self.base_runtime.bot_state.touch_session(owner_id)
            if session is None:
                await interaction.response.send_message("Calculator session missing.", ephemeral=True)
                return
            await self._edit_calc_panel(interaction, session)
            return
        if action in {"close", "cancel"}:
            session = await self.base_runtime.bot_state.remove_session(owner_id)
            async with self._pending_lock:
                self._pending_rescans.pop(owner_id, None)
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
            session = await self.base_runtime.bot_state.current_session(owner_id)
            if session is None:
                await interaction.response.send_message("Calculator session missing.", ephemeral=True)
                return
            if not session.items:
                await interaction.response.send_message("Add at least one package or item before printing the receipt.", ephemeral=True)
                return
            if session.payment_proof_path and session.payment_proof_source_url:
                try:
                    processing = await self.base_runtime.bot_state.mark_proof_processing(owner_id)
                except ValueError:
                    return
                await interaction.response.edit_message(
                    content="",
                    embeds=[
                        embed_from_payload(
                            calc_processing_embed(
                                self.base_runtime.catalog.items,
                                processing,
                                "Receipt proof already attached. Finalizing receipt now...",
                            )
                        )
                    ],
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
                        owner_id,
                        awaiting_proof=False,
                    )
                    await interaction.edit_original_response(
                        content="",
                        embeds=[
                            embed_from_payload(
                                calc_failure_embed(
                                    self.base_runtime.catalog.items,
                                    session,
                                    f"Receipt finalization failed for <@{interaction.user.id}>: {error}",
                                )
                            )
                        ],
                        view=PayloadView(
                            self,
                            self.shared_runtime,
                            ReplyPayload(components=calc_action_rows(session.user_id, False, session.rescan_active)),
                        ),
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
                        embeds=[
                            embed_from_payload(
                                task_status_embed(
                                    "Bakunawa Mech Rescan",
                                    "Receipt saved. No more undocumented receipt candidates remain in this queue.",
                                )
                            )
                        ],
                        view=None,
                    )
                if original is not None:
                    self._track_task(_safe_delete_message_later(original, 10))
                return
            try:
                waiting = await self.base_runtime.bot_state.mark_waiting_for_proof(owner_id)
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
        if action == "note" and len(parts) >= 4:
            creator_user_id = parts[2]
            receipt_id = parts[3]
            if str(interaction.user.id) != creator_user_id and not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message(
                    "Only the receipt creator or an admin can edit that note.",
                    ephemeral=True,
                )
                return
            receipt = await self.base_runtime.database.get_receipt(receipt_id)
            if receipt is None:
                await interaction.response.send_message(f"Receipt `{receipt_id}` was not found.", ephemeral=True)
                return
            await interaction.response.send_modal(ReceiptNoteModal(self, receipt.id, receipt.admin_note))
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
        if action == "log_note" and len(parts) >= 3:
            if not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message("You are not an admin for this bot.", ephemeral=True)
                return
            receipt_id = parts[2]
            receipt = await self.base_runtime.database.get_receipt(receipt_id)
            if receipt is None:
                await interaction.response.send_message(f"Receipt `{receipt_id}` was not found.", ephemeral=True)
                return
            await interaction.response.send_modal(ReceiptNoteModal(self, receipt.id, receipt.admin_note))
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
        if action == "detail_note" and len(parts) >= 4:
            owner_id = int(parts[2])
            if owner_id != interaction.user.id:
                await interaction.response.send_message("This receipt panel belongs to another user.", ephemeral=True)
                return
            if not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message("You are not an admin for this bot.", ephemeral=True)
                return
            receipt_id = parts[3]
            receipt = await self.base_runtime.database.get_receipt(receipt_id)
            if receipt is None:
                await interaction.response.send_message(f"Receipt `{receipt_id}` was not found.", ephemeral=True)
                return
            await interaction.response.send_modal(ReceiptNoteModal(self, receipt.id, receipt.admin_note))
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
                    embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Reset", "This reset prompt expired. Run `bm!reset` again."))],
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
                            "Bakunawa Mech Reset",
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
                            "Run `bm!help` or `/mechhelp` again to reopen the help panel.",
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
        if prefix_message is not None and not self._is_admin_messageable_channel(prefix_message.channel):
            await _safe_delete_message(prefix_message)

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
            embeds=[task_warning_embed("Bakunawa Mech Reset", _reset_warning_message(mode, user_ids))],
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
        if prefix_message is not None and not self._is_admin_messageable_channel(prefix_message.channel):
            await _safe_delete_message(prefix_message)

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
                    "Bakunawa Mech Import",
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
        if prefix_message is not None and not self._is_admin_channel_id(prefix_message.channel.id):
            await _safe_delete_message(prefix_message)

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
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Import", str(error)))]
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
                    "Bakunawa Mech Import",
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
        if prefix_message is not None and not self._is_admin_channel_id(prefix_message.channel.id):
            await _safe_delete_message(prefix_message)

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
                            "Bakunawa Mech Import",
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
            embeds=[task_warning_embed("Bakunawa Mech Import", _import_confirmation_message(status_override, preview))],
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
                await interaction.response.send_message(embed=embed_from_payload(task_error_embed("Bakunawa Mech Rescan", error_text)), ephemeral=True)
            else:
                await invocation_channel.send(embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Rescan", error_text))])
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
                await interaction.response.send_message(embed=embed_from_payload(task_error_embed("Bakunawa Mech Rescan", str(error))), ephemeral=True)
            else:
                await invocation_channel.send(embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Rescan", str(error)))])
            return
        progress_payload = ReplyPayload(
            embeds=[
                task_status_embed(
                    "Bakunawa Mech Rescan",
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
        if prefix_message is not None and not self._is_admin_channel_id(prefix_message.channel.id):
            await _safe_delete_message(prefix_message)

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
                    f"BM-RESCAN-{candidate_message.id}",
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
                                "Bakunawa Mech Rescan",
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
                            "Bakunawa Mech Rescan",
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
                    "Bakunawa Mech Rebuild Logs",
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
        if prefix_message is not None and not self._is_admin_channel_id(prefix_message.channel.id):
            await _safe_delete_message(prefix_message)

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
                await _maintenance_write_pause()
            log_threads = await self._collect_employee_threads(log_channel, RECEIPT_LOG_THREAD_PREFIX)
            for thread in log_threads:
                await _ensure_thread_open(thread)
                async for existing in thread.history(limit=None):
                    inspected_messages += 1
                    if existing.author.id != bot_user.id:
                        continue
                    try:
                        await existing.delete()
                        deleted_messages += 1
                    except discord.HTTPException:
                        delete_failures += 1
                    await _maintenance_write_pause()
        if dispatch.reply_message is not None:
            await dispatch.reply_message.edit(
                embeds=[
                    embed_from_payload(
                        task_status_embed(
                            "Bakunawa Mech Rebuild Logs",
                            (
                                f"Rebuilding receipt log channel <#{log_channel_id}> into employee threads from the database. "
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
                target_log_channel = log_channel
                if isinstance(log_channel, discord.TextChannel):
                    target_log_channel = await self._get_or_create_employee_thread(
                        log_channel,
                        RECEIPT_LOG_THREAD_PREFIX,
                        receipt.creator_user_id,
                        receipt.creator_display_name or receipt.creator_username,
                    )
                await target_log_channel.send(**spec.send_kwargs())
                reposted_receipts += 1
            except discord.HTTPException:
                repost_failures += 1
            await _maintenance_write_pause()
        if dispatch.reply_message is not None:
            await dispatch.reply_message.edit(
                embeds=[
                    embed_from_payload(
                        task_status_embed(
                            "Bakunawa Mech Rebuild Logs",
                            (
                                f"Receipt log rebuild finished for <#{log_channel_id}>.\n"
                                f"Receipts replayed: {reposted_receipts}\n"
                                "Log destination: employee threads\n"
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

    async def _repair_proof_previews(
        self,
        actor: discord.abc.User,
        channel: discord.abc.MessageableChannel,
        interaction: discord.Interaction[Any] | None = None,
        prefix_message: discord.Message | None = None,
    ) -> None:
        progress_payload = ReplyPayload(
            embeds=[
                task_status_embed(
                    "Bakunawa Mech Fix Previews",
                    "Scanning saved receipts and scoped employee threads for proof preview cards...",
                )
            ],
            ephemeral=interaction is not None,
        )
        dispatch = DiscordDispatchContext(
            actor=_actor_from_user(actor),
            channel=_channel_from_id(self.base_runtime, _configured_parent_channel_id(channel)),
            channel_object=channel,
            is_interaction=interaction is not None,
            interaction=interaction,
            invocation_message=prefix_message,
        )
        await self._send_reply(dispatch, progress_payload)
        if prefix_message is not None and not self._is_admin_channel_id(_configured_parent_channel_id(prefix_message.channel)):
            await _safe_delete_message(prefix_message)

        receipts = await self.base_runtime.database.list_all_receipts()
        result = ProofPreviewRepairResult(receipts_checked=len(receipts))
        repairable_receipts = {}
        for receipt in receipts:
            if not split_proof_values(receipt.payment_proof_path) and not split_proof_values(receipt.payment_proof_source_url):
                continue
            result.receipts_with_proofs += 1
            if _has_existing_local_proof_path(receipt.payment_proof_path):
                repairable_receipts[receipt.id] = receipt
                result.receipts_with_local_proofs += 1
            else:
                result.receipts_missing_local_proofs += 1

        channels, channel_failures = await self._collect_proof_preview_repair_channels(receipts, channel)
        result.channel_failures += channel_failures
        result.channels_scanned = len(channels)
        if dispatch.reply_message is not None:
            await dispatch.reply_message.edit(
                embeds=[
                    embed_from_payload(
                        task_status_embed(
                            "Bakunawa Mech Fix Previews",
                            (
                                f"Scanning {result.channels_scanned} channel/thread(s).\n"
                                f"Receipts with local saved proofs: {result.receipts_with_local_proofs}\n"
                                "Repairing old CDN-link proof previews at a Discord-safe pace..."
                            ),
                        )
                    )
                ],
                view=None,
            )

        display_cache: dict[str, ReceiptDisplayContext | None] = {}
        bot_user_id = self.user.id if self.user is not None else None
        for target_channel in channels:
            try:
                async for message in target_channel.history(limit=None):
                    result.messages_inspected += 1
                    receipt_id = _receipt_id_from_message(message)
                    if receipt_id is None or receipt_id not in repairable_receipts:
                        continue
                    if not _is_matching_receipt_message(message, receipt_id, bot_user_id):
                        continue
                    if _receipt_message_has_attachment_preview(message):
                        continue

                    receipt = repairable_receipts[receipt_id]
                    if receipt_id not in display_cache:
                        display_cache[receipt_id] = await self._load_receipt_display_context(
                            receipt.id,
                            receipt.creator_user_id,
                        )
                    display = display_cache[receipt_id]
                    payload = (
                        receipt_log_payload(receipt, display)
                        if self._is_receipt_log_messageable_channel(target_channel)
                        else receipt_main_payload(receipt, display)
                    )
                    spec = reply_payload_to_spec(self, self.shared_runtime, payload)
                    try:
                        await message.edit(**spec.edit_kwargs())
                        result.messages_refreshed += 1
                    except (discord.HTTPException, discord.NotFound):
                        result.refresh_failures += 1
                    await _maintenance_write_pause()
            except (discord.Forbidden, discord.HTTPException):
                result.channel_failures += 1

        if dispatch.reply_message is not None:
            await dispatch.reply_message.edit(
                embeds=[
                    embed_from_payload(
                        task_status_embed(
                            "Bakunawa Mech Fix Previews",
                            _proof_preview_repair_result_description(result),
                        )
                    )
                ],
                view=None,
            )
        LOGGER.info(
            "receipts_checked=%s receipts_with_proofs=%s local_proof_receipts=%s channels_scanned=%s messages_inspected=%s messages_refreshed=%s refresh_failures=%s channel_failures=%s proof previews repaired",
            result.receipts_checked,
            result.receipts_with_proofs,
            result.receipts_with_local_proofs,
            result.channels_scanned,
            result.messages_inspected,
            result.messages_refreshed,
            result.refresh_failures,
            result.channel_failures,
        )

    async def _collect_proof_preview_repair_channels(
        self,
        receipts,
        invocation_channel: discord.abc.MessageableChannel,
    ) -> tuple[list[discord.TextChannel | discord.Thread], int]:
        channels: dict[int, discord.TextChannel | discord.Thread] = {}
        failures = 0

        async def add_channel_id(channel_id: int) -> None:
            nonlocal failures
            channel = self.get_channel(channel_id)
            if channel is None:
                try:
                    channel = await self.fetch_channel(channel_id)
                except (discord.Forbidden, discord.HTTPException):
                    failures += 1
                    return
            if isinstance(channel, (discord.TextChannel, discord.Thread)):
                channels[channel.id] = channel

        if isinstance(invocation_channel, (discord.TextChannel, discord.Thread)):
            channels[invocation_channel.id] = invocation_channel

        for configured_id in [
            *self.base_runtime.config.discord.allowed_channel_ids,
            *self.base_runtime.config.discord.admin_channel_ids,
        ]:
            if configured_id > 0:
                await add_channel_id(configured_id)

        log_channel_id = self.base_runtime.config.discord.receipt_log_channel_id
        if log_channel_id is not None and log_channel_id > 0:
            await add_channel_id(log_channel_id)

        for receipt in receipts:
            try:
                await add_channel_id(int(receipt.channel_id))
            except (TypeError, ValueError):
                failures += 1

        parent_channels = [channel for channel in channels.values() if isinstance(channel, discord.TextChannel)]
        for parent in parent_channels:
            prefixes = []
            if parent.id in self.base_runtime.config.discord.allowed_channel_ids:
                prefixes.append(CALCULATOR_THREAD_PREFIX)
            if parent.id in self.base_runtime.config.discord.admin_channel_ids:
                prefixes.append(CALCULATOR_THREAD_PREFIX)
            if parent.id == log_channel_id:
                prefixes.append(RECEIPT_LOG_THREAD_PREFIX)
            for prefix in prefixes:
                for thread in await self._collect_employee_threads(parent, prefix):
                    channels[thread.id] = thread

        return list(channels.values()), failures

    def _is_receipt_log_messageable_channel(self, channel: discord.abc.MessageableChannel) -> bool:
        log_channel_id = self.base_runtime.config.discord.receipt_log_channel_id
        if log_channel_id is None:
            return False
        return _configured_parent_channel_id(channel) == log_channel_id

    async def _collect_employee_threads(
        self,
        parent: discord.TextChannel,
        prefix: str,
    ) -> list[discord.Thread]:
        threads: list[discord.Thread] = [
            thread for thread in parent.threads if thread.name.startswith(prefix)
        ]
        seen_ids = {thread.id for thread in threads}
        try:
            async for thread in parent.archived_threads(limit=None):
                if thread.id in seen_ids or not thread.name.startswith(prefix):
                    continue
                threads.append(thread)
                seen_ids.add(thread.id)
        except (discord.Forbidden, discord.HTTPException):
            LOGGER.debug("channel_id=%s archived employee thread collection failed", parent.id)
        return threads

    async def _clean_non_log_messages(
        self,
        channel: discord.abc.MessageableChannel,
        *,
        preserve_message_ids: set[int] | None = None,
    ) -> CleanResult:
        result = CleanResult()
        bot_user_id = self.user.id if self.user is not None else None
        preserve_message_ids = preserve_message_ids or set()
        if not isinstance(channel, (discord.TextChannel, discord.Thread)):
            return result

        async for existing in channel.history(limit=None):
            result.inspected += 1
            if existing.id in preserve_message_ids or existing.pinned:
                result.preserved += 1
                continue
            if _is_log_related_message(existing, bot_user_id):
                result.preserved += 1
                continue
            try:
                await existing.delete()
                result.deleted += 1
            except (discord.Forbidden, discord.HTTPException, discord.NotFound):
                result.failed += 1
        return result

    async def _set_receipt_note(
        self,
        receipt_id: str,
        note: str | None,
        *,
        actor_user_id: str,
        actor_display_name: str,
    ) -> bool:
        normalized_note = _normalize_receipt_note(note)
        actor = AuditEventInput(
            actor_user_id=actor_user_id,
            actor_display_name=actor_display_name,
            action="receipt_note_updated",
            target_receipt_id=receipt_id,
            detail_json={
                "has_note": normalized_note is not None,
                "note": normalized_note,
            },
        )
        updated = await self.base_runtime.database.update_receipt_note(
            receipt_id,
            normalized_note,
            actor,
        )
        if not updated:
            return False
        receipt = await self.base_runtime.database.get_receipt(receipt_id)
        if receipt is not None:
            await self._refresh_posted_receipt_messages(receipt)
        return True

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
        title = "Bakunawa Mech Restart" if restart else "Bakunawa Mech Stop"
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
            if prefix_message is not None and not self._is_admin_channel_id(prefix_message.channel.id):
                await _safe_delete_message(prefix_message)
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

    async def _open_add_item_panel(
        self,
        interaction: discord.Interaction[Any],
        owner_user_id: int,
        category_key: str | None = None,
    ) -> None:
        session = await self.base_runtime.bot_state.touch_session(owner_user_id)
        if session is None:
            await interaction.response.send_message("Calculator session missing.", ephemeral=True)
            return
        embeds = [embed_from_payload(embed) for embed in calc_embeds(self.base_runtime.catalog.items, session)]
        if category_key is None:
            embeds.append(
                discord.Embed(
                    title="Add Individual Items: Select Category",
                    description="Pick a category first, then choose the exact item.",
                    color=0xC99700,
                )
            )
            view = self._build_add_item_category_view(owner_user_id)
        else:
            if not _individual_items_for_category(self.base_runtime.catalog.items, category_key):
                await interaction.response.send_message("That item category is no longer available.", ephemeral=True)
                return
            category_label = _catalog_category_label(category_key)
            embeds.append(
                discord.Embed(
                    title=f"Add Individual Items: {category_label}",
                    description="Choose one item from this category.",
                    color=0xC99700,
                )
            )
            view = self._build_add_item_view(owner_user_id, session, category_key)
        await interaction.response.edit_message(content="", embeds=embeds, view=view)

    async def _open_add_package_panel(self, interaction: discord.Interaction[Any], owner_user_id: int) -> None:
        session = await self.base_runtime.bot_state.touch_session(owner_user_id)
        if session is None:
            await interaction.response.send_message("Calculator session missing.", ephemeral=True)
            return
        embeds = [embed_from_payload(embed) for embed in calc_embeds(self.base_runtime.catalog.items, session)]
        embeds[0].add_field(name="Next Step", value="Choose a package to add.", inline=False)
        view = self._build_package_view(owner_user_id)
        await interaction.response.edit_message(content="", embeds=embeds, view=view)

    async def _handle_package_selection(
        self,
        interaction: discord.Interaction[Any],
        owner_user_id: int,
        package_key: str,
    ) -> None:
        choices: dict[str, str] = {}
        counts: dict[str, int] = {}
        session = await self.base_runtime.bot_state.current_session(owner_user_id)
        if package_key == "full_upgrades":
            if session is not None:
                choices.update(_inherited_full_tuning_choices(session))
        await self._continue_or_add_package(interaction, owner_user_id, package_key, choices, counts)

    async def _continue_or_add_package(
        self,
        interaction: discord.Interaction[Any],
        owner_user_id: int,
        package_key: str,
        choices: dict[str, str],
        counts: dict[str, int],
    ) -> None:
        next_group = _next_package_choice_group(self.base_runtime.packages, package_key, choices)
        if next_group is not None:
            await self._open_package_choice_panel(
                interaction,
                owner_user_id,
                package_key,
                choices,
                counts,
                next_group,
            )
            return

        await interaction.response.send_modal(
            PackageSpecialPriceModal(self, owner_user_id, package_key, choices, counts)
        )

    async def _add_package_with_optional_override(
        self,
        interaction: discord.Interaction[Any],
        owner_user_id: int,
        package_key: str,
        choices: dict[str, str],
        counts: dict[str, int],
        override_unit_price: int | None,
    ) -> None:
        try:
            expansion = expand_package(
                self.base_runtime.packages,
                self.base_runtime.catalog,
                PackageSelection(
                    package_key=package_key,
                    choices=choices,
                    counts=counts,
                    override_unit_price=override_unit_price,
                ),
            )
        except ValueError as error:
            await interaction.response.send_message(str(error), ephemeral=True)
            return
        session = await self.base_runtime.bot_state.update_session(
            owner_user_id,
            lambda current: _apply_package_expansion(current, expansion),
        )
        if session is None:
            await interaction.response.send_message("Calculator session missing.", ephemeral=True)
            return
        notice = f"Added {expansion.display_name}."
        if override_unit_price is not None:
            notice += f" Special price applied: ${override_unit_price:,}."
        if expansion.price_pending:
            notice += " Pricing is pending, so this line is currently a placeholder."
        payload = calc_reply_payload(self.base_runtime.catalog.items, session)
        kwargs = reply_payload_to_spec(self, self.shared_runtime, payload).edit_kwargs()
        kwargs["content"] = notice
        await interaction.response.edit_message(**kwargs)

    async def _open_package_choice_panel(
        self,
        interaction: discord.Interaction[Any],
        owner_user_id: int,
        package_key: str,
        choices: dict[str, str],
        counts: dict[str, int],
        group: str,
    ) -> None:
        session = await self.base_runtime.bot_state.touch_session(owner_user_id)
        if session is None:
            await interaction.response.send_message("Calculator session missing.", ephemeral=True)
            return
        definition = self.base_runtime.packages.find_package(package_key)
        if definition is None:
            await interaction.response.send_message("Package is no longer configured.", ephemeral=True)
            return
        embeds = [embed_from_payload(embed) for embed in calc_embeds(self.base_runtime.catalog.items, session)]
        group_label = _choice_group_label(group)
        embeds.append(
            discord.Embed(
                title=f"{definition.label}: Select {group_label}",
                description=(
                    f"Choose the {group_label.lower()} for this package. "
                    "The order will not continue until this selection is made."
                ),
                color=0xC99700,
            )
        )
        view = self._build_package_choice_view(owner_user_id, package_key, choices, counts, group)
        await interaction.response.edit_message(content="", embeds=embeds, view=view)

    async def _open_remove_item_panel(self, interaction: discord.Interaction[Any], owner_user_id: int) -> None:
        session = await self.base_runtime.bot_state.touch_session(owner_user_id)
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
        view = self._build_remove_item_view(owner_user_id, session)
        await interaction.response.edit_message(content="", embeds=embeds, view=view)

    async def _open_contract_panel(self, interaction: discord.Interaction[Any]) -> None:
        session = await self.base_runtime.bot_state.touch_session(interaction.user.id)
        if session is None:
            await interaction.response.send_message("Calculator session missing.", ephemeral=True)
            return
        embed = discord.Embed(
            title="Bakunawa Mech Contract Picker",
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

    def _build_add_item_category_view(self, owner_user_id: int) -> discord.ui.View:
        timeout = self.base_runtime.config.discord.transient_message_timeout_seconds
        view = DispatchView(self, timeout)
        options = [
            discord.SelectOption(
                label=category.label,
                value=category.key,
                description=f"{category.count} available item(s)",
            )
            for category in _individual_item_categories(self.base_runtime.catalog.items)
        ]
        view.add_item(
            DispatchSelect(
                client=self,
                custom_id=f"calc|item_category|{owner_user_id}",
                placeholder="Select item category",
                options=options[:25],
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

    def _build_add_item_view(
        self,
        owner_user_id: int,
        session: CalculatorSession,
        category_key: str,
    ) -> discord.ui.View:
        timeout = self.base_runtime.config.discord.transient_message_timeout_seconds
        view = DispatchView(self, timeout)
        item_entries = _individual_items_for_category(self.base_runtime.catalog.items, category_key)
        options = [
            discord.SelectOption(
                label=_display_item_name(item.name),
                value=str(index),
                description=_catalog_item_option_description(item),
            )
            for index, item in item_entries[:25]
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
                if item.active and _catalog_category_key(item) == category_key:
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

    def _build_package_view(self, owner_user_id: int) -> discord.ui.View:
        timeout = self.base_runtime.config.discord.transient_message_timeout_seconds
        view = DispatchView(self, timeout)
        options = [
            discord.SelectOption(
                label=definition.label,
                value=definition.key,
                description=_package_option_description(definition.key),
            )
            for definition in self.base_runtime.packages.package_options()
        ]
        view.add_item(
            DispatchSelect(
                client=self,
                custom_id=f"calc|package_pick|{owner_user_id}",
                placeholder="Select a package",
                options=options,
                row=0,
            )
        )
        view.add_item(
            DispatchButton(
                client=self,
                label="Back",
                style=discord.ButtonStyle.secondary,
                custom_id=f"calc|add|{owner_user_id}",
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

    def _build_package_choice_view(
        self,
        owner_user_id: int,
        package_key: str,
        choices: dict[str, str],
        counts: dict[str, int],
        group: str,
    ) -> discord.ui.View:
        timeout = self.base_runtime.config.discord.transient_message_timeout_seconds
        view = DispatchView(self, timeout)
        choice_map = self.base_runtime.packages.choices[group]
        options = [
            discord.SelectOption(label=label, value=value)
            for value, label in choice_map.items()
        ]
        view.add_item(
            DispatchSelect(
                client=self,
                custom_id=(
                    f"calc|package_choice|{owner_user_id}|{package_key}|"
                    f"{_encode_package_choices(choices)}|{_encode_package_counts(counts)}"
                ),
                placeholder=f"Select {_choice_group_label(group)}",
                options=options,
                row=0,
            )
        )
        view.add_item(
            DispatchButton(
                client=self,
                label="Back",
                style=discord.ButtonStyle.secondary,
                custom_id=f"calc|package|{owner_user_id}",
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
                label=item.item_name[:100],
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
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Import", "This import prompt expired. Run `!import` again."))],
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
                        "Bakunawa Mech Import",
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
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Import", f"Import failed for `{review.attachment_filename}`: {error}"))],
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
                        "Bakunawa Mech Import",
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
                embeds=[embed_from_payload(task_error_embed("Bakunawa Mech Reset", "This reset prompt expired. Run `bm!reset` again."))],
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
                if (
                    event.target_receipt_id is not None
                    and event.target_receipt_id in affected_receipt_ids
                )
                or (
                    action is ResetAction.MARK_PAID
                    and event.action == "payout_adjustment_snapshot"
                    and _reset_adjustment_snapshot_matches_scope(event, scope.mode, scope.user_ids)
                )
            ],
        )
        summary = render_stats_description(_leaderboard_from_receipts(affected_receipts))
        receipt_ids = [receipt.id for receipt in affected_receipts]
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
        settled_adjustments = 0
        if action is ResetAction.MARK_PAID:
            open_adjustment_user_ids = await self.base_runtime.database.list_open_payout_adjustment_user_ids()
            adjustment_user_ids = _reset_adjustment_target_user_ids(
                scope.mode,
                scope.user_ids,
                open_adjustment_user_ids,
            )
            settled_adjustments = await self.base_runtime.database.settle_payout_adjustments(
                adjustment_user_ids if adjustment_user_ids is not None else None,
                str(interaction.user.id),
                getattr(interaction.user, "display_name", interaction.user.name),
                "reset_mark_paid",
            )
        await self._clear_reset_sessions(scope.mode, scope.user_ids)
        completion = (
            f"{_reset_scope_action(scope.mode, scope.user_ids, action)}.\n"
            f"{updated} active receipt(s) updated.\n"
            f"{settled_adjustments} payout adjustment(s) settled.\n"
            f"Backup saved to `{path}`\n"
            f"Previous summary:\n{summary}"
        )
        await interaction.edit_original_response(content=completion, embeds=[], view=None)
        main_channel_id = next(iter(self.base_runtime.config.discord.allowed_channel_ids), interaction.channel_id)
        main_channel = self.get_channel(main_channel_id)
        notice = (
            f"{_reset_scope_action(scope.mode, scope.user_ids, action)} by <@{interaction.user.id}>.\n"
            f"Payout adjustments settled: {settled_adjustments}\n"
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
        if not isinstance(main_channel, (discord.TextChannel, discord.Thread)):
            try:
                fetched_main = await self.fetch_channel(main_channel_id)
            except discord.HTTPException:
                fetched_main = None
            if isinstance(fetched_main, (discord.TextChannel, discord.Thread)):
                main_channel = fetched_main
        if isinstance(main_channel, (discord.TextChannel, discord.Thread)):
            main_payload = receipt_main_payload(receipt, display)
            await self._refresh_matching_receipt_messages_in_channel(
                main_channel,
                receipt.id,
                main_payload,
            )

        log_channel_id = self.base_runtime.config.discord.receipt_log_channel_id
        if log_channel_id is None or log_channel_id == main_channel_id:
            return
        log_channel = await self._resolve_receipt_log_thread(receipt)
        if log_channel is not None:
            log_payload = receipt_log_payload(receipt, display)
            await self._refresh_matching_receipt_messages_in_channel(
                log_channel,
                receipt.id,
                log_payload,
            )
            return
        log_parent_channel = self.get_channel(log_channel_id)
        if not isinstance(log_parent_channel, discord.TextChannel):
            try:
                fetched_log = await self.fetch_channel(log_channel_id)
            except discord.HTTPException:
                fetched_log = None
            if isinstance(fetched_log, discord.TextChannel):
                log_parent_channel = fetched_log
        if isinstance(log_parent_channel, discord.TextChannel):
            log_payload = receipt_log_payload(receipt, display)
            await self._refresh_matching_receipt_messages_in_channel(
                log_parent_channel,
                receipt.id,
                log_payload,
            )

    async def _refresh_matching_receipt_messages_in_channel(
        self,
        channel: discord.TextChannel | discord.Thread,
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
        accounting_policy = AccountingPolicy.PROCUREMENT_FUNDS
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
                "company_cost": receipt.procurement_cost,
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
        log_channel = await self._resolve_receipt_log_thread(receipt)
        if log_channel is None or log_channel.id == main_channel_id:
            return
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
            name="mechcalc",
            description="Open the calculator panel.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        @app_commands.describe(
            user="Optional user to credit the receipt to",
            items="Optional comma-separated items",
        )
        async def mechcalc(
            interaction: discord.Interaction[Any],
            user: discord.Member | None = None,
            items: str | None = None,
        ) -> None:
            channel = interaction.channel
            if channel is None:
                await interaction.response.send_message("This interaction is missing a channel context.", ephemeral=True)
                return
            if not self._is_allowed_messageable_channel(channel):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("Bakunawa Mech Access", "This command is not enabled in this channel.")),
                    ephemeral=True,
                )
                return
            prefilled_items = parse_calc_prefix_input(self.base_runtime.catalog, items or "")[1]
            target_channel = await self._resolve_calculator_thread(channel, interaction.user)
            await self._start_calc_session(
                actor=interaction.user,
                channel=target_channel,
                target_user_id=user.id if user is not None else None,
                prefilled_items=prefilled_items,
                interaction=interaction,
            )

        @self.tree.command(
            name="mechmanage",
            description="Open the receipt manager.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        async def mechmanage(interaction: discord.Interaction[Any]) -> None:
            channel = interaction.channel
            if channel is None:
                await interaction.response.send_message("This interaction is missing a channel context.", ephemeral=True)
                return
            if not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("Bakunawa Mech Access", "You are not an admin for this bot.")),
                    ephemeral=True,
                )
                return
            if not self._is_admin_messageable_channel(channel):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("Bakunawa Mech Access", "This admin command is not enabled in this channel.")),
                    ephemeral=True,
                )
                return
            await self._send_manage_panel_message(interaction.user, channel, interaction)

        @self.tree.command(name="mechhelp", description="Open the paged help panel.", **command_kwargs)
        @app_commands.guild_only()
        async def mechhelp(interaction: discord.Interaction[Any]) -> None:
            await self._run_slash_command(interaction, _build_slash_input("mechhelp"))

        @self.tree.command(name="mechealth", description="Check bot setup and permissions.", **command_kwargs)
        @app_commands.guild_only()
        async def mechealth(interaction: discord.Interaction[Any]) -> None:
            await self._run_slash_command(interaction, _build_slash_input("mechealth"))

        @self.tree.command(name="mechstats", description="Show the leaderboard.", **command_kwargs)
        @app_commands.guild_only()
        @app_commands.describe(sort="Optional sort: sales, company cost, or count.")
        @app_commands.choices(
            sort=[
                app_commands.Choice(name="Sales", value="sales"),
                app_commands.Choice(name="Company Cost", value="procurement"),
                app_commands.Choice(name="Receipt Count", value="count"),
            ]
        )
        async def mechstats(
            interaction: discord.Interaction[Any],
            sort: app_commands.Choice[str] | None = None,
        ) -> None:
            await self._run_slash_command(
                interaction,
                _build_slash_input("mechstats", sort.value if sort is not None else None),
            )

        @self.tree.command(
            name="mechpricesheet",
            description="Show the current catalog price sheet.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        async def mechpricesheet(interaction: discord.Interaction[Any]) -> None:
            await self._run_slash_command(interaction, _build_slash_input("mechpricesheet"))

        @self.tree.command(
            name="mechpayouts",
            description="Show current employee payout totals.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        @app_commands.describe(user="Optional user to filter the payout list.")
        async def mechpayouts(
            interaction: discord.Interaction[Any],
            user: discord.Member | None = None,
        ) -> None:
            await self._run_slash_command(
                interaction,
                _build_slash_input("mechpayouts", _member_token(user)),
            )

        @self.tree.command(
            name="mechpayoutoffset",
            description="Add a payout-time credit or deduction for one user.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        @app_commands.describe(
            user="User receiving the payout adjustment.",
            amount="Use a positive value to add or a negative value to deduct.",
            reason="Optional note explaining the adjustment.",
        )
        async def mechpayoutoffset(
            interaction: discord.Interaction[Any],
            user: discord.Member,
            amount: str,
            reason: str | None = None,
        ) -> None:
            await self._run_slash_command(
                interaction,
                _build_slash_input("mechpayoutoffset", _member_token(user), amount, reason),
            )

        @self.tree.command(
            name="mechpayoutsplit",
            description="Split one user's current payout across other staff.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        @app_commands.describe(
            source="User whose current payout should be split.",
            recipients="Use `everyone` or mention users separated by spaces.",
            reason="Optional note explaining the split.",
        )
        async def mechpayoutsplit(
            interaction: discord.Interaction[Any],
            source: discord.Member,
            recipients: str,
            reason: str | None = None,
        ) -> None:
            await self._run_slash_command(
                interaction,
                _build_slash_input("mechpayoutsplit", _member_token(source), recipients, reason),
            )

        @self.tree.command(
            name="mechrefresh",
            description="Reload catalog and contract files from disk.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        async def mechrefresh(interaction: discord.Interaction[Any]) -> None:
            await self._run_slash_command(interaction, _build_slash_input("mechrefresh"))

        @self.tree.command(
            name="mechadjustprices",
            description="Edit the live catalog and refresh it.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        async def mechadjustprices(interaction: discord.Interaction[Any]) -> None:
            channel = interaction.channel
            if channel is None:
                await interaction.response.send_message("This interaction is missing a channel context.", ephemeral=True)
                return
            if not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("Bakunawa Mech Access", "You are not an admin for this bot.")),
                    ephemeral=True,
                )
                return
            if not self._is_admin_messageable_channel(channel):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("Bakunawa Mech Access", "This admin command is not enabled in this channel.")),
                    ephemeral=True,
                )
                return
            await self._send_adjustprices_panel(interaction.user, channel, interaction)

        @self.tree.command(
            name="mechtemplates",
            description="Show or reload the live templates file.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        @app_commands.describe(action="Use reload to refresh the templates JSON from disk.")
        @app_commands.choices(action=[app_commands.Choice(name="Reload", value="reload")])
        async def mechtemplates(
            interaction: discord.Interaction[Any],
            action: app_commands.Choice[str] | None = None,
        ) -> None:
            await self._run_slash_command(
                interaction,
                _build_slash_input("mechtemplates", action.value if action is not None else None),
            )

        @self.tree.command(
            name="mechreset",
            description="Backup active receipts, then mark them paid or invalidate them.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        async def mechreset(interaction: discord.Interaction[Any]) -> None:
            channel = interaction.channel
            if channel is None:
                await interaction.response.send_message("This interaction is missing a channel context.", ephemeral=True)
                return
            if not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("Bakunawa Mech Access", "You are not an admin for this bot.")),
                    ephemeral=True,
                )
                return
            if not self._is_admin_channel_id(channel.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("Bakunawa Mech Access", "This admin command is not enabled in this channel.")),
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
            name="mechimport",
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
        async def mechimport(
            interaction: discord.Interaction[Any],
            mode: app_commands.Choice[str] | None = None,
        ) -> None:
            channel = interaction.channel
            if channel is None:
                await interaction.response.send_message("This interaction is missing a channel context.", ephemeral=True)
                return
            if not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("Bakunawa Mech Access", "You are not an admin for this bot.")),
                    ephemeral=True,
                )
                return
            if not self._is_admin_channel_id(channel.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("Bakunawa Mech Access", "This admin command is not enabled in this channel.")),
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
            name="mechrebuildlogs",
            description="Rebuild the receipt log channel from the database.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        async def mechrebuildlogs(interaction: discord.Interaction[Any]) -> None:
            channel = interaction.channel
            if channel is None:
                await interaction.response.send_message("This interaction is missing a channel context.", ephemeral=True)
                return
            if not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("Bakunawa Mech Access", "You are not an admin for this bot.")),
                    ephemeral=True,
                )
                return
            if not self._is_admin_messageable_channel(channel):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("Bakunawa Mech Access", "This admin command is not enabled in this channel.")),
                    ephemeral=True,
                )
                return
            await self._rebuild_receipt_logs(interaction.user, channel, interaction)

        @self.tree.command(
            name="mechfixpreviews",
            description="Repair old receipt proof previews using saved proof attachments.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        async def mechfixpreviews(interaction: discord.Interaction[Any]) -> None:
            channel = interaction.channel
            if channel is None:
                await interaction.response.send_message("This interaction is missing a channel context.", ephemeral=True)
                return
            if not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("Bakunawa Mech Access", "You are not an admin for this bot.")),
                    ephemeral=True,
                )
                return
            if not self._is_admin_messageable_channel(channel):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("Bakunawa Mech Access", "This admin command is not enabled in this channel.")),
                    ephemeral=True,
                )
                return
            await self._repair_proof_previews(interaction.user, channel, interaction)

        @self.tree.command(
            name="mechclean",
            description="Clean non-log messages from the current channel or thread.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        async def mechclean(interaction: discord.Interaction[Any]) -> None:
            channel = interaction.channel
            if channel is None:
                await interaction.response.send_message("This interaction is missing a channel context.", ephemeral=True)
                return
            if not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("Bakunawa Mech Access", "You are not an admin for this bot.")),
                    ephemeral=True,
                )
                return
            if not self._is_scoped_messageable_channel(channel):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("Bakunawa Mech Access", "This command is not enabled in this channel.")),
                    ephemeral=True,
                )
                return
            await interaction.response.defer(ephemeral=True, thinking=True)
            result = await self._clean_non_log_messages(channel)
            await interaction.edit_original_response(
                embeds=[embed_from_payload(task_status_embed("Bakunawa Mech Clean", _clean_result_description(result)))],
            )

        @self.tree.command(
            name="mechnote",
            description="Add, update, or clear a receipt note.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        @app_commands.describe(
            receipt_id="Receipt ID to update.",
            note="New note text. Leave blank to open the note editor; submit blank in the editor to clear.",
        )
        async def mechnote(
            interaction: discord.Interaction[Any],
            receipt_id: str,
            note: str | None = None,
        ) -> None:
            channel = interaction.channel
            if channel is None:
                await interaction.response.send_message("This interaction is missing a channel context.", ephemeral=True)
                return
            if not self._is_scoped_messageable_channel(channel):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("Bakunawa Mech Access", "This command is not enabled in this channel.")),
                    ephemeral=True,
                )
                return
            receipt = await self.base_runtime.database.get_receipt(receipt_id)
            if receipt is None:
                await interaction.response.send_message(f"Receipt `{receipt_id}` was not found.", ephemeral=True)
                return
            if str(interaction.user.id) != receipt.creator_user_id and not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message(
                    "Only the receipt creator or an admin can edit that note.",
                    ephemeral=True,
                )
                return
            if note is None:
                await interaction.response.send_modal(ReceiptNoteModal(self, receipt.id, receipt.admin_note))
                return
            updated = await self._set_receipt_note(
                receipt.id,
                note,
                actor_user_id=str(interaction.user.id),
                actor_display_name=getattr(interaction.user, "display_name", interaction.user.name),
            )
            if not updated:
                await interaction.response.send_message(f"Receipt `{receipt.id}` was not found.", ephemeral=True)
                return
            status = "cleared" if _normalize_receipt_note(note) is None else "updated"
            await interaction.response.send_message(
                f"Receipt `{receipt.id}` note {status}. Matching receipt cards were refreshed.",
                ephemeral=True,
            )

        @self.tree.command(
            name="mechrestartbot",
            description="Restart the bot remotely.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        async def mechrestartbot(interaction: discord.Interaction[Any]) -> None:
            channel = interaction.channel
            if channel is None:
                await interaction.response.send_message("This interaction is missing a channel context.", ephemeral=True)
                return
            if not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("Bakunawa Mech Access", "You are not an admin for this bot.")),
                    ephemeral=True,
                )
                return
            if not self._is_admin_channel_id(channel.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("Bakunawa Mech Access", "This admin command is not enabled in this channel.")),
                    ephemeral=True,
                )
                return
            await self._request_shutdown(interaction.user, channel, restart=True, interaction=interaction)

        @self.tree.command(
            name="mechstop",
            description="Shut the bot down gracefully.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        async def mechstop(interaction: discord.Interaction[Any]) -> None:
            channel = interaction.channel
            if channel is None:
                await interaction.response.send_message("This interaction is missing a channel context.", ephemeral=True)
                return
            if not self._is_admin_user(interaction.user.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("Bakunawa Mech Access", "You are not an admin for this bot.")),
                    ephemeral=True,
                )
                return
            if not self._is_admin_channel_id(channel.id):
                await interaction.response.send_message(
                    embed=embed_from_payload(task_error_embed("Bakunawa Mech Access", "This admin command is not enabled in this channel.")),
                    ephemeral=True,
                )
                return
            await self._request_shutdown(interaction.user, channel, restart=False, interaction=interaction)

        @self.tree.command(
            name="mechexport",
            description="Export the current database as JSON.",
            **command_kwargs,
        )
        @app_commands.guild_only()
        async def mechexport(interaction: discord.Interaction[Any]) -> None:
            await self._run_slash_command(interaction, _build_slash_input("mechexport"))

    async def _sync_app_commands(self) -> None:
        guild = self._test_guild_object()
        if guild is not None:
            try:
                synced = await self.tree.sync(guild=guild)
            except discord.Forbidden:
                LOGGER.warning(
                    "could not sync slash commands to test guild %s; falling back to global sync",
                    guild.id,
                )
            else:
                LOGGER.info("synced %s slash commands to test guild %s", len(synced), guild.id)
                return
        try:
            synced = await self.tree.sync()
        except discord.Forbidden:
            LOGGER.warning("could not sync global slash commands; continuing with direct ! commands")
            return
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


class ReceiptNoteModal(discord.ui.Modal, title="Receipt Note"):
    note = discord.ui.TextInput(
        label="Receipt Note",
        custom_id="note",
        required=False,
        style=discord.TextStyle.paragraph,
        max_length=4000,
        placeholder="Example: discounted due to repeat customer / event comp / staff approval.",
    )

    def __init__(
        self,
        client: BakunawaMechDiscordClient,
        receipt_id: str,
        current_note: str | None,
    ) -> None:
        super().__init__(timeout=300)
        self._client = client
        self._receipt_id = receipt_id
        self.note.default = current_note or ""

    async def on_submit(self, interaction: discord.Interaction[Any]) -> None:
        updated = await self._client._set_receipt_note(
            self._receipt_id,
            str(self.note),
            actor_user_id=str(interaction.user.id),
            actor_display_name=getattr(interaction.user, "display_name", interaction.user.name),
        )
        if not updated:
            await interaction.response.send_message(
                f"Receipt `{self._receipt_id}` was not found.",
                ephemeral=True,
            )
            return
        status = "cleared" if _normalize_receipt_note(str(self.note)) is None else "updated"
        await interaction.response.send_message(
            f"Receipt `{self._receipt_id}` note {status}. Matching receipt cards were refreshed.",
            ephemeral=True,
        )


class AddItemModal(discord.ui.Modal, title="Add Item"):
    override_unit_price = discord.ui.TextInput(
        label="Special Pricing (optional)",
        custom_id="override_unit_price",
        required=False,
        default="",
    )

    def __init__(
        self,
        client: BakunawaMechDiscordClient,
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
            manual_override_unit_price = parse_add_item_inputs(str(self.override_unit_price))
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
            self._owner_user_id,
            lambda current: _insert_item_into_session(
                current,
                self._client.base_runtime.contracts,
                catalog_item.name,
                1,
                manual_override_unit_price,
            ),
        )
        if session is None:
            await interaction.response.send_message("Calculator session expired.", ephemeral=True)
            return

        payload = calc_reply_payload(self._client.base_runtime.catalog.items, session)
        spec = reply_payload_to_spec(self._client, self._client.shared_runtime, payload)
        await interaction.response.edit_message(**spec.edit_kwargs())


class PackageSpecialPriceModal(discord.ui.Modal, title="Package Special Pricing"):
    override_unit_price = discord.ui.TextInput(
        label="Special Pricing (optional)",
        custom_id="override_unit_price",
        required=False,
        default="",
    )

    def __init__(
        self,
        client: BakunawaMechDiscordClient,
        owner_user_id: int,
        package_key: str,
        choices: dict[str, str],
        counts: dict[str, int],
    ) -> None:
        super().__init__(timeout=300)
        self._client = client
        self._owner_user_id = owner_user_id
        self._package_key = package_key
        self._choices = dict(choices)
        self._counts = dict(counts)
        definition = client.base_runtime.packages.find_package(package_key)
        if definition is not None:
            self.title = f"{definition.label} Pricing"

    async def on_submit(self, interaction: discord.Interaction[Any]) -> None:
        if interaction.user.id != self._owner_user_id:
            await interaction.response.send_message(
                "This calculator panel belongs to another user.",
                ephemeral=True,
            )
            return
        try:
            override_unit_price = parse_add_item_inputs(str(self.override_unit_price))
        except ValueError as error:
            await interaction.response.send_message(str(error), ephemeral=True)
            return
        await self._client._add_package_with_optional_override(
            interaction,
            self._owner_user_id,
            self._package_key,
            self._choices,
            self._counts,
            override_unit_price,
        )


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

    def __init__(self, client: BakunawaMechDiscordClient, owner_user_id: int) -> None:
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
                embed=embed_from_payload(task_error_embed("Bakunawa Mech Contracts", str(error))),
                ephemeral=True,
            )
            return

        self._client.base_runtime.contracts = contracts
        await interaction.response.send_message(
            embed=embed_from_payload(
                task_status_embed(
                    "Bakunawa Mech Contracts",
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
    unit_cost_active = discord.ui.TextInput(
        label="Unit Cost,Active (example: 4500,true)",
        custom_id="unit_cost_active",
        required=True,
    )

    def __init__(
        self,
        client: BakunawaMechDiscordClient,
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
                embed=embed_from_payload(task_warning_embed("Bakunawa Mech Adjust Prices", f"Price update failed. Nothing live was changed.\n{error}")),
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
    if item.unit_cost is not None:
        details.append(f"Cost ${item.unit_cost:,}")
    if item.price_pending:
        details.append("Pricing pending")
    return " | ".join(details)


def _display_item_name(name: str) -> str:
    replacements = {
        "Semi Slick": "Semi-Slick",
        "Offroad": "Off-Road",
        "Awd": "AWD",
        "Fwd": "FWD",
        "Rwd": "RWD",
        "Ev": "EV",
        "I4": "I4",
        "V6": "V6",
        "V8": "V8",
        "V12": "V12",
    }
    title = name.title()
    title = title.replace(" Tyres Upgrade", " Tyres")
    title = title.replace("Drift Tuning Kit", "Drift Kit")
    title = title.replace("Full Cosmetic", "Full Cosmetics")
    for number in range(1, 10):
        title = title.replace(f"{number}X", f"{number}x")
    for raw, display in replacements.items():
        title = title.replace(raw, display)
    return title


@dataclass(frozen=True)
class IndividualItemCategory:
    key: str
    label: str
    count: int


INDIVIDUAL_CATEGORY_ORDER = {
    "tuning": 0,
    "engine_upgrade": 1,
    "maintenance": 2,
    "cosmetic": 3,
    "repair": 4,
    "takeout": 5,
    "other": 6,
}


def _individual_item_categories(catalog_items: list[CatalogItem]) -> list[IndividualItemCategory]:
    counts: dict[str, int] = {}
    for _, item in _individual_catalog_entries(catalog_items):
        key = _catalog_category_key(item)
        counts[key] = counts.get(key, 0) + 1
    return [
        IndividualItemCategory(key=key, label=_catalog_category_label(key), count=count)
        for key, count in sorted(
            counts.items(),
            key=lambda entry: (
                INDIVIDUAL_CATEGORY_ORDER.get(entry[0], 100),
                _catalog_category_label(entry[0]),
            ),
        )
    ]


def _individual_items_for_category(
    catalog_items: list[CatalogItem],
    category_key: str,
) -> list[tuple[int, CatalogItem]]:
    normalized_key = category_key.strip().lower()
    return [
        (index, item)
        for index, item in _individual_catalog_entries(catalog_items)
        if _catalog_category_key(item) == normalized_key
    ]


def _individual_catalog_entries(catalog_items: list[CatalogItem]) -> list[tuple[int, CatalogItem]]:
    return [
        (index, item)
        for index, item in enumerate(catalog_items)
        if item.active and _catalog_category_key(item) != "package"
    ]


def _catalog_category_key(item: CatalogItem) -> str:
    raw = (item.category or "other").strip()
    if not raw:
        raw = "other"
    return raw.lower().replace(" ", "_")


def _catalog_category_label(category_key: str) -> str:
    labels = {
        "tuning": "Tuning",
        "engine_upgrade": "Engine Upgrades",
        "maintenance": "Maintenance",
        "cosmetic": "Cosmetics",
        "repair": "Repair",
        "takeout": "Takeout",
        "other": "Other",
        "package": "Packages",
    }
    return labels.get(category_key, category_key.replace("_", " ").title())


def _choice_group_label(group: str) -> str:
    labels = {
        "tire": "Tire Type",
        "drift": "Drift Option",
        "drivetrain": "Drivetrain",
        "engine": "Engine",
        "maintenance_type": "Maintenance Type",
    }
    return labels.get(group, group.replace("_", " ").title())


def _package_option_description(package_key: str) -> str:
    descriptions = {
        "full_tuning": "TIER 2: tuning plus Tier 1 and repair included",
        "full_maintenance": "Standard or EV maintenance package",
        "full_upgrades": "TIER 3: Full Tuning plus engine included",
        "full_performance_upgrade": "TIER 1: includes 5x Performance Parts",
        "full_cosmetics": "TIER 1: includes vehicle-specific cosmetics as ??x",
        "repair": "REPAIR: quick 15k repair kit",
    }
    return descriptions.get(package_key, "Package")


def _encode_package_choices(choices: dict[str, str]) -> str:
    if not choices:
        return "-"
    return ",".join(f"{key}={value}" for key, value in sorted(choices.items()))


def _decode_package_choices(encoded: str) -> dict[str, str]:
    if not encoded or encoded == "-":
        return {}
    choices: dict[str, str] = {}
    for part in encoded.split(","):
        if not part or "=" not in part:
            continue
        key, value = part.split("=", 1)
        choices[key] = value
    return choices


def _encode_package_counts(counts: dict[str, int]) -> str:
    if not counts:
        return "-"
    return ",".join(f"{key}={value}" for key, value in sorted(counts.items()))


def _decode_package_counts(encoded: str) -> dict[str, int]:
    if not encoded or encoded == "-":
        return {}
    counts: dict[str, int] = {}
    for part in encoded.split(","):
        if not part or "=" not in part:
            continue
        key, value = part.split("=", 1)
        try:
            counts[key] = int(value)
        except ValueError:
            continue
    return counts


def _next_package_choice_group(packages, package_key: str, choices: dict[str, str]) -> str | None:
    definition = packages.find_package(package_key)
    if definition is None:
        return None
    for group in definition.required_choices:
        if group not in choices:
            return group
    return None


def _pricing_source_label(pricing_source: PricingSource) -> str:
    if pricing_source is PricingSource.BULK:
        return "Catalog"
    if pricing_source is PricingSource.OVERRIDE:
        return "Package/Override"
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
    del contracts
    contract_override_unit_price = None
    contract_name = None

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


def _apply_package_expansion(session: CalculatorSession, expansion: PackageExpansion) -> None:
    existing_package_keys = {item.package_key for item in session.items if item.package_key is not None}
    if expansion.package_key in {"full_tuning", "full_performance_upgrade", "full_cosmetics", "repair"}:
        if "full_upgrades" in existing_package_keys:
            return
    if expansion.package_key in {"full_performance_upgrade", "full_cosmetics", "repair"}:
        if "full_tuning" in existing_package_keys:
            return
    if expansion.package_key == "full_upgrades":
        session.items = [
            item
            for item in session.items
            if item.package_key
            not in {
                "full_tuning",
                "full_performance_upgrade",
                "full_cosmetics",
            }
            and item.item_name != "REPAIR KIT"
        ]
    if expansion.package_key == "full_tuning":
        session.items = [
            item
            for item in session.items
            if item.package_key
            not in {
                "full_performance_upgrade",
                "full_cosmetics",
            }
            and item.item_name != "REPAIR KIT"
        ]
    append_unique_items(session.items, expansion.draft_items)


def _inherited_full_tuning_choices(session: CalculatorSession) -> dict[str, str]:
    for item in session.items:
        if item.package_key == "full_tuning":
            return {
                key: value
                for key, value in item.package_choices.items()
                if key in {"drift", "tire", "drivetrain"}
            }
    return {}


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


async def _maintenance_write_pause() -> None:
    if MAINTENANCE_WRITE_DELAY_SECONDS > 0:
        await asyncio.sleep(MAINTENANCE_WRITE_DELAY_SECONDS)


def _is_lifecycle_status_message(message: discord.Message) -> bool:
    return any(embed.title == "Bakunawa Mech Status" for embed in message.embeds)


def _is_log_related_message(message: discord.Message, bot_user_id: int | None) -> bool:
    if bot_user_id is not None and message.author.id != bot_user_id:
        return False
    if _is_lifecycle_status_message(message):
        return True
    content = message.content or ""
    if content.startswith("Receipt `") and " saved for " in content:
        return True
    return any((embed.title or "").startswith("Bakunawa Mech Receipt ") for embed in message.embeds)


def _clean_result_description(result: CleanResult) -> str:
    return (
        "Clean complete.\n"
        f"Messages inspected: {result.inspected}\n"
        f"Non-log messages deleted: {result.deleted}\n"
        f"Log/status/pinned messages preserved: {result.preserved}\n"
        f"Delete failures: {result.failed}"
    )


def _proof_preview_repair_result_description(result: ProofPreviewRepairResult) -> str:
    return (
        "Proof preview repair complete.\n"
        f"Receipts checked: {result.receipts_checked}\n"
        f"Receipts with proof records: {result.receipts_with_proofs}\n"
        f"Receipts with saved local proofs: {result.receipts_with_local_proofs}\n"
        f"Receipts missing saved local proofs: {result.receipts_missing_local_proofs}\n"
        f"Channels/threads scanned: {result.channels_scanned}\n"
        f"Messages inspected: {result.messages_inspected}\n"
        f"Receipt cards refreshed: {result.messages_refreshed}\n"
        f"Refresh failures: {result.refresh_failures}\n"
        f"Channel scan failures: {result.channel_failures}"
    )


def _has_existing_local_proof_path(proof_path: str | None) -> bool:
    return any(Path(value).is_file() for value in split_proof_values(proof_path))


RECEIPT_CONTENT_PATTERN = re.compile(r"Receipt `(?P<receipt_id>BM-[^`]+)`")
RECEIPT_TITLE_PATTERN = re.compile(r"^Bakunawa Mech Receipt (?P<receipt_id>BM-\S+)")


def _receipt_id_from_message(message: discord.Message) -> str | None:
    content_match = RECEIPT_CONTENT_PATTERN.search(message.content or "")
    if content_match is not None:
        return content_match.group("receipt_id")
    for embed in message.embeds:
        title = embed.title or ""
        title_match = RECEIPT_TITLE_PATTERN.search(title)
        if title_match is not None:
            return title_match.group("receipt_id")
    return None


def _receipt_message_has_attachment_preview(message: discord.Message) -> bool:
    for embed in message.embeds:
        image = getattr(embed, "image", None)
        image_url = getattr(image, "url", None)
        if isinstance(image_url, str) and image_url.startswith("attachment://"):
            return True
    return False


def _parse_note_command(remainder: str) -> tuple[str, str | None]:
    tokens = remainder.strip().split(maxsplit=1)
    if not tokens:
        raise ValueError("Use `bm!note <receipt_id> <note>`. Use `bm!note <receipt_id> clear` to clear the note.")
    receipt_id = tokens[0].strip()
    if len(tokens) == 1:
        raise ValueError("Add note text after the receipt ID, or use `clear` to remove the current note.")
    note = _normalize_receipt_note(tokens[1])
    return receipt_id, note


def _normalize_receipt_note(note: str | None) -> str | None:
    normalized = "\n".join(line.rstrip() for line in (note or "").strip().splitlines()).strip()
    if not normalized or normalized.lower() in {"clear", "none", "remove", "delete"}:
        return None
    return normalized[:4000]


def _configured_parent_channel_id(channel: discord.abc.MessageableChannel) -> int:
    if isinstance(channel, discord.Thread) and channel.parent_id is not None:
        return channel.parent_id
    return channel.id


async def _ensure_thread_open(thread: discord.Thread) -> None:
    if not thread.archived and not thread.locked:
        return
    try:
        kwargs: dict[str, Any] = {}
        if thread.archived:
            kwargs["archived"] = False
        if thread.locked:
            kwargs["locked"] = False
        if kwargs:
            await thread.edit(**kwargs)
    except discord.HTTPException:
        LOGGER.debug("thread_id=%s failed to reopen employee thread", thread.id)


def _employee_thread_name(prefix: str, display_name: str, user_id: int | str) -> str:
    suffix = f"[{user_id}]"
    cleaned = _clean_thread_display_name(display_name)
    base = f"{prefix}{cleaned} "
    max_base_length = max(0, 100 - len(suffix) - 1)
    return f"{base[:max_base_length].rstrip()} {suffix}".strip()


def _thread_matches_employee(thread: discord.Thread, prefix: str, user_id: int | str) -> bool:
    name = thread.name.strip()
    return name.startswith(prefix) and name.endswith(f"[{user_id}]")


def _clean_thread_display_name(display_name: str) -> str:
    cleaned = " ".join(display_name.replace("[", "(").replace("]", ")").split())
    return cleaned or "Employee"


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
        return datetime.strptime(normalized, "%H:%M:%S %m/%d/%y")
    except ValueError:
        return None


def _is_matching_receipt_message(
    message: discord.Message,
    receipt_id: str,
    bot_user_id: int | None,
) -> bool:
    if bot_user_id is not None and message.author.id != bot_user_id:
        return False
    if f"Receipt `{receipt_id}`" in (message.content or ""):
        return True
    expected_title = f"Bakunawa Mech Receipt {receipt_id}"
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


def _reset_adjustment_target_user_ids(
    mode: ResetMode,
    user_ids: list[int],
    open_adjustment_user_ids: list[str],
) -> list[str] | None:
    if mode is ResetMode.ALL:
        return None
    targeted_ids = {str(user_id) for user_id in user_ids}
    if mode is ResetMode.ONLY:
        return [user_id for user_id in open_adjustment_user_ids if user_id in targeted_ids]
    return [user_id for user_id in open_adjustment_user_ids if user_id not in targeted_ids]


def _reset_adjustment_snapshot_matches_scope(
    audit_event: AuditEvent,
    mode: ResetMode,
    user_ids: list[int],
) -> bool:
    if audit_event.action != "payout_adjustment_snapshot":
        return False
    detail = dict(audit_event.detail_json)
    adjustment_user_id = detail.get("user_id")
    if adjustment_user_id is None or detail.get("settled_at") is not None:
        return False
    return _reset_scope_matches(mode, user_ids, str(adjustment_user_id))


def _reset_progress_message(mode: ResetMode, user_ids: list[int], action: ResetAction) -> str:
    return f"{action.progress_prefix()} {_reset_scope_subject(mode, user_ids)} now..."


def _reset_export_label(mode: ResetMode, action: ResetAction) -> str:
    if mode is ResetMode.ALL:
        return f"bakunawa-mech-reset-all-{action.export_suffix()}"
    if mode is ResetMode.ONLY:
        return f"bakunawa-mech-reset-selected-{action.export_suffix()}"
    return f"bakunawa-mech-reset-except-{action.export_suffix()}"


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


def _adjustprices_embed(catalog_items: list[CatalogItem], notice: str | None = None) -> EmbedPayload:
    if not catalog_items:
        description = "No catalog items are configured yet."
    else:
        description = "\n".join(
            (
                f"{index + 1}. {item.name} | Unit ${item.unit_price:,} | "
                f"{(f'Cost ${item.unit_cost:,}' if item.unit_cost is not None else 'Cost n/a')} | "
                f"{'Active' if item.active else 'Inactive'}"
            )
            for index, item in enumerate(catalog_items)
        )
    return task_status_embed("Bakunawa Mech Adjust Prices", description).field(
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
        bulk_price=None,
        bulk_min_qty=None,
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
    lock_path = preflight_config.storage.database_path.parent / "bakunawa-mech.lock"
    pid_path = preflight_config.storage.database_path.parent / "bakunawa-mech.pid"
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
    client = BakunawaMechDiscordClient(runtime)
    try:
        LOGGER.info("starting Discord runtime")
        await client.start(runtime.config.discord.token)
        return 0
    finally:
        try:
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


