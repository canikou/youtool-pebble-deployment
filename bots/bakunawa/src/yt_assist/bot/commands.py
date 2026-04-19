"""Real command routing for the local parity runner."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from yt_assist.domain.backup import save_export_async
from yt_assist.domain.catalog import Catalog
from yt_assist.domain.contracts import Contracts
from yt_assist.domain.models import StatsSort
from yt_assist.domain.packages import PackageCatalog
from yt_assist.domain.templates import (
    TemplateLoadStatus,
)
from yt_assist.domain.templates import (
    reload as reload_templates,
)

from .render import (
    ReplyPayload,
    health_embed,
    help_action_rows,
    help_page_embed,
    payouts_embed,
    pricesheet_embed,
    stats_embed,
    task_error_embed,
    task_status_embed,
    task_warning_embed,
)

LOGGER = logging.getLogger(__name__)
TEMPLATES_CHANNEL_ID = 1494151323526889633
HANDLED_USER_ERROR = "__handled_user_error__"


@dataclass(slots=True)
class ConsoleActor:
    user_id: int
    username: str
    display_name: str


@dataclass(slots=True)
class ConsoleChannel:
    channel_id: int
    label: str


@dataclass(slots=True)
class CommandContext:
    runtime: Any
    actor: ConsoleActor
    channel: ConsoleChannel
    is_interaction: bool
    raw_input: str

    @property
    def prefix(self) -> str:
        return self.runtime.config.discord.prefix

    def is_admin(self) -> bool:
        return self.actor.user_id in self.runtime.config.discord.admin_user_ids

    def is_allowed_channel(self) -> bool:
        config = self.runtime.config.discord
        return (
            self.channel.channel_id in config.allowed_channel_ids
            or self.channel.channel_id in config.admin_channel_ids
        )

    def is_admin_channel(self) -> bool:
        return self.channel.channel_id in self.runtime.config.discord.admin_channel_ids


@dataclass(slots=True)
class CommandEvent:
    type: str
    reply: ReplyPayload | None = None
    target: str | None = None
    after_seconds: int | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"type": self.type}
        if self.reply is not None:
            payload["reply"] = self.reply.to_dict()
        if self.target is not None:
            payload["target"] = self.target
        if self.after_seconds is not None:
            payload["after_seconds"] = self.after_seconds
        return payload


@dataclass(slots=True)
class CommandResult:
    canonical_name: str | None
    events: list[CommandEvent] = field(default_factory=list)
    handled: bool = True


class HandledCommandError(Exception):
    def __init__(self, result: CommandResult) -> None:
        super().__init__(HANDLED_USER_ERROR)
        self.result = result


CommandHandler = Callable[[CommandContext, list[str]], Awaitable[CommandResult]]

PREFIX_COMMAND_ALIASES: dict[str, str] = {
    "help": "help",
    "health": "health",
    "stats": "stats",
    "pricesheet": "pricesheet",
    "payouts": "payouts",
    "refresh": "refresh",
    "templates": "templates",
    "export": "export",
}

SLASH_COMMAND_ALIASES: dict[str, str] = {
    "mechhelp": "help",
    "mechealth": "health",
    "mechstats": "stats",
    "mechpricesheet": "pricesheet",
    "mechpayouts": "payouts",
    "mechrefresh": "refresh",
    "mechtemplates": "templates",
    "mechexport": "export",
}


def all_prefix_commands() -> list[str]:
    return list(dict.fromkeys(PREFIX_COMMAND_ALIASES.keys()))


def best_prefix_command_suggestion(input_text: str) -> str | None:
    attempted = input_text.strip().lower()
    if not attempted:
        return None
    best_match: tuple[str, int] | None = None
    for name in all_prefix_commands():
        distance = _levenshtein(attempted, name.lower())
        close_by_prefix = name.startswith(attempted) or attempted.startswith(name)
        if distance > 3 and not close_by_prefix:
            continue
        if best_match is None or distance < best_match[1]:
            best_match = (name, distance)
    return best_match[0] if best_match is not None else None


def _send(reply: ReplyPayload) -> CommandEvent:
    return CommandEvent(type="send", reply=reply)


def _edit(reply: ReplyPayload) -> CommandEvent:
    return CommandEvent(type="edit", reply=reply)


def _delete(target: str) -> CommandEvent:
    return CommandEvent(type="delete", target=target)


def _schedule_delete(target: str, after_seconds: int) -> CommandEvent:
    return CommandEvent(type="schedule_delete", target=target, after_seconds=after_seconds)


def _cleanup_prefix_invocation_events(ctx: CommandContext) -> list[CommandEvent]:
    if not ctx.is_interaction and not ctx.is_admin_channel():
        return [_delete("invocation")]
    return []


def _schedule_transient_cleanup_events(ctx: CommandContext) -> list[CommandEvent]:
    timeout = ctx.runtime.config.discord.transient_message_timeout_seconds
    events = [_schedule_delete("reply", timeout)]
    if not ctx.is_interaction and not ctx.is_admin_channel():
        events.append(_schedule_delete("invocation", timeout))
    return events


def _handled(result: CommandResult) -> HandledCommandError:
    return HandledCommandError(result)


def _error_reply(ctx: CommandContext, title: str, description: str) -> ReplyPayload:
    return ReplyPayload(embeds=[task_error_embed(title, description)], ephemeral=ctx.is_interaction)


async def _ensure_allowed_channel(ctx: CommandContext) -> None:
    if ctx.is_allowed_channel():
        return
    result = CommandResult(
        canonical_name=None,
        events=[_send(_error_reply(ctx, "Bakunawa Mech Access", "This command is not enabled in this channel."))],
    )
    result.events.extend(_schedule_transient_cleanup_events(ctx))
    raise _handled(result)


async def _ensure_admin(ctx: CommandContext) -> None:
    if ctx.is_admin():
        return
    result = CommandResult(
        canonical_name=None,
        events=[_send(_error_reply(ctx, "Bakunawa Mech Access", "You are not an admin for this bot."))],
    )
    result.events.extend(_schedule_transient_cleanup_events(ctx))
    raise _handled(result)


async def _ensure_admin_channel(ctx: CommandContext) -> None:
    if ctx.is_admin_channel():
        return
    result = CommandResult(
        canonical_name=None,
        events=[_send(_error_reply(ctx, "Bakunawa Mech Access", "This admin command is not enabled in this channel."))],
    )
    result.events.extend(_schedule_transient_cleanup_events(ctx))
    raise _handled(result)


async def _handle_help(ctx: CommandContext, args: list[str]) -> CommandResult:
    del args
    reply = ReplyPayload(
        embeds=[help_page_embed(ctx.prefix, 0)],
        components=help_action_rows(ctx.actor.user_id, 0),
        ephemeral=ctx.is_interaction,
    )
    result = CommandResult(canonical_name="help", events=[_send(reply)])
    result.events.append(
        _schedule_delete("reply", ctx.runtime.config.discord.transient_message_timeout_seconds)
    )
    if not ctx.is_interaction and not ctx.is_admin_channel():
        result.events.append(
            _schedule_delete("invocation", ctx.runtime.config.discord.transient_message_timeout_seconds)
        )
    return result


async def _handle_stats(ctx: CommandContext, args: list[str]) -> CommandResult:
    await _ensure_allowed_channel(ctx)
    sort = StatsSort.parse(args[0] if args else None)
    entries = await ctx.runtime.database.leaderboard(sort)
    result = CommandResult(
        canonical_name="stats",
        events=[_send(ReplyPayload(embeds=[stats_embed(sort, entries)], ephemeral=False))],
    )
    result.events.extend(_schedule_transient_cleanup_events(ctx))
    return result


async def _handle_pricesheet(ctx: CommandContext, args: list[str]) -> CommandResult:
    del args
    await _ensure_allowed_channel(ctx)
    reply = ReplyPayload(embeds=[pricesheet_embed(ctx.runtime.catalog.items)], ephemeral=ctx.is_interaction)
    result = CommandResult(canonical_name="pricesheet", events=[_send(reply)])
    result.events.extend(_schedule_transient_cleanup_events(ctx))
    return result


async def _handle_payouts(ctx: CommandContext, args: list[str]) -> CommandResult:
    await _ensure_admin(ctx)
    await _ensure_admin_channel(ctx)
    target_user_id = args[0] if args else None
    entries = await ctx.runtime.database.payouts(target_user_id)
    events = _cleanup_prefix_invocation_events(ctx)
    events.append(_send(ReplyPayload(embeds=[payouts_embed(entries)], ephemeral=ctx.is_interaction)))
    return CommandResult(canonical_name="payouts", events=events)


async def _handle_refresh(ctx: CommandContext, args: list[str]) -> CommandResult:
    del args
    await _ensure_admin(ctx)
    await _ensure_admin_channel(ctx)
    events = [
        _send(
            ReplyPayload(
                embeds=[task_status_embed("Bakunawa Mech Refresh", "Reloading catalog and package files from disk now...")],
                ephemeral=ctx.is_interaction,
            )
        )
    ]
    catalog = Catalog.load_from(ctx.runtime.config.storage.catalog_path)
    packages = PackageCatalog.load_from(ctx.runtime.config.storage.packages_path)
    contracts = Contracts.load_from(ctx.runtime.config.storage.contracts_path, catalog)
    ctx.runtime.catalog = catalog
    ctx.runtime.packages = packages
    ctx.runtime.contracts = contracts
    events.append(
        _edit(
            ReplyPayload(
                embeds=[
                    task_status_embed(
                        "Bakunawa Mech Refresh",
                        (
                            "Runtime data refreshed.\n"
                            f"Catalog items loaded: {len(catalog.items)}\n"
                            f"Package definitions loaded: {len(packages.packages)}"
                        ),
                    )
                ],
                ephemeral=ctx.is_interaction,
            )
        )
    )
    return CommandResult(canonical_name="refresh", events=events)


async def _handle_templates(ctx: CommandContext, args: list[str]) -> CommandResult:
    await _ensure_allowed_channel(ctx)
    action = args[0].strip().lower() if args else None
    if action == "reload":
        await _ensure_admin(ctx)
        await _ensure_admin_channel(ctx)
        report = reload_templates()
        if report.status is TemplateLoadStatus.LOADED:
            description = (
                f"Templates reloaded from `{report.path}`.\n"
                f"Run `{ctx.prefix}templates` to open the templates channel."
            )
            embed = task_status_embed("Bakunawa Mech Templates", description)
        elif report.status is TemplateLoadStatus.CREATED_DEFAULT_FILE:
            description = (
                f"Templates file was missing, so a default file was created at `{report.path}`.\n"
                f"Run `{ctx.prefix}templates` to open the templates channel."
            )
            embed = task_status_embed("Bakunawa Mech Templates", description)
        else:
            description = (
                f"{report.warning or 'Templates JSON was malformed.'}\n"
                "The bot stayed online and is currently serving default templates until the JSON is fixed and reloaded."
            )
            embed = task_warning_embed("Bakunawa Mech Templates", description)
        events = _cleanup_prefix_invocation_events(ctx)
        events.append(_send(ReplyPayload(embeds=[embed], ephemeral=ctx.is_interaction)))
        return CommandResult(canonical_name="templates", events=events)

    if action is not None:
        result = CommandResult(
            canonical_name="templates",
            events=[
                _send(
                    _error_reply(
                        ctx,
                        "Bakunawa Mech Templates",
                        (
                            f"Unknown templates action `{action}`. "
                            f"Use `{ctx.prefix}templates` or `{ctx.prefix}templates reload`."
                        ),
                    )
                )
            ],
        )
        result.events.extend(_schedule_transient_cleanup_events(ctx))
        raise _handled(result)

    admin_channel = ctx.is_admin_channel()
    reply = ReplyPayload(
        content=f"Current Bakunawa Mech templates are maintained in <#{TEMPLATES_CHANNEL_ID}>.",
        embeds=[
            task_status_embed(
                "Bakunawa Mech Templates",
                f"Open <#{TEMPLATES_CHANNEL_ID}> for the current announcement templates.",
            )
        ],
        ephemeral=ctx.is_interaction and admin_channel,
    )
    events = [_send(reply)]
    if not admin_channel:
        events.append(
            _schedule_delete("reply", ctx.runtime.config.discord.transient_message_timeout_seconds)
        )
    events.extend(_cleanup_prefix_invocation_events(ctx))
    return CommandResult(canonical_name="templates", events=events)


async def _handle_health(ctx: CommandContext, args: list[str]) -> CommandResult:
    del args
    report_builder = getattr(ctx.runtime, "build_health_report", None)
    if callable(report_builder):
        checks, issues = await report_builder()
    else:
        checks = [
            "Running in local console mode. Live Discord permission checks are unavailable here.",
            "Authenticated as `LOCAL_CONSOLE` (`0`) using the configured bot token.",
        ]
        issues: list[str] = []
        config = ctx.runtime.config.discord
        if not config.admin_user_ids:
            issues.append("`admin_user_ids` is empty. Add at least one Discord user ID.")
        else:
            checks.append(f"Configured admin users: {len(config.admin_user_ids)}")
        if not config.allowed_channel_ids:
            issues.append("`allowed_channel_ids` is empty. Add at least one main channel ID.")
        else:
            checks.append(f"Configured main channels: {len(config.allowed_channel_ids)}")
        if not config.admin_channel_ids:
            issues.append("`admin_channel_ids` is empty. Admin commands will not be usable anywhere.")
        else:
            checks.append(f"Configured admin channels: {len(config.admin_channel_ids)}")
        if config.receipt_log_channel_id is None:
            issues.append("`receipt_log_channel_id` is not set. Admin log cards are currently disabled.")
        else:
            checks.append(f"Receipt log channel configured: <#{config.receipt_log_channel_id}>")
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
    reply = ReplyPayload(
        embeds=[health_embed(checks, issues, ctx.runtime.config.discord.message_content_intent)],
        ephemeral=ctx.is_interaction,
    )
    result = CommandResult(canonical_name="health", events=[_send(reply)])
    result.events.extend(_cleanup_prefix_invocation_events(ctx))
    return result


async def _handle_export(ctx: CommandContext, args: list[str]) -> CommandResult:
    del args
    await _ensure_admin(ctx)
    await _ensure_admin_channel(ctx)
    events = [
        _send(
            ReplyPayload(
                embeds=[task_status_embed("Bakunawa Mech Export", "Exporting the current database now...")],
                ephemeral=ctx.is_interaction,
            )
        )
    ]
    bundle = await ctx.runtime.database.export_bundle(ctx.runtime.catalog)
    path = await save_export_async(bundle, ctx.runtime.config.storage.export_dir, "bakunawa-mech-export")
    events.append(
        _edit(
            ReplyPayload(
                embeds=[
                    task_status_embed(
                        "Bakunawa Mech Export",
                        f"Export completed.\nSaved file: `{path}`",
                    )
                ],
                ephemeral=ctx.is_interaction,
            )
        )
    )
    LOGGER.info("path=%s export created", path)
    return CommandResult(canonical_name="export", events=events)


async def execute_input(ctx: CommandContext, input_text: str) -> CommandResult:
    canonical_name = "unknown"
    try:
        parsed = _parse_input(ctx, input_text)
        if parsed is None:
            return CommandResult(canonical_name=None, handled=False)

        canonical_name, args = parsed
        LOGGER.info(
            "command=%s user_id=%s channel_id=%s command received",
            canonical_name,
            ctx.actor.user_id,
            ctx.channel.channel_id,
        )
        result = await COMMAND_HANDLERS[canonical_name](ctx, args)
    except HandledCommandError as error:
        LOGGER.error(
            "command=%s user_id=%s channel_id=%s error=%s command error",
            "unknown",
            ctx.actor.user_id,
            ctx.channel.channel_id,
            HANDLED_USER_ERROR,
        )
        return error.result
    except Exception as error:  # noqa: BLE001
        LOGGER.error(
            "command=%s user_id=%s channel_id=%s error=%r command error",
            canonical_name,
            ctx.actor.user_id,
            ctx.channel.channel_id,
            error,
        )
        return CommandResult(
            canonical_name=canonical_name,
            events=[
                _send(
                    ReplyPayload(
                        content=f"Command failed: {error}",
                        ephemeral=ctx.is_interaction,
                    )
                )
            ],
        )

    LOGGER.info(
        "command=%s user_id=%s channel_id=%s command completed",
        canonical_name,
        ctx.actor.user_id,
        ctx.channel.channel_id,
    )
    return result


async def handle_component_click(ctx: CommandContext, custom_id: str) -> CommandResult:
    parts = custom_id.split("|")
    if parts[:2] != ["help", "page"] or len(parts) != 4:
        return CommandResult(
            canonical_name="component",
            events=[
                _send(
                    ReplyPayload(
                        embeds=[task_error_embed("Bakunawa Mech Help", f"Unknown component `{custom_id}`.")],
                        ephemeral=ctx.is_interaction,
                    )
                )
            ],
        )
    owner_user_id = int(parts[2])
    page = int(parts[3])
    if owner_user_id != ctx.actor.user_id:
        return CommandResult(
            canonical_name="component",
            events=[
                _send(
                    ReplyPayload(
                        embeds=[task_error_embed("Bakunawa Mech Help", "This help panel belongs to another user.")],
                        ephemeral=ctx.is_interaction,
                    )
                )
            ],
        )
    reply = ReplyPayload(
        embeds=[help_page_embed(ctx.prefix, page)],
        components=help_action_rows(ctx.actor.user_id, page),
        ephemeral=ctx.is_interaction,
    )
    return CommandResult(canonical_name="help", events=[_edit(reply)])


def _parse_input(ctx: CommandContext, input_text: str) -> tuple[str, list[str]] | None:
    stripped = input_text.strip()
    if not stripped:
        return None

    prefix = ctx.runtime.config.discord.prefix
    if stripped.startswith(prefix):
        body = stripped[len(prefix) :].strip()
        if not body:
            return None
        tokens = body.split()
        command_name = tokens[0].lower()
        if command_name in PREFIX_COMMAND_ALIASES:
            return PREFIX_COMMAND_ALIASES[command_name], tokens[1:]
        suggestion = best_prefix_command_suggestion(command_name)
        if suggestion is None or not (ctx.is_allowed_channel() or ctx.is_admin_channel()):
            return None
        result = CommandResult(
            canonical_name=None,
            events=[
                _send(
                    ReplyPayload(
                        embeds=[
                            task_error_embed(
                                "Bakunawa Mech Help",
                                f"Did you mean `{prefix}{suggestion}`?",
                            )
                        ]
                    )
                ),
                _schedule_delete("reply", ctx.runtime.config.discord.transient_message_timeout_seconds),
            ],
        )
        if not ctx.is_admin_channel():
            result.events.append(
                _schedule_delete(
                    "invocation",
                    ctx.runtime.config.discord.transient_message_timeout_seconds,
                )
            )
        raise HandledCommandError(result)

    if stripped.startswith("/"):
        body = stripped[1:].strip()
        if not body:
            return None
        tokens = body.split()
        command_name = tokens[0].lower()
        canonical = SLASH_COMMAND_ALIASES.get(command_name)
        if canonical is None:
            return None
        return canonical, tokens[1:]

    return None


COMMAND_HANDLERS: dict[str, CommandHandler] = {
    "help": _handle_help,
    "health": _handle_health,
    "stats": _handle_stats,
    "pricesheet": _handle_pricesheet,
    "payouts": _handle_payouts,
    "refresh": _handle_refresh,
    "templates": _handle_templates,
    "export": _handle_export,
}


def _levenshtein(left: str, right: str) -> int:
    if left == right:
        return 0
    if not left:
        return len(right)
    if not right:
        return len(left)
    previous = list(range(len(right) + 1))
    current = [0] * (len(right) + 1)
    for left_index, left_char in enumerate(left):
        current[0] = left_index + 1
        for right_index, right_char in enumerate(right):
            substitution_cost = 0 if left_char == right_char else 1
            current[right_index + 1] = min(
                current[right_index] + 1,
                previous[right_index + 1] + 1,
                previous[right_index] + substitution_cost,
            )
        previous = current[:]
    return previous[len(right)]


