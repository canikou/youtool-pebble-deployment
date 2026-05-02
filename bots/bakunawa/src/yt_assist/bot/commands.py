"""Real command routing for the local parity runner."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import timedelta, timezone
from typing import Any

from yt_assist.domain.backup import save_export_async
from yt_assist.domain.catalog import Catalog
from yt_assist.domain.contracts import Contracts
from yt_assist.domain.models import LeaderboardEntry, StatsSort, utcnow
from yt_assist.domain.packages import PackageCatalog
from yt_assist.domain.templates import (
    TemplateLoadStatus,
)
from yt_assist.domain.templates import (
    reload as reload_templates,
)

from .command_support import parse_signed_amount_cents_text, parse_user_token
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
UTC_PLUS_8 = timezone(timedelta(hours=8))
WEEKLY_TOP_MECH_BONUS_CENTS = 100_000_000
WEEKLY_RUNNER_UP_BONUS_CENTS = 50_000_000


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
    "payoutoffset": "payoutoffset",
    "payoutsplit": "payoutsplit",
    "weeklypayout": "weeklypayout",
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
    "mechpayoutoffset": "payoutoffset",
    "mechpayoutsplit": "payoutsplit",
    "mechweeklypayout": "weeklypayout",
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
    target_user_id = None
    if args:
        parsed_user_id = parse_user_token(args[0])
        target_user_id = str(parsed_user_id) if parsed_user_id is not None else args[0]
    entries = await ctx.runtime.database.payouts(target_user_id)
    events = _cleanup_prefix_invocation_events(ctx)
    events.append(_send(ReplyPayload(embeds=[payouts_embed(entries)], ephemeral=ctx.is_interaction)))
    return CommandResult(canonical_name="payouts", events=events)


async def _handle_payoutoffset(ctx: CommandContext, args: list[str]) -> CommandResult:
    await _ensure_admin(ctx)
    await _ensure_admin_channel(ctx)
    try:
        user_id, amount_cents, reason = _parse_payout_offset_args(args)
    except ValueError as error:
        raise _handled(
            CommandResult(
                canonical_name="payoutoffset",
                events=[_send(_error_reply(ctx, "Bakunawa Mech Payout Offset", str(error)))],
            )
        ) from error
    existing_entries = await ctx.runtime.database.payouts(user_id)
    display_name = existing_entries[0].display_name if existing_entries else user_id
    inserted = await ctx.runtime.database.apply_payout_adjustments(
        [(user_id, display_name, amount_cents, reason, None, None)],
        str(ctx.actor.user_id),
        ctx.actor.display_name,
    )
    updated_entries = await ctx.runtime.database.payouts(user_id)
    updated_total = updated_entries[0].adjusted_total_payout_cents if updated_entries else amount_cents
    description = (
        f"Applied {inserted} payout adjustment for <@{user_id}>.\n"
        f"Amount: {_format_signed_amount_cents(amount_cents)}\n"
        f"Reason: {reason}\n"
        f"Updated final payout: ${_format_amount_cents(updated_total)}"
    )
    events = _cleanup_prefix_invocation_events(ctx)
    events.append(
        _send(
            ReplyPayload(
                embeds=[task_status_embed("Bakunawa Mech Payout Offset", description)],
                ephemeral=ctx.is_interaction,
            )
        )
    )
    return CommandResult(canonical_name="payoutoffset", events=events)


async def _handle_payoutsplit(ctx: CommandContext, args: list[str]) -> CommandResult:
    await _ensure_admin(ctx)
    await _ensure_admin_channel(ctx)
    try:
        source_user_id, recipient_user_ids, everyone_mode, reason = _parse_payout_split_args(args)
    except ValueError as error:
        raise _handled(
            CommandResult(
                canonical_name="payoutsplit",
                events=[_send(_error_reply(ctx, "Bakunawa Mech Payout Split", str(error)))],
            )
        ) from error
    entries = await ctx.runtime.database.payouts(None)
    entry_by_user_id = {entry.user_id: entry for entry in entries}
    source_entry = entry_by_user_id.get(source_user_id)
    if source_entry is None:
        raise _handled(
            CommandResult(
                canonical_name="payoutsplit",
                events=[
                    _send(
                        _error_reply(
                            ctx,
                            "Bakunawa Mech Payout Split",
                            f"No active payout was found for <@{source_user_id}>.",
                        )
                    )
                ],
            )
        )

    distributable_cents = source_entry.adjusted_total_payout_cents
    if distributable_cents <= 0:
        raise _handled(
            CommandResult(
                canonical_name="payoutsplit",
                events=[
                    _send(
                        _error_reply(
                            ctx,
                            "Bakunawa Mech Payout Split",
                            f"<@{source_user_id}> has no positive payout left to split.",
                        )
                    )
                ],
            )
        )

    if everyone_mode:
        target_user_ids = [entry.user_id for entry in entries if entry.user_id != source_user_id]
    else:
        target_user_ids = recipient_user_ids
    target_user_ids = [user_id for user_id in target_user_ids if user_id != source_user_id]
    target_user_ids = list(dict.fromkeys(target_user_ids))
    if not target_user_ids:
        raise _handled(
            CommandResult(
                canonical_name="payoutsplit",
                events=[
                    _send(
                        _error_reply(
                            ctx,
                            "Bakunawa Mech Payout Split",
                            "No recipient users were found for this split.",
                        )
                    )
                ],
            )
        )

    target_count = len(target_user_ids)
    base_share = distributable_cents // target_count
    remainder = distributable_cents % target_count
    split_reason = reason or f"Split from {source_entry.display_name}"
    adjustments: list[tuple[str, str, int, str, str | None, str | None]] = [
        (
            source_user_id,
            source_entry.display_name,
            -distributable_cents,
            split_reason,
            source_user_id,
            source_entry.display_name,
        )
    ]
    per_user_lines: list[str] = []
    for index, user_id in enumerate(target_user_ids):
        share_cents = base_share + (1 if index < remainder else 0)
        if share_cents <= 0:
            continue
        display_name = entry_by_user_id.get(user_id, None).display_name if user_id in entry_by_user_id else user_id
        adjustments.append(
            (
                user_id,
                display_name,
                share_cents,
                split_reason,
                source_user_id,
                source_entry.display_name,
            )
        )
        per_user_lines.append(f"<@{user_id}>: +${_format_amount_cents(share_cents)}")
    if len(adjustments) <= 1:
        raise _handled(
            CommandResult(
                canonical_name="payoutsplit",
                events=[
                    _send(
                        _error_reply(
                            ctx,
                            "Bakunawa Mech Payout Split",
                            "The current payout is too small to split across that many recipients.",
                        )
                    )
                ],
            )
        )

    inserted = await ctx.runtime.database.apply_payout_adjustments(
        adjustments,
        str(ctx.actor.user_id),
        ctx.actor.display_name,
    )
    description = (
        f"Split ${_format_amount_cents(distributable_cents)} from <@{source_user_id}>.\n"
        f"Recipients: {len(per_user_lines)}\n"
        f"Source offset: -${_format_amount_cents(distributable_cents)}\n"
        f"Reason: {split_reason}\n"
        + "\n".join(per_user_lines)
    )
    events = _cleanup_prefix_invocation_events(ctx)
    events.append(
        _send(
            ReplyPayload(
                embeds=[
                    task_status_embed(
                        "Bakunawa Mech Payout Split",
                        f"{description}\nCreated adjustment rows: {inserted}",
                    )
                ],
                ephemeral=ctx.is_interaction,
            )
        )
    )
    return CommandResult(canonical_name="payoutsplit", events=events)


async def _handle_weeklypayout(ctx: CommandContext, args: list[str]) -> CommandResult:
    del args
    await _ensure_admin(ctx)
    await _ensure_admin_channel(ctx)

    week_key, week_start_at, week_end_at = _current_weekly_payout_window()
    snapshot = await ctx.runtime.database.get_weekly_snapshot(week_key)
    created = snapshot is None
    if snapshot is None:
        receipts = await ctx.runtime.database.list_weekly_eligible_receipts()
    else:
        receipts = await ctx.runtime.database.list_weekly_snapshot_receipts(snapshot.id)

    if not receipts:
        events = _cleanup_prefix_invocation_events(ctx)
        events.append(
            _send(
                ReplyPayload(
                    embeds=[
                        task_status_embed(
                            "Bakunawa Mech Weekly Payout",
                            (
                                f"Week `{week_key}` has no eligible active receipts to snapshot.\n"
                                "Nothing was frozen and no bonuses were applied."
                            ),
                        )
                    ],
                    ephemeral=ctx.is_interaction,
                )
            )
        )
        return CommandResult(canonical_name="weeklypayout", events=events)

    entries = await ctx.runtime.database.weekly_leaderboard(receipts=receipts)
    top_entry = entries[0] if entries else None
    runner_up_entry = entries[1] if len(entries) > 1 else None

    if snapshot is None:
        snapshot = await ctx.runtime.database.create_weekly_snapshot(
            week_key=week_key,
            week_start_at=week_start_at,
            week_end_at=week_end_at,
            ranking_basis="sales",
            receipts=receipts,
            top_mech_user_id=top_entry.user_id if top_entry is not None else None,
            runner_up_user_id=runner_up_entry.user_id if runner_up_entry is not None else None,
            actor_user_id=str(ctx.actor.user_id),
            actor_display_name=ctx.actor.display_name,
        )
    else:
        try:
            await ctx.runtime.database.clear_weekly_bonus_adjustments(snapshot.id)
        except ValueError as error:
            raise _handled(
                CommandResult(
                    canonical_name="weeklypayout",
                    events=[
                        _send(
                            _error_reply(
                                ctx,
                                "Bakunawa Mech Weekly Payout",
                                str(error),
                            )
                        )
                    ],
                )
            ) from error
        snapshot = await ctx.runtime.database.replace_weekly_snapshot(
            snapshot_id=snapshot.id,
            week_key=week_key,
            top_mech_user_id=top_entry.user_id if top_entry is not None else None,
            runner_up_user_id=runner_up_entry.user_id if runner_up_entry is not None else None,
            actor_user_id=str(ctx.actor.user_id),
            actor_display_name=ctx.actor.display_name,
        )

    winners: list[tuple[str, str, int, str]] = []
    if top_entry is not None:
        winners.append(
            (
                top_entry.user_id,
                top_entry.display_name,
                WEEKLY_TOP_MECH_BONUS_CENTS,
                f"Top Mech bonus for {week_key}",
            )
        )
    if runner_up_entry is not None:
        winners.append(
            (
                runner_up_entry.user_id,
                runner_up_entry.display_name,
                WEEKLY_RUNNER_UP_BONUS_CENTS,
                f"Runner Up bonus for {week_key}",
            )
        )

    inserted = await ctx.runtime.database.create_weekly_bonus_adjustments(
        snapshot.id,
        winners,
        str(ctx.actor.user_id),
        ctx.actor.display_name,
    )

    description = (
        f"Week `{week_key}` {'created' if created else 'replaced'}.\n"
        f"Window: {week_start_at.astimezone(UTC_PLUS_8):%Y-%m-%d %H:%M} to {week_end_at.astimezone(UTC_PLUS_8):%Y-%m-%d %H:%M} UTC+8\n"
        f"Frozen receipts: {len(receipts)}\n"
        f"Top Mech: {_weekly_winner_line(top_entry, WEEKLY_TOP_MECH_BONUS_CENTS)}\n"
        f"Runner Up: {_weekly_winner_line(runner_up_entry, WEEKLY_RUNNER_UP_BONUS_CENTS)}\n"
        f"Bonus adjustments created: {inserted}\n"
        "New receipts created after this snapshot now count toward the next payout cycle."
    )
    events = _cleanup_prefix_invocation_events(ctx)
    events.append(
        _send(
            ReplyPayload(
                embeds=[task_status_embed("Bakunawa Mech Weekly Payout", description)],
                ephemeral=ctx.is_interaction,
            )
        )
    )
    return CommandResult(canonical_name="weeklypayout", events=events)


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
    "payoutoffset": _handle_payoutoffset,
    "payoutsplit": _handle_payoutsplit,
    "weeklypayout": _handle_weeklypayout,
    "refresh": _handle_refresh,
    "templates": _handle_templates,
    "export": _handle_export,
}


def _parse_payout_offset_args(args: list[str]) -> tuple[str, int, str]:
    if len(args) < 2:
        raise ValueError("Use `bm!payoutoffset @user <amount> [reason]`.")
    parsed_user_id = parse_user_token(args[0])
    if parsed_user_id is None:
        raise ValueError("The first argument must be a user mention or numeric user ID.")
    amount_cents = parse_signed_amount_cents_text(args[1])
    reason = " ".join(args[2:]).strip() or "Manual payout adjustment"
    return str(parsed_user_id), amount_cents, reason


def _parse_payout_split_args(args: list[str]) -> tuple[str, list[str], bool, str]:
    if len(args) < 2:
        raise ValueError("Use `bm!payoutsplit @source everyone|@user... [reason]`.")
    parsed_source_user_id = parse_user_token(args[0])
    if parsed_source_user_id is None:
        raise ValueError("The first argument must be the source user mention or numeric user ID.")
    source_user_id = str(parsed_source_user_id)

    if args[1].lower() == "everyone":
        reason = " ".join(args[2:]).strip()
        return source_user_id, [], True, reason

    recipient_user_ids: list[str] = []
    reason_index = 1
    for index, token in enumerate(args[1:], start=1):
        parsed_user_id = parse_user_token(token)
        if parsed_user_id is None:
            reason_index = index
            break
        recipient_user_id = str(parsed_user_id)
        if recipient_user_id != source_user_id and recipient_user_id not in recipient_user_ids:
            recipient_user_ids.append(recipient_user_id)
        reason_index = index + 1

    if not recipient_user_ids:
        raise ValueError("Add at least one recipient mention or use `everyone`.")
    reason = " ".join(args[reason_index:]).strip()
    return source_user_id, recipient_user_ids, False, reason


def _format_amount_cents(amount_cents: int) -> str:
    absolute = abs(amount_cents)
    whole = absolute // 100
    cents = absolute % 100
    if cents:
        return f"{whole:,}.{cents:02d}"
    return f"{whole:,}"


def _format_signed_amount_cents(amount_cents: int) -> str:
    if amount_cents == 0:
        return "$0"
    sign = "+" if amount_cents > 0 else "-"
    return f"{sign}${_format_amount_cents(amount_cents)}"


def _current_weekly_payout_window():
    local_now = utcnow().astimezone(UTC_PLUS_8)
    iso_year, iso_week, iso_weekday = local_now.isocalendar()
    week_start = (local_now - timedelta(days=iso_weekday - 1)).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    week_end = week_start + timedelta(days=7)
    week_key = f"{iso_year}-W{iso_week:02d}"
    return week_key, week_start.astimezone(timezone.utc), week_end.astimezone(timezone.utc)


def _weekly_winner_line(entry: LeaderboardEntry | None, bonus_cents: int) -> str:
    if entry is None:
        return "n/a"
    profit = entry.total_sales - entry.procurement_cost
    return (
        f"<@{entry.user_id}> | Sales ${_format_amount_cents(entry.total_sales)} | "
        f"Profit ${_format_amount_cents(profit)} | Receipts {entry.receipt_count} | "
        f"Bonus ${_format_amount_cents(bonus_cents)}"
    )


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


