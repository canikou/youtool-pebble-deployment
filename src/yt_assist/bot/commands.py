"""Real command routing for the local parity runner."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from yt_assist.domain.backup import save_export_async
from yt_assist.domain.catalog import Catalog
from yt_assist.domain.contracts import Contracts
from yt_assist.domain.models import AuditEventInput, ReceiptStatus, StatsSort, utcnow
from yt_assist.domain.templates import (
    TemplateLoadStatus,
    current as current_templates,
    reload as reload_templates,
)

from .command_support import (
    SessionCreditTarget,
    custom_id_for_user,
    format_optional_actor,
    format_template_command_output,
    parse_positive_amount_text,
    parse_user_token,
    procurement_balance_message,
    procurement_ledger_success_message,
)
from .render import (
    BUTTON_STYLE_PRIMARY,
    ActionRowPayload,
    ButtonPayload,
    ReplyPayload,
    contracts_embed,
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
    "contracts": "contracts",
    "payouts": "payouts",
    "refresh": "refresh",
    "procurementcutover": "procurementcutover",
    "procurementfunds": "procurementfunds",
    "padd": "procurementfunds",
    "procurementreturn": "procurementreturn",
    "preturn": "procurementreturn",
    "procurementbalance": "procurementbalance",
    "pbal": "procurementbalance",
    "templates": "templates",
    "export": "export",
}

SLASH_COMMAND_ALIASES: dict[str, str] = {
    "ythelp": "help",
    "ythealth": "health",
    "ytstats": "stats",
    "ytpricesheet": "pricesheet",
    "ytcontracts": "contracts",
    "ytpayouts": "payouts",
    "ytrefresh": "refresh",
    "ytprocurementcutover": "procurementcutover",
    "ytprocurementfunds": "procurementfunds",
    "ytprocurementreturn": "procurementreturn",
    "ytprocurementbalance": "procurementbalance",
    "yttemplates": "templates",
    "ytexport": "export",
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
        events=[_send(_error_reply(ctx, "YouTool Access", "This command is not enabled in this channel."))],
    )
    result.events.extend(_schedule_transient_cleanup_events(ctx))
    raise _handled(result)


async def _ensure_admin(ctx: CommandContext) -> None:
    if ctx.is_admin():
        return
    result = CommandResult(
        canonical_name=None,
        events=[_send(_error_reply(ctx, "YouTool Access", "You are not an admin for this bot."))],
    )
    result.events.extend(_schedule_transient_cleanup_events(ctx))
    raise _handled(result)


async def _ensure_admin_channel(ctx: CommandContext) -> None:
    if ctx.is_admin_channel():
        return
    result = CommandResult(
        canonical_name=None,
        events=[_send(_error_reply(ctx, "YouTool Access", "This admin command is not enabled in this channel."))],
    )
    result.events.extend(_schedule_transient_cleanup_events(ctx))
    raise _handled(result)


async def _resolve_procurement_target(
    ctx: CommandContext,
    target_user_id: int | None,
    action_label: str,
) -> SessionCreditTarget:
    actor_user_id = ctx.actor.user_id
    resolved_user_id = target_user_id or actor_user_id
    if resolved_user_id != actor_user_id and not ctx.is_admin():
        result = CommandResult(
            canonical_name=None,
            events=[
                _send(
                    _error_reply(
                        ctx,
                        "YouTool Access",
                        f"Only admins can {action_label} for another user.",
                    )
                )
            ],
        )
        result.events.extend(_schedule_transient_cleanup_events(ctx))
        raise _handled(result)
    if resolved_user_id == actor_user_id:
        return SessionCreditTarget(
            user_id=str(actor_user_id),
            username=ctx.actor.username,
            display_name=ctx.actor.display_name,
        )
    return SessionCreditTarget.from_user_id(resolved_user_id)


async def _require_procurement_cutover(ctx: CommandContext):
    cutover = await ctx.runtime.database.procurement_cutover_state()
    if cutover is not None and cutover.cutover_at is not None:
        return cutover.cutover_at
    result = CommandResult(
        canonical_name=None,
        events=[
            _send(
                _error_reply(
                    ctx,
                    "YouTool Procurement",
                    (
                        "Procurement-funds mode has not been activated yet. "
                        f"An admin must run `{ctx.prefix}procurementcutover` first."
                    ),
                )
            )
        ],
    )
    result.events.extend(_schedule_transient_cleanup_events(ctx))
    raise _handled(result)


def _parse_optional_user(args: list[str]) -> int | None:
    if not args:
        return None
    return parse_user_token(args[0])


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
    reply = ReplyPayload(embeds=[pricesheet_embed(ctx.runtime.catalog.items)], ephemeral=False)
    result = CommandResult(canonical_name="pricesheet", events=[_send(reply)])
    result.events.extend(_schedule_transient_cleanup_events(ctx))
    return result


async def _handle_contracts(ctx: CommandContext, args: list[str]) -> CommandResult:
    await _ensure_allowed_channel(ctx)
    action = args[0].strip().lower() if args else None
    if action == "add":
        await _ensure_admin(ctx)
        reply = ReplyPayload(
            embeds=[task_status_embed("YouTool Contracts", "Press Add Contract to open the contract form.")],
            components=[
                ActionRowPayload(
                    components=[
                        ButtonPayload(
                            custom_id=custom_id_for_user("contracts|add", ctx.actor.user_id),
                            label="Add Contract",
                            style=BUTTON_STYLE_PRIMARY,
                        )
                    ]
                )
            ],
            ephemeral=ctx.is_interaction,
        )
        result = CommandResult(canonical_name="contracts", events=[_send(reply)])
        if ctx.is_interaction:
            result.events.extend(_cleanup_prefix_invocation_events(ctx))
        else:
            result.events.extend(_schedule_transient_cleanup_events(ctx))
        return result

    reply = ReplyPayload(
        embeds=[contracts_embed(ctx.runtime.contracts.entries)],
        ephemeral=False,
    )
    result = CommandResult(canonical_name="contracts", events=[_send(reply)])
    result.events.extend(_schedule_transient_cleanup_events(ctx))
    return result


async def _handle_payouts(ctx: CommandContext, args: list[str]) -> CommandResult:
    await _ensure_admin(ctx)
    await _ensure_admin_channel(ctx)
    target_user_id = _parse_optional_user(args)
    entries = await ctx.runtime.database.payouts(str(target_user_id) if target_user_id else None)
    reply = ReplyPayload(embeds=[payouts_embed(entries)], ephemeral=ctx.is_interaction)
    result = CommandResult(canonical_name="payouts", events=[_send(reply)])
    result.events.extend(_cleanup_prefix_invocation_events(ctx))
    return result


async def _handle_refresh(ctx: CommandContext, args: list[str]) -> CommandResult:
    del args
    await _ensure_admin(ctx)
    await _ensure_admin_channel(ctx)
    progress = ReplyPayload(
        embeds=[task_status_embed("YouTool Refresh", "Reloading catalog and contracts from disk now...")],
        ephemeral=ctx.is_interaction,
    )
    events = [_send(progress)]
    catalog = Catalog.load_from(ctx.runtime.config.storage.catalog_path)
    contracts = Contracts.load_from(ctx.runtime.config.storage.contracts_path, catalog)
    ctx.runtime.catalog = catalog
    ctx.runtime.contracts = contracts
    events.append(
        _edit(
            ReplyPayload(
                embeds=[
                    task_status_embed(
                        "YouTool Refresh",
                        (
                            "Runtime data refreshed.\n"
                            f"Catalog items loaded: {len(catalog.items)}\n"
                            f"Contracts loaded: {len(contracts.entries)}"
                        ),
                    )
                ],
                ephemeral=ctx.is_interaction,
            )
        )
    )
    return CommandResult(canonical_name="refresh", events=events)


async def _handle_procurement_cutover(ctx: CommandContext, args: list[str]) -> CommandResult:
    del args
    await _ensure_admin(ctx)
    await _ensure_admin_channel(ctx)
    events = _cleanup_prefix_invocation_events(ctx)
    existing = await ctx.runtime.database.procurement_cutover_state()
    if existing is not None:
        description = (
            "Procurement-funds mode is already active.\n"
            f"Cutover at: {existing.cutover_at.isoformat() if existing.cutover_at else 'unknown'}\n"
            f"Set by: {format_optional_actor(existing.actor_user_id, existing.actor_display_name)}\n"
            "New receipts finalized on or after that timestamp use procurement-funds accounting."
            if existing.cutover_at is not None
            else "Procurement cutover metadata exists, but no cutover timestamp is currently set."
        )
        events.append(
            _send(
                ReplyPayload(
                    embeds=[task_status_embed("YouTool Procurement Cutover", description)],
                    ephemeral=ctx.is_interaction,
                )
            )
        )
        return CommandResult(canonical_name="procurementcutover", events=events)

    cutover_at = utcnow()
    await ctx.runtime.database.set_procurement_cutover(
        cutover_at,
        str(ctx.actor.user_id),
        ctx.actor.display_name,
    )
    await ctx.runtime.database.insert_audit_event(
        AuditEventInput(
            actor_user_id=str(ctx.actor.user_id),
            actor_display_name=ctx.actor.display_name,
            action="procurement_cutover_command",
            target_receipt_id=None,
            detail_json={
                "cutover_at": cutover_at.isoformat(),
                "mode": "procurement_funds",
            },
        )
    )
    events.append(
        _send(
            ReplyPayload(
                embeds=[
                    task_status_embed(
                        "YouTool Procurement Cutover",
                        (
                            "Procurement-funds mode is now active.\n"
                            f"Cutover at: {cutover_at.isoformat()}\n"
                            "Legacy reimbursement remains preserved on older receipts.\n"
                            "New receipts finalized after this point will pay profit share only."
                        ),
                    )
                ],
                ephemeral=ctx.is_interaction,
            )
        )
    )
    return CommandResult(canonical_name="procurementcutover", events=events)


async def _handle_procurement_funds(ctx: CommandContext, args: list[str]) -> CommandResult:
    await _ensure_allowed_channel(ctx)
    if not args:
        raise ValueError(f"Missing amount. Example: `{ctx.prefix}procurementfunds 500000`.")
    amount = parse_positive_amount_text(args[0])
    target = await _resolve_procurement_target(
        ctx,
        parse_user_token(args[1]) if len(args) > 1 else None,
        "record procurement funds",
    )
    await _require_procurement_cutover(ctx)
    cutover_state = await ctx.runtime.database.procurement_cutover_state()

    await ctx.runtime.database.record_procurement_ledger_entry(
        target.user_id,
        amount,
        "withdrawal",
        None,
        str(ctx.actor.user_id),
        ctx.actor.display_name,
    )
    await ctx.runtime.database.insert_audit_event(
        AuditEventInput(
            actor_user_id=str(ctx.actor.user_id),
            actor_display_name=ctx.actor.display_name,
            action="procurement_funds_recorded",
            target_receipt_id=None,
            detail_json={
                "user_id": target.user_id,
                "user_display_name": target.display_name,
                "amount": amount,
                "direction": "withdrawal",
                "cutover_at": cutover_state.cutover_at.isoformat()
                if cutover_state is not None and cutover_state.cutover_at is not None
                else None,
            },
        )
    )
    balance = await ctx.runtime.database.procurement_balance(target.user_id)
    events = _cleanup_prefix_invocation_events(ctx)
    events.append(
        _send(
            ReplyPayload(
                embeds=[
                    task_status_embed(
                        "YouTool Procurement Funds",
                        procurement_ledger_success_message(
                            target,
                            amount,
                            "withdrawal",
                            ctx.actor.user_id,
                            balance,
                        ),
                    )
                ],
                ephemeral=ctx.is_interaction,
            )
        )
    )
    return CommandResult(canonical_name="procurementfunds", events=events)


async def _handle_procurement_return(ctx: CommandContext, args: list[str]) -> CommandResult:
    await _ensure_allowed_channel(ctx)
    if not args:
        raise ValueError(f"Missing amount. Example: `{ctx.prefix}procurementreturn 500000`.")
    amount = parse_positive_amount_text(args[0])
    target = await _resolve_procurement_target(
        ctx,
        parse_user_token(args[1]) if len(args) > 1 else None,
        "record a procurement return",
    )
    await _require_procurement_cutover(ctx)
    current_balance = await ctx.runtime.database.procurement_balance(target.user_id)
    if current_balance.available_total < amount:
        result = CommandResult(
            canonical_name="procurementreturn",
            events=[
                _send(
                    _error_reply(
                        ctx,
                        "YouTool Procurement Return",
                        (
                            f"{target.display_name} only has "
                            f"${max(current_balance.available_total, 0):,} of unspent company funds "
                            "available to return right now."
                        ),
                    )
                )
            ],
        )
        result.events.extend(_schedule_transient_cleanup_events(ctx))
        raise _handled(result)

    await ctx.runtime.database.record_procurement_ledger_entry(
        target.user_id,
        -amount,
        "return",
        None,
        str(ctx.actor.user_id),
        ctx.actor.display_name,
    )
    await ctx.runtime.database.insert_audit_event(
        AuditEventInput(
            actor_user_id=str(ctx.actor.user_id),
            actor_display_name=ctx.actor.display_name,
            action="procurement_return_recorded",
            target_receipt_id=None,
            detail_json={
                "user_id": target.user_id,
                "user_display_name": target.display_name,
                "amount": amount,
                "direction": "return",
            },
        )
    )
    balance = await ctx.runtime.database.procurement_balance(target.user_id)
    events = _cleanup_prefix_invocation_events(ctx)
    events.append(
        _send(
            ReplyPayload(
                embeds=[
                    task_status_embed(
                        "YouTool Procurement Return",
                        procurement_ledger_success_message(
                            target,
                            amount,
                            "return",
                            ctx.actor.user_id,
                            balance,
                        ),
                    )
                ],
                ephemeral=ctx.is_interaction,
            )
        )
    )
    return CommandResult(canonical_name="procurementreturn", events=events)


async def _handle_procurement_balance(ctx: CommandContext, args: list[str]) -> CommandResult:
    await _ensure_allowed_channel(ctx)
    target = await _resolve_procurement_target(
        ctx,
        parse_user_token(args[0]) if args else None,
        "view procurement balance",
    )
    cutover = await ctx.runtime.database.procurement_cutover_state()
    balance = await ctx.runtime.database.procurement_balance(target.user_id)
    events = _cleanup_prefix_invocation_events(ctx)
    events.append(
        _send(
            ReplyPayload(
                embeds=[
                    task_status_embed(
                        "YouTool Procurement Balance",
                        procurement_balance_message(target, cutover, balance),
                    )
                ],
                ephemeral=ctx.is_interaction,
            )
        )
    )
    return CommandResult(canonical_name="procurementbalance", events=events)


async def _handle_templates(ctx: CommandContext, args: list[str]) -> CommandResult:
    await _ensure_admin(ctx)
    await _ensure_allowed_channel(ctx)
    action = args[0].strip().lower() if args else None
    if action == "reload":
        await _ensure_admin_channel(ctx)
        report = reload_templates()
        if report.status is TemplateLoadStatus.LOADED:
            description = (
                f"Templates reloaded from `{report.path}`.\n"
                f"Run `{ctx.prefix}templates` to inspect the live commands."
            )
            embed = task_status_embed("YouTool Templates", description)
        elif report.status is TemplateLoadStatus.CREATED_DEFAULT_FILE:
            description = (
                f"Templates file was missing, so a default file was created at `{report.path}`.\n"
                f"Run `{ctx.prefix}templates` to inspect the live commands."
            )
            embed = task_status_embed("YouTool Templates", description)
        else:
            description = (
                f"{report.warning or 'Templates JSON was malformed.'}\n"
                "The bot stayed online and is currently serving default templates until the JSON is fixed and reloaded."
            )
            embed = task_warning_embed("YouTool Templates", description)
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
                        "YouTool Templates",
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

    templates = current_templates()
    if templates is None:
        raise RuntimeError("templates are not initialized")
    admin_channel = ctx.is_admin_channel()
    reply = ReplyPayload(
        content=format_template_command_output(templates),
        embeds=[
            task_status_embed(
                "YouTool Templates",
                (
                    f"Live templates from `{ctx.runtime.config.storage.templates_path}`.\n"
                    f"Edit the JSON file, then run `{ctx.prefix}templates reload` to refresh without recompiling."
                ),
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
    reply = ReplyPayload(embeds=[health_embed(checks, issues)], ephemeral=ctx.is_interaction)
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
                embeds=[task_status_embed("YouTool Export", "Exporting the current database now...")],
                ephemeral=ctx.is_interaction,
            )
        )
    ]
    bundle = await ctx.runtime.database.export_bundle(ctx.runtime.catalog)
    path = await save_export_async(bundle, ctx.runtime.config.storage.export_dir, "yt-assist-export")
    events.append(
        _edit(
            ReplyPayload(
                embeds=[
                    task_status_embed(
                        "YouTool Export",
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
                        embeds=[task_error_embed("YouTool Help", f"Unknown component `{custom_id}`.")],
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
                        embeds=[task_error_embed("YouTool Help", "This help panel belongs to another user.")],
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

    if stripped.startswith(ctx.prefix):
        body = stripped[len(ctx.prefix) :].strip()
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
                        embeds=[task_error_embed("YouTool Help", f"Did you mean `{ctx.prefix}{suggestion}`?")]
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
    "contracts": _handle_contracts,
    "payouts": _handle_payouts,
    "refresh": _handle_refresh,
    "procurementcutover": _handle_procurement_cutover,
    "procurementfunds": _handle_procurement_funds,
    "procurementreturn": _handle_procurement_return,
    "procurementbalance": _handle_procurement_balance,
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
