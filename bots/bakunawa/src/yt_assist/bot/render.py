"""Discord-style output rendering for the Python parity port."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from yt_assist.domain.catalog import CatalogItem
from yt_assist.domain.contracts import Contract
from yt_assist.domain.models import (
    AccountingPolicy,
    LeaderboardEntry,
    PayoutEntry,
    PersistedReceipt,
    PricedItem,
    ReceiptStatus,
    ReceiptSummary,
    StatsSort,
)
from yt_assist.domain.proof import split_proof_values

RECEIPTS_PER_PAGE = 10
THEME_SUCCESS = 0x2BA17E
THEME_INFO = 0x4B6BFB
THEME_WARNING = 0xD97706
THEME_ERROR = 0xC1121F
THEME_LIFECYCLE_ONLINE = 0x7C3AED
LIFECYCLE_STATUS_TITLE = "Bakunawa Mech Status"
LIFECYCLE_STATS_TITLE = "Bakunawa Mech Live Stats"
HELP_PAGE_COUNT = 4
STAFF_PAYOUT_PERCENT = 60
STAFF_PAYOUT_BPS = STAFF_PAYOUT_PERCENT * 100

BUTTON_STYLE_PRIMARY = 1
BUTTON_STYLE_SECONDARY = 2
BUTTON_STYLE_SUCCESS = 3
BUTTON_STYLE_DANGER = 4


def format_money(amount: int) -> str:
    return f"{amount:,}"


def _display_item_name(name: str) -> str:
    replacements = {
        "SEMI SLICK": "Semi-Slick",
        "OFFROAD": "Off-Road",
        "AWD": "AWD",
        "FWD": "FWD",
        "RWD": "RWD",
        "EV": "EV",
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
        title = title.replace(raw.title(), display)
    return title


def _material_line_sort_key(line: str) -> tuple[int, str]:
    text = line.removeprefix("- ").strip()
    quantity = 1
    marker = text.split(maxsplit=1)[0].lower() if text else ""
    if marker.endswith("x"):
        raw_quantity = marker[:-1]
        if raw_quantity.isdigit():
            quantity = int(raw_quantity)
        elif raw_quantity == "??":
            quantity = 0
    return (-quantity, text.lower())


def _display_material_line(name: str) -> str:
    parts = name.split(maxsplit=1)
    if parts and parts[0].lower().endswith("x") and len(parts) == 2:
        return f"{parts[0]} {_display_item_name(parts[1])}"
    return _display_item_name(name)


@dataclass(slots=True)
class EmbedFooter:
    text: str

    def to_dict(self) -> dict[str, Any]:
        return {"text": self.text}


@dataclass(slots=True)
class EmbedField:
    name: str
    value: str
    inline: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "value": self.value, "inline": self.inline}


@dataclass(slots=True)
class EmbedPayload:
    title: str
    description: str
    color: int
    fields: list[EmbedField] = field(default_factory=list)
    footer: EmbedFooter | None = None
    image_url: str | None = None

    def field(self, name: str, value: str, inline: bool) -> EmbedPayload:
        self.fields.append(EmbedField(name=name, value=value, inline=inline))
        return self

    def with_footer(self, text: str) -> EmbedPayload:
        self.footer = EmbedFooter(text=text)
        return self

    def with_image(self, url: str) -> EmbedPayload:
        self.image_url = url
        return self

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "title": self.title,
            "description": self.description,
            "color": self.color,
        }
        if self.fields:
            payload["fields"] = [field.to_dict() for field in self.fields]
        if self.footer is not None:
            payload["footer"] = self.footer.to_dict()
        if self.image_url is not None:
            payload["image"] = {"url": self.image_url}
        return payload


@dataclass(slots=True)
class ButtonPayload:
    custom_id: str
    label: str
    style: int
    disabled: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": 2,
            "custom_id": self.custom_id,
            "label": self.label,
            "style": self.style,
            "disabled": self.disabled,
        }


@dataclass(slots=True)
class ActionRowPayload:
    components: list[ButtonPayload]

    def to_dict(self) -> dict[str, Any]:
        return {"type": 1, "components": [component.to_dict() for component in self.components]}


@dataclass(slots=True)
class ReplyPayload:
    content: str | None = None
    embeds: list[EmbedPayload] = field(default_factory=list)
    components: list[ActionRowPayload] = field(default_factory=list)
    ephemeral: bool = False
    attachment_paths: list[str] = field(default_factory=list)
    silent_mentions: bool = False

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"ephemeral": self.ephemeral}
        if self.content is not None:
            payload["content"] = self.content
        if self.embeds:
            payload["embeds"] = [embed.to_dict() for embed in self.embeds]
        if self.components:
            payload["components"] = [row.to_dict() for row in self.components]
        if self.attachment_paths:
            payload["attachment_paths"] = list(self.attachment_paths)
        if self.silent_mentions:
            payload["silent_mentions"] = True
        return payload


@dataclass(slots=True)
class ReceiptDisplayContext:
    recorded_by_label: str | None = None
    accounting_policy: AccountingPolicy | None = None


def panel_embed(title: str, description: str) -> EmbedPayload:
    return EmbedPayload(title=title, description=description, color=THEME_SUCCESS)


def info_panel_embed(title: str, description: str) -> EmbedPayload:
    return EmbedPayload(title=title, description=description, color=THEME_INFO)


def warning_panel_embed(title: str, description: str) -> EmbedPayload:
    return EmbedPayload(title=title, description=description, color=THEME_WARNING)


def error_panel_embed(title: str, description: str) -> EmbedPayload:
    return EmbedPayload(title=title, description=description, color=THEME_ERROR)


def lifecycle_status_embed(state: str, description: str, *, channel_role: str = "main") -> EmbedPayload:
    normalized = state.strip().lower()
    if normalized in {"starting", "starting_up"}:
        label = "Starting Up"
        color = THEME_INFO
    elif normalized in {"online", "ready"}:
        label = "Online"
        color = THEME_LIFECYCLE_ONLINE
    elif normalized in {"stopped", "offline"}:
        label = "Stopped"
        color = THEME_ERROR
    else:
        label = "Shutting Down"
        color = THEME_WARNING

    if channel_role == "admin":
        field_name = "Admin Commands"
        field_value = (
            "`bm!manage` - open receipt manager\n"
            "`bm!payouts` - view payout totals\n"
            "`bm!reset` - close out active receipts\n"
            "`bm!rebuildlogs` - rebuild log threads"
        )
    elif channel_role == "log":
        field_name = "Log Commands"
        field_value = (
            "`bm!stats` - view all employee stats\n"
            "`bm!fixpreviews` - repair proof previews\n"
            "`bm!rebuildlogs` - rebuild log threads\n"
            "`bm!cleanlog` - clean log-channel clutter"
        )
    else:
        field_name = "Common Commands"
        field_value = (
            "`bm!calc` - create a new order\n"
            "`bm!stats` - view all employee's stats\n"
            "`bm!templates` - view current templates\n"
            "`bm!pricesheet` - view pricesheet"
        )

    return (
        EmbedPayload(
            title=LIFECYCLE_STATUS_TITLE,
            description=description,
            color=color,
        )
        .field("State", label, True)
        .field(field_name, field_value, False)
    )


def lifecycle_stats_embed(entries: list[LeaderboardEntry]) -> EmbedPayload:
    total_sales = sum(entry.total_sales for entry in entries)
    total_company_cost = sum(entry.procurement_cost for entry in entries)
    total_profit = total_sales - total_company_cost
    total_receipts = sum(entry.receipt_count for entry in entries)
    top_lines = (
        "No receipts counted in stats yet."
        if not entries
        else "\n".join(
            f"{index}. <@{entry.user_id}> | Sales ${format_money(entry.total_sales)} | Receipts {entry.receipt_count}"
            for index, entry in enumerate(entries[:5], start=1)
        )
    )
    return (
        EmbedPayload(
            title=LIFECYCLE_STATS_TITLE,
            description=top_lines,
            color=THEME_INFO,
        )
        .field("Employees", str(len(entries)), True)
        .field("Receipts", str(total_receipts), True)
        .field("Gross Profit", f"${format_money(total_profit)}", True)
        .field(
            "Totals",
            (
                f"Total Sales ${format_money(total_sales)}\n"
                f"Company Cost ${format_money(total_company_cost)}\n"
                f"Total Staff Payout ${format_money_cents(_staff_payout_cents(total_profit))}"
            ),
            False,
        )
    )


def render_stats_description(entries: list[LeaderboardEntry]) -> str:
    if not entries:
        return "No receipts counted in stats yet."
    return "\n".join(
        (
            f"{index}. <@{entry.user_id}> | Total Sales ${format_money(entry.total_sales)} | "
            f"Company Cost ${format_money(entry.procurement_cost)} | Receipts {entry.receipt_count}"
        )
        for index, entry in enumerate(entries, start=1)
    )


def render_payout_description(entries: list[PayoutEntry]) -> str:
    if not entries:
        return "No unpaid receipts waiting for payout."
    lines: list[str] = []
    for index, entry in enumerate(entries, start=1):
        if entry.adjustment_cents:
            lines.append(
                f"{index}. <@{entry.user_id}> | Raw ${format_money_cents(entry.total_payout_cents)} | "
                f"Offset {_format_signed_money_cents(entry.adjustment_cents)} | "
                f"Final **${format_money_cents(entry.adjusted_total_payout_cents)}**"
            )
            continue
        lines.append(f"{index}. <@{entry.user_id}> | Final **${format_money_cents(entry.adjusted_total_payout_cents)}**")
    return "\n".join(lines)


def render_pricesheet_description(items: list[CatalogItem]) -> str:
    active_items = [item for item in items if item.active]
    if not active_items:
        return "No active catalog items are configured."
    lines: list[str] = []
    for index, item in enumerate(active_items, start=1):
        cost = f"Cost ${format_money(item.unit_cost)}" if item.unit_cost is not None else "Cost n/a"
        pending = " | Price pending" if item.price_pending else ""
        category = f" | {item.category}" if item.category else ""
        lines.append(
            f"{index}. {_display_item_name(item.name)}{category} | Unit ${format_money(item.unit_price)} | {cost}{pending}"
        )
    return "\n".join(lines)


def render_contracts_description(contracts: list[Contract]) -> str:
    if not contracts:
        return "No contracts are configured yet."
    lines: list[str] = []
    for index, contract in enumerate(contracts, start=1):
        prices = " | ".join(
            f"{price.item_name} ${format_money(price.unit_price)}" for price in contract.prices
        )
        lines.append(f"{index}. {contract.name} | {prices}")
    return "\n".join(lines)


def format_money_cents(amount_cents: int) -> str:
    is_negative = amount_cents < 0
    absolute = abs(amount_cents)
    whole = absolute // 100
    cents = absolute % 100
    prefix = "-" if is_negative else ""
    if cents:
        return f"{prefix}{format_money(whole)}.{cents:02d}"
    return f"{prefix}{format_money(whole)}"


def _format_signed_money_cents(amount_cents: int) -> str:
    if amount_cents == 0:
        return "$0"
    if amount_cents > 0:
        return f"+${format_money_cents(amount_cents)}"
    return f"-${format_money_cents(abs(amount_cents))}"


def _staff_payout_cents(profit: int) -> int:
    return profit * STAFF_PAYOUT_BPS // 100


def _company_net_profit_after_staff_payout_cents(profit: int, total_payout_cents: int) -> int:
    return profit * 100 - total_payout_cents


def _clamp_help_page(page: int) -> int:
    return min(page, HELP_PAGE_COUNT - 1)


def _help_page_title(page: int) -> str:
    page = _clamp_help_page(page)
    if page == 0:
        return "Main Channel"
    if page == 1:
        return "Admin Channel"
    if page == 2:
        return "Receipt Actions"
    return "Workflow Tips"


def help_page_embed(prefix: str, page: int) -> EmbedPayload:
    page = _clamp_help_page(page)
    embed = panel_embed(
        f"Bakunawa Mech Help • {_help_page_title(page)}",
        f"Use `{prefix}[command]` or the matching slash command.",
    )
    if page == 0:
        embed.field(
            "Everyday Commands",
            (
                f"`{prefix}calc [@user]` / `/mechcalc [user]` - Open the upgrade receipt panel\n"
                f"`{prefix}stats` / `/mechstats` - Show the leaderboard for 1 minute\n"
                f"`{prefix}pricesheet` / `/mechpricesheet` - Show the current catalog price sheet\n"
                f"`{prefix}note <receipt_id> <note>` / `/mechnote` - Add or update a receipt note\n"
                f"`{prefix}help` / `/mechhelp` - Open this paged help panel\n"
                f"`{prefix}health` / `/mechealth` - Check bot setup and permissions"
            ),
            False,
        ).field(
            "Where To Use Them",
            "Use these in the main channel for everyday receipt work.",
            False,
        )
    elif page == 1:
        embed.field(
            "Admin Commands",
            (
                f"`{prefix}manage` / `/mechmanage` - Open the receipt manager\n"
                f"`{prefix}payouts` / `/mechpayouts` - Show staff payout totals\n"
                f"`{prefix}payoutoffset` / `/mechpayoutoffset` - Add a payout-time credit or deduction\n"
                f"`{prefix}payoutsplit` / `/mechpayoutsplit` - Split one user's payout across other staff\n"
                f"`{prefix}refresh` / `/mechrefresh` - Reload catalog and package files from disk\n"
                f"`{prefix}templates [reload]` / `/mechtemplates [reload]` - Show live announcement commands or reload the templates JSON\n"
                f"`{prefix}reset` / `/mechreset` - Backup active receipts, then mark them paid or invalidate them\n"
                f"`{prefix}export` / `/mechexport` - Export the current database as JSON\n"
                f"`{prefix}import [file] [mode]` / `/mechimport` - Import a reviewed export\n"
                f"`{prefix}rebuildlogs` / `/mechrebuildlogs` - Rebuild the receipt log channel from the database\n"
                f"`{prefix}fixpreviews` / `/mechfixpreviews` - Refresh old proof previews from saved files\n"
                f"`{prefix}cleanlog` / `/mechcleanlog` - Clean top-level clutter from the log channel\n"
                f"`{prefix}clean` / `/mechclean` - Delete non-log messages from the current channel or thread\n"
                f"`{prefix}restartbot` / `/mechrestartbot` - Restart the bot remotely\n"
                f"`{prefix}stop` / `/mechstop` - Shut the bot down gracefully"
            ),
            False,
        ).field(
            "Where To Use Them",
            "Use these in the admin channel for corrections, payouts, imports, exports, and maintenance.",
            False,
        )
    elif page == 2:
        embed.field(
            "Main Channel Receipt Cards",
            "`EDIT`, `INVALIDATE`, `NOTE`\nAvailable to the receipt creator or admins.",
            False,
        ).field(
            "Admin Or Log Receipt Cards",
            "`REFRESH`, `NOTE`, `MARK PAID`, `MARK UNPAID`, `INVALIDATE`, `RESTORE UNPAID`\nAvailable to admins.",
            False,
        ).field(
            "Receipt Status Rules",
            "Active receipts count in payouts and stats.\nPaid receipts stay in stats only.\nInvalidated receipts are excluded.",
            False,
        )
    else:
        embed.field(
            "Workflow Tips",
            (
                "Use Add Package for Tier 1, Tier 2, Tier 3, or Full Maintenance jobs.\n"
                "Use Add Individual Items for one-off upgrades or materials.\n"
                "`[TIER 1] Full Cosmetics` lists vehicle-specific Cosmetic Parts and Extras Kit usage as `??x`.\n"
                "`[TIER 3] Full Tuning + Engine` inherits existing Full Tuning selections when present.\n"
                f"`{prefix}calc @user` lets an admin record a receipt on someone else's behalf."
            ),
            False,
        ).field(
            "Permissions",
            "Everyone: `calc`, `stats`, `pricesheet`, `note`, `help`, `health`\nAdmins only: `manage`, `payouts`, `payoutoffset`, `payoutsplit`, `refresh`, `templates`, `reset`, `export`, `import`, `rebuildlogs`, `fixpreviews`, `cleanlog`, `clean`, `restartbot`, `stop`",
            False,
        )
    return embed.with_footer(f"Page {page + 1}/{HELP_PAGE_COUNT} • Use Prev/Next to browse.")


def help_action_rows(owner_user_id: int, page: int) -> list[ActionRowPayload]:
    page = _clamp_help_page(page)
    return [
        ActionRowPayload(
            components=[
                ButtonPayload(
                    custom_id=f"help|page|{owner_user_id}|{max(page - 1, 0)}",
                    label="Prev",
                    style=BUTTON_STYLE_PRIMARY,
                    disabled=page == 0,
                ),
                ButtonPayload(
                    custom_id=f"help|page_display|{owner_user_id}|{page}",
                    label=f"{page + 1}/{HELP_PAGE_COUNT}",
                    style=BUTTON_STYLE_SECONDARY,
                    disabled=True,
                ),
                ButtonPayload(
                    custom_id=f"help|page|{owner_user_id}|{page + 1}",
                    label="Next",
                    style=BUTTON_STYLE_PRIMARY,
                    disabled=page + 1 >= HELP_PAGE_COUNT,
                ),
            ]
        )
    ]


def stats_embed(sort: StatsSort, entries: list[LeaderboardEntry]) -> EmbedPayload:
    total_sales = sum(entry.total_sales for entry in entries)
    total_procurement = sum(entry.procurement_cost for entry in entries)
    total_profit = total_sales - total_procurement
    total_payout_cents = _staff_payout_cents(total_profit)
    company_net_profit_after_staff_payout = _company_net_profit_after_staff_payout_cents(
        total_profit,
        total_payout_cents,
    )
    total_receipts = sum(entry.receipt_count for entry in entries)
    embed = (
        panel_embed("Bakunawa Mech Stats", render_stats_description(entries))
        .field("Sort", _stats_sort_label(sort), True)
        .field("Employees", str(len(entries)), True)
        .field(
            "Totals",
            (
                f"Total Sales ${format_money(total_sales)}\n"
                f"Company Cost ${format_money(total_procurement)}\n"
                f"Gross Profit ${format_money(total_profit)}\n"
                f"Total Staff Payout ${format_money_cents(total_payout_cents)}\n"
                f"Company Net Profit ${format_money_cents(company_net_profit_after_staff_payout)}\n"
                f"Receipts {total_receipts}"
            ),
            False,
        )
    )
    if entries:
        return embed.with_footer(
            "Showing active and paid receipts. Invalidated receipts are excluded."
        )
    return embed.with_footer("No receipts counted in stats yet.")


def payouts_embed(entries: list[PayoutEntry]) -> EmbedPayload:
    total_payout_cents = sum(entry.total_payout_cents for entry in entries)
    total_adjustment_cents = sum(entry.adjustment_cents for entry in entries)
    adjusted_total_payout_cents = sum(entry.adjusted_total_payout_cents for entry in entries)
    total_profit = sum(entry.profit for entry in entries)
    company_net_profit_after_staff_payout = _company_net_profit_after_staff_payout_cents(
        total_profit,
        adjusted_total_payout_cents,
    )
    embed = (
        panel_embed("Bakunawa Mech Payouts", render_payout_description(entries))
        .field("Employees", str(len(entries)), True)
        .field("Raw Staff Payout", f"${format_money_cents(total_payout_cents)}", True)
        .field("Net Adjustments", _format_signed_money_cents(total_adjustment_cents), True)
        .field("Final Staff Payout", f"${format_money_cents(adjusted_total_payout_cents)}", True)
        .field(
            "Company Net Profit",
            f"${format_money_cents(company_net_profit_after_staff_payout)}",
            True,
        )
    )
    if entries:
        return embed.with_footer("Showing unpaid receipts only. Paid receipts stay in stats.")
    return embed.with_footer("No unpaid receipts waiting for payout.")


def pricesheet_embed(items: list[CatalogItem]) -> EmbedPayload:
    return panel_embed(
        "Bakunawa Mech Price Sheet",
        render_pricesheet_description(items),
    ).with_footer("Showing active catalog items from the current catalog.")


def contracts_embed(contracts: list[Contract]) -> EmbedPayload:
    return panel_embed(
        "Bakunawa Mech Contracts",
        render_contracts_description(contracts),
    ).with_footer("Contracts apply preset special pricing to matching calculator items.")


def health_embed(
    checks: list[str],
    issues: list[str],
    message_content_intent: bool = False,
) -> EmbedPayload:
    status = "Healthy" if not issues else "Needs Attention"
    embed = (
        panel_embed("Bakunawa Mech Health", f"Status: {status}")
        if not issues
        else warning_panel_embed("Bakunawa Mech Health", f"Status: {status}")
    )
    embed.field("Checks", "\n".join(checks), False)
    embed.field("What To Fix", "No problems detected." if not issues else "\n".join(issues), False)
    direct_command_status = (
        "Direct text commands are enabled. Keep Message Content Intent enabled in the Discord developer portal."
        if message_content_intent
        else "Direct text commands are disabled in config. Use slash commands, or enable Message Content Intent before turning them on."
    )
    embed.field(
        "Direct Commands",
        direct_command_status,
        False,
    )
    return embed.with_footer(
        "Review both main/admin channel permissions when health reports issues."
    )


def task_status_embed(title: str, description: str) -> EmbedPayload:
    return panel_embed(title, description)


def task_warning_embed(title: str, description: str) -> EmbedPayload:
    return warning_panel_embed(title, description)


def task_error_embed(title: str, description: str) -> EmbedPayload:
    return error_panel_embed(title, description)


def calc_message_content(session) -> str:
    if not getattr(session, "rescan_active", False):
        return ""
    proof_urls = getattr(session, "payment_proof_source_url", None)
    urls = [line.strip() for line in (proof_urls or "").splitlines() if line.strip()]
    if not urls:
        return ""
    return "Original proof:\n" + "\n".join(urls)


def calc_reply_payload(catalog_items: list[CatalogItem], session) -> ReplyPayload:
    return ReplyPayload(
        content=calc_message_content(session),
        embeds=calc_embeds(catalog_items, session),
        components=calc_action_rows(
            session.user_id,
            session.awaiting_proof,
            session.rescan_active,
        ),
        ephemeral=True,
        attachment_paths=split_proof_values(session.payment_proof_path),
    )


def calc_embeds(catalog_items: list[CatalogItem], session) -> list[EmbedPayload]:
    embeds = [calc_embed(catalog_items, session)]
    embeds.extend(
        proof_preview_embeds(
            session.payment_proof_source_url,
            "Bakunawa Mech Receipt Proof",
            session.payment_proof_path,
        )
    )
    return embeds


def calc_embed(catalog_items: list[CatalogItem], session) -> EmbedPayload:
    embed = panel_embed("Bakunawa Mech Calculator", render_session_description(catalog_items, session))
    embed.color = 0xDAA520 if session.awaiting_proof else THEME_SUCCESS
    if session.awaiting_proof:
        return embed.with_footer("Awaiting proof upload in this channel.")
    return embed.with_footer(
        "Use Add Package or Add Individual Items, then Print Receipt when ready."
    )


def calc_processing_embed(catalog_items: list[CatalogItem], session, status: str) -> EmbedPayload:
    return calc_embed_for_status(
        catalog_items,
        session,
        status,
        "Processing proof and saving receipt...",
        THEME_WARNING,
    )


def calc_failure_embed(catalog_items: list[CatalogItem], session, status: str) -> EmbedPayload:
    return calc_embed_for_status(
        catalog_items,
        session,
        status,
        "Upload another proof image in this channel to try again, or press Cancel.",
        THEME_WARNING,
    )


def calc_completed_embed(receipt_id: str) -> EmbedPayload:
    return panel_embed(
        "Bakunawa Mech Calculator",
        (
            f"Receipt Saved: `{receipt_id}`.\n"
            "You may view it to edit or invalidate this receipt.\n"
            "This message will be deleted in 10 seconds."
        ),
    ).with_footer("Receipt saved.")


def calc_timeout_warning_embed(catalog_items: list[CatalogItem], session) -> EmbedPayload:
    return calc_embed_for_status(
        catalog_items,
        session,
        f"Idle warning. {session_timeout_prompt(session)} Continue within 10 minutes or this receipt will close.",
        "Choose Still Working to reset the idle timer, or Close Receipt to remove it now.",
        THEME_WARNING,
    )


def calc_embed_for_status(
    catalog_items: list[CatalogItem],
    session,
    status: str,
    footer: str,
    color: int,
) -> EmbedPayload:
    description = render_session_description(catalog_items, session, force_not_awaiting=True)
    embed = EmbedPayload(
        title="Bakunawa Mech Calculator",
        description=f"{description}\n\n**Status:** {status}",
        color=color,
    )
    return embed.with_footer(footer)


def render_session_description(
    catalog_items: list[CatalogItem],
    session,
    *,
    force_not_awaiting: bool = False,
) -> str:
    if not session.items:
        return (
            f"**Credited To:** {session.credited_display_name} ({session.credited_user_id})\n"
            "**Total Sale:** $0\n"
            "**Company Cost:** $0\n"
            "**Profit:** $0\n"
            f"**Staff Pay ({STAFF_PAYOUT_PERCENT}% Profit):** $0\n\n"
            f"**Proof:** {session_proof_status(session)}\n\n"
            "**Billable Items:**\n"
            "- No items yet"
        )

    from yt_assist.domain.pricing import price_items

    class _Catalog:
        def __init__(self, items: list[CatalogItem]) -> None:
            self.items = items

        def find_item(self, input_text: str):
            for item in self.items:
                names = [item.name, *item.aliases]
                for name in names:
                    if name.lower() == input_text.lower():
                        return item
            return None

    priced = price_items(_Catalog(catalog_items), session.items)
    billable_lines: list[str] = []
    material_lines: list[str] = []
    for index, item in enumerate(priced.items, start=1):
        source = ""
        if item.pricing_source.value == "override":
            source = " [Package Material]" if item.line_sale_total == 0 else " [Special Pricing]"
        if (
            item.line_sale_total == 0
            and item.line_cost_total == 0
            and item.pricing_source.value == "override"
        ):
            material_lines.append(f"- {_display_material_line(item.item_name)}")
        else:
            pending = " [Pricing Pending]" if item.unit_sale_price == 0 else ""
            billable_lines.append(
                f"{index}. {item.item_name}{source}{pending} "
                f"(Sale: ${format_money(item.line_sale_total)}, Cost: ${format_money(item.line_cost_total)})"
            )

    workflow_notice = (
        f"\n**Workflow:** {session.workflow_notice}\n" if session.workflow_notice else ""
    )
    awaiting = ""
    if session.awaiting_proof and not force_not_awaiting:
        awaiting = "\n\n**Status:** Waiting for proof image upload in this channel."
    billable_text = "\n".join(billable_lines) if billable_lines else "- No billable items yet"
    material_text = (
        "\n".join(sorted(material_lines, key=_material_line_sort_key))
        if material_lines
        else "- No package materials yet"
    )
    staff_pay_cents = _staff_payout_cents(priced.profit)

    return (
        f"**Credited To:** {session.credited_display_name} ({session.credited_user_id})\n"
        f"**Total Sale:** ${format_money(priced.total_sale)}\n"
        f"**Company Cost:** ${format_money(priced.procurement_cost)}\n"
        f"**Profit:** ${format_money(priced.profit)}\n"
        f"**Staff Pay ({STAFF_PAYOUT_PERCENT}% Profit):** ${format_money_cents(staff_pay_cents)}\n"
        f"**Proof:** {session_proof_status(session)}\n"
        f"{workflow_notice}"
        f"**Billable Items:**\n{billable_text}\n\n"
        f"**Estimated Required Materials / Items:**\n{material_text}"
        f"{awaiting}"
    )


def session_proof_status(session) -> str:
    count = len([line for line in (session.payment_proof_source_url or "").splitlines() if line.strip()])
    if count == 0 and session.awaiting_proof:
        return "Waiting for upload"
    if count == 0:
        return "None"
    if count == 1:
        return "Attached (1 image)"
    return f"Attached ({count} images)"


def session_timeout_prompt(session) -> str:
    if getattr(session, "awaiting_proof", False):
        return "This receipt is waiting for proof upload."
    if getattr(session, "proof_processing", False):
        return "This receipt is finishing proof processing."
    if getattr(session, "rescan_active", False):
        return "This receipt is reviewing a rescan candidate."
    if not getattr(session, "items", []):
        return "This receipt is still waiting for package or item selection."
    return "This receipt is waiting for final review or Print Receipt."


def calc_action_rows(
    user_id: int,
    awaiting_proof: bool,
    rescan_active: bool,
) -> list[ActionRowPayload]:
    rows = [
        ActionRowPayload(
            components=[
                ButtonPayload(
                    custom_id=f"calc|package|{user_id}",
                    label="Add Package",
                    style=BUTTON_STYLE_PRIMARY,
                    disabled=awaiting_proof,
                ),
                ButtonPayload(
                    custom_id=f"calc|add|{user_id}",
                    label="Add Individual Items",
                    style=BUTTON_STYLE_PRIMARY,
                    disabled=awaiting_proof,
                ),
                ButtonPayload(
                    custom_id=f"calc|remove|{user_id}",
                    label="Remove Selection",
                    style=BUTTON_STYLE_SECONDARY,
                    disabled=awaiting_proof,
                ),
            ]
        ),
        ActionRowPayload(
            components=[
                ButtonPayload(
                    custom_id=f"calc|print|{user_id}",
                    label="Print Receipt",
                    style=BUTTON_STYLE_SUCCESS,
                    disabled=awaiting_proof,
                ),
                ButtonPayload(
                    custom_id=f"calc|cancel|{user_id}",
                    label="Cancel Rescan" if rescan_active else "Cancel",
                    style=BUTTON_STYLE_DANGER,
                    disabled=False,
                ),
            ]
        ),
    ]
    if rescan_active:
        rows.append(
            ActionRowPayload(
                components=[
                    ButtonPayload(
                        custom_id=f"calc|rescan_skip|{user_id}",
                        label="Skip Candidate",
                        style=BUTTON_STYLE_SECONDARY,
                        disabled=awaiting_proof,
                    )
                ]
            )
        )
    return rows


def calc_timeout_action_rows(user_id: int) -> list[ActionRowPayload]:
    return [
        ActionRowPayload(
            components=[
                ButtonPayload(
                    custom_id=f"calc|keepalive|{user_id}",
                    label="Still Working",
                    style=BUTTON_STYLE_SUCCESS,
                ),
                ButtonPayload(
                    custom_id=f"calc|close|{user_id}",
                    label="Close Receipt",
                    style=BUTTON_STYLE_DANGER,
                ),
            ]
        )
    ]


def manage_page_parts(
    owner_user_id: int,
    page: int,
    receipts: list[ReceiptSummary],
) -> tuple[EmbedPayload, list[ActionRowPayload]]:
    description = (
        "No receipts recorded yet."
        if not receipts
        else "\n".join(
            f"{index}. `{receipt.id}` | {receipt.creator_display_name} | "
            f"${format_money(receipt.total_sale)} / ${format_money(receipt.procurement_cost)} | "
            f"{admin_receipt_status_label(receipt.status)}"
            for index, receipt in enumerate(receipts, start=1)
        )
    )

    rows: list[ActionRowPayload] = []
    for chunk_index in range(2):
        buttons: list[ButtonPayload] = []
        for slot in range(5):
            absolute_slot = chunk_index * 5 + slot
            enabled = absolute_slot < len(receipts)
            buttons.append(
                ButtonPayload(
                    custom_id=f"manage|view|{owner_user_id}|{page}|{absolute_slot}",
                    label=str(absolute_slot + 1),
                    style=BUTTON_STYLE_SECONDARY,
                    disabled=not enabled,
                )
            )
        rows.append(ActionRowPayload(components=buttons))

    rows.append(
        ActionRowPayload(
            components=[
                ButtonPayload(
                    custom_id=f"manage|page|{owner_user_id}|{max(page - 1, 0)}",
                    label="Prev",
                    style=BUTTON_STYLE_PRIMARY,
                    disabled=page == 0,
                ),
                ButtonPayload(
                    custom_id=f"manage|page|{owner_user_id}|{page}",
                    label="Refresh",
                    style=BUTTON_STYLE_SECONDARY,
                    disabled=not receipts,
                ),
                ButtonPayload(
                    custom_id=f"manage|page|{owner_user_id}|{page + 1}",
                    label="Next",
                    style=BUTTON_STYLE_PRIMARY,
                    disabled=len(receipts) < RECEIPTS_PER_PAGE,
                ),
            ]
        )
    )

    embed = (
        panel_embed("Bakunawa Mech Receipt Manager", description)
        .field("Page", str(page + 1), True)
        .with_footer("Select a receipt number below to inspect or change status.")
    )
    return embed, rows


def receipt_detail_payload(
    receipt: PersistedReceipt,
    owner_user_id: int,
    display: ReceiptDisplayContext | None,
) -> ReplyPayload:
    description = "\n".join(_receipt_item_lines(receipt.items))
    base_embed = panel_embed(f"Bakunawa Mech Receipt {receipt.id}", description)
    base_embed.field(
        "Credited To",
        f"{receipt.creator_username} ({receipt.creator_user_id})",
        True,
    ).field("Total Sale", f"${format_money(receipt.total_sale)}", True).field(
        "Company Cost",
        f"${format_money(receipt.procurement_cost)}",
        True,
    ).field(
        "Profit",
        f"${format_money(receipt.profit)}",
        True,
    ).field(
        "Status",
        admin_receipt_status_label(receipt.status),
        True,
    ).field(
        "Finalized At",
        receipt.finalized_at.isoformat(),
        False,
    ).field(
        "Payment Proofs",
        payment_proof_field_value(receipt.payment_proof_source_url),
        False,
    ).with_footer(
        "Active receipts affect payouts. Paid receipts stay in stats only. Invalidated receipts are excluded. Replace Proof keeps the receipt ID."
    )
    if receipt.admin_note:
        base_embed.field("Receipt Note", _receipt_note_field_value(receipt.admin_note), False)
    apply_receipt_display_context(base_embed, display)

    buttons = [
        ButtonPayload(
            custom_id=f"receipt|detail_status|{owner_user_id}|{receipt.id}|{action[0]}",
            label=action[1],
            style=action[2],
        )
        for action in admin_receipt_actions(receipt.status)
    ]
    buttons.append(
        ButtonPayload(
            custom_id=f"receipt|proof|{owner_user_id}|{receipt.id}",
            label="Replace Proof",
            style=BUTTON_STYLE_PRIMARY,
        )
    )
    buttons.append(
        ButtonPayload(
            custom_id=f"receipt|detail_note|{owner_user_id}|{receipt.id}",
            label="NOTE",
            style=BUTTON_STYLE_SECONDARY,
        )
    )
    return ReplyPayload(
        embeds=[
            base_embed,
            *proof_preview_embeds(
                receipt.payment_proof_source_url,
                "Bakunawa Mech Receipt Proof",
                receipt.payment_proof_path,
            ),
        ],
        components=[ActionRowPayload(components=buttons)],
        attachment_paths=split_proof_values(receipt.payment_proof_path),
        ephemeral=True,
    )


def receipt_main_payload(
    receipt,
    display: ReceiptDisplayContext | None,
) -> ReplyPayload:
    embed = receipt_embed(
        receipt.id,
        (receipt.creator_username, receipt.creator_user_id),
        receipt.total_sale,
        receipt.procurement_cost,
        receipt.profit,
        receipt.items,
        display,
        getattr(receipt, "admin_note", None),
    )
    if getattr(receipt, "status", ReceiptStatus.ACTIVE) is not ReceiptStatus.ACTIVE:
        embed.field("Status", admin_receipt_status_label(receipt.status), True)
    return ReplyPayload(
        content=receipt_message_content(receipt.id, receipt.creator_user_id),
        embeds=[
            embed,
            *proof_preview_embeds(
                receipt.payment_proof_source_url,
                "Payment Proof",
                receipt.payment_proof_path,
            ),
        ],
        components=[ActionRowPayload(components=receipt_main_action_buttons(receipt.id, receipt.creator_user_id))],
        attachment_paths=split_proof_values(receipt.payment_proof_path),
        ephemeral=False,
    )


def receipt_log_payload(
    receipt,
    display: ReceiptDisplayContext | None,
) -> ReplyPayload:
    embed = receipt_embed(
        receipt.id,
        (receipt.creator_username, receipt.creator_user_id),
        receipt.total_sale,
        receipt.procurement_cost,
        receipt.profit,
        receipt.items,
        display,
        getattr(receipt, "admin_note", None),
    )
    embed.field("Status", admin_receipt_status_label(receipt.status), True)
    return ReplyPayload(
        content=receipt_message_content(receipt.id, receipt.creator_user_id),
        embeds=[
            embed,
            *proof_preview_embeds(
                receipt.payment_proof_source_url,
                "Payment Proof",
                receipt.payment_proof_path,
            ),
        ],
        components=[ActionRowPayload(components=receipt_log_action_buttons(receipt.id, receipt.creator_user_id, receipt.status))],
        attachment_paths=split_proof_values(receipt.payment_proof_path),
        silent_mentions=True,
        ephemeral=False,
    )


def receipt_main_action_buttons(receipt_id: str, creator_user_id: str) -> list[ButtonPayload]:
    return [
        ButtonPayload(
            custom_id=f"receipt|edit|{creator_user_id}|{receipt_id}",
            label="EDIT",
            style=BUTTON_STYLE_PRIMARY,
        ),
        ButtonPayload(
            custom_id=f"receipt|delete|{creator_user_id}|{receipt_id}",
            label="INVALIDATE",
            style=BUTTON_STYLE_DANGER,
        ),
        ButtonPayload(
            custom_id=f"receipt|note|{creator_user_id}|{receipt_id}",
            label="NOTE",
            style=BUTTON_STYLE_SECONDARY,
        ),
    ]


def receipt_log_action_buttons(
    receipt_id: str,
    creator_user_id: str,
    status: ReceiptStatus,
) -> list[ButtonPayload]:
    buttons = [
        ButtonPayload(
            custom_id=f"receipt|log_refresh|{creator_user_id}|{receipt_id}",
            label="REFRESH",
            style=BUTTON_STYLE_SECONDARY,
        )
    ]
    buttons.append(
        ButtonPayload(
            custom_id=f"receipt|log_note|{receipt_id}",
            label="NOTE",
            style=BUTTON_STYLE_SECONDARY,
        )
    )
    buttons.extend(
        ButtonPayload(
            custom_id=f"receipt|log_status|{receipt_id}|{action[0]}",
            label=action[1],
            style=action[2],
        )
        for action in admin_receipt_actions(status)
    )
    return buttons


def receipt_message_content(receipt_id: str, creator_user_id: str) -> str:
    return f"Receipt `{receipt_id}` saved for <@{creator_user_id}>."


def receipt_embed(
    receipt_id: str,
    creator: tuple[str, str],
    total_sale: int,
    procurement_cost: int,
    profit: int,
    items: list[PricedItem],
    display: ReceiptDisplayContext | None,
    admin_note: str | None = None,
) -> EmbedPayload:
    creator_username, creator_user_id = creator
    items_text = "\n".join(_receipt_item_lines(items))
    description = (
        f"**Total Sale:** ${format_money(total_sale)}\n"
        f"**Company Cost:** ${format_money(procurement_cost)}\n"
        f"**Profit:** ${format_money(profit)}\n"
        f"**Staff Pay ({STAFF_PAYOUT_PERCENT}% Profit):** ${format_money_cents(_staff_payout_cents(profit))}\n"
        f"**Credited To:** {creator_username} ({creator_user_id})\n\n"
        f"**Items and Materials:**\n{items_text}"
    )
    embed = EmbedPayload(
        title=f"Bakunawa Mech Receipt {receipt_id}",
        description=description,
        color=THEME_SUCCESS,
    )
    if admin_note:
        embed.field("Receipt Note", _receipt_note_field_value(admin_note), False)
    apply_receipt_display_context(embed, display)
    return embed


def apply_receipt_display_context(
    embed: EmbedPayload,
    display: ReceiptDisplayContext | None,
) -> EmbedPayload:
    if display is None:
        return embed
    if display.recorded_by_label:
        embed.field("Recorded By", display.recorded_by_label, True)
    return embed


def accounting_policy_label(policy: AccountingPolicy) -> str:
    if policy is AccountingPolicy.PROCUREMENT_FUNDS:
        return "Company billed"
    return "Company billed"


def payment_proof_field_value(payment_proof_source_url: str | None) -> str:
    urls = [line.strip() for line in (payment_proof_source_url or "").splitlines() if line.strip()]
    return "\n".join(urls) if urls else "No payment proof URL recorded."


def _receipt_note_field_value(admin_note: str) -> str:
    note = admin_note.strip()
    if len(note) <= 1000:
        return note
    return f"{note[:997].rstrip()}..."


def proof_preview_embeds(
    proof_urls: str | None,
    title_prefix: str,
    proof_paths: str | None = None,
) -> list[EmbedPayload]:
    local_urls = [
        f"attachment://{path.name}"
        for path in (Path(value) for value in split_proof_values(proof_paths))
        if path.is_file() and path.name
    ]
    urls = local_urls or split_proof_values(proof_urls)
    return [
        info_panel_embed(f"{title_prefix} {index}", f"Open: {url}").with_image(url)
        for index, url in enumerate(urls, start=1)
    ]


def _receipt_item_lines(items: list[PricedItem]) -> list[str]:
    lines: list[str] = []
    for item in items:
        if (
            item.line_sale_total == 0
            and item.line_cost_total == 0
            and item.pricing_source.value == "override"
        ):
            lines.append(f"- {item.item_name}")
            continue
        pending = " [Pricing Pending]" if item.unit_sale_price == 0 else ""
        lines.append(
            f"- {item.item_name}{pending} "
            f"(Sale ${format_money(item.line_sale_total)}, Cost ${format_money(item.line_cost_total)})"
        )
    return lines or ["- No items recorded"]


def admin_receipt_actions(status: ReceiptStatus) -> list[tuple[str, str, int]]:
    if status is ReceiptStatus.ACTIVE:
        return [
            ("paid", "Mark Paid", BUTTON_STYLE_SUCCESS),
            ("invalidated", "Invalidate", BUTTON_STYLE_DANGER),
        ]
    if status is ReceiptStatus.PAID:
        return [
            ("active", "Mark Unpaid", BUTTON_STYLE_PRIMARY),
            ("invalidated", "Invalidate", BUTTON_STYLE_DANGER),
        ]
    return [
        ("active", "Restore Unpaid", BUTTON_STYLE_SUCCESS),
        ("paid", "Mark Paid", BUTTON_STYLE_PRIMARY),
    ]


def admin_receipt_status_label(status: ReceiptStatus) -> str:
    if status is ReceiptStatus.ACTIVE:
        return "Active"
    if status is ReceiptStatus.PAID:
        return "Paid"
    return "Invalidated"


def _stats_sort_label(sort: StatsSort) -> str:
    if sort is StatsSort.PROCUREMENT:
        return "Company Cost"
    if sort is StatsSort.COUNT:
        return "Receipt Count"
    return "Sales"
