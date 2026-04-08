"""Discord-style output rendering for the Python parity port."""

from __future__ import annotations

from dataclasses import dataclass, field
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

RECEIPTS_PER_PAGE = 10
THEME_SUCCESS = 0x2BA17E
THEME_INFO = 0x4B6BFB
THEME_WARNING = 0xD97706
THEME_ERROR = 0xC1121F
THEME_LIFECYCLE_ONLINE = 0x7C3AED
LIFECYCLE_STATUS_TITLE = "YouTool Status"
HELP_PAGE_COUNT = 4

BUTTON_STYLE_PRIMARY = 1
BUTTON_STYLE_SECONDARY = 2
BUTTON_STYLE_SUCCESS = 3
BUTTON_STYLE_DANGER = 4


def format_money(amount: int) -> str:
    return f"{amount:,}"


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

    def field(self, name: str, value: str, inline: bool) -> "EmbedPayload":
        self.fields.append(EmbedField(name=name, value=value, inline=inline))
        return self

    def with_footer(self, text: str) -> "EmbedPayload":
        self.footer = EmbedFooter(text=text)
        return self

    def with_image(self, url: str) -> "EmbedPayload":
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
    suppress_mentions: bool = False

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "ephemeral": self.ephemeral,
            "suppress_mentions": self.suppress_mentions,
        }
        if self.content is not None:
            payload["content"] = self.content
        if self.embeds:
            payload["embeds"] = [embed.to_dict() for embed in self.embeds]
        if self.components:
            payload["components"] = [row.to_dict() for row in self.components]
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


def lifecycle_status_embed(state: str, description: str) -> EmbedPayload:
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

    return (
        EmbedPayload(
            title=LIFECYCLE_STATUS_TITLE,
            description=description,
            color=color,
        ).field("State", label, True)
    )


def render_stats_description(entries: list[LeaderboardEntry]) -> str:
    if not entries:
        return "No receipts counted in stats yet."
    return "\n".join(
        (
            f"{index}. <@{entry.user_id}> | Total Sales ${format_money(entry.total_sales)} | "
            f"Total Procurement ${format_money(entry.procurement_cost)} | Receipts {entry.receipt_count}"
        )
        for index, entry in enumerate(entries, start=1)
    )


def render_payout_description(entries: list[PayoutEntry]) -> str:
    if not entries:
        return "No unpaid receipts waiting for payout."
    lines: list[str] = []
    for index, entry in enumerate(entries, start=1):
        base_pay = format_half_money(entry.total_payout_half_units)
        if entry.company_balance == 0:
            lines.append(f"{index}. <@{entry.user_id}> | Pay: **${base_pay}**")
            continue
        if entry.company_balance > 0:
            adjustment = f"-${format_money(entry.company_balance)}"
            reason = "due to remaining unreturned/unused company balance"
        else:
            adjustment = f"+${format_money(abs(entry.company_balance))}"
            reason = "due to shouldering procurement cost"
        lines.append(
            f"{index}. <@{entry.user_id}> | Pay: ${base_pay} "
            f"({adjustment} = **${format_half_money(entry.adjusted_total_payout_half_units)}**) {reason}"
        )
    return "\n".join(lines)


def render_pricesheet_description(items: list[CatalogItem]) -> str:
    active_items = [item for item in items if item.active]
    if not active_items:
        return "No active catalog items are configured."
    lines: list[str] = []
    for index, item in enumerate(active_items, start=1):
        if item.bulk_price is not None and item.bulk_min_qty is not None:
            bulk = f"Bulk ${format_money(item.bulk_price)} @ {format_money(item.bulk_min_qty)}+"
        else:
            bulk = "Bulk n/a"
        cost = f"Cost ${format_money(item.unit_cost)}" if item.unit_cost is not None else "Cost n/a"
        lines.append(
            f"{index}. {item.name} | Unit ${format_money(item.unit_price)} | {bulk} | {cost}"
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


def format_half_money(amount_times_two: int) -> str:
    is_negative = amount_times_two < 0
    absolute = abs(amount_times_two)
    whole = absolute // 2
    prefix = "-" if is_negative else ""
    if absolute % 2:
        return f"{prefix}{format_money(whole)}.50"
    return f"{prefix}{format_money(whole)}"


def _staff_payout_half_units(reimbursement: int, profit: int) -> int:
    return reimbursement * 2 + profit


def _youtool_profit_after_staff_payout_half_units(
    total_sales: int,
    total_payout_half_units: int,
) -> int:
    return total_sales * 2 - total_payout_half_units


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


def _add_wrapped_field(
    embed: EmbedPayload,
    title: str,
    lines: list[str],
    *,
    limit: int = 1024,
) -> EmbedPayload:
    chunks: list[str] = []
    current: list[str] = []
    current_length = 0
    for line in lines:
        added_length = len(line) + (1 if current else 0)
        if current and current_length + added_length > limit:
            chunks.append("\n".join(current))
            current = [line]
            current_length = len(line)
            continue
        current.append(line)
        current_length += added_length
    if current:
        chunks.append("\n".join(current))
    if not chunks:
        chunks = ["-"]
    for index, chunk in enumerate(chunks):
        embed.field(title if index == 0 else f"{title} (cont.)", chunk, False)
    return embed


def help_page_embed(prefix: str, page: int) -> EmbedPayload:
    page = _clamp_help_page(page)
    embed = panel_embed(
        f"YouTool Help • {_help_page_title(page)}",
        f"Use `{prefix}[command]` or the matching slash command.",
    )
    if page == 0:
        _add_wrapped_field(
            embed,
            "Everyday Commands",
            [
                f"`{prefix}calc [@user] [items...]` / `/ytcalc [user] [items]` - Open the calculator panel; admins can credit a receipt to someone else and freeform items like `3 shovel 2 gloves` prefill the receipt",
                f"`{prefix}stats` / `/ytstats` - Show the leaderboard for 1 minute",
                f"`{prefix}pricesheet` / `/ytpricesheet` - Show the current catalog price sheet",
                f"`{prefix}contracts` / `/ytcontracts` - Show configured contract pricing",
                f"`{prefix}procurementfunds <amount> [@user]` / `/ytprocurementfunds` - Record a procurement-funds withdrawal",
                f"`{prefix}padd <amount> [@user]` - Short alias for procurement-funds withdrawal",
                f"`{prefix}procurementreturn <amount> [@user]` / `/ytprocurementreturn` - Record unused company funds returned",
                f"`{prefix}preturn <amount> [@user]` - Short alias for procurement return",
                f"`{prefix}procurementbalance [@user]` / `/ytprocurementbalance` - Show remaining company procurement funds",
                f"`{prefix}pbal [@user]` - Short alias for procurement balance",
                f"`{prefix}help` / `/ythelp` - Open this paged help panel",
                f"`{prefix}health` / `/ythealth` - Check bot setup and permissions",
            ],
        ).field("Where To Use Them", "Use these in the main channel for everyday receipt work.", False)
    elif page == 1:
        _add_wrapped_field(
            embed,
            "Admin Commands",
            [
                f"`{prefix}manage` / `/ytmanage` - Open the receipt manager",
                f"`{prefix}payouts` / `/ytpayouts` - Show employee payout totals with procurement-balance adjustments",
                f"`{prefix}refresh` / `/ytrefresh` - Reload catalog and contract files from disk",
                f"`{prefix}adjustprices` / `/ytadjustprices` - Edit the live catalog and refresh it",
                f"`{prefix}procurementcutover` / `/ytprocurementcutover` - Switch new receipts to procurement-funds accounting",
                f"`{prefix}templates [reload]` / `/yttemplates [reload]` - Show live announcement commands or reload the templates JSON",
                f"`{prefix}rescan <message-link-or-id>` / `/ytrescan` - Scan the main channel for undocumented receipts after a message and review them one by one",
                f"`{prefix}contracts add` / `/ytcontracts add` - Add or update a contract preset",
                f"`{prefix}reset` / `/ytreset` - Backup active receipts, then mark them paid or invalidate them",
                f"`{prefix}export` / `/ytexport` - Export the current database as JSON",
                f"`{prefix}import [file] [mode]` / `/ytimport` - Upload an export or load one from `/import`, review the preview, then confirm import",
                f"`{prefix}sanitize` / `/ytsanitize` - Preview safe proof/file cleanup, create a backup export, then apply it",
                f"`{prefix}rebuildlogs` / `/ytrebuildlogs` - Rebuild the receipt log channel from the database",
                f"`{prefix}restartbot` / `/ytrestartbot` - Restart the bot remotely",
                f"`{prefix}stop` / `/ytstop` - Shut the bot down gracefully",
            ],
        ).field("Where To Use Them", "Use these in the admin channel for corrections, payouts, imports, exports, and maintenance.", False)
    elif page == 2:
        embed.field(
            "Main Channel Receipt Cards",
            "`EDIT`, `INVALIDATE`\nAvailable to the receipt creator or admins.",
            False,
        ).field(
            "Admin Or Log Receipt Cards",
            "`REFRESH`, `MARK PAID`, `MARK UNPAID`, `INVALIDATE`, `RESTORE UNPAID`, `REPLACE PROOF`\nAvailable to admins.",
            False,
        ).field(
            "Receipt Status Rules",
            "Active receipts count in payouts and stats.\nPaid receipts stay in stats only.\nInvalidated receipts are excluded.",
            False,
        )
    else:
        _add_wrapped_field(
            embed,
            "Workflow Tips",
            [
                f"Attach one or more images to `{prefix}calc` to preload proof on prefix usage.",
                f"You can also prefill items with freeform text such as `{prefix}calc 3 shovel 2 gloves 1 bucket`.",
                "Quantity defaults to `1` when you add an item manually.",
                f"Use `{prefix}calc @user` if you are an admin recording a receipt on someone else's behalf.",
                f"Use `{prefix}procurementbalance @user` or `{prefix}pbal @user` to see whether company procurement funds are still available or personal spending has begun.",
                f"Use `{prefix}rescan <message-link-or-id>` to review unregistered receipt candidates after downtime.",
                f"Use `{prefix}import paid` or `{prefix}import invalidated` to override imported receipt statuses after reviewing the preview.",
                f"Use `{prefix}sanitize` after heavy export/import churn to normalize proof filenames and delete safe leftovers.",
            ],
        ).field(
            "Permissions",
            "Everyone: `calc`, `stats`, `pricesheet`, `contracts`, `procurementfunds`, `padd`, `procurementreturn`, `preturn`, `procurementbalance`, `pbal`, `help`, `health`\nAdmins only: `manage`, `payouts`, `refresh`, `adjustprices`, `procurementcutover`, `templates`, `rescan`, `contracts add`, `reset`, `export`, `import`, `sanitize`, `rebuildlogs`, `restartbot`, `stop`",
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
    total_payout_half_units = _staff_payout_half_units(total_procurement, total_profit)
    youtool_profit_after_staff_payout = _youtool_profit_after_staff_payout_half_units(
        total_sales,
        total_payout_half_units,
    )
    total_receipts = sum(entry.receipt_count for entry in entries)
    embed = (
        panel_embed("YouTool Stats", render_stats_description(entries))
        .field("Sort", _stats_sort_label(sort), True)
        .field("Employees", str(len(entries)), True)
        .field(
            "Totals",
            (
                f"Total Sales ${format_money(total_sales)}\n"
                f"Total Procurement ${format_money(total_procurement)}\n"
                f"Gross Profit ${format_money(total_profit)}\n"
                f"Total Staff Payout ${format_half_money(total_payout_half_units)}\n"
                f"YouTool Net Profit ${format_half_money(youtool_profit_after_staff_payout)}\n"
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
    total_payout_half_units = sum(entry.adjusted_total_payout_half_units for entry in entries)
    total_sales = sum(entry.reimbursement + entry.profit for entry in entries)
    youtool_profit_after_staff_payout = _youtool_profit_after_staff_payout_half_units(
        total_sales,
        total_payout_half_units,
    )
    embed = (
        panel_embed("YouTool Payouts", render_payout_description(entries))
        .field("Employees", str(len(entries)), True)
        .field("Total Staff Payout", f"${format_half_money(total_payout_half_units)}", True)
        .field(
            "YouTool Net Profit",
            f"${format_half_money(youtool_profit_after_staff_payout)}",
            True,
        )
    )
    if entries:
        return embed.with_footer("Showing unpaid receipts only. Paid receipts stay in stats.")
    return embed.with_footer("No unpaid receipts waiting for payout.")


def pricesheet_embed(items: list[CatalogItem]) -> EmbedPayload:
    return panel_embed(
        "YouTool Price Sheet",
        render_pricesheet_description(items),
    ).with_footer("Showing active catalog items from the current catalog.")


def contracts_embed(contracts: list[Contract]) -> EmbedPayload:
    return panel_embed(
        "YouTool Contracts",
        render_contracts_description(contracts),
    ).with_footer("Contracts apply preset special pricing to matching calculator items.")


def health_embed(checks: list[str], issues: list[str]) -> EmbedPayload:
    status = "Healthy" if not issues else "Needs Attention"
    embed = (
        panel_embed("YouTool Health", f"Status: {status}")
        if not issues
        else warning_panel_embed("YouTool Health", f"Status: {status}")
    )
    embed.field("Checks", "\n".join(checks), False)
    embed.field("What To Fix", "No problems detected." if not issues else "\n".join(issues), False)
    embed.field(
        "Reminder",
        "Prefix commands require Message Content Intent to stay enabled in the Discord developer portal.",
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
    )


def calc_embeds(catalog_items: list[CatalogItem], session) -> list[EmbedPayload]:
    embeds = [calc_embed(catalog_items, session)]
    embeds.extend(proof_preview_embeds(session.payment_proof_source_url, "YouTool Receipt Proof"))
    return embeds


def calc_embed(catalog_items: list[CatalogItem], session) -> EmbedPayload:
    embed = panel_embed("YouTool Calculator", render_session_description(catalog_items, session))
    embed.color = 0xDAA520 if session.awaiting_proof else THEME_SUCCESS
    if session.awaiting_proof:
        return embed.with_footer("Awaiting proof upload in this channel.")
    return embed.with_footer(
        "Use Add an Item / Remove an Item / Set Contract, then Print Receipt when ready."
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
        "YouTool Calculator",
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
        "This receipt will close in 1 minute unless you confirm you are still working.",
        "Choose Still Working to keep this receipt open, or Close Receipt to remove it now.",
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
        title="YouTool Calculator",
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
            "**Procurement:** $0\n"
            "**Profit:** $0\n"
            f"**Contract:** {session.selected_contract_name or 'None'}\n"
            f"**Proof:** {session_proof_status(session)}\n\n"
            "**Running List Of Items:**\n"
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
    lines: list[str] = []
    for index, item in enumerate(priced.items, start=1):
        source = ""
        if item.pricing_source.value == "bulk":
            source = " [Bulk Discount]"
        elif item.pricing_source.value == "override":
            draft = session.items[index - 1]
            if draft.contract_name:
                source = f" [Contract: {draft.contract_name}]"
            else:
                source = " [Special Pricing]"
        lines.append(
            f"{index}. {item.quantity}x {item.item_name}{source} (Line Total: ${format_money(item.line_sale_total)})"
        )

    awaiting = ""
    if session.awaiting_proof and not force_not_awaiting:
        awaiting = "\n\n**Status:** Waiting for proof image upload in this channel."
    workflow_notice = (
        f"\n**Workflow:** {session.workflow_notice}\n" if session.workflow_notice else ""
    )

    return (
        f"**Credited To:** {session.credited_display_name} ({session.credited_user_id})\n"
        f"**Total Sale:** ${format_money(priced.total_sale)}\n"
        f"**Procurement:** ${format_money(priced.procurement_cost)}\n"
        f"**Profit:** ${format_money(priced.profit)}\n"
        f"**Contract:** {session.selected_contract_name or 'None'}\n"
        f"**Proof:** {session_proof_status(session)}\n"
        f"{workflow_notice}"
        f"**Running List Of Items:**\n"
        f"- " + "\n- ".join(lines) + awaiting
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


def calc_action_rows(
    user_id: int,
    awaiting_proof: bool,
    rescan_active: bool,
) -> list[ActionRowPayload]:
    rows = [
        ActionRowPayload(
            components=[
                ButtonPayload(
                    custom_id=f"calc|add|{user_id}",
                    label="Add an Item",
                    style=BUTTON_STYLE_PRIMARY,
                    disabled=awaiting_proof,
                ),
                ButtonPayload(
                    custom_id=f"calc|remove|{user_id}",
                    label="Remove an Item",
                    style=BUTTON_STYLE_SECONDARY,
                    disabled=awaiting_proof,
                ),
                ButtonPayload(
                    custom_id=f"calc|contract|{user_id}",
                    label="Set Contract",
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
        panel_embed("YouTool Receipt Manager", description)
        .field("Page", str(page + 1), True)
        .with_footer("Select a receipt number below to inspect, change status, or replace proof.")
    )
    return embed, rows


def receipt_detail_payload(
    receipt: PersistedReceipt,
    owner_user_id: int,
    display: ReceiptDisplayContext | None,
) -> ReplyPayload:
    description = "\n".join(
        f"{index}. {item.quantity}x {item.item_name} | ${format_money(item.unit_sale_price)} each | "
        f"${format_money(item.line_sale_total)} total | {item.pricing_source.value}"
        for index, item in enumerate(receipt.items, start=1)
    )
    base_embed = panel_embed(f"YouTool Receipt {receipt.id}", description)
    base_embed.field(
        "Credited To",
        f"{receipt.creator_username} ({receipt.creator_user_id})",
        True,
    ).field("Total Sale", f"${format_money(receipt.total_sale)}", True).field(
        "Procurement",
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
        "Active receipts affect payouts. Paid receipts stay in stats only. Invalidated receipts are excluded. Edit Items and Replace Proof keep the receipt ID."
    )
    apply_receipt_display_context(base_embed, display)

    buttons = [
        ButtonPayload(
            custom_id=f"receipt|items|{owner_user_id}|{receipt.id}",
            label="Edit Items",
            style=BUTTON_STYLE_SECONDARY,
        )
    ]
    buttons.extend(
        ButtonPayload(
            custom_id=f"receipt|detail_status|{owner_user_id}|{receipt.id}|{action[0]}",
            label=action[1],
            style=action[2],
        )
        for action in admin_receipt_actions(receipt.status)
    )
    buttons.append(
        ButtonPayload(
            custom_id=f"receipt|proof|{owner_user_id}|{receipt.id}",
            label="Replace Proof",
            style=BUTTON_STYLE_PRIMARY,
        )
    )
    return ReplyPayload(
        embeds=[base_embed, *proof_preview_embeds(receipt.payment_proof_source_url, "YouTool Receipt Proof")],
        components=[ActionRowPayload(components=buttons)],
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
    )
    if getattr(receipt, "status", ReceiptStatus.ACTIVE) is not ReceiptStatus.ACTIVE:
        embed.field("Status", admin_receipt_status_label(receipt.status), True)
    return ReplyPayload(
        content=receipt_message_content(receipt.id, receipt.creator_user_id),
        embeds=[embed, *proof_preview_embeds(receipt.payment_proof_source_url, "Payment Proof")],
        components=[ActionRowPayload(components=receipt_main_action_buttons(receipt.id, receipt.creator_user_id))],
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
    )
    embed.field("Status", admin_receipt_status_label(receipt.status), True)
    return ReplyPayload(
        content=receipt_message_content(receipt.id, receipt.creator_user_id),
        embeds=[embed, *proof_preview_embeds(receipt.payment_proof_source_url, "Payment Proof")],
        components=[ActionRowPayload(components=receipt_log_action_buttons(receipt.id, receipt.creator_user_id, receipt.status))],
        ephemeral=False,
        suppress_mentions=True,
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
) -> EmbedPayload:
    creator_username, creator_user_id = creator
    items_text = "\n".join(
        f"- {item.quantity}x {item.item_name} (Unit ${format_money(item.unit_sale_price)}, Line ${format_money(item.line_sale_total)})"
        for item in items
    )
    description = (
        f"**Total Sale:** ${format_money(total_sale)}\n"
        f"**Procurement:** ${format_money(procurement_cost)}\n"
        f"**Profit:** ${format_money(profit)}\n"
        f"**Credited To:** {creator_username} ({creator_user_id})\n\n"
        f"**Running List Of Items:**\n{items_text}"
    )
    embed = EmbedPayload(
        title=f"YouTool Receipt {receipt_id}",
        description=description,
        color=THEME_SUCCESS,
    )
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
    if display.accounting_policy is not None:
        embed.field("Accounting", accounting_policy_label(display.accounting_policy), True)
    return embed


def accounting_policy_label(policy: AccountingPolicy) -> str:
    if policy is AccountingPolicy.PROCUREMENT_FUNDS:
        return "Procurement funds"
    return "Legacy reimbursement"


def payment_proof_field_value(payment_proof_source_url: str | None) -> str:
    urls = [line.strip() for line in (payment_proof_source_url or "").splitlines() if line.strip()]
    return "\n".join(urls) if urls else "No payment proof URL recorded."


def proof_preview_embeds(proof_urls: str | None, title_prefix: str) -> list[EmbedPayload]:
    urls = [line.strip() for line in (proof_urls or "").splitlines() if line.strip()]
    return [
        info_panel_embed(f"{title_prefix} {index}", f"Open: {url}").with_image(url)
        for index, url in enumerate(urls, start=1)
    ]


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
        return "Procurement"
    if sort is StatsSort.COUNT:
        return "Receipt Count"
    return "Sales"
