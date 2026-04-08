"""Shared command helper logic mirrored from the Rust bot."""

from __future__ import annotations

from dataclasses import dataclass

from yt_assist.domain.catalog import Catalog
from yt_assist.domain.models import ProcurementBalance, ProcurementCutoverState
from yt_assist.domain.templates import AnnouncementTemplates

from .render import format_money


@dataclass(slots=True)
class SessionCreditTarget:
    user_id: str
    username: str
    display_name: str

    @classmethod
    def from_user_id(cls, user_id: int) -> "SessionCreditTarget":
        raw = str(user_id)
        return cls(user_id=raw, username=raw, display_name=raw)


def custom_id_for_user(prefix: str, user_id: int) -> str:
    return f"{prefix}|{user_id}"


def split_custom_id(custom_id: str) -> list[str]:
    return custom_id.split("|")


def parse_positive_amount_text(input_text: str) -> int:
    cleaned = (
        input_text.strip().lstrip("$").replace(",", "").replace("_", "").replace(" ", "")
    )
    amount = int(cleaned)
    if amount <= 0:
        raise ValueError("Amount must be greater than 0.")
    return amount


def parse_user_token(token: str) -> int | None:
    token = token.strip()
    if token.startswith("<@!") and token.endswith(">"):
        inner = token[3:-1]
        return int(inner) if inner.isdigit() else None
    if token.startswith("<@") and token.endswith(">"):
        inner = token[2:-1]
        return int(inner) if inner.isdigit() else None
    return int(token) if len(token) >= 1 and token.isdigit() else None


def parse_calc_prefix_input(
    catalog: Catalog,
    scope: str | None,
) -> tuple[int | None, list["DraftItem"]]:
    from yt_assist.domain.models import DraftItem

    normalized = (scope or "").strip()
    if not normalized:
        return None, []

    parts = normalized.split(maxsplit=1)
    first = parts[0]
    remainder = parts[1].strip() if len(parts) > 1 else ""
    target_user_id = _parse_leading_user_token(first)
    if target_user_id is not None:
        return target_user_id, parse_prefill_items(catalog, remainder)
    return None, parse_prefill_items(catalog, normalized)


def parse_prefill_items(catalog: Catalog, input_text: str) -> list["DraftItem"]:
    from yt_assist.domain.models import DraftItem

    tokens = [
        token
        for token in (_sanitize_prefill_token(part) for part in input_text.split())
        if token
    ]
    if not tokens:
        return []

    items: list[DraftItem] = []
    index = 0
    max_item_tokens = max(
        (len(item.name.split()) for item in catalog.items),
        default=1,
    )

    while index < len(tokens):
        quantity = _parse_prefill_quantity(tokens[index])
        if quantity is None:
            index += 1
            continue

        matched_name: str | None = None
        matched_next_index = index + 1
        for length in range(max_item_tokens, 0, -1):
            end = index + 1 + length
            if end > len(tokens):
                continue
            candidate = " ".join(tokens[index + 1 : end])
            catalog_item = _match_catalog_prefill_item(catalog, candidate)
            if catalog_item is None:
                continue
            matched_name = catalog_item.name
            matched_next_index = end
            break

        if matched_name is None:
            index += 1
            continue

        items.append(
            DraftItem(
                item_name=matched_name,
                quantity=quantity,
                override_unit_price=None,
                contract_name=None,
            )
        )
        index = matched_next_index

    return items


def parse_add_item_inputs(
    quantity_input: str,
    override_unit_price_input: str | None,
) -> tuple[int, int | None]:
    quantity_raw = quantity_input.strip() or "1"
    try:
        quantity = int(quantity_raw)
    except ValueError as error:
        raise ValueError("Quantity ordered must be a whole number.") from error
    if quantity <= 0:
        raise ValueError("Quantity ordered must be greater than 0.")

    override_raw = (override_unit_price_input or "").strip()
    if not override_raw:
        return quantity, None

    try:
        override = int(override_raw)
    except ValueError as error:
        raise ValueError("Special pricing must be a whole number.") from error
    if override <= 0:
        raise ValueError("Special pricing must be greater than 0 when provided.")
    return quantity, override


def procurement_ledger_success_message(
    target: SessionCreditTarget,
    amount: int,
    direction: str,
    actor_user_id: int,
    balance: ProcurementBalance,
) -> str:
    action_text = "return" if direction == "return" else "withdrawal"
    if balance.available_total >= 0:
        remaining = f"Company funds still available: ${format_money(balance.available_total)}"
    else:
        remaining = (
            "Company funds are exhausted. Personal spend currently exceeds company funds by "
            f"${format_money(abs(balance.available_total))}."
        )
    return (
        f"Recorded a procurement {action_text} of ${format_money(amount)} for <@{target.user_id}>.\n"
        f"Recorded by: <@{actor_user_id}>\n"
        f"Total withdrawn: ${format_money(balance.withdrawn_total)}\n"
        f"Total returned: ${format_money(balance.returned_total)}\n"
        f"Spent on new-policy receipts: ${format_money(balance.spent_total)}\n"
        f"{remaining}"
    )


def procurement_balance_message(
    target: SessionCreditTarget,
    cutover: ProcurementCutoverState | None,
    balance: ProcurementBalance,
) -> str:
    if cutover is not None and cutover.cutover_at is not None:
        policy_line = f"Policy: procurement funds active since {cutover.cutover_at.isoformat()}"
    else:
        policy_line = (
            "Policy: legacy reimbursement is still active until an admin runs the procurement cutover command."
        )
    if balance.available_total >= 0:
        availability_line = f"Company funds remaining: ${format_money(balance.available_total)}"
    else:
        availability_line = (
            "Company funds exhausted. Personal money currently used beyond company funds: "
            f"${format_money(abs(balance.available_total))}"
        )
    return (
        f"Employee: <@{target.user_id}>\n"
        f"{policy_line}\n"
        f"Total withdrawn: ${format_money(balance.withdrawn_total)}\n"
        f"Total returned: ${format_money(balance.returned_total)}\n"
        f"Spent on new-policy receipts: ${format_money(balance.spent_total)}\n"
        f"Ledger total: ${format_money(balance.ledger_total)}\n"
        f"{availability_line}"
    )


def format_optional_actor(user_id: str | None, display_name: str | None) -> str:
    if user_id and display_name:
        return f"{display_name} ({user_id})"
    if user_id:
        return user_id
    if display_name:
        return display_name
    return "unknown"


def format_template_command_output(templates: AnnouncementTemplates) -> str:
    sections = [
        ("YouTool is Open", templates.open),
        ("YouTool is Closed", templates.closed),
        ("YouTool is on the road", templates.on_the_road),
        ("YouTool may be unattended", templates.unattended),
    ]
    formatted: list[str] = []
    for label, command in sections:
        sanitized_command = command.replace("`", "'")
        formatted.append(f"# {label}\n`{sanitized_command}`")
    return "\n\n".join(formatted)


def _sanitize_prefill_token(token: str) -> str:
    return token.strip(" ,.:;!?()[]{}\"'").lower()


def _parse_prefill_quantity(token: str) -> int | None:
    candidate = token[:-1] if token.endswith("x") else token
    if not candidate:
        return None
    try:
        quantity = int(candidate)
    except ValueError:
        return None
    return quantity if quantity > 0 else None


def _match_catalog_prefill_item(catalog: Catalog, candidate: str):
    item = catalog.find_item(candidate)
    if item is not None:
        return item

    words = candidate.split()
    if not words:
        return None
    singular_words = list(words)
    last = singular_words[-1]
    if last.endswith("es"):
        singular_words[-1] = last[:-2]
    elif last.endswith("s"):
        singular_words[-1] = last[:-1]
    singular = " ".join(singular_words)
    return catalog.find_item(singular)


def _parse_leading_user_token(token: str) -> int | None:
    mention = None
    if token.startswith("<@!") and token.endswith(">"):
        mention = token[3:-1]
    elif token.startswith("<@") and token.endswith(">"):
        mention = token[2:-1]
    if mention is not None:
        return int(mention) if mention.isdigit() else None
    return int(token) if len(token) >= 17 and token.isdigit() else None
