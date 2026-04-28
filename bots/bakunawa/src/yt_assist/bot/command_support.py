"""Shared command helper logic mirrored from the Rust bot."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from yt_assist.domain.catalog import Catalog
from yt_assist.domain.models import DraftItem
from yt_assist.domain.templates import AnnouncementTemplates


@dataclass(slots=True)
class SessionCreditTarget:
    user_id: str
    username: str
    display_name: str

    @classmethod
    def from_user_id(cls, user_id: int) -> SessionCreditTarget:
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


def parse_signed_amount_cents_text(input_text: str) -> int:
    cleaned = (
        input_text.strip()
        .replace("$", "")
        .replace(",", "")
        .replace("_", "")
        .replace(" ", "")
    )
    if not cleaned:
        raise ValueError("Amount is required.")
    try:
        amount = Decimal(cleaned)
    except InvalidOperation as error:
        raise ValueError("Amount must be a valid number.") from error
    cents = int((amount * Decimal("100")).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    if cents == 0:
        raise ValueError("Amount must not be zero.")
    return cents


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
) -> tuple[int | None, list[DraftItem]]:
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


def parse_prefill_items(catalog: Catalog, input_text: str) -> list[DraftItem]:
    items: list[DraftItem] = []
    seen: set[str] = set()
    candidates = [part.strip() for part in input_text.replace(";", ",").split(",") if part.strip()]
    for candidate in candidates:
        catalog_item = _match_catalog_prefill_item(catalog, candidate)
        if catalog_item is None:
            continue
        key = catalog_item.name.lower()
        if key in seen:
            continue
        seen.add(key)
        items.append(
            DraftItem(
                item_name=catalog_item.name,
                quantity=1,
                override_unit_price=None,
                contract_name=None,
            )
        )

    return items


def parse_add_item_inputs(
    override_unit_price_input: str | None,
) -> int | None:
    override_raw = (override_unit_price_input or "").strip()
    if not override_raw:
        return None

    try:
        override = int(override_raw)
    except ValueError as error:
        raise ValueError("Special pricing must be a whole number.") from error
    if override <= 0:
        raise ValueError("Special pricing must be greater than 0 when provided.")
    return override


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
        ("BAKUNAWA MECH is open", templates.open),
        ("BAKUNAWA MECH is closed", templates.closed),
        ("BAKUNAWA MECH is conducting a city-wide sweep", templates.on_the_road),
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
