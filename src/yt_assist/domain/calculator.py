from __future__ import annotations

from enum import Enum
from typing import List

from .models import DraftItem


class InsertDisposition(str, Enum):
    ADDED_SEPARATE_LINE = "added_separate_line"
    MERGED_INTO_EXISTING_LINE = "merged_into_existing_line"


def insert_draft_item(
    items: List[DraftItem],
    new_item: DraftItem,
    force_separate_line: bool,
) -> InsertDisposition:
    if not force_separate_line:
        for existing in items:
            if (
                existing.item_name.lower() == new_item.item_name.lower()
                and existing.override_unit_price == new_item.override_unit_price
                and existing.contract_name == new_item.contract_name
            ):
                existing.quantity += new_item.quantity
                return InsertDisposition.MERGED_INTO_EXISTING_LINE

    items.append(new_item)
    return InsertDisposition.ADDED_SEPARATE_LINE

