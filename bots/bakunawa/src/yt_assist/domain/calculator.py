from __future__ import annotations

from enum import Enum

from .models import DraftItem


class InsertDisposition(str, Enum):
    ADDED_SEPARATE_LINE = "added_separate_line"
    MERGED_INTO_EXISTING_LINE = "merged_into_existing_line"


def insert_draft_item(
    items: list[DraftItem],
    new_item: DraftItem,
    force_separate_line: bool,
) -> InsertDisposition:
    del force_separate_line
    new_key = (new_item.display_name or new_item.item_name).strip().lower()
    for existing in items:
        existing_key = (existing.display_name or existing.item_name).strip().lower()
        if existing_key == new_key:
            return InsertDisposition.MERGED_INTO_EXISTING_LINE

    items.append(new_item)
    return InsertDisposition.ADDED_SEPARATE_LINE

