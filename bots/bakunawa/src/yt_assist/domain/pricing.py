from __future__ import annotations

from .catalog import Catalog
from .models import (
    DraftItem,
    PricedItem,
    PricedReceipt,
    PricingSource,
    saturating_add,
    saturating_mul,
)


def price_items(catalog: Catalog, items: list[DraftItem]) -> PricedReceipt:
    priced_items: list[PricedItem] = []
    total_sale = 0
    procurement_cost = 0
    used_override = False
    used_bulk = False

    for item in items:
        if item.quantity <= 0:
            raise ValueError(f"quantity must be positive for {item.item_name}")

        catalog_item = catalog.find_item(item.item_name)
        if catalog_item is None and not (
            item.override_unit_price is not None and item.override_unit_cost is not None
        ):
            raise ValueError(f"unknown catalog item: {item.item_name}")

        override_unit_price = item.override_unit_price
        quantity = item.quantity
        price_pending = catalog_item is not None and catalog_item.price_pending
        if price_pending and override_unit_price is None:
            unit_cost = 0
        else:
            unit_cost = item.override_unit_cost if item.override_unit_cost is not None else catalog_item.unit_cost or 0

        if price_pending and override_unit_price is None:
            unit_sale_price = 0
            pricing_source = PricingSource.DEFAULT
        elif override_unit_price is not None:
            used_override = True
            unit_sale_price = override_unit_price
            pricing_source = PricingSource.OVERRIDE
        else:
            unit_sale_price = catalog_item.unit_price
            pricing_source = PricingSource.DEFAULT

        line_sale_total = saturating_mul(unit_sale_price, quantity)
        line_cost_total = saturating_mul(unit_cost, quantity)
        total_sale = saturating_add(total_sale, line_sale_total)
        procurement_cost = saturating_add(procurement_cost, line_cost_total)
        priced_items.append(
            PricedItem(
                item_name=item.display_name or (catalog_item.name if catalog_item is not None else item.item_name),
                quantity=quantity,
                unit_sale_price=unit_sale_price,
                unit_cost=unit_cost,
                pricing_source=pricing_source,
                line_sale_total=line_sale_total,
                line_cost_total=line_cost_total,
            )
        )

    return PricedReceipt(
        items=priced_items,
        total_sale=total_sale,
        procurement_cost=procurement_cost,
        profit=total_sale - procurement_cost,
        used_override=used_override,
        used_bulk=used_bulk,
    )

