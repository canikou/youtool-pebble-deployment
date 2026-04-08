from __future__ import annotations

from .models import DraftItem, PricingSource, PricedItem, PricedReceipt, saturating_add, saturating_mul


def price_items(catalog: "Catalog", items: list[DraftItem]) -> PricedReceipt:
    priced_items: list[PricedItem] = []
    total_sale = 0
    procurement_cost = 0
    used_override = False
    used_bulk = False

    for item in items:
        if item.quantity <= 0:
            raise ValueError(f"quantity must be positive for {item.item_name}")

        catalog_item = catalog.find_item(item.item_name)
        if catalog_item is None:
            raise ValueError(f"unknown catalog item: {item.item_name}")

        override_unit_price = item.override_unit_price if (item.override_unit_price or 0) > 0 else None
        quantity = item.quantity
        unit_cost = catalog_item.unit_cost or 0

        if override_unit_price is not None:
            used_override = True
            unit_sale_price = override_unit_price
            pricing_source = PricingSource.OVERRIDE
        elif (
            catalog_item.bulk_price is not None
            and catalog_item.bulk_min_qty is not None
            and quantity >= catalog_item.bulk_min_qty
        ):
            used_bulk = True
            unit_sale_price = catalog_item.bulk_price
            pricing_source = PricingSource.BULK
        else:
            unit_sale_price = catalog_item.unit_price
            pricing_source = PricingSource.DEFAULT

        line_sale_total = saturating_mul(unit_sale_price, quantity)
        line_cost_total = saturating_mul(unit_cost, quantity)
        total_sale = saturating_add(total_sale, line_sale_total)
        procurement_cost = saturating_add(procurement_cost, line_cost_total)
        priced_items.append(
            PricedItem(
                item_name=catalog_item.name,
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

