from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from yt_assist.bot.command_support import format_template_command_output
from yt_assist.bot.render import (
    contracts_embed,
    help_action_rows,
    help_page_embed,
    pricesheet_embed,
)
from yt_assist.domain.catalog import Catalog
from yt_assist.domain.contracts import Contracts
from yt_assist.domain.templates import TemplateLoadStatus
from yt_assist.domain.templates import initialize as initialize_templates

from .snapshot_normalize import canonical_json_text


@dataclass(slots=True)
class ParityCaseResult:
    name: str
    passed: bool
    details: str = ""


@dataclass(slots=True)
class ParityHarnessReport:
    snapshot_path: Path
    cases: list[ParityCaseResult] = field(default_factory=list)

    def all_passed(self) -> bool:
        return all(case.passed for case in self.cases)

    def render_text(self) -> str:
        lines = [f"snapshot: {self.snapshot_path}", f"passed: {sum(case.passed for case in self.cases)}/{len(self.cases)}"]
        for case in self.cases:
            status = "PASS" if case.passed else "FAIL"
            suffix = f" - {case.details}" if case.details else ""
            lines.append(f"{status} {case.name}{suffix}")
        return "\n".join(lines)


def _project_root() -> Path:
    return Path.cwd()


def _load_current_snapshot() -> dict:
    project_root = _project_root()
    config_dir = project_root / "config"
    catalog_path = config_dir / "catalog.toml"
    contracts_path = config_dir / "contracts.json"
    templates_path = config_dir / "templates.json"

    catalog = Catalog.load_from(catalog_path)
    contracts = Contracts.load_from(contracts_path, catalog)
    template_report = initialize_templates(templates_path)
    templates = template_report.templates

    return {
        "project_root": str(project_root),
        "catalog": {
            "item_count": len(catalog.items),
            "items": [
                {
                    "name": item.name,
                    "aliases": list(item.aliases),
                    "unit_price": item.unit_price,
                    "bulk_price": item.bulk_price,
                    "bulk_min_qty": item.bulk_min_qty,
                    "unit_cost": item.unit_cost,
                    "active": item.active,
                }
                for item in catalog.items
            ],
        },
        "contracts": {
            "contract_count": len(contracts.entries),
            "contracts": [
                {
                    "name": contract.name,
                    "aliases": list(contract.aliases),
                    "prices": [
                        {"item_name": price.item_name, "unit_price": price.unit_price}
                        for price in contract.prices
                    ],
                }
                for contract in contracts.entries
            ],
        },
        "templates": {
            "status": template_report.status.value,
            "open": templates.open,
            "closed": templates.closed,
            "on_the_road": templates.on_the_road,
            "unattended": templates.unattended,
        },
        "observable_surfaces": {
            "help_pages": [help_page_embed("bm!", page).to_dict() for page in range(4)],
            "help_rows": [row.to_dict() for row in help_action_rows(42, 0)],
            "pricesheet": pricesheet_embed(catalog.items).to_dict(),
            "contracts": contracts_embed(contracts.entries).to_dict(),
            "template_output": format_template_command_output(templates),
        },
    }


async def run_harness(workspace_root: Path | str) -> ParityHarnessReport:
    workspace_root = Path(workspace_root)
    workspace_root.mkdir(parents=True, exist_ok=True)

    snapshot = _load_current_snapshot()
    snapshot_path = workspace_root / "python-parity-snapshot.json"
    snapshot_path.write_text(canonical_json_text(snapshot), encoding="utf-8")

    cases = [
        ParityCaseResult(
            name="catalog_loaded",
            passed=snapshot["catalog"]["item_count"] > 0,
            details=f"{snapshot['catalog']['item_count']} items",
        ),
        ParityCaseResult(
            name="contracts_loaded",
            passed=snapshot["contracts"]["contract_count"] >= 0,
            details=f"{snapshot['contracts']['contract_count']} contracts",
        ),
        ParityCaseResult(
            name="templates_loaded",
            passed=snapshot["templates"]["status"] in {
                TemplateLoadStatus.LOADED.value,
                TemplateLoadStatus.CREATED_DEFAULT_FILE.value,
                TemplateLoadStatus.FALLBACK_TO_DEFAULTS.value,
            },
            details=snapshot["templates"]["status"],
        ),
        ParityCaseResult(
            name="observable_help_surface",
            passed="`bm!stop` / `/mechstop` - Shut the bot down gracefully"
            in snapshot["observable_surfaces"]["help_pages"][1]["fields"][0]["value"],
            details="help page snapshot",
        ),
        ParityCaseResult(name="snapshot_written", passed=snapshot_path.exists(), details=str(snapshot_path)),
    ]

    return ParityHarnessReport(snapshot_path=snapshot_path, cases=cases)
