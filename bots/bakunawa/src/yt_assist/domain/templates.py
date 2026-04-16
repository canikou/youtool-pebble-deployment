from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from threading import RLock


class TemplateLoadStatus(str, Enum):
    LOADED = "loaded"
    CREATED_DEFAULT_FILE = "created_default_file"
    FALLBACK_TO_DEFAULTS = "fallback_to_defaults"


@dataclass(slots=True)
class AnnouncementTemplates:
    open: str
    closed: str
    on_the_road: str
    unattended: str

    @classmethod
    def default_values(cls) -> AnnouncementTemplates:
        return cls(
            open=(
                "/mech2 [ BAKUNAWA MECH ] The hydraulics are hissing at Postal 9035! Whether "
                "you're leaking oil or looking for more horsepower, the bays are clear and the "
                "wrenches are turning. Bring your rig in for a real tune-up."
            ),
            closed=(
                "/mech2 [ BAKUNAWA MECH ] The torches are cold and the compressor is off. "
                "We've locked down Postal 9035 for the night to recharge the crew. "
                "Catch us on the next shift--stay shiny out there."
            ),
            on_the_road=(
                "/mech2 [ BAKUNAWA MECH ] PUBLIC NOTICE: Our recovery units are hitting the "
                "pavement for a city-wide sweep. Abandoned husks and illegal parkers are being "
                "cleared under government contract. If you left it on the curb, we've probably "
                "already got it on a hook. Head to Postal 9035 to settle the bill."
            ),
            unattended=(
                "/mech2 [ BAKUNAWA MECH ] The hydraulics are hissing at Postal 9035! Whether "
                "you're leaking oil or looking for more horsepower, the bays are clear and the "
                "wrenches are turning. Bring your rig in for a real tune-up."
            ),
        )

    def to_pretty_json(self) -> str:
        return json.dumps(
            {
                "open": self.open,
                "closed": self.closed,
                "on_the_road": self.on_the_road,
                "unattended": self.unattended,
            },
            indent=2,
            ensure_ascii=False,
        )


@dataclass(slots=True)
class TemplateLoadReport:
    path: Path
    templates: AnnouncementTemplates
    status: TemplateLoadStatus
    warning: str | None


_template_path: Path | None = None
_live_templates: AnnouncementTemplates | None = None
_lock = RLock()


def _write_templates(path: Path, templates: AnnouncementTemplates) -> None:
    path.write_text(templates.to_pretty_json() + "\n", encoding="utf-8")


def load_from_path(path: Path | str) -> TemplateLoadReport:
    path = Path(path)
    if not path.exists():
        templates = AnnouncementTemplates.default_values()
        path.parent.mkdir(parents=True, exist_ok=True)
        _write_templates(path, templates)
        return TemplateLoadReport(
            path=path,
            templates=templates,
            status=TemplateLoadStatus.CREATED_DEFAULT_FILE,
            warning=None,
        )

    raw = path.read_text(encoding="utf-8")
    try:
        data = json.loads(raw)
        templates = AnnouncementTemplates(
            open=str(data["open"]),
            closed=str(data["closed"]),
            on_the_road=str(data["on_the_road"]),
            unattended=str(data["unattended"]),
        )
        return TemplateLoadReport(
            path=path,
            templates=templates,
            status=TemplateLoadStatus.LOADED,
            warning=None,
        )
    except Exception as exc:  # noqa: BLE001
        return TemplateLoadReport(
            path=path,
            templates=AnnouncementTemplates.default_values(),
            status=TemplateLoadStatus.FALLBACK_TO_DEFAULTS,
            warning=f"failed to parse templates at {path}: {exc}",
        )


def initialize(path: Path | str) -> TemplateLoadReport:
    global _template_path, _live_templates
    report = load_from_path(path)
    with _lock:
        _template_path = report.path
        _live_templates = report.templates
    return report


def reload() -> TemplateLoadReport:
    if _template_path is None:
        raise RuntimeError("templates have not been initialized yet")
    return initialize(_template_path)


def current() -> AnnouncementTemplates | None:
    return _live_templates


def write_default_file(path: Path | str) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    _write_templates(path, AnnouncementTemplates.default_values())

