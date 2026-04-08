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
    def default_values(cls) -> "AnnouncementTemplates":
        return cls(
            open="/yt MONTEFALCO ENTERPRISES | YOUTOOL is officially open at Postal 3052. From heavy-duty farming kits to the finest tools in the city - don't get caught unprepared. Swing by and gear up!",
            closed="/yt The lights are dim at MONTEFALCO ENTERPRISES YOUTOOL. We're restocking and recharging for the next shift. We'll see you back at Postal 3052 soon. Stay safe out there!",
            on_the_road="/yt MONTEFALCO ON THE MOVE. We've taken the shop mobile! Our Postal 3052 location is temporarily closed while we bring the tools to the streets. Keep your eyes peeled for the Montefalco truck!",
            unattended="/yt We're In! MONTEFALCO ENTERPRISES YOUTOOL is open at Postal 3052, though we might be in the back organizing the warehouse. Ring the buzzer and we'll be right out to assist you!",
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

