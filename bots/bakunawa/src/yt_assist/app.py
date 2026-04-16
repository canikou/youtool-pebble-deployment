"""Application bootstrapping."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from yt_assist.bot.state import BotState
from yt_assist.config import AppConfig, load_runtime_config
from yt_assist.domain.catalog import Catalog
from yt_assist.domain.contracts import Contracts
from yt_assist.domain.packages import PackageCatalog
from yt_assist.domain.templates import TemplateLoadStatus
from yt_assist.domain.templates import initialize as initialize_templates
from yt_assist.logging import LoggingGuards, cleanup_old_log_files, init_logging
from yt_assist.storage.database import Database

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class RuntimeContext:
    config: AppConfig
    logging_guards: LoggingGuards
    catalog: Catalog
    packages: PackageCatalog
    contracts: Contracts
    database: Database
    bot_state: BotState


async def build_runtime_context(config_path: Path | None = None) -> RuntimeContext:
    return await build_runtime_context_with_options(config_path, require_token=True)


async def build_runtime_context_with_options(
    config_path: Path | None = None,
    *,
    require_token: bool,
) -> RuntimeContext:
    config = load_runtime_config(config_path, require_token=require_token)
    config.ensure_directories()

    logging_guards = init_logging(config)
    LOGGER.info("loaded config")
    removed = cleanup_old_log_files(config.storage.log_dir, 14)
    if removed:
        LOGGER.info("cleaned up %s old log files", removed)

    template_report = initialize_templates(config.storage.templates_path)
    if template_report.status == TemplateLoadStatus.LOADED:
        LOGGER.info("loaded templates from %s", template_report.path)
    elif template_report.status == TemplateLoadStatus.CREATED_DEFAULT_FILE:
        LOGGER.info("created default templates at %s", template_report.path)
    elif template_report.warning:
        LOGGER.warning("%s", template_report.warning)

    catalog = Catalog.load_from(config.storage.catalog_path)
    LOGGER.info("loaded catalog with %s items", len(catalog.items))

    packages = PackageCatalog.load_from(config.storage.packages_path)
    LOGGER.info("loaded %s package definitions", len(packages.packages))

    contracts = Contracts.load_from(config.storage.contracts_path, catalog)
    LOGGER.info("loaded %s contract presets", len(contracts.entries))

    database = await Database.connect(config.storage.database_path)
    LOGGER.info("database ready")

    return RuntimeContext(
        config=config,
        logging_guards=logging_guards,
        catalog=catalog,
        packages=packages,
        contracts=contracts,
        database=database,
        bot_state=BotState(),
    )


async def run(config_path: Path | None = None) -> int:
    from yt_assist.bot.runtime import run_discord

    return await run_discord(config_path)
