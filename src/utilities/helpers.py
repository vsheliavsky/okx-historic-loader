import logging
import sys
import tomllib
from datetime import datetime
from pathlib import Path

from .custom_types import InstrumentId, TradeId


def setup_logging(level=logging.INFO) -> logging.Logger:
    log_filename = f"backup_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

    logging.basicConfig(
        level=level,
        format=log_format,
        handlers=[logging.FileHandler(log_filename), logging.StreamHandler(sys.stdout)],
    )

    return logging.getLogger("BackupSystem")


def load_defaults(config_name: str = "default_params.toml") -> dict:
    default_path = Path(__file__).parent.parent.parent / config_name
    with open(default_path, "rb") as f:
        return tomllib.load(f)


def get_latest_trade_id(instrument_id: InstrumentId) -> TradeId | None:
    # Placeholder function to get the latest trade ID for a given instrument
    # In a real implementation, this would query a database or storage system
    return None
