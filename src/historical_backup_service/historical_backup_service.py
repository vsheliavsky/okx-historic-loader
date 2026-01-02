from collections.abc import Sequence
from logging import getLogger

from file_storage import StorageRouter, StorageReader
from okx_trade_fetcher.okx_trade_fetcher import OKXTradeFetcher
from utilities.custom_types import InstrumentId, HistoricalTradeFetchParams

logger = getLogger(__name__)


class HistoricalBackupService:
    def __init__(
        self,
        okx_trade_fetcher: OKXTradeFetcher,
        storage_reader: StorageReader,
        storage_router: StorageRouter,
    ):
        self.okx_trade_fetcher = okx_trade_fetcher
        self.storage_reader = storage_reader
        self.storage_router = storage_router

    def _run_backup_single_instrument(
        self,
        instrument_id: str,
    ) -> None:
        logger.info(f"Starting backup for instrument {instrument_id}")

        latest_trade_id = self.storage_reader.get_latest_trade_id(instrument_id=instrument_id)
        trades = self.okx_trade_fetcher.yield_historical_trades(instrument_id=instrument_id, after=latest_trade_id)  # TODO: fix before/after

        self.storage_router.process_trades(trades=trades)

        logger.info(f"Finished backup for instrument {instrument_id}")

    def run_backup(self, instrument_ids: Sequence[InstrumentId]) -> None:
        for instrument_id in instrument_ids:
            self._run_backup_single_instrument(instrument_id=instrument_id)
