from file_storage import LocalStorageReader, LocalStorageWriter, StorageRouter
from historical_backup_service.historical_backup_service import HistoricalBackupService
from okx_trade_fetcher.okx_trade_fetcher import OKXTradeFetcher
from utilities.helpers import load_defaults, setup_logging


def main() -> None:
    #################
    # --- Setup --- #
    #################
    logger = setup_logging()
    logger.info("Setting up...")

    defaults = load_defaults()

    file_storage_reader = LocalStorageReader(
        base_dir=defaults["file_storage"]["base_local_path"],
    )
    file_storage_writer = LocalStorageWriter(
        base_dir=defaults["file_storage"]["base_local_path"],
    )
    storage_router = StorageRouter(
        storage_writer=file_storage_writer,
        chunk_size=defaults["file_storage"]["chunk_size"],
    )

    okx_trade_fetcher = OKXTradeFetcher()

    backup_service = HistoricalBackupService(
        okx_trade_fetcher=okx_trade_fetcher,
        storage_router=storage_router,
        storage_reader=file_storage_reader,
    )

    instrument_ids = defaults["api_params"]["instrument_ids"]
    logger.info(f"Running for instruments: {instrument_ids}")

    ###############
    # --- Run --- #
    ###############
    backup_service.run_backup(instrument_ids=instrument_ids)


if __name__ == "__main__":
    main()
