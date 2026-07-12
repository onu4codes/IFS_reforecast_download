#!/usr/bin/env python3
"""
batch_download.py

Orchestrates the full pipeline across all model version dates in a
named config_model_version_dates.json entry:

    for each model date (up to n_workers in parallel):
        download 4 GRIB files (download_single_date.download_single_date_files)
        build the merged in-memory dataset (zarr_writer.build_dataset)
    <-- workers stop here, return the dataset to the main process -->
    main process (serial, one at a time):
        append the dataset to the shared zarr store (zarr_writer.write_zarr)
        delete that date's GRIB files on success (kept on failure, for debugging)

Workers do NOT write to the zarr store themselves -- concurrent writes
to the same store from separate processes risk corruption. Only the
main process ever calls write_zarr(), serially, one date at a time.

Resume support: at startup, if the target zarr store already exists,
its current 'time' values are read once; any model date whose full
hindcast-year set is already present is skipped entirely.

Logging: all worker processes send log records through a
multiprocessing.Queue to a single QueueListener running in the main
process, which writes them to the one shared pipeline log file
(config_paths.json -> log_file). This avoids the file-corruption risk
of multiple separate OS processes writing to the same file directly.
Per project convention, nothing is printed to the terminal.

CLI usage:
    python batch_download.py
    python batch_download.py --dates-config-key s2s_2025 --n-workers 4
"""

import argparse
import logging
import logging.handlers
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import numpy as np

from utils import config_loader
from utils import date_generator
from utils import zarr_writer
import download_single_date

logger = logging.getLogger(__name__)


def _worker_logging_init(log_queue):
    """
    Run once per worker process at startup (ProcessPoolExecutor
    initializer). Configures the worker's root logger to send
    everything through the shared queue instead of writing to any
    file directly -- the main process's QueueListener does the actual
    file writing.
    """
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(logging.handlers.QueueHandler(log_queue))


def _process_one_date_worker(model_date, area_key, grid_key, paths_key, dates_config_key):
    """
    Runs in a worker process. Downloads all variable groups for one
    model date and builds the merged in-memory dataset. Does NOT write
    to the zarr store -- returns the dataset (and downloaded file
    paths) to the main process for that.

    Returns one of:
        ("success", model_date, dataset, downloaded_paths_dict)
        ("failed", model_date, error_message)
    """
    log = logging.getLogger(__name__)
    try:
        downloaded = download_single_date.download_single_date_files(
            model_date,
            area_key=area_key,
            grid_key=grid_key,
            paths_key=paths_key,
            dates_config_key=dates_config_key,
        )
        ds = zarr_writer.build_dataset(downloaded, model_date)
        # Force all data into memory (plain numpy, not lazy/dask-backed)
        # so the dataset can be safely pickled back to the main process.
        ds = ds.load()

        log.info(f"Worker finished download+build for model_date={model_date}")
        return ("success", model_date, ds, downloaded)

    except Exception as e:
        log.error(f"Worker failed for model_date={model_date}: {e}")
        return ("failed", model_date, str(e))


def _get_existing_zarr_times(zarr_path):
    """
    Return the set of existing 'time' values (as datetime64[D]) in the
    target zarr store, or an empty set if it doesn't exist yet.
    """
    if not Path(zarr_path).exists():
        return set()
    try:
        import xarray as xr
        ds = xr.open_zarr(zarr_path)
        return set(np.array(ds["time"].values, dtype="datetime64[D]"))
    except Exception as e:
        logger.warning(f"Could not read existing zarr times for resume check: {e}")
        return set()


def _is_date_already_done(model_date, hyear_list, existing_times):
    """Check if all of a model date's hindcast years are already in the zarr."""
    expected = {
        np.datetime64(f"{hy}-{model_date.month:02d}-{model_date.day:02d}", "D")
        for hy in hyear_list
    }
    return expected.issubset(existing_times)


def run_batch_download(
    dates_config_key="s2s_2025",
    area_key="south_asia",
    grid_key="grids_1deg",
    paths_key="default",
    n_workers=4,
):
    """
    Main orchestration function. See module docstring for the full
    pipeline description.
    """
    zarr_path = config_loader.get_zarr_path(paths_key)

    all_pairs = date_generator.get_model_dates_with_hindcast_years(dates_config_key)
    existing_times = _get_existing_zarr_times(zarr_path)

    pending = [
        (model_date, hyear_list)
        for model_date, hyear_list in all_pairs
        if not _is_date_already_done(model_date, hyear_list, existing_times)
    ]
    n_skipped = len(all_pairs) - len(pending)

    logger.info(
        f"Batch run starting: {len(all_pairs)} total model dates, "
        f"{n_skipped} already done (skipped), {len(pending)} pending. "
        f"zarr={zarr_path}, n_workers={n_workers}"
    )

    n_success = 0
    n_failed = 0

    log_queue = multiprocessing.Queue(-1)
    queue_listener = logging.handlers.QueueListener(log_queue, *logging.getLogger().handlers)
    queue_listener.start()

    try:
        with ProcessPoolExecutor(
            max_workers=n_workers,
            initializer=_worker_logging_init,
            initargs=(log_queue,),
        ) as executor:
            futures = {
                executor.submit(
                    _process_one_date_worker, model_date, area_key, grid_key, paths_key, dates_config_key
                ): model_date
                for model_date, _ in pending
            }

            for future in as_completed(futures):
                model_date = futures[future]
                result = future.result()

                if result[0] == "success":
                    _, _, dataset, downloaded_paths = result
                    try:
                        zarr_writer.write_zarr(dataset, zarr_path)
                        for path in downloaded_paths.values():
                            Path(path).unlink(missing_ok=True)
                        logger.info(f"SUCCESS {model_date}: appended to zarr, cleaned up GRIB files")
                        n_success += 1
                    except Exception as e:
                        logger.error(
                            f"FAILED {model_date}: zarr write/cleanup error: {e}. "
                            f"GRIB files kept: {list(downloaded_paths.values())}"
                        )
                        n_failed += 1
                else:
                    _, _, error_message = result
                    logger.error(f"FAILED {model_date}: {error_message}")
                    n_failed += 1
    finally:
        queue_listener.stop()

    logger.info(
        f"Batch run complete: {n_success} succeeded, {n_skipped} skipped, {n_failed} failed"
    )
    if n_failed > 0:
        logger.info("Re-run the same command to retry failed/remaining dates (resume-safe).")

    return {"succeeded": n_success, "skipped": n_skipped, "failed": n_failed}


def _setup_main_logging():
    """Same single-shared-log-file setup as download_single_date.py."""
    log_path = config_loader.get_log_path()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    already_configured = any(
        isinstance(h, logging.FileHandler) and Path(h.baseFilename) == log_path
        for h in root_logger.handlers
    )
    if not already_configured:
        fh = logging.FileHandler(log_path, mode="a")
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        root_logger.addHandler(fh)

    return log_path


def _parse_args():
    p = argparse.ArgumentParser(description="Batch download+convert S2S reforecast data across all model version dates")
    p.add_argument("--dates-config-key", default="s2s_2025", help="Named model dates config key (default: s2s_2025)")
    p.add_argument("--area-key", default="south_asia", help="Named area config key (default: south_asia)")
    p.add_argument("--grid-key", default="grids_1deg", help="Named grid config key (default: grids_1deg)")
    p.add_argument("--paths-key", default="default", help="Named paths config key (default: default)")
    p.add_argument("--n-workers", type=int, default=4, help="Number of parallel date workers (default: 4)")
    return p.parse_args()


def main():
    args = _parse_args()
    log_path = _setup_main_logging()

    try:
        run_batch_download(
            dates_config_key=args.dates_config_key,
            area_key=args.area_key,
            grid_key=args.grid_key,
            paths_key=args.paths_key,
            n_workers=args.n_workers,
        )
    finally:
        logger.info(f"Log written to {log_path}")


if __name__ == "__main__":
    main()