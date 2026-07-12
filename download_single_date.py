#!/usr/bin/env python3
"""
download_single_date.py

Top-level entry point: download all variable groups (from
config_variables.json) for ONE model version date, using the
config-driven pipeline (config_loader, date_generator, request_builder).

Exposes download_single_date_files() as an importable function so
batch_download.py can call it directly (no subprocess spawning), plus
a CLI wrapper for standalone runs.

Fail-fast: if any group's download fails, the exception propagates
immediately. Deciding what to do about it (log, keep files for
debugging, skip to next date, etc.) is the caller's responsibility --
this script does not catch or suppress errors itself.

Per project convention: nothing is printed to the terminal. All
progress/errors go to a log file only.

CLI usage:
    python download_single_date.py --model-date 2025-06-01
    python download_single_date.py --model-date 2025-06-01 --groups wind_msl precip
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

try:
    import cdsapi
except ImportError:
    sys.exit("cdsapi is not installed. Install it with:\n    pip install cdsapi")

from utils import config_loader
from utils import date_generator
from utils import request_builder
from utils import zarr_writer
from utils.config_loader import ConfigError

logger = logging.getLogger(__name__)


def download_single_date_files(
    model_date,
    groups=None,
    area_key="south_asia",
    grid_key="grids_1deg",
    paths_key="default",
    dates_config_key="s2s_2025",
    client=None,
):
    """
    Download all (or a specified subset of) variable groups for one
    model version date.

    Returns a dict of {group_name: downloaded_file_path}.

    Raises on the first failure -- does not attempt remaining groups
    once one fails, and does not clean up partially-downloaded files
    (that's the caller's decision).
    """
    if groups is None:
        groups = config_loader.list_variable_groups()

    dates_config = config_loader.get_model_dates_config(dates_config_key)
    hindcast_years_back = dates_config["hindcast_years_back"]
    hyear_list = date_generator.get_hindcast_years(model_date, hindcast_years_back)

    workdir = config_loader.get_workdir_path(paths_key)
    workdir.mkdir(parents=True, exist_ok=True)

    if client is None:
        client = cdsapi.Client()

    downloaded = {}
    for group_name in groups:
        dataset, request = request_builder.build_request(
            group_name, model_date, hyear_list, area_key, grid_key
        )
        target = workdir / zarr_writer.group_filename(group_name, model_date)

        logger.info(f"Retrieving group='{group_name}' model_date={model_date} -> {target}")
        client.retrieve(dataset, request).download(str(target))
        logger.info(f"Downloaded group='{group_name}' -> {target}")

        downloaded[group_name] = target

    logger.info(
        f"Completed all {len(downloaded)} group(s) for model_date={model_date}: "
        f"{list(downloaded.keys())}"
    )
    return downloaded


def _setup_logging():
    """
    Configure root logger with a single FileHandler pointing at the
    shared pipeline log file (config_paths.json -> log_file). Appends
    rather than overwrites, so multiple runs/dates accumulate into one
    continuous record rather than one file per date or per invocation.

    No StreamHandler is attached -- nothing prints to the terminal.
    """
    log_path = config_loader.get_log_path()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Avoid attaching duplicate handlers if this is somehow called more
    # than once in the same process.
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
    p = argparse.ArgumentParser(description="Download one model version date's S2S reforecast data")
    p.add_argument("--model-date", required=True, help="Model version date, YYYY-MM-DD")
    p.add_argument("--groups", nargs="+", default=None,
                    help="Variable group names to download (default: all groups in config_variables.json)")
    p.add_argument("--area-key", default="south_asia", help="Named area config key (default: south_asia)")
    p.add_argument("--grid-key", default="grids_1deg", help="Named grid config key (default: grids_1deg)")
    p.add_argument("--paths-key", default="default", help="Named paths config key (default: default)")
    p.add_argument("--dates-config-key", default="s2s_2025",
                    help="Named model dates config key, used for hindcast_years_back (default: s2s_2025)")
    return p.parse_args()


def main():
    args = _parse_args()
    model_date = datetime.strptime(args.model_date, "%Y-%m-%d").date()

    log_path = _setup_logging()

    try:
        download_single_date_files(
            model_date,
            groups=args.groups,
            area_key=args.area_key,
            grid_key=args.grid_key,
            paths_key=args.paths_key,
            dates_config_key=args.dates_config_key,
        )
    except (ConfigError, Exception) as e:
        logger.error(f"download_single_date_files failed for {model_date}: {e}")
        raise
    finally:
        logger.info(f"Log written to {log_path}")


if __name__ == "__main__":
    main()