"""
utils/request_builder.py

Assembles the actual cdsapi request dict for one variable group, one
model version date (with its hindcast year list), and named area/grid
configs.

Replaces the hardcoded base_fields() + per-group retrieve_*() functions
from the original download_s2s_single_date.py with one generic,
config-driven function.

Fixed values that have never varied across anything tested so far
(origin, time, forecast_type) are hardcoded constants here rather than
config fields -- see module-level constants below.

No file I/O beyond what config_loader already does, no printing --
uses the standard logging module, consistent with the rest of utils/.
"""

import logging

from utils import config_loader
from utils.config_loader import ConfigError

logger = logging.getLogger(__name__)

# Fixed values, confirmed unchanging across everything built so far.
# If this ever needs to vary, move it into a config file instead.
ORIGIN = "ecmwf"
TIME = "00:00"
FORECAST_TYPE = "control_forecast"

DATASET = "s2s-reforecasts"


def build_leadtime_hour(group_config):
    """
    Expand leadtime_start/leadtime_end/leadtime_step into the actual
    leadtime_hour list, per the group's leadtime_type.

    'point'  -> ["0", "24", ..., end] (single-value steps)
    'range'  -> ["0_24", "24_48", ..., "end_end+step"] (period-mean ranges)
    """
    leadtime_type = group_config["leadtime_type"]
    start = group_config["leadtime_start"]
    end = group_config["leadtime_end"]
    step = group_config["leadtime_step"]

    if leadtime_type == "point":
        return [str(h) for h in range(start, end + step, step)]
    elif leadtime_type == "range":
        return [f"{h}_{h + step}" for h in range(start, end + step, step)]
    else:
        raise ConfigError(
            f"Unknown leadtime_type '{leadtime_type}'. Expected 'point' or 'range'."
        )


def build_request(group_name, model_date, hyear_list, area_key, grid_key):
    """
    Build the full cdsapi request dict for one variable group, one
    model version date, its hindcast year list, and named area/grid
    configs.

    Returns (dataset_name, request_dict) -- dataset_name is always
    "s2s-reforecasts" for now, returned alongside the request dict for
    convenience at the call site (client.retrieve(dataset_name, request)).
    """
    group_config = config_loader.get_variable_group(group_name)

    if group_config["level_type"] == "pressure" and not group_config["level_value"]:
        raise ConfigError(
            f"Variable group '{group_name}' has level_type='pressure' but no level_value defined."
        )

    request = {
        "origin": ORIGIN,
        "year": f"{model_date.year:04d}",
        "month": f"{model_date.month:02d}",
        "day": f"{model_date.day:02d}",
        "time": TIME,
        "hyear": hyear_list,
        "hmonth": [f"{model_date.month:02d}"],
        "hday": [f"{model_date.day:02d}"],
        "level_type": group_config["level_type"],
        "variable": group_config["cds_variable"],
        "forecast_type": FORECAST_TYPE,
        "leadtime_hour": build_leadtime_hour(group_config),
        "data_format": "grib",
        "area": config_loader.get_area(area_key),
        "grid": config_loader.get_grid(grid_key),
    }

    if group_config["level_value"]:
        request["level_value"] = group_config["level_value"]

    logger.info(
        f"Built request for group='{group_name}' model_date={model_date} "
        f"n_hyears={len(hyear_list)} area='{area_key}' grid='{grid_key}'"
    )
    return DATASET, request