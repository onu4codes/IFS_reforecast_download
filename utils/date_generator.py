"""
utils/date_generator.py

Generates the list of model version dates (and each one's corresponding
hindcast year list) from a named entry in config_model_version_dates.json.

Cadence rules are implemented as a lookup table of functions, keyed by
the 'cadence' string in the config -- adding a new cadence later (e.g.
'monday_thursday' for pre-Cycle-49r1 data) means adding one function
and one dict entry here, not touching any calling code.

No file I/O beyond what config_loader already does, no printing --
uses the standard logging module, consistent with the rest of utils/.
"""

import logging
from datetime import datetime, timedelta

from utils import config_loader
from utils.config_loader import ConfigError

logger = logging.getLogger(__name__)


def _cadence_odd_day_of_month(start_date, end_date):
    """All calendar days with an odd day-of-month value, inclusive of both ends."""
    dates = []
    d = start_date
    while d <= end_date:
        if d.day % 2 == 1:
            dates.append(d)
        d += timedelta(days=1)
    return dates


# Lookup table: cadence name (as used in config_model_version_dates.json) -> function.
_CADENCE_HANDLERS = {
    "odd_day_of_month": _cadence_odd_day_of_month,
}


def _parse_date(date_str, field_name):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError as e:
        raise ConfigError(f"Invalid {field_name} '{date_str}', expected YYYY-MM-DD") from e


def generate_model_dates(config_key):
    """
    Return the list of model version dates (datetime.date objects) for
    the named config entry, per its 'cadence' rule.
    """
    cfg = config_loader.get_model_dates_config(config_key)

    start_date = _parse_date(cfg["start_date"], "start_date")
    end_date = _parse_date(cfg["end_date"], "end_date")
    if start_date > end_date:
        raise ConfigError(
            f"start_date {start_date} is after end_date {end_date} in model dates config '{config_key}'"
        )

    cadence = cfg["cadence"]
    handler = _CADENCE_HANDLERS.get(cadence)
    if handler is None:
        raise ConfigError(
            f"Unknown cadence '{cadence}' in model dates config '{config_key}'. "
            f"Implemented cadences: {sorted(_CADENCE_HANDLERS.keys())}"
        )

    dates = handler(start_date, end_date)
    logger.info(
        f"Generated {len(dates)} model version dates for config '{config_key}' "
        f"(cadence='{cadence}', {start_date} to {end_date})"
    )
    return dates


def get_hindcast_years(model_date, hindcast_years_back):
    """
    Return the list of hindcast years (as strings, e.g. '2005') for one
    model version date, given how many years back to go.

    E.g. model_date.year=2025, hindcast_years_back=20 -> ['2005', ..., '2024']
    """
    if hindcast_years_back < 1:
        raise ConfigError(f"hindcast_years_back must be >= 1, got {hindcast_years_back}")

    end_year = model_date.year - 1
    start_year = model_date.year - hindcast_years_back
    return [str(y) for y in range(start_year, end_year + 1)]


def get_model_dates_with_hindcast_years(config_key):
    """
    Main entry point for callers (e.g. batch_download.py).

    Returns a list of (model_date, hyear_list) tuples for the named
    config entry -- one tuple per model version date, each paired with
    its own hindcast year list.
    """
    cfg = config_loader.get_model_dates_config(config_key)
    hindcast_years_back = cfg["hindcast_years_back"]

    model_dates = generate_model_dates(config_key)

    result = [
        (model_date, get_hindcast_years(model_date, hindcast_years_back))
        for model_date in model_dates
    ]

    logger.info(
        f"Built {len(result)} (model_date, hyear_list) pairs for config '{config_key}' "
        f"(hindcast_years_back={hindcast_years_back})"
    )
    return result