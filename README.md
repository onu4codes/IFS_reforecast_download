# IFS S2S Reforecast Download Pipeline

A config-driven pipeline for downloading ECMWF IFS Sub-seasonal to Seasonal
(S2S) reforecast data via the ECMWF Data Store (ECDS) `cdsapi`, converting it
into a merged Zarr store, and keeping it properly time-sorted -- built to run
either interactively or as a Slurm batch job on a shared cluster.

---

## Table of contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Repository structure](#repository-structure)
4. [Data specification](#data-specification)
5. [Config files](#config-files)
6. [`utils/` modules](#utils-modules)
7. [Top-level scripts](#top-level-scripts)
8. [Typical workflows](#typical-workflows)
9. [Adding a new scenario](#adding-a-new-scenario)
10. [Known gotchas and lessons learned](#known-gotchas-and-lessons-learned)
11. [Troubleshooting](#troubleshooting)
12. [Sharing output data](#sharing-output-data)

---

## Overview

This pipeline downloads S2S IFS reforecast data for a configurable set of
variables, area, grid resolution, and date range, and assembles it into a
single Zarr store with the structure:

```
Dimensions: (time, step, lat, lon)
  time -> actual hindcast start dates (e.g. 2005-06-01 ... 2024-06-01),
          NOT the model version reference date
  step -> integer lead-day index, 0 to N
  lat, lon -> the configured grid
```

Everything that varies between runs -- which variables, which region, which
resolution, which dates, where output lands -- is controlled by JSON config
files, not by editing Python code. The only things you should ever need to
touch to run a new scenario are the config files and command-line flags.

---

## Prerequisites

### Software

```bash
pip install cdsapi xarray cfgrib zarr dask numpy
```

### ECDS credentials

Register at https://ecds.ecmwf.int/, then save your API key to `~/.cdsapirc`:

```
url: https://ecds.ecmwf.int/api
key: <your-api-key>
```

Accept the S2S dataset licence on the ECDS website before your first request,
or requests will fail with a permissions error regardless of everything else
being correct.

### Conda environment note (cluster-specific)

If running under Slurm, `conda activate` inside a non-interactive batch shell
does **not** reliably work via `source ~/.bashrc` -- many `.bashrc` files have
an early-exit guard for non-interactive shells that skips the conda init
lines. Source conda directly instead:

```bash
source /opt/conda/etc/profile.d/conda.sh
conda activate climate
```

Also, do **not** use `set -euo pipefail` in Slurm scripts that activate
conda -- conda's own activation/deactivation hook scripts reference unset
variables and will crash under `-u` (nounset). Use `set -eo pipefail`
(no `-u`) instead.

---

## Repository structure

```
IFS_reforecast_download/
├── config/
│   ├── config_area.json
│   ├── config_grid.json
│   ├── config_variables.json
│   ├── config_model_version_dates.json
│   └── config_paths.json
├── utils/
│   ├── __init__.py
│   ├── config_loader.py
│   ├── date_generator.py
│   ├── request_builder.py
│   ├── postprocess.py
│   └── zarr_writer.py
├── download_single_date.py
├── batch_download.py
├── sort_zarr_by_time.py
├── submit_batch_download.sh
├── submit_sort_zarr.sh
├── run_pipeline.sh
├── .gitignore
└── README.md
```

At runtime, these additional paths get created as siblings to `config/` and
`utils/` (all gitignored):

```
├── s2s_reforecast.zarr/          (main output, grows via append)
├── s2s_reforecast_sorted.zarr/   (time-sorted counterpart, regenerated each run)
├── s2s_work/                     (scratch dir for in-flight GRIB downloads)
└── logs/
    └── pipeline.log              (single shared log file for the whole pipeline)
```

---

## Data specification

The default configuration (`combination_1` variable set, `south_asia` area,
`grids_1deg` grid, `s2s_2025` dates) produces:

| Property | Value |
|---|---|
| Area | 38.5N-6.5N, 66.5E-100.5E (South Asia) |
| Grid | 1.0 x 1.0 degree |
| Lead time | 0-42 days |
| Ensemble | control forecast only |
| Hindcast years | 20 years back from each model version date |
| Model version cadence | odd calendar days of the month (IFS Cycle 49r1) |
| Origin | ecmwf (IFS) |

### Variable groups (in `combination_1`)

| Group | Variables | Level type | Postprocessing |
|---|---|---|---|
| `wind_msl` | 10m U/V wind, MSLP | single_level | none |
| `precip` | total precipitation | single_level | deaccumulate (cumulative -> true daily totals) |
| `t2m` | 2m temperature | single_level | realign period-mean steps onto common axis |
| `pressure_level_vars` | specific humidity, temperature, U/V wind | pressure (1000/850/500/200 hPa) | flatten pressure levels into separate named variables |

Final output variables (21 total): `10m_u_component_of_wind`,
`10m_v_component_of_wind`, `mean_sea_level_pressure`, `total_precipitation`,
`2m_temperature`, and 4 pressure levels each of `specific_humidity_*`,
`temperature_*`, `u_component_of_wind_*`, `v_component_of_wind_*`.

**Important quirk:** `2m_temperature` has `NaN` at `step=0`. This is correct,
not a bug -- `t2m` is requested as period-mean ranges (`"0_24"`, `"24_48"`,
...), and there is no valid "mean over day 0" (day 0 is the instantaneous
initialization point, not a 24-hour window).

---

## Config files

All config files live in `config/` and use a consistent **named-entry**
pattern: each file is a JSON object whose top-level keys are names you choose,
and whose values are the actual settings. This lets you add new scenarios by
adding a new named entry, without ever touching Python code.

### `config_area.json`

```json
{
  "south_asia": {
    "area": [38.5, 66.5, 6.5, 100.5],
    "description": "South Asia domain used for IFS S2S reforecast download"
  }
}
```
`area` is `[North, West, South, East]`.

### `config_grid.json`

```json
{
  "grids_1deg": { "grid": ["1.0", "1.0"], "description": "..." },
  "grids_0p25deg": { "grid": ["0.25", "0.25"], "description": "..." }
}
```
**Note:** `grids_1deg` is confirmed working (tested against real ECDS
requests). `grids_0p25deg` has **not** been verified end-to-end -- test it
with a small isolated request before relying on it at scale.

### `config_variables.json`

Nested one level deeper than the other configs: `{variable_set_name: {group_name: {group_config}}}`.

```json
{
  "combination_1": {
    "wind_msl": {
      "cds_variable": ["10_m_u_component_of_wind", "10_m_v_component_of_wind", "mean_sea_level_pressure"],
      "level_type": "single_level",
      "level_value": null,
      "leadtime_type": "point",
      "leadtime_start": 0,
      "leadtime_end": 1008,
      "leadtime_step": 24,
      "output_names": {
        "u10": "10m_u_component_of_wind",
        "v10": "10m_v_component_of_wind",
        "msl": "mean_sea_level_pressure"
      },
      "postprocess": null
    },
    "precip": { "...": "..." },
    "t2m": { "...": "..." },
    "pressure_level_vars": { "...": "..." }
  },
  "rainfall_only": {
    "precip": { "...same full definition as in combination_1..." }
  }
}
```

Field reference for each group:

| Field | Meaning |
|---|---|
| `cds_variable` | List of CDS API variable name(s) to request |
| `level_type` | `"single_level"` or `"pressure"` |
| `level_value` | List of pressure levels (e.g. `["1000_hpa", ...]`), or `null` for single_level |
| `leadtime_type` | `"point"` (single-value steps) or `"range"` (period-mean ranges like `"0_24"`) |
| `leadtime_start` / `leadtime_end` / `leadtime_step` | Expanded into the actual `leadtime_hour` list |
| `output_names` | Maps cfgrib shortName (e.g. `"u10"`) to the final descriptive variable name |
| `postprocess` | `null`, `"deaccumulate"`, `"realign_step_range_end_labeled"`, or `"flatten_pressure_levels"` |

**JSON has no comment syntax.** You cannot "comment out" a group to disable
it -- either delete it, or (much safer) define a separate named variable set
that only includes the groups you want. See
[Adding a new scenario](#adding-a-new-scenario).

### `config_model_version_dates.json`

```json
{
  "s2s_2025": {
    "start_date": "2025-01-01",
    "end_date": "2025-12-31",
    "cadence": "odd_day_of_month",
    "hindcast_years_back": 20,
    "description": "IFS Cycle 49r1 reforecast cadence, full year 2025"
  }
}
```
`cadence` is a named rule recognized by `utils/date_generator.py`. Currently
implemented: `"odd_day_of_month"`. Add a new cadence by adding both a new
entry here and a new handler function in `date_generator.py`.

### `config_paths.json`

```json
{
  "default": {
    "zarr_store": "s2s_reforecast.zarr",
    "zarr_store_sorted": "s2s_reforecast_sorted.zarr",
    "workdir": "s2s_work",
    "log_file": "logs/pipeline.log",
    "description": "..."
  }
}
```
All paths are relative to the **repo root** (parent of `config/` and
`utils/`), resolved to absolute paths regardless of the caller's current
working directory.

**Important:** use a different `paths_key` (a new named entry here) for any
run with a different area, grid, or variable set than an existing run.
Mixing incompatible dimensions into the same Zarr store will corrupt it or
fail to write.

---

## `utils/` modules

Reusable logic, no CLI, no side effects beyond what's documented. Every
function defaults its `variable_set`/`area_key`/`grid_key`/etc. parameters to
match the original `combination_1` / `south_asia` / `grids_1deg` setup, so
existing calls that don't pass these parameters keep working unchanged.

| Module | Purpose |
|---|---|
| `config_loader.py` | Loads and validates all 5 config files; provides `get_area()`, `get_grid()`, `get_variable_group()`, `get_model_dates_config()`, `get_zarr_path()`, `get_workdir_path()`, `get_log_path()`, etc. Raises `ConfigError` with clear messages (listing valid keys) on typos. |
| `date_generator.py` | `generate_model_dates(config_key)` applies the named cadence rule; `get_hindcast_years(model_date, years_back)` computes the hyear list; `get_model_dates_with_hindcast_years(config_key)` combines both into `(model_date, hyear_list)` pairs. |
| `request_builder.py` | `build_request(group_name, model_date, hyear_list, area_key, grid_key, variable_set=...)` assembles the actual `cdsapi` request dict, including expanding `leadtime_hour` per `leadtime_type`. |
| `postprocess.py` | `deaccumulate()`, `realign_step_range_end_labeled()`, `flatten_pressure_levels()`, dispatched via `apply_postprocess(ds, group_config)`. |
| `zarr_writer.py` | `process_group_file()` standardizes+postprocesses+renames one group's GRIB file; `build_dataset()` merges all groups for one date; `write_zarr()` creates/appends to the Zarr store. `group_filename()` is the shared file-naming convention used by both this module and the download scripts. |

---

## Top-level scripts

### `download_single_date.py`

Downloads all (or a subset of) variable groups for **one** model version
date. Exposes `download_single_date_files(...)` as an importable function
(used by `batch_download.py`) plus a CLI.

```bash
python download_single_date.py --model-date 2025-06-01
python download_single_date.py --model-date 2025-06-01 --variable-set rainfall_only --grid-key grids_0p25deg
python download_single_date.py --model-date 2025-06-01 --groups wind_msl precip
```

Fail-fast: if any group's download fails, the exception propagates
immediately -- it does not attempt remaining groups, and does not clean up
partially-downloaded files (that decision belongs to the caller).

### `batch_download.py`

Orchestrates the full pipeline across all model version dates in a named
`config_model_version_dates.json` entry, with 4 (configurable) dates
processed in parallel. Each worker downloads + builds the merged dataset;
only the **main process** ever writes to the Zarr store (serially, to avoid
concurrent-write corruption).

```bash
python batch_download.py
python batch_download.py --dates-config-key s2s_2025 --variable-set combination_1 --n-workers 4
python batch_download.py --grid-key grids_0p25deg --variable-set rainfall_only --paths-key rainfall_0p25deg
```

**Resume-safe:** at startup, reads the target Zarr store's existing `time`
values once; any model date whose full hindcast-year set is already present
is skipped. Safe to re-run the same command after any interruption or
partial failure.

**Cleanup policy:** GRIB files are deleted after a date's data is
successfully appended to the Zarr store; kept (for debugging) if that date
failed.

### `sort_zarr_by_time.py`

The Zarr store's `time` axis is **not** globally sorted after a batch run --
each model version date's block of hindcast years gets appended in the order
dates were processed, not chronological hindcast order. This script sorts it,
writing the result to the configured `zarr_store_sorted` path (always
overwritten fresh, since the source may have grown since the last sort).

```bash
python sort_zarr_by_time.py
```

Loads the full dataset into memory to sort -- fine for the current data size
(a few GB), but would need a chunked/out-of-core approach if the dataset
grows much larger.

### Slurm scripts

| Script | Purpose |
|---|---|
| `submit_batch_download.sh` | Slurm wrapper for `batch_download.py`. Edit `--partition`, `--account` (if required), and the `python batch_download.py` flags for your scenario before submitting. |
| `submit_sort_zarr.sh` | Slurm wrapper for `sort_zarr_by_time.py`. Runs as a scheduled job rather than on a login node, since the in-memory sort can exceed login-node memory limits. |
| `run_pipeline.sh` | Plain bash (not itself an `sbatch` job) that submits both jobs above, chaining the sort job with `--dependency=afterok:<download_job_id>` so it only starts once the download job exits successfully. |

```bash
chmod +x run_pipeline.sh
./run_pipeline.sh
```

**Note:** `batch_download.py`'s `main()` exits 0 even if some individual
dates failed (they're logged, not fatal to the whole run) -- so `afterok`
will still trigger the sort job even with some failures. Check
`logs/pipeline.log` for `FAILED` entries after the pipeline finishes, and
re-run `submit_batch_download.sh` to pick up stragglers if needed.

---

## Typical workflows

### Quick single-date test (no Slurm, run directly)

```bash
python download_single_date.py --model-date 2025-06-01
python sort_zarr_by_time.py  # only meaningful after multiple dates
```

### Full year, default scenario, via Slurm

```bash
chmod +x run_pipeline.sh
./run_pipeline.sh
squeue -u $USER
tail -f logs/pipeline.log
```

### Checking progress mid-run

```bash
grep -c "SUCCESS" logs/pipeline.log
grep -c "FAILED" logs/pipeline.log
python3 -c "
import xarray as xr
ds = xr.open_zarr('s2s_reforecast.zarr')
print(ds.sizes['time'])
"
```

### Resuming after an interruption or partial failure

Just re-run the same command (`./run_pipeline.sh` or
`python batch_download.py ...`) -- resume-skip logic handles the rest.

### Testing a new grid/variable-set combination before a full run

Run `download_single_date.py` for a single date first with the new
`--variable-set`/`--grid-key`, and inspect the resulting GRIB structure
(`grib_dump`, or open with `cfgrib`/`xarray`) before committing to a full
186-date batch run.

---

## Adding a new scenario

Every "new scenario" is a combination of area + grid + variable set + dates
+ output path. None of these require editing Python code -- only config
files and command-line flags.

| Want to change... | Edit this file | Then pass |
|---|---|---|
| Region | `config_area.json`: add a new named entry | `--area-key <name>` |
| Resolution | `config_grid.json`: add a new named entry | `--grid-key <name>` |
| Which variables | `config_variables.json`: add a new named **variable set** (do not delete/comment out groups in an existing set that another run depends on) | `--variable-set <name>` |
| Date range / cadence / hindcast window | `config_model_version_dates.json`: add a new named entry | `--dates-config-key <name>` |
| Output location | `config_paths.json`: add a new named entry (**required** if area/grid/variable-set differs from an existing run, to avoid corrupting that store) | `--paths-key <name>` |

Example: rainfall-only at 0.25 degree, without touching the existing full
1-degree run:

1. Add `"grids_0p25deg"` to `config_grid.json` (may already exist -- verify
   it actually works with a small test request first, since this hasn't been
   confirmed against real ECDS yet)
2. Add `"rainfall_only"` to `config_variables.json` as a new top-level set
   containing just the `precip` group (full definition, not a reference --
   JSON can't reference across a file)
3. Add a new named entry to `config_paths.json` (e.g. `"rainfall_0p25deg"`)
   pointing at a fresh Zarr store / workdir
4. Run:
   ```bash
   python batch_download.py --grid-key grids_0p25deg --variable-set rainfall_only --paths-key rainfall_0p25deg
   ```

---

## Known gotchas and lessons learned

These are things that actually broke during development -- worth reading
before you hit them yourself.

- **Config is read fresh on every single call, with no caching.** Editing a
  config JSON file while a batch job is actively running will cause a
  cascading failure for every request made after the edit (each one hits a
  `JSONDecodeError` or `ConfigError`). The overall job usually keeps running
  and exits cleanly (since each date's failure is caught and logged, not
  fatal) -- meaning a `run_pipeline.sh`-style Slurm dependency chain will
  still trigger the sort job, even though most dates after the edit failed.
  **Never edit config files while a batch job is running.** Wait for it to
  finish, or cancel it first.

- **JSON has no comment syntax.** You cannot "comment out" a variable group.
  Use separate named variable sets instead (see
  [Adding a new scenario](#adding-a-new-scenario)).

- **CDS variable naming is picky about underscore placement.** It's
  `10_m_u_component_of_wind` (with the underscore between `10` and `m`), not
  `10m_u_component_of_wind`. Getting this wrong doesn't error clearly --
  MARS silently tries to interpret the malformed string as something else
  entirely (e.g. an aerosol parameter), and the error message ("Ambiguous:
  X could be Y or Z") can be confusing if you don't know to look for a
  variable-name typo.

- **`grid` overrides work, but must be explicit.** Without an explicit
  `"grid"` key in the request, ECDS returns data at a much finer native
  resolution (~0.25 degree observed) than the older MARS-archive default
  (1.5 degree) -- there is no implicit "coarse default." Always set `grid`
  explicitly via `config_grid.json` if you need a specific resolution.

- **`2m_temperature`'s period-mean range format (`"0_24"`, etc.) labels each
  step by its END, not its start.** This means its native step values are
  offset by one relative to the other (point-type) groups, and it has no
  `step=0` at all. `postprocess.realign_step_range_end_labeled()` handles
  this, but if you add a new range-type variable group, expect the same
  offset behavior.

- **Total precipitation is archived as a running cumulative total since
  forecast start, not per-day totals.** `postprocess.deaccumulate()` handles
  the conversion (`day_N = tp[N] - tp[N-1]`), but if you ever bypass this
  postprocessing step, raw `tp` values will look wrong (monotonically
  increasing, not daily amounts).

- **Rewriting a Zarr store with different chunking than it was originally
  written with requires clearing stale chunk encoding first.** Otherwise
  `to_zarr()` raises a chunk-conflict error. See `sort_zarr_by_time.py` for
  the fix (`ds[var].encoding.pop("chunks", None)` before rechunking).

- **`xr.open_zarr()`'s default consolidated-metadata cache can appear stale**
  immediately after an append -- `ds.sizes['time']` may show an outdated
  count even though the underlying data has genuinely grown. Use
  `xr.open_zarr(path, consolidated=False)` to bypass the cache and check the
  live state, or run `zarr.consolidate_metadata(path)` to refresh it.

- **Conda activation inside Slurm batch scripts needs
  `source /opt/conda/etc/profile.d/conda.sh`**, not `source ~/.bashrc` --
  and avoid `set -u` (nounset), since conda's own hook scripts reference
  unset variables. See [Prerequisites](#prerequisites).

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `mars - ERROR - Ambiguous: X could be Y or Z` | Malformed CDS variable name (wrong underscore placement) | Check `cds_variable` entries in `config_variables.json` against a known-working request |
| `400 Client Error: Bad Request` with no further detail, isolated to one date sandwiched between successes | Transient ECDS server-side issue | Just retry -- re-run the same batch command, resume-skip will only retry the failed date |
| `CondaError: Run 'conda init' before 'conda activate'` in a Slurm job | Non-interactive shell doesn't source `~/.bashrc`'s conda init lines | Use `source /opt/conda/etc/profile.d/conda.sh` directly |
| `... /geotiff-deactivate.sh: line 5: _CONDA_SET_GEOTIFF_CSV: unbound variable` | `set -u` conflicting with conda's activation hooks | Remove `-u` from `set -euo pipefail` -> `set -eo pipefail` |
| Large fraction of dates fail right after a specific timestamp, all with the same generic error | Config file was edited while the batch job was running | Fix the config, then just re-run -- resume-skip handles the rest |
| `ds.sizes['time']` looks smaller than expected right after an append | Stale consolidated-metadata cache | `xr.open_zarr(path, consolidated=False)` or `zarr.consolidate_metadata(path)` |
| Zarr sort fails with a chunk-encoding conflict | Rechunking without clearing inherited chunk encoding | See `sort_zarr_by_time.py`'s encoding-clearing step |
| Zarr store "not found" or breaks when downloaded by someone else via Box/cloud storage | GUI zip/upload tools sometimes silently drop hidden files (`.zattrs`, `.zarray`, `.zgroup`) | Use command-line `zip -r` or `tar` (not a GUI "compress" tool) to package the store |

---

## Sharing output data

A Zarr store is a directory, not a single file -- most file-sharing services
(Box, etc.) handle directories poorly or drop hidden files during
upload/download. Package it into a single archive first:

```bash
# tar (recommended -- preserves dotfiles reliably, no GUI tool risk)
tar -cvf s2s_reforecast_sorted.tar s2s_reforecast_sorted.zarr

# or zip, if a .zip is specifically required
zip -r s2s_reforecast_sorted.zip s2s_reforecast_sorted.zarr
```

A `.tar` (or `.zip`) is an ordinary single file -- share it directly, no need
to extract and re-package on either end. The recipient just runs:

```bash
tar -xvf s2s_reforecast_sorted.tar
```

**For large transfers over an unstable connection**, prefer `rsync` over
`scp` -- `rsync -avP` supports resuming an interrupted transfer from where it
left off, while `scp` always restarts from scratch:

```bash
rsync -avP user@host:/path/to/s2s_reforecast_sorted.tar .
```

Check your destination service's per-file size limit before uploading --
this pipeline's full-year output can be several GB, which may exceed
free-tier limits on some services.