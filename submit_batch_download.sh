#!/bin/bash
#SBATCH --job-name=s2s_batch_download
#SBATCH --output=slurm_batch_download_%j.log
#SBATCH --error=slurm_batch_download_%j.err
#SBATCH --time=12:00:00
#SBATCH --mem=32G
#SBATCH --cpus-per-task=8
#SBATCH --partition=general
##SBATCH --account=CHANGE_ME_IF_REQUIRED

# ---------------------------------------------------------------------------
# submit_batch_download.sh
#
# Slurm wrapper for batch_download.py -- downloads and converts all
# pending model version dates (per config_model_version_dates.json)
# into the shared zarr store, using n_workers parallel date-workers.
#
# All actual progress/errors go to the single shared pipeline log file
# (config_paths.json -> log_file, e.g. logs/pipeline.log) -- the
# slurm_batch_download_<jobid>.log/.err files here only capture Slurm's
# own job-level stdout/stderr (should be nearly empty in normal runs,
# since the Python scripts print nothing to the terminal).
#
# Resume-safe: if this job is killed by the walltime limit or fails
# partway through, just resubmit -- batch_download.py's resume-skip
# logic picks up exactly where it left off.
#
# TO RUN A DIFFERENT SCENARIO (e.g. rainfall-only at 0.25 degree
# instead of the full variable set at 1 degree), edit the python
# command below:
#   --variable-set  : which named group of variables to download, from
#                      config_variables.json (e.g. "combination_1" for
#                      everything, "rainfall_only" for just precip)
#   --grid-key      : which named resolution, from config_grid.json
#                      (e.g. "grids_1deg", "grids_0p25deg")
#   --paths-key     : which named output location, from config_paths.json
#                      -- IMPORTANT: use a DIFFERENT paths-key for a
#                      different variable_set/grid/area combination, since
#                      mixing incompatible dimensions into the same zarr
#                      store will corrupt it. Add a new named entry to
#                      config_paths.json first if one doesn't already
#                      exist for this scenario.
#
# BEFORE SUBMITTING, edit:
#   --partition   : set to a valid partition on your cluster
#                    (check with `sinfo`)
#   --account     : uncomment and set if your cluster requires a
#                    billing/allocation account
#                    (check with `sacctmgr show associations user=$USER`)
#   --time        : 12:00:00 assumes the 'general' partition's cap;
#                    with n_workers=4 and ~186 dates at ~5 min/date
#                    sequential, expect roughly ~4 hours for a full
#                    year at the full variable set -- a smaller
#                    variable_set (like rainfall_only) or fewer dates
#                    will finish faster; adjust down if you're confident,
#                    or just leave the generous default
#   --mem         : 32G is a generous safety margin; each worker only
#                    holds one date's data in memory at a time (a few
#                    hundred MB), so this has headroom
#   --cpus-per-task: should be >= n_workers below, plus a little
#                    overhead for the main process
#
# Submit with:
#   sbatch submit_batch_download.sh
#
# Monitor with:
#   squeue -u $USER
#   tail -f logs/pipeline.log      (the actual pipeline log)
#   tail -f slurm_batch_download_<jobid>.log   (Slurm's own job log)
# ---------------------------------------------------------------------------

set -eo pipefail  # deliberately no -u: conda's own activation hooks
                   # reference unset variables and break under nounset

echo "Job started on $(hostname) at $(date)"
echo "Job ID: $SLURM_JOB_ID"

source /opt/conda/etc/profile.d/conda.sh
conda activate climate

cd "$SLURM_SUBMIT_DIR"

python batch_download.py \
    --dates-config-key s2s_2025_monsoon \
    --area-key south_asia \
    --grid-key grids_1deg \
    --paths-key s2s_downscaling \
    --variable-set combination_2 \
    --n-workers 4

echo "Job finished at $(date)"