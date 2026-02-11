#!/bin/bash
#SBATCH --job-name=ecostress_dl
#SBATCH --output=/home/jcoldiron/iowa-corn-project/logs/ecostress_%A_%a.out
#SBATCH --error=/home/jcoldiron/iowa-corn-project/logs/ecostress_%A_%a.err
#SBATCH --time=4:00:00
#SBATCH --mem=4G
#SBATCH --cpus-per-task=1
#SBATCH --array=1-60

# ============================================================================
# ECOSTRESS Monthly Download - Iowa 2019-2023
# Submits 60 parallel SLURM jobs (12 months x 5 years)
#
# Array task ID mapping:
#   1-12  -> 2019 Jan-Dec
#   13-24 -> 2020 Jan-Dec
#   25-36 -> 2021 Jan-Dec
#   37-48 -> 2022 Jan-Dec
#   49-60 -> 2023 Jan-Dec
# ============================================================================

# Load Python module
module load python

# Install requests if needed
pip install --user requests

# Set NASA Earthdata credentials
export EARTHDATA_USER="jcoldiron"
export EARTHDATA_PASS="yvHaNJUCV!qt_9"

# Project paths
PROJ_DIR="/home/jcoldiron/iowa-corn-project"
CODE_DIR="$PROJ_DIR/code/SIF-Analysis"

# Compute year and month from array task ID (1-60)
TASK_ID=$SLURM_ARRAY_TASK_ID
YEAR=$((2019 + (TASK_ID - 1) / 12))
MONTH=$(((TASK_ID - 1) % 12 + 1))

echo "Starting download for ${YEAR}-$(printf '%02d' $MONTH) (task $TASK_ID of 60)"

# Run the download for this month
python "$CODE_DIR/src/scripts/ecostress_monthly.py" \
    --year $YEAR \
    --month $MONTH \
    --aoi "$PROJ_DIR/data/aoi/iowa.geojson" \
    --outdir "$PROJ_DIR/data/raw/ECOSTRESS"

echo "Completed ${YEAR}-$(printf '%02d' $MONTH)"
