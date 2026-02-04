#!/bin/bash
#SBATCH --job-name=ecostress_2023
#SBATCH --output=/home/jcoldiron/iowa-corn-project/logs/ecostress_%A_%a.out
#SBATCH --error=/home/jcoldiron/iowa-corn-project/logs/ecostress_%A_%a.err
#SBATCH --time=4:00:00
#SBATCH --mem=4G
#SBATCH --cpus-per-task=1
#SBATCH --array=1-12

# Load Python module (this will work on the compute node)
module load python

# Install requests if needed (on compute node)
pip install --user requests

# Set NASA Earthdata credentials
export EARTHDATA_USER="jcoldiron"
export EARTHDATA_PASS="yvHaNJUCV!qt_9"

# Project paths
PROJ_DIR="/home/jcoldiron/iowa-corn-project"
CODE_DIR="$PROJ_DIR/code/SIF-Analysis"

cd $CODE_DIR

# Month number from array task ID (1-12)
MONTH=$SLURM_ARRAY_TASK_ID

echo "Starting download for month $MONTH of 2023"

# Run the download for this month
python ecostress_monthly.py \
    --year 2023 \
    --month $MONTH \
    --aoi $PROJ_DIR/data/aoi/iowa.geojson \
    --outdir $PROJ_DIR/data/raw/ECOSTRESS

echo "Completed month $MONTH"
