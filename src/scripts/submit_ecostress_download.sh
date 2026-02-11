#!/bin/bash
#SBATCH --job-name=ecostress_dl
#SBATCH --output=/work/jcoldiron/logs/ecostress_download_%j.log
#SBATCH --error=/work/jcoldiron/logs/ecostress_download_%j.err
#SBATCH --time=24:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=4
#SBATCH --partition=compute
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=jcoldiron@ucsb.edu

set -euo pipefail

# Create log directory if it doesn't exist
mkdir -p /work/jcoldiron/logs

echo "=================================================="
echo "ECOSTRESS Download Job"
echo "=================================================="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURMD_NODENAME"
echo "Start time: $(date)"
echo "Submit dir: ${SLURM_SUBMIT_DIR}"
echo "=================================================="
echo ""

# Always run from the directory you submitted from (important!)
cd "$SLURM_SUBMIT_DIR"

# Load Python module
module purge
module load python/3.12

echo "Python:"
which python3
python3 --version
echo ""

# Optional: show that your netrc exists + perms (do NOT print contents)
if [[ -f "$HOME/.netrc" ]]; then
  echo "~/.netrc exists"
  ls -l "$HOME/.netrc"
else
  echo "ERROR: ~/.netrc not found. earthaccess login will likely fail in batch mode."
  exit 1
fi

# Recommended: ensure pip installs (your --target dir) are found
# This matches your script's target_dir so imports work reliably:
export PYTHONPATH="$HOME/.local/lib/python3.12/site-packages:${PYTHONPATH:-}"

echo "PYTHONPATH:"
echo "$PYTHONPATH"
echo ""

echo "Starting download script..."
echo ""

# IMPORTANT: run the correct filename (change to ecostress_monthly.py if that's the real one)
python3 src/scripts/ecostress_monthly.py

echo ""
echo "=================================================="
echo "Job completed"
echo "End time: $(date)"
echo "=================================================="
