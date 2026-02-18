#!/bin/bash
#SBATCH --job-name=ecostress_dl
#SBATCH --output=/home/jcoldiron/iowa-corn-project/logs/ecostress_download_%j.log
#SBATCH --error=/home/jcoldiron/iowa-corn-project/logs/ecostress_download_%j.err
#SBATCH --time=24:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=4
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=jcoldiron@ucsb.edu

mkdir -p /home/jcoldiron/iowa-corn-project/logs

cd /home/jcoldiron/iowa-corn-project/code/SIF-Analysis/src/scripts

export PYTHONPATH="$HOME/.local/lib/python3.12/site-packages:${PYTHONPATH:-}"

/usr/bin/python3 ecostress_monthly.py
