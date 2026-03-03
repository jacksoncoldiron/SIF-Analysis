#!/bin/bash
#SBATCH --job-name=openet_dl
#SBATCH --output=/home/jcoldiron/iowa-corn-project/logs/openet_download_%j.log
#SBATCH --error=/home/jcoldiron/iowa-corn-project/logs/openet_download_%j.err
#SBATCH --time=06:00:00
#SBATCH --mem=8G
#SBATCH --cpus-per-task=2
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=jcoldiron@ucsb.edu

mkdir -p /home/jcoldiron/iowa-corn-project/logs

cd /home/jcoldiron/iowa-corn-project/code/SIF-Analysis/src/scripts/download

export PYTHONPATH="$HOME/.local/lib/python3.12/site-packages:${PYTHONPATH:-}"

/usr/bin/python3 openet_download.py
