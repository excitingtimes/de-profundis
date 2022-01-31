#!/bin/bash
# #SBATCH --array=0-1 # 11
# #SBATCH --output=/home/mila/s/sonnery.hugo/scratch/logs/de-profundis/slurm-%A-%a-%j-output.out # Write the output log on scratch
# #SBATCH --error=/home/mila/s/sonnery.hugo/scratch/logs/de-profundis/slurm-%A-%a-%j-error.out # Write the error log on scratch
#SBATCH --output=/home/mila/s/sonnery.hugo/scratch/logs/de-profundis/slurm-%j-output.out # Write the output log on scratch
#SBATCH --error=/home/mila/s/sonnery.hugo/scratch/logs/de-profundis/slurm-%j-error.out # Write the error log on scratch
#SBATCH --job-name=de-profundis
#SBATCH --mail-user=<sonnery.hugo@mila.quebec>
#SBATCH --mail-type=NONE,BEGIN,END,FAIL
#SBATCH --signal=SIGUSR1@90 # Enable auto-resubmitting of the job

#SBATCH --partition=cpus-only
#SBATCH --time=4-00:00:00

#SBATCH --mem=30G
#SBATCH --cpus-per-node=4
#SBATCH --nodes=4

# ======================================================================================
# Generic code
# ======================================================================================

# Pretty printing
ansi()          { echo -e "\e[${1}m${*:2}\e[0m"; }
bold()          { ansi 1 "$@"; }
italic()        { ansi 3 "$@"; }
underline()     { ansi 4 "$@"; }
strikethrough() { ansi 9 "$@"; }
red()           { ansi 31 "$@"; }

export INIT_FIRST_TIME="false"

# Loading the Anaconda / Python environment
module purge
module avail

module load anaconda/3

# conda clean --all # Cleans the Conda cache if you usually install / uninstall a lot of packages
conda activate de-profundis

conda info --envs

cd $HOME/de-profundis

# Print basic information about the process
hostname
nvidia-smi
sleep 1
pwd
hostname
date
python --version
sprio
sshare
df

# Debugging flags for PyTorch Lightning (optional)
export NCCL_DEBUG=INFO
export PYTHONFAULTHANDLER=1

wandb login

python main.py