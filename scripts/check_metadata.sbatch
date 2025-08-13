#!/bin/bash
#
#SBATCH --job-name=metadata_check
#SBATCH --array=1 # 
#SBATCH --time=12:00:00 # for the initial run, ~1300 datasets, requires a lot more time. 
#SBATCH --cpus-per-task=10
#SBATCH --mem-per-cpu=10GB
#SBATCH -p russpold,normal,owners
# Outputs ----------------------------------
#SBATCH --output=./logs/metadata.%A_%a.out
#SBATCH --error=./logs/metadata.%A_%a.err
#SBATCH --mail-user=demidenm@stanford.edu
#SBATCH --mail-type=ALL
# ------------------------------------------

num_cpus=10

# Check if full or minimal run
if [ -z "$1" ]; then
  echo "Error: Required whether full or partial metadata check."
  echo "Usage: sbatch check_metadata.sh full"
  exit 1
fi

check_type=${1}

# config paths
script_dir=`pwd`
inputdir="/oak/stanford/groups/russpold/data/openneuro_metadata/openneuro"
outputdir="${script_dir}/../output"
mkdir -p "${outputdir}"

# set paths
echo "Input Directory: ${inputdir}"
echo "Output Directory: ${outputdir}"
echo "Script Directory: ${script_dir}"
echo "Check Type: ${check_type}"


echo

# set up `uv` environment
echo "Setting up Python environment with uv..."
source "${script_dir}/../.venv/bin/activate"

if [ "$check_type" = "full" ]; then
    echo "Running: full check"

    uv run python first_complete_run.py \
        --dir_path ${inputdir} \
        --out_folder ${outputdir} \
        --n_cpus ${num_cpus}
        
elif [ "$check_type" = "partial" ]; then
    echo "Running: partial check"
    uv run python partial_run.py \
        --dir_path ${inputdir} \
        --out_folder ${outputdir} \
        --n_cpus ${num_cpus}
else
    echo "type should be of 'partial' or 'full', provided ${check_type}"
    exit 1
fi


if [ $? -ne 0 ]; then
    echo "Meta data ${check_type} failed"
    exit 1
else
    echo "Meta data ${check_type} completed. Check ${outputdir}"
fi

 
