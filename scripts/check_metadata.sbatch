#!/bin/bash
#SBATCH --job-name=metadata_check
#SBATCH --array=1-750%24 # 751-1445
#SBATCH --signal=B:USR1@180 # failed 180s / 3min before 2hr time-out for logging
#SBATCH --time=02:00:00
#SBATCH --cpus-per-task=6
#SBATCH --mem=24GB
#SBATCH -p russpold,normal,owners
# Outputs ----------------------------------
#SBATCH --output=./logs/metadata.%A_%a.out
#SBATCH --error=./logs/metadata.%A_%a.err
#SBATCH --mail-user=demidenm@stanford.edu
#SBATCH --mail-type=ALL
# ------------------------------------------

# update based on wall-time
timeout_time=6900s # 115minutes * 60 second python timeout time

# Timeout handler function
timeout_handler() {
    echo "JOB TIMEOUT: Dataset ${dataset_id} (run_id: ${run_id}) timed out"
    echo -e "${dataset_id}\t${run_id}\t$(date '+%Y-%m-%d %H:%M:%S')\tTIMEOUT" >> "${failed_list}"
    exit 124  # Standard timeout exit code
}

# SLURM's pre-timeout signal
trap timeout_handler SIGUSR1

# Get current array task ID
run_id=${SLURM_ARRAY_TASK_ID}
date=$(date '+%Y-%m-%d %H:%M:%S')

# Config paths
script_dir=$(pwd)
dataset_list="${script_dir}/rerun_details/datasets_torun.tsv"
completed_list="${script_dir}/rerun_details/completed_datasets.tsv"
failed_list="${script_dir}/rerun_details/failed_datasets.tsv" 
dataset_id=$(sed -n "${run_id}p" "${dataset_list}")
inputdir="/oak/stanford/groups/russpold/data/openneuro_metadata/openneuro"
outputdir="${script_dir}/../output"

# Create necessary directories
mkdir -p "${outputdir}"
mkdir -p "$(dirname "${completed_list}")"
mkdir -p "$(dirname "${failed_list}")"
mkdir -p "./logs"

# Validate inputs
if [[ -z "${dataset_id}" ]]; then
    echo "ERROR: Could not extract dataset_id for run_id ${run_id}"
    exit 1
fi

if [[ ! -d "${inputdir}" ]]; then
    echo "ERROR: Input directory does not exist: ${inputdir}"
    exit 1
fi

# Display configuration
echo "=== Job Configuration ==="
echo "Run ID: ${run_id}"
echo "Dataset ID: ${dataset_id}"
echo "Input Directory: ${inputdir}"
echo "Output Directory: ${outputdir}"
echo "Script Directory: ${script_dir}"
echo "Date: ${date}"
echo "=========================="
echo

# Set up Python environment
echo "Setting up Python environment with uv..."
if [[ ! -f "${script_dir}/../.venv/bin/activate" ]]; then
    echo "ERROR: Virtual environment not found at ${script_dir}/../.venv/bin/activate"
    exit 1
fi

source "${script_dir}/../.venv/bin/activate"

# Run the Python script with timeout awareness
echo "Running metadata processing for dataset: ${dataset_id}"
timeout ${timeout_time} uv run python run_metacheck.py \
    --dir_path "${inputdir}" \
    --out_folder "${outputdir}" \
    --openneuro_id "${dataset_id}"

# Check exit status and handle accordingly
exit_code=$?
if [[ ${exit_code} -eq 124 ]]; then
    echo "TIMEOUT: Metadata processing timed out for dataset: ${dataset_id} (run_id: ${run_id})"
    echo -e "${dataset_id}\t${run_id}\t$(date '+%Y-%m-%d %H:%M:%S')\tTIMEOUT" >> "${failed_list}"
    exit 124
elif [[ ${exit_code} -ne 0 ]]; then
    echo "FAILED: Metadata processing FAILED for dataset: ${dataset_id} (run_id: ${run_id})"
    echo -e "${dataset_id}\t${run_id}\t$(date '+%Y-%m-%d %H:%M:%S')\t${exit_code}" >> "${failed_list}"
    exit 1
else
    echo "SUCCESS: Metadata processing completed for dataset: ${dataset_id}"
    
    # Remove completed dataset from the list
    if [[ -f "${dataset_list}" ]]; then
        sed -i "/^${dataset_id}$/d" "${dataset_list}"
    fi
    
    # Log successful completion
    echo -e "${dataset_id}\t${run_id}\t$(date '+%Y-%m-%d %H:%M:%S')\tCOMPLETED" >> "${completed_list}"
    
    echo "Check output in: ${outputdir}"
fi