import concurrent.futures
import sys
import os
import argparse
import logging
import ssl
import urllib.request
import pandas as pd
from check_files import process_study
from datetime import datetime
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S',
    stream=sys.stdout
)

# Parse arguments
parser = argparse.ArgumentParser(description="Complete Meta Data run for OpenNeuro")
parser.add_argument("--dir_path", type=str, required=True, help="Directory with sub-directory of OpenNeuro folders")
parser.add_argument("--repo_dir", type=str, required=True, help="Path to repository directory")


def fetch_openneuro_metadata():
    """Fetch OpenNeuro metadata from GitHub repository."""
    ssl_context = ssl._create_unverified_context()
    url = 'https://raw.githubusercontent.com/jbwexler/openneuro_metadata/main/metadata.csv'
    
    try:
        with urllib.request.urlopen(url, context=ssl_context) as response:
            return pd.read_csv(response)
    except Exception as e:
        logging.error(f"Failed to fetch OpenNeuro metadata: {e}")
        sys.exit(1)


def load_completed_datasets(completed_file):
    """Load list of completed datasets."""
    expected_columns = ["study_id", "run_id", "date_completed", "completion_status"]
    
    if completed_file.exists():
        try:
            df = pd.read_csv(completed_file, sep='\t')
            
            # set columns for df as no headeer
            logging.warning(f"Completed datasets has no columns. Expected: {expected_columns}, Found: {list(df.columns)}")
            return pd.DataFrame(columns=expected_columns)
                
        except Exception as e:
            logging.warning(f"Could not read completed datasets file: {e}")
            return pd.DataFrame(columns=expected_columns)
    else:
        return pd.DataFrame(columns=expected_columns)


if __name__ == "__main__":
    parsed_args = parser.parse_args()
    dataset_dir = Path(parsed_args.dir_path)
    out_folder = Path(parsed_args.repo_dir)
    check_out = out_folder / "scripts" / "rerun_details"
    ran_files = out_folder / "output" / "dataset_output"


    # Validate inputs
    if not dataset_dir.exists():
        logging.error(f"Dataset directory does not exist: {dataset_dir}")
        sys.exit(1)
    
    # Create output folder if it doesn't exist
    check_out.mkdir(parents=True, exist_ok=True)
    ran_files.mkdir(parents=True, exist_ok=True)
    
    # Get list of current OpenNeuro data
    logging.info("Fetching OpenNeuro metadata...")
    openneuro_list = fetch_openneuro_metadata()
    logging.info(f"Found {len(openneuro_list)} datasets in OpenNeuro metadata")

    # get file names Path(ran_files).rglob("*basics_summary.csv").sep("_")[0], study_id is first field
    # get YYYY-MM-DD file was create
    # create pd.DataFrame with unqiue study_id and date_created
    files = Path(ran_files).rglob("*basics_summary.csv")
    filelist_data = [(f.stem.split("_")[0], datetime.fromtimestamp(os.path.getctime(f)).strftime("%Y-%m-%d")) 
            for f in files]
    completed_df = pd.DataFrame(filelist_data, columns=['study_id', 'date_created']).drop_duplicates('study_id')

    #completed_file = out_folder / "completed_datasets.tsv"
    #completed_df = load_completed_datasets(completed_file)
    
    logging.info(f"Checking ran OpenNeuro datasets versus available OpenNeuro datasets")
    logging.info(f"Dataset directory: {dataset_dir}")
    logging.info(f"Output folder: {out_folder}")
    logging.info(f"Previously completed datasets: {len(completed_df)}")

    # identify missing datasets
    missing = openneuro_list[~openneuro_list["accession_number"].isin(completed_df["study_id"])]
    logging.info(f"Missing {len(missing)} datasets to process")

    # save list of datasets to run
    run_dataset_list = check_out / "datasets_torun.tsv"
    missing["accession_number"].to_csv(run_dataset_list, sep='\t', index=False, header=False)
    logging.info(f"Saved list of datasets to process: {run_dataset_list}")
    
    if len(missing) == 0:
        logging.info("All datasets have been processed!")
    else:
        logging.info(f"Ready to process {len(missing)} datasets")
        