import concurrent.futures
import sys
import argparse
import logging
import ssl
import shutil
import urllib.request
import pandas as pd
from check_files import process_study
from download_data import clone_data
from datetime import datetime
from pathlib import Path

# Set up simple logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S',
    stream=sys.stdout  # log to .out instead of error
)

# parse arguments
parser = argparse.ArgumentParser(description="Complete Meta Data run for OpenNeuro")
parser.add_argument("--dir_path", type=str, required=True, help="Directory with sub-directory of OpenNeuro folders")
parser.add_argument("--out_folder", type=str, required=True, help="Folder to save files, details")
parser.add_argument("--n_cpus", type=str, required=True, help="Number of CPUs to parallelize over")

def append_csv(new_df, file_path, index=False, backup=True):
    """
    backup, then append data to existing CSV or raise ValueError if it doesn't exist     
    Args:
        new_df: DataFrame to append
        file_path: Path to CSV file
        index: Whether to include index in CSV
        backup: Whether to create backup before operation
    """
    file_path = Path(file_path)
    
    if new_df is None or new_df.empty:
        print(f"Warning: No data to append to {file_path.name}")
        return
    
    if not file_path.exists():
        raise ValueError(f"CSV file {file_path} does not exist. Cannot append data to non-existent file.")
    
    try:
        # If true, create backup
        backup_path = None
        if backup:
            timestamp = datetime.now().strftime('%Y%m%d')
            backup_path = file_path.with_stem(f'{file_path.stem}_backup-{timestamp}')
            shutil.copy2(file_path, backup_path)
            print(f"Backup at: {backup_path.name}. Remove after validation")
        
        # Load data
        existing_df = pd.read_csv(file_path)
        original_len = len(existing_df)
        
        # Combine data
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        
        combined_df.to_csv(file_path, index=index)
        
        print(f"Appended to {file_path.name}: {original_len} + {len(new_df)} = {len(combined_df)} rows")
        
            
    except Exception as e:
        # Restore from backup if something went wrong
        if backup and backup_path and backup_path.exists():
            shutil.copy2(backup_path, file_path)
            backup_path.unlink()
            print(f"Restored from backup due to error: {e}")
        raise ValueError(f"Issue creating {file_path}. Confirm that file exists.")

if __name__ == "__main__":
    parsed_args = parser.parse_args()
    dataset_dir = Path(parsed_args.dir_path)
    out_folder = Path(parsed_args.out_folder)
    ncpus = int(parsed_args.n_cpus) - 1  # to avoid issues
    
    # create output folder if it doesn't exist
    out_folder.mkdir(parents=True, exist_ok=True)
    
    logging.info(f"Starting processing with {ncpus} CPUs")
    logging.info(f"Dataset directory: {dataset_dir}")

    # get list of current open neuro data
    ssl_context = ssl._create_unverified_context()
    url = 'https://raw.githubusercontent.com/jbwexler/openneuro_metadata/main/metadata.csv'
    with urllib.request.urlopen(url, context=ssl_context) as response:
        df = pd.read_csv(response)

    # get completed cloned directories
    ds_names = [d.name for d in dataset_dir.iterdir() if d.name.startswith("ds") and d.is_dir()]
    missing = df[~df["accession_number"].isin(ds_names)]
    print(f"Missing {len(missing['accession_number'])} datasets in {dataset_dir}")
    print("Cloning each dataset using datalad and then checking metadata")

    study_list = missing['accession_number'][:2]
    
    # clone datasets
    for dataset in study_list:
        print("Cloning:", dataset)
        clone_target = dataset_dir / dataset
        clone_data(openneuro_study=dataset, output_dir=clone_target)

    # set empty result variables
    all_basics = []
    all_compilesumm = []
    all_descriptors = []
    all_participants = []
    all_events = []

    # process datasets
    start_time = datetime.now()

    with concurrent.futures.ThreadPoolExecutor(max_workers=ncpus) as executor:
        # concurrent tasks with process_study function with study and dataset_dir parameters
        future_to_study = {executor.submit(process_study, study, dataset_dir): study for study in study_list}
        
        results = []
        completed = 0
        
        for future in concurrent.futures.as_completed(future_to_study):
            study = future_to_study[future]
            completed += 1
            
            try:
                result = future.result()
                results.append(result)
            except Exception as exc:
                logging.error(f"Dataset {study} failed: {exc}")
                results.append({})  # Empty dict for failed processing
            
            # Log progress every 10 datasets
            if completed % 10 == 0:
                logging.info(f"Completed {completed}/{len(study_list)} datasets")

    logging.info(f"Processing completed in {datetime.now() - start_time}")

    # combine results (filter out None values)
    results = [r for r in results if r is not None and isinstance(r, dict)]

    all_basics = [result["basics"] for result in results if "basics" in result and result["basics"] is not None]
    all_compilesumm = [result["compilesumm"] for result in results if "compilesumm" in result and result["compilesumm"] is not None]
    all_descriptors = [result["descriptor"] for result in results if "descriptor" in result and result["descriptor"] is not None]
    all_participants = [result["participant"] for result in results if "participant" in result and result["participant"] is not None]
    all_events = [event for result in results if "events" in result and isinstance(result["events"], list) for event in result["events"]]

    # Create and save DataFrames
    logging.info("Saving results...")

    all_basics_df = pd.concat(all_basics, ignore_index=True) if all_basics else pd.DataFrame()
    all_compilesumm_df = pd.concat(all_compilesumm, ignore_index=True) if all_compilesumm else pd.DataFrame()
    all_descriptors_df = pd.concat(all_descriptors, ignore_index=True) if all_descriptors else pd.DataFrame()
    all_participants_df = pd.concat(all_participants, ignore_index=True) if all_participants else pd.DataFrame()
    all_events_df = pd.concat(all_events, ignore_index=True) if all_events else pd.DataFrame()

    # Save files using append_csv function
    append_csv(all_basics_df, out_folder / "final_basics_summary.csv", index=False)
    append_csv(all_compilesumm_df, out_folder / "final_counts_summary.csv", index=False)
    append_csv(all_descriptors_df, out_folder / "final_descriptors.csv", index=False)
    append_csv(all_participants_df, out_folder / "final_participants.csv", index=False)
    append_csv(all_events_df, out_folder / "final_events.csv", index=False)