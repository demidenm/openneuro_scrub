import concurrent.futures
import sys
import argparse
import logging
from datetime import datetime
import pandas as pd
from pathlib import Path
from check_files import process_study

# Set up simple logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S',
    stream=sys.stdout # log to .out instead of error
)

# parse arguments
parser = argparse.ArgumentParser(description="Complete Meta Data run for OpenNeuro")
parser.add_argument("--dir_path", type=str, required=True, help="Directory with sub-directory of OpenNeuro folders")
parser.add_argument("--out_folder", type=str, required=True, help="Folder to save files, details")
parser.add_argument("--n_cpus", type=str, required=True, help="Number of CPUs to parallelize over")

if __name__ == "__main__":
    parsed_args = parser.parse_args()
    dataset_dir = Path(parsed_args.dir_path)
    out_folder = Path(parsed_args.out_folder)
    ncpus = int(parsed_args.n_cpus) - 1  # to avoid issues
    
    # Create output folder if it doesn't exist
    out_folder.mkdir(parents=True, exist_ok=True)
    
    logging.info(f"Starting processing with {ncpus} CPUs")
    logging.info(f"Dataset directory: {dataset_dir}")

    # Initialize result containers
    all_basics = []
    all_compilesumm = []
    all_descriptors = []
    all_participants = []
    all_events = []

    study_list = sorted(
        [d.name for d in dataset_dir.iterdir() if d.name.startswith("ds") and d.is_dir()]
    )
    
    logging.info(f"Found {len(study_list)} datasets to process")

    # Process datasets
    start_time = datetime.now()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=ncpus) as executor:
        # Submit all tasks with dataset_dir parameter
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
            
            # Log progress every 25 datasets
            if completed % 25 == 0:
                logging.info(f"Completed {completed}/{len(study_list)} datasets")
    
    logging.info(f"Processing completed in {datetime.now() - start_time}")

    # Combine results (filter out None values)
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

    # Save files
    all_basics_df.to_csv(out_folder / "final_basics_summary.csv", index=False)
    all_compilesumm_df.to_csv(out_folder / "final_counts_summary.csv", index=False)
    all_descriptors_df.to_csv(out_folder / "final_descriptors.csv", index=False)
    all_participants_df.to_csv(out_folder / "final_participants.csv", index=False)
    all_events_df.to_csv(out_folder / "final_events.csv", index=False)
    
    logging.info(f"Complete! Total time: {datetime.now() - start_time}")