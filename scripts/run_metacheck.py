import sys
import shutil
import argparse
import logging
from datetime import datetime
import pandas as pd
from pathlib import Path
from check_files import process_study
from download_data import clone_data


# argument parse
parser = argparse.ArgumentParser(description="Meta Data run for OpenNeuro Dataset")
parser.add_argument("--openneuro_id", type=str, required=True, help="Dataset ID")
parser.add_argument("--dir_path", type=str, required=True, help="Directory with sub-directory of OpenNeuro folders")
parser.add_argument("--out_folder", type=str, required=True, help="Folder to save files, details")
 
# Set up simple logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S',
    stream=sys.stdout  # log to .out instead of error
)
def save_results_to_files(results, dataset_id, out_folder):
    """Save results to final output files"""
    # Save basics
    if results.get("basics") is not None:
        results["basics"].to_csv(out_folder / f"{dataset_id}_basics_summary.csv", index=False)

    # Save compilesumm
    if results.get("compilesumm") is not None:
        results["compilesumm"].to_csv(out_folder / f"{dataset_id}_counts_summary.csv", index=False)

    # Save descriptors
    if results.get("descriptor") is not None:
        results["descriptor"].to_csv(out_folder / f"{dataset_id}_descriptors.csv", index=False)

    # Save participants
    if results.get("participant") is not None:
        results["participant"].to_csv(out_folder / f"{dataset_id}_participants.csv", index=False)

    # Save events as a single combined DataFrame
    if results.get("events") is not None:
        all_events = pd.concat(results['events'], ignore_index=True) if results['events'] else pd.DataFrame()
        all_events.to_csv(out_folder / f"{dataset_id}_events.csv", index=False)



if __name__ == "__main__":
    parsed_args = parser.parse_args()
    dataset_dir = Path(parsed_args.dir_path)
    out_folder = Path(parsed_args.out_folder) / "dataset_output"
    openneuro_id = parsed_args.openneuro_id
    
    # Create output folder if it doesn't exist
    out_folder.mkdir(parents=True, exist_ok=True)
    
    logging.info(f"Starting processing {openneuro_id} dataset")
    logging.info(f"Dataset directory: {dataset_dir}")
    run_start_time = datetime.now()
    
    target_dir = dataset_dir / openneuro_id

    if target_dir.exists():
        try:
            shutil.rmtree(target_dir)
            print(f"{target_dir} has been removed.")
        except FileNotFoundError:
            print(f"{target_dir} does not exist (or was already removed).")
        except PermissionError:
            print(f"No permission to remove {target_dir}.")
    else:
        print(f"{target_dir} does not exist.")

    try:
        print("Cloning:", openneuro_id)
        clone_data(openneuro_study=openneuro_id, output_dir=target_dir)

    except Exception as e:
        logging.error(f"Error cloning {openneuro_id}: {e}")
        sys.exit(1)  

    try:
        result = process_study(open_neuro_id=openneuro_id, datadir=dataset_dir)
        run_elapsed = datetime.now() - run_start_time
        logging.info(f"Run time: {run_elapsed}")

        if result:
            save_results_to_files(result, openneuro_id, out_folder)
            logging.info(f"Successfully processed {openneuro_id}")
            sys.exit(0)  
        else:
            logging.warning(f"No results returned for {openneuro_id}")
            sys.exit(1)  

    except Exception as e:
        logging.error(f"Error processing {openneuro_id}: {e}")
        sys.exit(1)  