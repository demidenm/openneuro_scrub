import concurrent.futures
import sys
import argparse
import logging
from datetime import datetime
import pandas as pd
from pathlib import Path
from check_files import process_study

# argument parse
parser = argparse.ArgumentParser(description="Complete Meta Data run for OpenNeuro")
parser.add_argument("--dir_path", type=str, required=True, help="Directory with sub-directory of OpenNeuro folders")
parser.add_argument("--out_folder", type=str, required=True, help="Folder to save files, details")
parser.add_argument("--n_cpus", type=str, required=True, help="Number of CPUs to parallelize over")


# Set up simple logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S',
    stream=sys.stdout # log to .out instead of error
)

def append_results_to_files(results, out_folder):
    """Append results to the final output files"""
    # Filter out None values
    results = [r for r in results if r is not None and isinstance(r, dict)]
    
    if not results:
        logging.warning("No valid results to append")
        return
    
    # Extract data from results
    basics = [result["basics"] for result in results if "basics" in result and result["basics"] is not None]
    compilesumm = [result["compilesumm"] for result in results if "compilesumm" in result and result["compilesumm"] is not None]
    descriptors = [result["descriptor"] for result in results if "descriptor" in result and result["descriptor"] is not None]
    participants = [result["participant"] for result in results if "participant" in result and result["participant"] is not None]
    events = [event for result in results if "events" in result and isinstance(result["events"], list) for event in result["events"]]
    
    # Append to files (create if they don't exist)
    if basics:
        basics_df = pd.concat(basics, ignore_index=True)
        file_path = out_folder / "final_basics_summary.csv"
        if file_path.exists():
            basics_df.to_csv(file_path, mode='a', header=False, index=False)
        else:
            basics_df.to_csv(file_path, index=False)
    
    if compilesumm:
        compilesumm_df = pd.concat(compilesumm, ignore_index=True)
        file_path = out_folder / "final_counts_summary.csv"
        if file_path.exists():
            compilesumm_df.to_csv(file_path, mode='a', header=False, index=False)
        else:
            compilesumm_df.to_csv(file_path, index=False)
    
    if descriptors:
        descriptors_df = pd.concat(descriptors, ignore_index=True)
        file_path = out_folder / "final_descriptors.csv"
        if file_path.exists():
            descriptors_df.to_csv(file_path, mode='a', header=False, index=False)
        else:
            descriptors_df.to_csv(file_path, index=False)
    
    if participants:
        participants_df = pd.concat(participants, ignore_index=True)
        file_path = out_folder / "final_participants.csv"
        if file_path.exists():
            participants_df.to_csv(file_path, mode='a', header=False, index=False)
        else:
            participants_df.to_csv(file_path, index=False)
    
    if events:
        events_df = pd.concat(events, ignore_index=True)
        file_path = out_folder / "final_events.csv"
        if file_path.exists():
            events_df.to_csv(file_path, mode='a', header=False, index=False)
        else:
            events_df.to_csv(file_path, index=False)

if __name__ == "__main__":
    parsed_args = parser.parse_args()
    dataset_dir = Path(parsed_args.dir_path)
    out_folder = Path(parsed_args.out_folder)
    ncpus = int(parsed_args.n_cpus) - 1  # to avoid issues
    
    # Create output folder if it doesn't exist
    out_folder.mkdir(parents=True, exist_ok=True)
    
    logging.info(f"Starting processing with {ncpus} CPUs")
    logging.info(f"Dataset directory: {dataset_dir}")

    study_list = sorted(
        [d.name for d in dataset_dir.iterdir() if d.name.startswith("ds") and d.is_dir()]
    )
    
    # Skip datasets with too many files and track them
    ids_skipped = []
    filtered_study_list = []
    for study_id in study_list:
        try:
            study_path = dataset_dir / study_id
            file_count = len(list(study_path.iterdir()))
            if file_count > 900:
                ids_skipped.append(study_id)
            else:
                filtered_study_list.append(study_id)
        except (OSError, FileNotFoundError):
            # If we can't count files, skip it
            ids_skipped.append(study_id)
    
    study_list = filtered_study_list
    
    #  skipped datasets in red
    if ids_skipped:
        print("\033[91m" + "\nSKIPPED DATASETS:")
        for skipped_id in ids_skipped:
            print(f"  {skipped_id}")
        print("\033[0m") 

    # ****** if failed and want to start after last finished index/ID *********
    if "ds004146" in study_list:
        start_idx = study_list.index("ds004146") + 1
        study_list = study_list[start_idx:]
    #                *************************
    
    logging.info(f"Found {len(study_list)} datasets to process ({len(ids_skipped)} skipped)")

    overall_start_time = datetime.now()
    batch_size = 50

    for i in range(0, len(study_list), batch_size):
        batch = study_list[i:i + batch_size]
        batch_start_time = datetime.now()
        
        logging.info(f"Processing batch: datasets {i+1}-{min(i+batch_size, len(study_list))} of {len(study_list)}")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=ncpus) as executor:
            future_to_study = {executor.submit(process_study, study, dataset_dir): study for study in batch}
            
            completed = 0
            batch_results = []
            for future in concurrent.futures.as_completed(future_to_study, timeout=3600):
                study = future_to_study[future]
                completed += 1
                
                try:
                    result = future.result(timeout=500)
                    if result is not None:
                        batch_results.append(result)
                    logging.info(f"✓ {study} completed ({completed}/{len(batch)})")
                except concurrent.futures.TimeoutError:
                    logging.error(f"✗ {study} timed out ({completed}/{len(batch)})")
                except Exception as exc:
                    logging.error(f"✗ {study} failed: {exc} ({completed}/{len(batch)})")
                
                # Show batch progress every 5 completions
                if completed % 5 == 0:
                    batch_elapsed = datetime.now() - batch_start_time
                    logging.info(f"Batch progress: {completed}/{len(batch)} - elapsed: {batch_elapsed}")
        
        # Batch complete - write results to files
        batch_time = datetime.now() - batch_start_time
        logging.info(f"Batch complete: {len(batch)} studies in {batch_time}")
        
        # Append batch results to output files
        if batch_results:
            append_results_to_files(batch_results, out_folder)  # only save the current batch
            logging.info(f"Batch added {len(batch_results)} results. Total accumulated: {len(all_results)} results written to files")

    total_time = datetime.now() - overall_start_time 
    logging.info(f"Complete! Total time: {total_time}")
    
    # Summary of skipped datasets
    if ids_skipped:
        print("\033[91m" + "\nSKIPPED DATASETS - Rerun these individually:")
        for skipped_id in ids_skipped:
            print(f"  {skipped_id}")
        print("\033[0m")
