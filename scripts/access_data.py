import os
import sys
import fnmatch
import access_functions 
from concurrent.futures import ThreadPoolExecutor, as_completed

# Note, Chris recommends to access/query openneuro using: datalad install -r ///openneuro and find {...linux... functions...}
def process_dataset(data_id, output_fold):
    """
    Process a single dataset: download files and save them locally.

    Args:
        data_id (str): The ID of the dataset to process.
        output_fold (str): The directory where dataset files will be saved.
    """
    print("Downloading File data for dataset: {}".format(data_id))
    dataset_id = data_id
    dataset_out = os.path.join(output_fold, dataset_id)

    snapshots = access_functions.get_snapshots(dataset_id)
    if not snapshots:
        print(f"No snapshots found for dataset {dataset_id}.")
        return

    latest_snapshot = snapshots[-1].split(":")[1]
    print(latest_snapshot)

    response = access_functions.get_participants_tsv(dataset_id, tag=latest_snapshot)
    if not os.path.exists(dataset_out):
        os.mkdir(dataset_out)

    if response:
        files_urls = access_functions.extract_filenames_and_urls(response)
        for info in files_urls:
            if info["filename"] in ["participants.json", "participants.tsv", "dataset_description.json"] or fnmatch.fnmatch(info["filename"], '*_events.json'):
                for url in info["urls"]:
                    access_functions.download_file(url, f"{dataset_out}/{info['filename']}")
    else:
        print(f"Failed to retrieve participant TSV for dataset {dataset_id}.")

if __name__ == "__main__":
    access_token_path = os.path.join(os.path.dirname(__file__),'..','api_key.txt')
    output_fold = os.path.join(os.path.dirname(__file__),'..','..','outdata')

    try:
        with open("api_key.txt") as f:
            access_functions.headers["accessToken"] = f.readline().strip()
    except FileNotFoundError:
        print(
            "You must first generate an API key at OpenNeuro.org and store it in api_key.txt"
        )
        sys.exit()

    dataset_list = access_functions.get_dataset_ids()

    with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(process_dataset, data_id, output_fold) for data_id in dataset_list]
            for future in as_completed(futures):
                try:
                    future.result()  
                except Exception as e:
                    print(f"An error occurred: {e}")