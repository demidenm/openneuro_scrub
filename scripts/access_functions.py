import requests
import json
import sys


headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Connection": "keep-alive",
    "DNT": "1",
    "Origin": "https://openneuro.org",
    "accessToken": None,
}


def get_dataset_ids():
    """
    Get all dataset IDs from OpenNeuro.

    Returns:
        list: A list of dataset IDs.
    """
    query = '{"query":"query { datasets { edges { cursor, node { id } } } }"}'
    url = "https://openneuro.org/crn/graphql"

    dataset_ids = []
    while True:
        response = requests.post(url, headers=headers, data=query).json()
        edges = response.get("data", {}).get("datasets", {}).get("edges", [])

        for edge in edges:
            dataset_id = edge["node"]["id"]
            dataset_ids.append(dataset_id)

        if len(edges) < 25:
            break

        next_cursor = edges[-1]["cursor"]
        query = (
            f'{{"query": "query {{ datasets(after: \\"{next_cursor}\\") {{ edges {{ cursor, node {{ id }} }} }} }}"}}'
        )

    return dataset_ids


def get_snapshots(dataset_id):
    """
    Get snapshots for a specific dataset.

    Args:
        dataset_id (str): The dataset ID.

    Returns:
        list: A list of snapshot IDs.
    """
    query = f'{{"query": "query {{ dataset(id: \\"{dataset_id}\\") {{ snapshots {{ id }} }} }}"}}'
    url = "https://openneuro.org/crn/graphql"

    response = requests.post(url, headers=headers, data=query).json()
    snapshots = response.get("data", {}).get("dataset", {}).get("snapshots", [])

    return [snapshot["id"] for snapshot in snapshots] if snapshots else []


def get_metadata(dataset_id, verbose=False):
    """
    Get metadata for a specific dataset or snapshot.

    Args:
        dataset_id (str): The dataset or snapshot ID.
        verbose (bool): Whether to print the GraphQL query.

    Returns:
        dict: The metadata response.
    """
    if ":" in dataset_id:
        dataset_id, tag = dataset_id.split(":")
        query = f'{{"query": "query {{ snapshot(datasetId: \\"{dataset_id}\\", tag: \\"{tag}\\") {{ id description {{ Name Funding Acknowledgements }} }}"}}'
    else:
        query = f'{{"query": "query {{ dataset(id: \\"{dataset_id}\\") {{ latestSnapshot {{ id readme description {{ Name Funding Acknowledgements }} }} }}"}}'

    if verbose:
        print(query)

    url = "https://openneuro.org/crn/graphql"
    response = requests.post(url, headers=headers, data=query)

    return response.json()

def get_headers():
    """
    Load headers with API key from a file.

    Returns:
        dict: Headers with the API key.
    """
    try:
        with open("api_key.txt") as f:
            headers["accessToken"] = f.readline().strip()
    except FileNotFoundError:
        print(
            "You must first generate an API key at OpenNeuro.org and store it in api_key.txt"
        )
        sys.exit()

    return headers


def get_participants_tsv(dataset_id, tag=None):
    """
    Get participant TSV file for a dataset.

    Args:
        dataset_id (str): The dataset ID.
        tag (str, optional): The snapshot tag.

    Returns:
        dict or None: The response JSON or None if the tag is not provided.
    """
    headers = get_headers()

    if tag:
        query = f"""
        query snapshotFiles {{
            snapshot(datasetId: "{dataset_id}", tag: "{tag}") {{
                files {{
                    id
                    key
                    filename
                    size
                    directory
                    annexed
                    urls
                }}
            }}
        }}"""
    else:
        print("Provide latest snapshot tag for dataset")
        return None

    url = "https://openneuro.org/crn/graphql"
    response = requests.post(url, json={"query": query}, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Request failed with status code {response.status_code}")
        return None
    

def extract_filenames_and_urls(response):
    """
    Extract filenames and URLs from the OpenNeuro response.

    Args:
        response (dict): The response JSON from the API. This should contain
                         the 'data' key with a nested structure leading to
                         'snapshot' and 'files'.

    Returns:
        list: A list of dictionaries containing filenames and URLs.
              Each dictionary has 'filename' and 'urls' keys.
              
    Raises:
        ValueError: If the response is not a dictionary or is missing expected keys.
    """
    if not isinstance(response, dict):
        raise ValueError("Response must be a dictionary.")
    
    snapshot = response.get("data", {}).get("snapshot", {})
    if not isinstance(snapshot, dict):
        raise ValueError("'snapshot' key is missing or not a dictionary.")
    
    files = snapshot.get("files", [])
    if not isinstance(files, list):
        raise ValueError("'files' key is missing or not a list.")
    
    return [
        {"filename": file.get("filename"), "urls": file.get("urls", [])}
        for file in files
        if isinstance(file, dict)  # Ensure each file is a dictionary
    ]

def download_file(url, local_filename):
    """
    Download a file from a URL to a local filename.

    Args:
        url (str): The URL of the file.
        local_filename (str): The local path where the file will be saved.
    """
    response = requests.get(url, stream=True)

    if response.status_code == 200:
        with open(local_filename, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f"Downloaded {local_filename} successfully.")
    else:
        print(f"Failed to download file. Status code: {response.status_code}")