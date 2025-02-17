import os
import json
from glob import glob
import numpy as np
import pandas as pd
from bids import BIDSLayout
from bids.exceptions import BIDSConflictingValuesError, BIDSValidationError

def basic_layout(root_dir: str, open_id: str) -> BIDSLayout:
    """
    Create a BIDSLayout object for a given study.

    Parameters:
    root_dir (str): The root directory where BIDS datasets are stored.
    study (str): The study ID to load.

    Returns:
    BIDSLayout: A BIDSLayout object for the specified study.
    """
    study_path = os.path.join(root_dir,open_id)
    
    try:
        bids_layout = BIDSLayout(study_path, is_derivative=False, reset_database=True)
        dir_type = "bids_input"

        if  not bids_layout.get_subjects():
            bids_layout = BIDSLayout(study_path, is_derivative=True)
            dir_type = "bids_derivative"

        tasks = bids_layout.get_tasks()
        non_rest = [task for task in tasks if not task.startswith("rest")]
        subs = bids_layout.get_subjects()
        
        runs = bids_layout.get_runs()
        if not runs:
            runs = [1]  # Default to a single run if none exist

        sessions = bids_layout.get_sessions()
        if not sessions:
            sessions = ["01"]  # Default to session '01' if none exist

    except Exception as e:
        print("Basic Layout error:", e)
        bids_layout = subs = tasks = non_rest = runs = sessions = dir_type = "bids_error"
    
    return bids_layout, subs, tasks, non_rest, runs, sessions, dir_type


def check_basics(root_dir: str, study: str, 
                files_to_check: list = ["CHANGES", "README", "participants.json", 
                                        "participants.tsv", "dataset_description.json"]) -> pd.DataFrame:
    """
    Check if top-level files exist in a BIDS dataset for a given list of studies.
    
    Parameters:
    root_dir (str): The root directory where BIDS datasets are stored.
    study (str): List of study IDs to check.
    files_to_check (list): List of files to check for in the BIDS dataset.

    Returns:
    pd.DataFrame: A DataFrame indicating 1) study ID, 2) file name, 3) location (top/subj lvl), 4) the presence (1) or absence (0) of files.
    """

    try:
        study_dir = os.path.join(root_dir, study)
        path = study_dir
        layout = BIDSLayout(path, reset_database=True)
        
        tasks = layout.get_tasks()
        non_rest = [task for task in tasks if not task.startswith("rest")]
        
        study_files_to_check = files_to_check.copy()
        if non_rest:
            for task in non_rest:
                study_files_to_check.extend([
                    f"task-{task}_events.json",
                    f"task-{task}_events.tsv",
                    f"task-{task}_bold.json",
                ])
        
        file_status = []
        file_n = []
        for file in study_files_to_check:
            n_files = []
            if layout.get_file(file):
                status = "top"
                n_files = 1
            elif (task_lab := file.split("_")[0].replace("task-", "")) in non_rest:
                file_exten = os.path.splitext(file)[1]
                task_files = layout.get(task=task_lab, suffix="events", extension=file_exten, return_type="file") or []
                status = "func" if task_files else "missing"
                n_files = len(task_files)
            else:
                status = "missing"
                n_files = 0
            file_status.append(status)
            file_n.append(n_files)
        
        filelist_loc = pd.DataFrame({"study_id": study, "file": study_files_to_check, "location": file_status, "n_files": file_n})
        filelist_loc["presence"] = np.where(filelist_loc["location"] == "missing", 0, 1)

    except BIDSConflictingValuesError as e:
        print(f"BIDS metadata conflict in study {study}: {e}")
        filelist_loc = pd.DataFrame(
            {"study_id": [study], "file": [None], "location": [None], "n_files": [None], "presence": ["bids_conflict_error"]}
        )
    except BIDSValidationError as e:
        print(f"BIDS metadata conflict in study {study}: {e}")
        filelist_loc = pd.DataFrame(
            {"study_id": [study], "file": [None], "location": [None], "n_files": [None], "presence": ["bids_validation_error"]}
        )
    except Exception as e:
        print(f"Unexpected error in study {study}: {e}")
        filelist_loc = pd.DataFrame(
            {"study_id": [study], "file": [None], "location": [None], "n_files": [None], "presence": ["bids_conflict_error"]}
        )
    return filelist_loc


# Compile run, task, file information
def compile_study_df(open_id, layout, subs, tasks, runs, sessions, type_dir):
    """
    Compiles study metadata into a DataFrame, summarizing the number of subjects, 
    tasks, runs, and sessions, as well as the existence of key neuroimaging files.

    Parameters:
        open_id (str): Study identifier.
        layout: A BIDS Layout object providing access to dataset files.
        subs (list): List of subject IDs.
        tasks (list): List of task names.
        runs (list): List of run identifiers.
        sessions (list): List of session identifiers.
        type_dir (str): whether bids_input or bids_derivative 

    Returns:
        pd.DataFrame: A DataFrame containing study-level metadata, including:
            - study_id: Study identifier.
            - root_type: whether BIDS or BIDS Deriv
            - num_subs: Number of subjects.
            - num_tasks: Number of tasks.
            - num_runs: Number of runs.
            - num_sessions: Number of sessions.
            - max_sessions: Maximum number of sessions per subject.
            - min_sessions: Minimum number of sessions per subject.
            - tasks: List of tasks.
            - nonrest_tasks: List of tasks excluding "rest".
            - nifti_exists, dwi_exists, t1w_exists, t2w_exists, bold_exists, events_exists: 
              Binary indicators for the presence of specific neuroimaging files.
    """
    data = {
        "study_id": [open_id],
        "root_type": [type_dir],
        "num_subs": [len(subs)],
        "num_tasks": [len(tasks)],
        "num_runs": [len(runs)],
        "num_sessions": [len(sessions)],
        "max_sessions": [max((len(layout.get(return_type='id', target='session', subject=sub)) for sub in subs),
                                            default=1  # Set a default value in case of an empty sequence
                                            )],
        "min_sessions": [min((len(layout.get(return_type='id', target='session', subject=sub)) for sub in subs),
                                            default=1  # Set a default value in case of an empty sequence
                                            )],
        "tasks": [tasks],
        "nonrest_tasks": [[task for task in tasks if not task.startswith("rest")]],
        "nifti_exists": [int(bool(layout.get(extension="nii.gz", return_type='file')))],
        "dwi_exists": [int(bool(layout.get(suffix="dwi", return_type='file')))],
        "t1w_exists": [int(bool(layout.get(suffix="T1w", return_type='file')))],
        "t2w_exists": [int(bool(layout.get(suffix="T2w", return_type='file')))],
        "bold_exists": [int(bool(layout.get(suffix="bold", return_type='file')))]
    }
    
    return pd.DataFrame(data)

    
     
# compile dataset description information, key value pairs
def create_df_descriptor(layout, open_id):
    """
    Converts the dataset description from a layout object into a DataFrame.

    Parameters:
        layout: An object that provides the `get_dataset_description()` method.
        open_id: The study identifier.

    Returns:
        pd.DataFrame: A DataFrame containing 'study_id', 'key', and 'value' columns.
    """
    return pd.DataFrame([
        {"study_id": open_id, "key": key, "value": value} 
        for key, value in layout.get_dataset_description().items()
    ])


# compile participant information, value key pairs and json details
def process_participant_data(layout, open_id, part_json_exists=True):
    """
    Processes participant data from a BIDS dataset, converting it into a long-format DataFrame
    with key-value pairs and checking if keys exist in the corresponding JSON metadata.

    Parameters:
        layout: An object that provides the `get()` method to retrieve file paths.
        open_id: The study identifier.
        part_json_exists=True: Whether participant json exists and to run, otherwise fill as N/A

    Returns:
        pd.DataFrame: A DataFrame containing 'study_id', 'key', 'value', and 'eventkeys_in_json' columns.
    """
    # Load participants.tsv
    tsv_files = layout.get(suffix="participants", extension="tsv", return_type="file")
    if not tsv_files:
        raise FileNotFoundError("No participants.tsv file found.")
    
    part_df = pd.read_csv(tsv_files[0], sep='\t')

    # Convert columns into key-value pairs
    part_long = pd.DataFrame([
        {"study_id": open_id, "key": col, "value": row[col]}
        for _, row in part_df.iterrows()
        for col in part_df.columns if col != "study_id"
    ])

    # Load JSON file and extract keys
    if part_json_exists:
        json_files = layout.get(suffix="participants", extension="json", return_type="file")
        json_keys = set()
        if json_files:
            with open(json_files[0]) as f:
                json_keys = set(json.load(f).keys())
        # Add a column indicating if the key exists in JSON
        part_long["partkeys_in_json"] = part_long["key"].isin(json_keys).astype(int)

    else:
        part_long["partkeys_in_json"] = "json_notavailable"

    return part_long


def process_event_data(layout, open_id, task, events_json_exists=True):
    """
    Processes event data from a BIDS dataset, converting it into a long-format DataFrame
    with key-value pairs and checking if keys exist in the corresponding JSON metadata.

    Parameters:
        layout: An object that provides the `get()` method to retrieve file paths.
        open_id: The study identifier.
        task: The task name to retrieve the corresponding events file.

    Returns:
        pd.DataFrame: A DataFrame containing 'study_id', 'task', 'key', 'value', and 'eventkeys_in_json' columns.
    """
    # Load events.tsv
    tsv_files = layout.get(task=task, suffix="events", extension="tsv", return_type="file")
    if not tsv_files:
        raise FileNotFoundError(f"No events.tsv file found for task: {task}")
    
    eventsdf = pd.read_csv(tsv_files[0], sep='\t')

    # Convert columns into key-value pairs
    eventsdf_long = pd.DataFrame([
        {"study_id": open_id, "task": task, "key": col, "value": row[col]}
        for _, row in eventsdf.iterrows()
        for col in eventsdf.columns if col != "study_id"
    ])


    # Load JSON file and extract keys
    if events_json_exists:
        # Load JSON file and extract keys
        json_files = layout.get(task=task, suffix="events", extension="json", return_type="file")
        json_keys = set()
        if json_files:
            with open(json_files[0]) as f:
                json_keys = set(json.load(f).keys())

        # Add a column indicating if the key exists in JSON
        eventsdf_long["eventkeys_in_json"] = eventsdf_long["key"].isin(json_keys).astype(int)

    else:
        eventsdf_long["eventkeys_in_json"] = "json_notavailable"

    return eventsdf_long



