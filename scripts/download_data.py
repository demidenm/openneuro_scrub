import time
import subprocess
from pathlib import Path
import subprocess

# Note, Chris recommends to access/query openneuro using: datalad install -r ///openneuro and find {...linux... functions...}
# Alternatively, by study use below

def clone_data(openneuro_study, output_dir):
    """
    Clone open neuro using datalab from git_repo_url
    """
    git_repo_url = f"https://github.com/OpenNeuroDatasets/{openneuro_study}.git"
    
    try:
        # Clone dataset
        print(f"Cloning BIDS and downloading \n")
        subprocess.run(['datalad', 'clone', git_repo_url, output_dir], check=True)

        try:
            subprocess.run(['datalad', 'siblings', '-d', output_dir, 'enable', '-s', 's3-PRIVATE'], check=True)
        except subprocess.CalledProcessError:
            print("        Warning: 's3-PRIVATE' sibling not found or could not be enabled. Continuing...")

        print(f"    {openneuro_study}. Dataset cloned successfully (no files downloaded).")
    except subprocess.CalledProcessError as e:
        if 'error: unknown option `show-origin`' in str(e):
            print("     Error: Your Git version may be outdated. Please confirm and update Git.")
            print("     Use 'git --version' to check your version.")
        else:
            print(f"        An error occurred while cloning the dataset: {e}")

