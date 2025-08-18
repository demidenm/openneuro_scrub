import time
import subprocess
from pathlib import Path
import subprocess

# Note, Chris recommends to access/query openneuro using: datalad install -r ///openneuro and find {...linux... functions...}
# Alternatively, by study use below

import subprocess

def clone_data(openneuro_study, output_dir):
    """
    Clone an OpenNeuro dataset using DataLad from GitHub.
    """
    git_repo_url = f"https://github.com/OpenNeuroDatasets/{openneuro_study}.git"
    
    try:
        # Clone dataset
        print(f"Cloning {openneuro_study} into {output_dir}\n")
        subprocess.run(
            ['datalad', 'clone', git_repo_url, str(output_dir)],
            check=True,
            capture_output=True,
            text=True
        )

        # Try enabling the s3-PRIVATE sibling
        try:
            subprocess.run(
                ['datalad', 'siblings', '-d', str(output_dir), 'enable', '-s', 's3-PRIVATE'],
                check=True,
                capture_output=True,
                text=True
            )
        except subprocess.CalledProcessError:
            print("    Warning: 's3-PRIVATE' sibling not found or could not be enabled. Continuing...")

    except subprocess.CalledProcessError as e:
        if 'error: unknown option `show-origin`' in e.stderr:
            print("    Error: Your Git version may be outdated. Please confirm and update Git.")
            print("    Use 'git --version' to check your version.")
        else:
            print(f"    An error occurred while cloning {openneuro_study}:")
            print("    stdout:", e.stdout)
            print("    stderr:", e.stderr)

