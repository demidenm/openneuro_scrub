import numpy as np
import re
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
from wordcloud import WordCloud
import ssl
import urllib.request
import requests
import os
from pathlib import Path
from collections import Counter

# Configuration
script_dir = Path(__file__).parent
output_dir = script_dir / "../output"
fig_dir = script_dir / "../figures"
top_n_words = 20

# Ensure output directory exists
fig_dir.mkdir(parents=True, exist_ok=True)

def load_csv_safe(filename, directory):
    """Load CSV file with error handling"""
    filepath = directory / filename
    if filepath.exists():
        return pd.read_csv(filepath)
    else:
        print(f"Missing: {filename}")
        return None

def save_plot(filename, dpi=300):
    """Save plot and close figure"""
    plt.savefig(fig_dir / filename, dpi=dpi, bbox_inches='tight')
    plt.close()

def create_frequency_barplot(data, title, filename, x_col='freq', y_col='name'):
    """Create horizontal bar plot with frequency and percentages"""
    plt.figure(figsize=(12, 8))
    # Fix: Add hue parameter and set legend=False to resolve seaborn warning
    sns.barplot(data=data, y=y_col, x=x_col, hue=y_col, palette='viridis', legend=False)
    plt.title(title)
    plt.xlabel('Frequency (N)')
    plt.ylabel('Column Name')
    
    # Add percentage labels
    for i, (freq, percent) in enumerate(zip(data[x_col], data['percent'])):
        plt.text(freq + 1, i, f'{percent:.1f}%', va='center', fontsize=10)
    
    plt.tight_layout()
    save_plot(filename)

def fetch_derivative_repos():
    """Fetch all derivative repositories from OpenNeuroDerivatives organization"""
    repos = []
    page = 1
    per_page = 100
    
    while True:
        url = f"https://api.github.com/orgs/OpenNeuroDerivatives/repos?page={page}&per_page={per_page}"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            if not data:  # No more repositories
                break
                
            repos.extend(data)
            page += 1
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching repositories: {e}")
            break
    
    return repos

if __name__ == "__main__":

    # Load data files
    print("Loading data files...")
    all_basics = load_csv_safe("final_basics_summary.csv", output_dir)
    all_compilesumm = load_csv_safe("final_counts_summary.csv", output_dir)
    all_descriptors = load_csv_safe("final_descriptors.csv", output_dir)
    all_participants = load_csv_safe("final_participants.csv", output_dir)
    all_events = load_csv_safe("final_events.csv", output_dir)

    # Load OpenNeuro metadata
    print("Loading OpenNeuro metadata...")
    ssl_context = ssl._create_unverified_context()
    url = 'https://raw.githubusercontent.com/jbwexler/openneuro_metadata/main/metadata.csv'
    with urllib.request.urlopen(url, context=ssl_context) as response:
        openneuro_joe = pd.read_csv(response)

    # 1. File Type Percentage Analysis
    print("Creating file type percentage plot...")
    if all_basics is not None:
        all_basics["file_list"] = all_basics["file"].str.replace(
            r"(task-)[^_]+(_(events|bold)\.(tsv|json))", r"\1*\2", regex=True
        )
        all_basics["presence"] = pd.to_numeric(all_basics["presence"], errors="coerce")
        
        percent_type = (
            all_basics.groupby("file_list", as_index=False)["presence"]
            .mean()
            .assign(percent=lambda df: df["presence"] * 100)
            .rename(columns={"file_list": "type"})
            .sort_values(by="percent", ascending=False)
        )
        
        plt.figure(figsize=(12, 8))
        plt.bar(percent_type['type'], percent_type['percent'])
        plt.xlabel('')
        plt.ylabel('Percentage Exists (%)')
        plt.title('Percentage of Folders Containing Each File Type\nOpenNeuro Datasets')
        plt.xticks(rotation=45, ha='right')
        plt.ylim(0, 100)
        plt.tight_layout()
        save_plot("file_type_percentage.png")

    # 2. Subject Distribution
    print("Creating subject distribution plot...")
    if all_compilesumm is not None:
        data_to_plot = all_compilesumm[["num_subs"]].dropna()
        max_value = data_to_plot["num_subs"].max()
        max_study = all_compilesumm.loc[all_compilesumm["num_subs"] == max_value, "study_id"].values
        
        plt.figure(figsize=(8, 6))
        # Fix: Replace deprecated 'labels' parameter with 'tick_labels'
        plt.boxplot(data_to_plot.values, tick_labels=data_to_plot.columns, patch_artist=True)
        plt.text(1, max_value, f"{', '.join(max_study)} ({max_value})", 
                ha="center", va="bottom", fontsize=10, color="red")
        plt.ylabel("Subject Counts")
        plt.title("Distribution of Subjects Across Studies")
        plt.grid(axis="y", linestyle="--", alpha=0.7)
        save_plot("file_counts-subjects.png")

    # 3. Tasks/Runs Distribution
    print("Creating tasks/runs distribution plot...")
    if all_compilesumm is not None:
        data_to_plot = all_compilesumm[["num_runs", "num_tasks"]].dropna()
        max_values = data_to_plot.max()
        
        plt.figure(figsize=(8, 6))
        # Fix: Replace deprecated 'labels' parameter with 'tick_labels'
        plt.boxplot(data_to_plot.values, tick_labels=data_to_plot.columns, patch_artist=True)
        
        for i, col in enumerate(data_to_plot.columns, start=1):
            max_value = max_values[col]
            max_studies = all_compilesumm.loc[all_compilesumm[col] == max_value, "study_id"].values
            plt.text(i, max_value + 2, f"{', '.join(max_studies)} ({max_value})", 
                    ha="center", va="bottom", fontsize=10, color="red")
        
        plt.ylabel("Counts")
        plt.title("Distribution of Runs & Tasks Across Studies")
        plt.grid(axis="y", linestyle="--", alpha=0.7)
        save_plot("file_counts-runs-tasks.png")

    # 4. Sessions Distribution
    print("Creating sessions distribution plot...")
    if all_compilesumm is not None:
        data_to_plot = all_compilesumm[["num_sessions", "max_sessions", "min_sessions"]].dropna()
        max_values = data_to_plot.max()
        
        plt.figure(figsize=(8, 6))
        # Fix: Replace deprecated 'labels' parameter with 'tick_labels'
        plt.boxplot(data_to_plot.values, tick_labels=data_to_plot.columns, patch_artist=True)
        
        for i, col in enumerate(data_to_plot.columns, start=1):
            max_value = max_values[col]
            max_studies = all_compilesumm.loc[all_compilesumm[col] == max_value, "study_id"].values
            plt.text(i, max_value + 10, f"{', '.join(max_studies)} ({max_value})", 
                    ha="center", va="bottom", fontsize=8, color="red")
        
        plt.ylabel("Session Counts")
        plt.title("Distribution of # Sessions Per\nBIDSlayout & Min/Max across subjects within dataset")
        plt.grid(axis="y", linestyle="--", alpha=0.7)
        save_plot("file_counts-sessions-minmax.png")

    # 5. Descriptor Frequency Analysis
    print("Creating descriptor frequency plot...")
    if all_descriptors is not None:
        uniq_studies = all_descriptors['study_id'].unique()
        total_studies = len(uniq_studies)
        
        freq_words = Counter(all_descriptors['key'])
        top_words_df = pd.DataFrame(freq_words.most_common(top_n_words), 
                                columns=['name', 'freq'])
        top_words_df['percent'] = (top_words_df['freq'] / total_studies) * 100
        
        create_frequency_barplot(
            top_words_df, 
            f'Top {top_n_words} Words across {total_studies} OpenNeuro descriptor files',
            "descriptor-freq_top-20words.png"
        )

    # 6. Participant Frequency Analysis
    print("Creating participant frequency plot...")
    if all_participants is not None:
        unique_participants = all_participants[['study_id', 'key']].drop_duplicates()
        uniq_studies = unique_participants['study_id'].unique()
        total_studies = len(uniq_studies)
        
        freq_words = Counter(unique_participants['key'])
        top_words_df = pd.DataFrame(freq_words.most_common(top_n_words), 
                                columns=['name', 'freq'])
        top_words_df['percent'] = (top_words_df['freq'] / total_studies) * 100
        
        create_frequency_barplot(
            top_words_df,
            f'Top {top_n_words} Words across {total_studies} OpenNeuro participant.tsv files',
            "participant-freq_top-20words.png"
        )

    # 7. Participant JSON Keys Analysis
    print("Creating participant JSON keys plot...")
    if all_participants is not None:
        keys_to_keep = ['participant_id', 'sex', 'Sex', 'gender', 'age', 'Age', 
                    'group', 'handedness', 'hand', 'Handedness']
        
        filtered_partkeys = (all_participants[all_participants['key'].isin(keys_to_keep)]
                            [['study_id', 'key', 'partkeys_in_json']]
                            .drop_duplicates())
        
        # Filter and convert to numeric
        filtered_partkeys = filtered_partkeys[
            filtered_partkeys['partkeys_in_json'] != "json_notavailable"
        ].copy()
        filtered_partkeys['partkeys_in_json'] = filtered_partkeys['partkeys_in_json'].astype(int)
        
        # Calculate percentages
        key_counts = filtered_partkeys.groupby('key')['partkeys_in_json'].sum()
        total_counts = filtered_partkeys.groupby('key')['study_id'].nunique()
        percentages = (key_counts / total_counts) * 100
        
        plot_df = pd.DataFrame({
            'key': percentages.index, 
            'percent': percentages.values
        }).sort_values(by='percent', ascending=False)
        
        plt.figure(figsize=(12, 8))
        # Fix: Add hue parameter and set legend=False to resolve seaborn warning
        sns.barplot(data=plot_df, y='key', x='percent', hue='key', palette='viridis', legend=False)
        plt.title('Percentage of Columns in Json Key (if exists)')
        plt.xlabel('Percentage (%)')
        plt.ylabel('Column Name')
        
        for i, (percent, key) in enumerate(zip(plot_df['percent'], plot_df['key'])):
            plt.text(percent + 1, i, f'{percent:.1f}%', va='center', fontsize=10)
        
        plt.tight_layout()
        save_plot("participantjson-freq_top-20words.png")

    # 8. Events Frequency Analysis
    print("Creating events frequency plot...")
    if all_events is not None:
        unique_events = all_events[['study_id', 'key']].drop_duplicates()
        uniq_studies = unique_events['study_id'].unique()
        total_studies = len(uniq_studies)
        
        freq_words = Counter(unique_events['key'])
        top_words_df = pd.DataFrame(freq_words.most_common(top_n_words), 
                                columns=['name', 'freq'])
        top_words_df['percent'] = (top_words_df['freq'] / total_studies) * 100
        
        create_frequency_barplot(
            top_words_df,
            f'Top {top_n_words} Words across {total_studies} OpenNeuro events.tsv files',
            "events-freq_top-20words.png"
        )

    # 9. Participant Word Cloud
    print("Creating participant word cloud...")
    if all_participants is not None:
        import unicodedata
        all_participants['key'] = all_participants['key'].apply(
            lambda x: unicodedata.normalize('NFKD', str(x)).encode('ascii', 'ignore').decode('ascii')
        )
        
        freq_words = Counter(all_participants['key'])
        
        wordcloud = WordCloud(width=800, height=400, background_color='white', 
                            colormap='viridis').generate_from_frequencies(freq_words)
        
        plt.figure(figsize=(12, 6))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis('off')
        plt.title('OpenNeuro: Wordcloud of column names in participants.tsv files')
        plt.tight_layout()
        save_plot("participant-wordcloud.png")

    # 10. Create multi-panel counts, time figure for open-neuor
    print("Creating multi-repo growth plot of openneuro datasets...")
    openneuro_joe['made_public'] = pd.to_datetime(openneuro_joe['made_public'])
    openneuro_joe = openneuro_joe.dropna(subset=['made_public'])
    
    # Fix: Handle timezone conversion warning by using tz_localize(None) to remove timezone info
    if openneuro_joe['made_public'].dt.tz is not None:
        openneuro_joe['made_public'] = openneuro_joe['made_public'].dt.tz_localize(None)
    openneuro_joe['yr_mon'] = openneuro_joe['made_public'].dt.to_period('M')

    #       1. cumulative datasets
    datasets_cumulative = openneuro_joe.groupby('yr_mon').size().cumsum()

    #       2. cumulative subjects
    subjects_cumulative = openneuro_joe.groupby('yr_mon')['num_subjects'].sum().cumsum()

    #       3. fetch derivatives info
    all_repos = fetch_derivative_repos()

    # Filter for fMRIPrep and MRIQC repositories
    fmriprep_repos = [repo for repo in all_repos if 'fmriprep' in repo['name'].lower()]
    mriqc_repos = [repo for repo in all_repos if 'mriqc' in repo['name'].lower()]

    # Process fMRIPrep repositories
    if fmriprep_repos:
        fmriprep_data = []
        for repo in fmriprep_repos:
            fmriprep_data.append({
                'name': repo['name'],
                'created_at': repo['created_at'],
                'updated_at': repo['updated_at']
            })
        
        fmriprep_df = pd.DataFrame(fmriprep_data)
        fmriprep_df['created_at'] = pd.to_datetime(fmriprep_df['created_at'])
        # Fix: Handle timezone conversion warning
        if fmriprep_df['created_at'].dt.tz is not None:
            fmriprep_df['created_at'] = fmriprep_df['created_at'].dt.tz_localize(None)
        fmriprep_df['yr_mon'] = fmriprep_df['created_at'].dt.to_period('M')
        fmriprep_cumulative = fmriprep_df.groupby('yr_mon').size().cumsum()
        
        print(f"Found {len(fmriprep_repos)} fMRIPrep repositories")
    else:
        print("No fMRIPrep repositories found")
        fmriprep_cumulative = pd.Series(dtype='int64')

    # Process MRIQC repositories
    if mriqc_repos:
        mriqc_data = []
        for repo in mriqc_repos:
            mriqc_data.append({
                'name': repo['name'],
                'created_at': repo['created_at'],
                'updated_at': repo['updated_at']
            })
        
        mriqc_df = pd.DataFrame(mriqc_data)
        mriqc_df['created_at'] = pd.to_datetime(mriqc_df['created_at'])
        # Fix: Handle timezone conversion warning
        if mriqc_df['created_at'].dt.tz is not None:
            mriqc_df['created_at'] = mriqc_df['created_at'].dt.tz_localize(None)
        mriqc_df['yr_mon'] = mriqc_df['created_at'].dt.to_period('M')
        mriqc_cumulative = mriqc_df.groupby('yr_mon').size().cumsum()
        
        print(f"Found {len(mriqc_repos)} MRIQC repositories")
    else:
        print("No MRIQC repositories found")
        mriqc_cumulative = pd.Series(dtype='int64')

    #       4 openneuro growth figure
    plt.figure(figsize=(10, 6))
    plt.plot(datasets_cumulative.index.to_timestamp(), datasets_cumulative.values, marker='o', label='OpenNeuro Datasets')
    if not fmriprep_cumulative.empty:
        plt.plot(fmriprep_cumulative.index.to_timestamp(), fmriprep_cumulative.values, marker='s', color='green', label='fMRIPrep Derivs')

    if not mriqc_cumulative.empty:
        plt.plot(mriqc_cumulative.index.to_timestamp(), mriqc_cumulative.values, marker='^', color='red', label='MRIQC Derivs')

    plt.ylabel('# of Datasets')
    plt.title('Datasets Since 2016')
    plt.grid(True)
    plt.legend()
    plt.tight_layout()

    # save and close
    plt.tight_layout()
    save_plot("openneuro_datasets-growth.png")
    plt.close()
    print(f"\nAnalysis complete! All plots saved to: {fig_dir}\n")

