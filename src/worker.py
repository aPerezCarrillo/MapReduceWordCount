import re
import string
import sys
import requests
import os
from collections import Counter
import yaml
import time

import nltk
from nltk import word_tokenize
nltk.download('punkt', quiet=True)
nltk.download('punkt_tab', quiet=True)

def load_config() -> dict:
    """
    Load the configuration from the YAML file.

    Returns:
        dict: The configuration settings loaded from the YAML file.
    """
    with open('../config.yaml', 'r') as file:
        return yaml.safe_load(file)

config = load_config()

# Initialize configuration
INTERMEDIATE_DIR = config['directories']['intermediate']
OUTPUT_DIR = config['directories']['output']
INPUT_DIR = config['directories']['input']
HOST = config['driver']['host']
PORT = config['driver']['port']
RETRY_ATTEMPTS = config['task_settings']['retry_attempts']
RETRY_DELAY = config['task_settings']['retry_delay']
M = config['mapreduce']['num_reduce_tasks']

def separate_words(text: str) -> list[str]:
    """
    Tokenize the input text into words, removing punctuation and converting to lowercase.

    Args:
        text (str): The input text to be tokenized.

    Returns:
        List[str]: A list of cleaned and lowercased words.
    """
    # Tokenize the text into words
    words = word_tokenize(text)
    # Remove punctuation from each word
    cleaned_words = [word.strip(string.punctuation) for word in words]
    # Filter out any empty strings that may result from stripping
    cleaned_words = [word.lower() for word in cleaned_words if word]
    return cleaned_words

def map_task(task: dict) -> dict:
    """
    Process a map task by reading assigned files and writing words to intermediate files.

    Args:
        task (dict): The task containing the task_id and files to process.

    Returns:
        dict: A dictionary indicating the completion of the map task.
    """
    task_id = task['task_id']

    # Process each assigned file
    for filename in task['files']:
        file_path = os.path.join(INPUT_DIR, filename)
        with open(file_path, 'r') as f:
            for line in f:
                words = separate_words(line)
                for word in words:
                    if word:  # Skip empty words
                        # Determine which reduce task should handle this word
                        bucket_id = ord(word[0].lower()) % M
                        # Write to intermediate file
                        intermediate_file = os.path.join(INTERMEDIATE_DIR, f'mr-{task_id}-{bucket_id}')
                        with open(intermediate_file, 'a') as out:
                            out.write(f'{word}\n')

    return {'type': 'map', 'task_id': task_id}

def get_reduce_task_filenames(intermediate_dir: str) -> dict:
    """
    Retrieve filenames associated with reduce tasks from the intermediate directory.

    Args:
        intermediate_dir (str): The path to the intermediate directory.

    Returns:
        dict: A dictionary mapping reduce task IDs to their corresponding filenames.
    """
    reduce_task_dict = {}  # Dictionary to hold reduce_task_id and associated filenames
    pattern = re.compile(r'mr-\d+-(\d+)')  # Regex pattern to match reduce_task_id

    # List all files in the specified directory
    for filename in os.listdir(intermediate_dir):
        match = pattern.match(filename)  # Check if the filename matches the pattern
        if match:
            reduce_task_id = int(match.group(1))  # Extract the reduce_task_id

            # Add the filename to the corresponding reduce_task_id in the dictionary
            if reduce_task_id not in reduce_task_dict:
                reduce_task_dict[reduce_task_id] = []  # Initialize list if key doesn't exist
            reduce_task_dict[reduce_task_id].append(filename)  # Append the filename

    return reduce_task_dict

def reduce_task(task: dict) -> dict:
    """
    Process a reduce task by aggregating word counts from intermediate files.

    Args:
        task (dict): The task containing the task_id to process.

    Returns:
        dict: A dictionary indicating the completion of the reduce task.
    """
    reduce_task_id = task['task_id']

    word_count = Counter()
    reduce_tasks_dict = get_reduce_task_filenames(INTERMEDIATE_DIR)

    # Read from all intermediate files for this reduce task
    for filename in reduce_tasks_dict[reduce_task_id]:
        intermediate_file = os.path.join(INTERMEDIATE_DIR, filename)
        if os.path.exists(intermediate_file):
            with open(intermediate_file, 'r') as f:
                words = f.read().splitlines()
                word_count.update(words)

    # Write the final counts
    output_file = os.path.join(OUTPUT_DIR, f'out-{reduce_task_id}')
    with open(output_file, 'w') as f:
        for word, count in sorted(word_count.items()):
            f.write(f'{word} {count}\n')

    return {'type': 'reduce', 'task_id': reduce_task_id}

def process_task(task: dict) -> dict:
    """
    Process a task based on its type (map or reduce).

    Args:
        task (dict): The task to be processed.

    Returns:
        dict: The result of processing the task.
    """
    if task['type'] == 'map':
        return map_task(task)
    else:  # reduce
        return reduce_task(task)

def run_worker(worker_id: int) -> None:
    """
    Continuously request and process tasks from the driver until no tasks are available.

    Args:
        worker_id (int): The ID of the worker for logging purposes.
    """
    while True:
        try:
            response = requests.get(f'http://{HOST}:{PORT}/get_task')
            task_data = response.json()

            if response.status_code == 200 and not task_data.get('task'):
                print(f"Worker {worker_id}: No more tasks available. Exiting.")
                break

            if response.status_code == 200:
                task = task_data['task']
                print(f"Worker {worker_id}: Processing {task['type']} task {task['task_id']}")

                # Process the task and get completion info
                completion_info = process_task(task)

                # Notify the driver of task completion
                requests.post(
                    f'http://{HOST}:{PORT}/task_completed',
                    json=completion_info
                )
                print(f"Worker {worker_id}: Completed {task['type']} task {task['task_id']}")
            elif response.status_code == 202:
                print(
                    f"Worker {worker_id}: received wait signal. Waiting for {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)  # Wait before retrying

            else:
                print(f"Worker {worker_id}: encountered an error: {response.status_code}")
                break  # Exit if there's an unexpected error

        except requests.RequestException as e:
            print(f"Worker {worker_id}: Error communicating with driver: Disconnecting")
            time.sleep(RETRY_DELAY)
            break

if __name__ == '__main__':
    # Get command-line arguments
    args = sys.argv[1:]  # Exclude the script name
    worker_id = 0
    try:
        worker_id = int(args[0])
    except:
        print('usage: python worker.py id')
        exit(0)

    run_worker(worker_id)
