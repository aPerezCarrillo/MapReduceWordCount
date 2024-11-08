import requests
from flask import Flask, request, jsonify, Response
import os
import threading
import yaml
from typing import List, Dict, Union, Tuple
import shutil

# Declare Flask app
app = Flask(__name__)

def load_config() -> Dict:
    """
    Load the configuration from the YAML file.

    Returns:
        Dict: The configuration settings loaded from the YAML file.
    """
    with open('../config.yaml', 'r') as file:
        return yaml.safe_load(file)

config = load_config()

def delete_and_create_folder(folder_path: str) -> None:
    """
    Delete a folder and its contents if it exists, then create a new folder.

    Args:
        folder_path (str): The path of the folder to delete and recreate.
    """
    # Check if the folder exists
    if os.path.exists(folder_path):
        # Remove the folder and all its contents
        shutil.rmtree(folder_path)
        print(f"Deleted folder: {folder_path}")

    # Create a new folder
    os.makedirs(folder_path)
    print(f"Created folder: {folder_path}")

def group_number_indexes(nums: List[int], k: int) -> List[List[int]]:
    """
    Group numbers into k groups based on their sizes, aiming to balance the sums of the groups.

    Args:
        nums (List[int]): A list of numbers to be grouped.
        k (int): The number of groups to create.

    Returns:
        List[List[int]]: A list of groups, where each group contains the indexes of the original numbers.
    """
    # Sort the numbers in descending order while keeping track of their original indexes
    indexed_nums = sorted(enumerate(nums), key=lambda x: x[1], reverse=True)

    # Initialize groups and their sums
    groups = [[] for _ in range(k)]
    group_sums = [0] * k

    # Distribute numbers into groups
    for index, num in indexed_nums:
        # Find the group with the smallest sum
        min_group_index = group_sums.index(min(group_sums))

        # Add the index of the number to that group
        groups[min_group_index].append(index)
        group_sums[min_group_index] += num

    return groups

class TaskManager:
    def __init__(self, config: Dict):
        """
        Initialize the TaskManager with configuration settings.

        Args:
            config (Dict): The configuration settings for the MapReduce tasks.
        """
        self.shutdown_flag = False
        self.N = config['mapreduce']['num_map_tasks']
        self.M = config['mapreduce']['num_reduce_tasks']
        self.INPUT_DIR = config['directories']['input']
        self.map_tasks: List[Dict] = []
        self.reduce_tasks: List[Dict] = []
        self.completed_map_tasks = {}
        self.completed_reduce_tasks = {}
        self.lock = threading.Lock()

    def create_tasks(self) -> None:
        """
        Create map and reduce tasks based on the input files in the specified directory.
        """
        # Get all input files
        input_files = [f for f in os.listdir(self.INPUT_DIR) if f.endswith('.txt')]
        self.N = min(self.N, len(input_files))
        file_sizes = [os.path.getsize(self.INPUT_DIR + '/' + f) for f in os.listdir(self.INPUT_DIR) if f.endswith('.txt')]

        # Distribute files among N map tasks
        file_groups = group_number_indexes(file_sizes, self.N)
        # Create map tasks
        for i, file_idx in enumerate(file_groups):
            task_files = [input_files[i] for i in file_idx]

            if task_files:  # Only create task if there are files to process
                self.map_tasks.append({
                    'type': 'map',
                    'task_id': i,
                    'files': task_files
                })

        # Create reduce tasks
        for i in range(self.M):
            self.reduce_tasks.append({
                'type': 'reduce',
                'task_id': i,
            })
        return

    def get_next_task(self) -> Union[Dict, None]:
        """
        Retrieve the next available task (map or reduce) for processing.

        Returns:
            Union[Dict, None]: The next task to be processed, or None if no tasks are available.
        """
        with self.lock:
            # If there are map tasks remaining, assign them first
            if self.map_tasks:
                return self.map_tasks.pop(0)

            # If all map tasks are completed, start assigning reduce tasks
            if self.completed_map_tasks and len(self.completed_map_tasks) == self.N and self.reduce_tasks:
                return self.reduce_tasks.pop(0)

            return None

    def complete_task(self, task_type: str, task_id: int) -> None:
        """
        Mark a task as completed.

        Args:
            task_type (str): The type of the task ('map' or 'reduce').
            task_id (int): The ID of the task to mark as completed.
        """
        with self.lock:
            if task_type == 'map':
                if task_id not in self.completed_map_tasks:
                    self.completed_map_tasks[task_id] = True
            elif task_type == 'reduce':
                if task_id not in self.completed_reduce_tasks:
                    self.completed_reduce_tasks[task_id] = True

    def is_task_started(self, task_type: str, task_id: int) -> bool:
        """
        Check if a task has been started.

        Args:
            task_type (str): The type of the task ('map' or 'reduce').
            task_id (int): The ID of the task to check.

        Returns:
            bool: True if the task has been started, False otherwise.
        """
        if task_type == 'map':
            return any(task['task_id'] == task_id for task in self.map_tasks) or task_id in self.completed_map_tasks
        elif task_type == 'reduce':
            return any(task['task_id'] == task_id for task in self.reduce_tasks) or task_id in self.completed_reduce_tasks
        return False

    def is_all_completed(self) -> bool:
        """
        Check if all tasks (map and reduce) have been completed.

        Returns:
            bool: True if all tasks are completed, False otherwise.
        """
        return (len(self.completed_map_tasks) == self.N and
                len(self.completed_reduce_tasks) == self.M)

    def shutdown(self) -> None:
        """
        Initiate the shutdown process for the TaskManager.
        """
        print("All tasks completed. Shutting down the server.")
        self.shutdown_flag = True  # Set the shutdown flag

@app.route('/get_task', methods=['GET'])
def get_task() -> Response | tuple[Response, int]:
    """
    Endpoint to retrieve the next available task.

    Returns:
        str: JSON response containing the task or a status message.
    """
    task = task_manager.get_next_task()
    if task:
        return jsonify({'task': task})
    else:
        # Check if all map tasks have been started
        if task_manager.completed_map_tasks and len(task_manager.completed_map_tasks) < task_manager.N:
            return jsonify({'status': 'wait', 'message': 'All map tasks started, please wait.'}), 202
        return jsonify({'task': None}), 204

@app.route('/task_completed', methods=['POST'])
def task_completed() -> str:
    """
    Endpoint to mark a task as completed.

    Returns:
        str: JSON response indicating the status of the operation.
    """
    data = request.json
    task_type = data['type']
    task_id = data['task_id']

    task_manager.complete_task(task_type, task_id)
    return jsonify({'status': 'success'})

@app.route('/status', methods=['GET'])
def status() -> Response:
    """
    Endpoint to retrieve the current status of completed tasks.

    Returns:
        str: JSON response containing the status of completed tasks.
    """
    return jsonify({
        'completed_map_tasks': len(task_manager.completed_map_tasks),
        'completed_reduce_tasks': len(task_manager.completed_reduce_tasks),
        'all_completed': task_manager.is_all_completed()
    })

@app.route('/shutdown', methods=['POST'])
def shutdown() -> Response:
    """
    Endpoint to initiate server shutdown.

    Returns:
        str: JSON response indicating the shutdown status.
    """
    print("Driver: Shutting down the server.")
    func = request.environ.get('werkzeug.server.shutdown')
    if func is not None:
        func()
    return jsonify({'status': 'shutdown initiated'})

def monitor_tasks() -> None:
    """
    Monitor the completion of tasks and initiate shutdown when all tasks are completed.
    """
    while True:
        if task_manager.is_all_completed():
            # Make a request to the shutdown route
            requests.post(f'http://{config["driver"]["host"]}:{config["driver"]["port"]}/shutdown')
            break

if __name__ == '__main__':
    # Create directories
    delete_and_create_folder(config['directories']['intermediate'])
    delete_and_create_folder(config['directories']['output'])

    task_manager = TaskManager(config)
    task_manager.create_tasks()

    # Start a background thread to monitor task completion
    threading.Thread(target=monitor_tasks, daemon=True).start()

    app.run(port=config['driver']['port'])
