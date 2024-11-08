import time

import requests
from flask import Flask, request, jsonify
import os
import threading
import yaml
from typing import List, Dict, Union
import shutil
import nltk

# Make sure to download the necessary NLTK resources
nltk.download('punkt')

# Declare Flask app
app = Flask(__name__)


def load_config():
    with open('../config.yaml', 'r') as file:
        return yaml.safe_load(file)

config = load_config()

# Initialize configuration
# N = config['mapreduce']['num_map_tasks']
# M = config['mapreduce']['num_reduce_tasks']
PORT = config['driver']['port']
# INPUT_DIR = config['directories']['input']


def delete_and_create_folder(folder_path):
    # Check if the folder exists
    if os.path.exists(folder_path):
        # Remove the folder and all its contents
        shutil.rmtree(folder_path)
        print(f"Deleted folder: {folder_path}")

    # Create a new folder
    os.makedirs(folder_path)
    print(f"Created folder: {folder_path}")

def group_number_indexes(nums: list, k: int):
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
    def __init__(self, config):
        self.shutdown_flag = False
        self.N = config['mapreduce']['num_map_tasks']
        self.M = config['mapreduce']['num_reduce_tasks']
        self.INPUT_DIR = config['directories']['input']
        self.map_tasks: List[Dict] = []
        self.reduce_tasks: List[Dict] = []
        # self.completed_map_tasks = 0
        # self.completed_reduce_tasks = 0
        self.completed_map_tasks = {}
        self.completed_reduce_tasks = {}
        self.lock = threading.Lock()
        # self.all_map_tasks_completed = False

    def create_tasks(self):
        # Get all input files
        input_files = [f for f in os.listdir(self.INPUT_DIR) if f.endswith('.txt')]
        self.N = min(self.N, len(input_files))
        # total_files = len(input_files)
        file_sizes = [os.path.getsize(self.INPUT_DIR + '/' + f)  for f in os.listdir(self.INPUT_DIR) if f.endswith('.txt')]

        # Distribute files among N map tasks
        file_groups = group_number_indexes(file_sizes, self.N)
        # Create map tasks
        for i, file_idx in enumerate(file_groups):
            task_files = [input_files[i] for i in file_idx]

            if task_files:  # Only create task if there are files to process
                self.map_tasks.append({
                    'type': 'map',
                    'task_id': i,
                    # 'num_reduce_tasks': M,
                    'files': task_files
                })

        # Create reduce tasks
        for i in range(self.M):
            self.reduce_tasks.append({
                'type': 'reduce',
                'task_id': i,
                # 'num_map_tasks': N
            })
        return

    def get_next_task(self) -> Union[Dict, None]:
        with self.lock:
            # If there are map tasks remaining, assign them first
            if self.map_tasks:
                return self.map_tasks.pop(0)

            # If all map tasks are completed, start assigning reduce tasks
            # if self.completed_map_tasks == self.N and self.reduce_tasks:
            if self.completed_map_tasks and len(self.completed_map_tasks) == self.N and self.reduce_tasks:
                return self.reduce_tasks.pop(0)

            return None

    def complete_task(self, task_type: str, task_id: int):
        with self.lock:
            if task_type == 'map':
                if task_id not in self.completed_map_tasks:
                    self.completed_map_tasks[task_id] = True
            elif task_type == 'reduce':
                if task_id not in self.completed_reduce_tasks:
                    self.completed_reduce_tasks[task_id] = True

    #def is_all_map_tasks_completed(self) -> bool:
    #    return self.completed_map_tasks == self.N
    def is_task_started(self, task_type: str, task_id: int) -> bool:
        if task_type == 'map':
            return any(task['task_id'] == task_id for task in self.map_tasks) or task_id in self.completed_map_tasks
        elif task_type == 'reduce':
            return any(task['task_id'] == task_id for task in self.reduce_tasks) or task_id in self.completed_reduce_tasks
        return False

    def is_all_completed(self) -> bool:
        return (len(self.completed_map_tasks) == self.N and
                len(self.completed_reduce_tasks) == self.M)

    def shutdown(self):
        print("All tasks completed. Shutting down the server.")
        self.shutdown_flag = True  # Set the shutdown flag


@app.route('/get_task', methods=['GET'])
def get_task():
    task = task_manager.get_next_task()
    if task:
        return jsonify({'task': task})
    else:
        # Check if all map tasks have been started
        if task_manager.completed_map_tasks and len(task_manager.completed_map_tasks) < task_manager.N:
            return jsonify({'status': 'wait', 'message': 'All map tasks started, please wait.'}), 202
        return jsonify({'task': None}), 204
    #else:
    #    return jsonify({'task': None})


@app.route('/task_completed', methods=['POST'])
def task_completed():
    data = request.json
    task_type = data['type']
    task_id = data['task_id']

    # Verify if the task was started
    #if not task_manager.is_task_started(task_type, task_id):
    #    return jsonify({'status': 'error', 'message': 'Task not started or invalid task_id'}), 400

    task_manager.complete_task(task_type, task_id)
    return jsonify({'status': 'success'})


@app.route('/status', methods=['GET'])
def status():
    return jsonify({
        'completed_map_tasks': len(task_manager.completed_map_tasks),
        'completed_reduce_tasks': len(task_manager.completed_reduce_tasks),
        'all_completed': task_manager.is_all_completed()
    })

@app.route('/shutdown', methods=['POST'])
def shutdown():
    print("Shutting down the server.")
    func = request.environ.get('werkzeug.server.shutdown')
    if func is not None:
        func()
    return jsonify({'status': 'shutdown initiated'})


def monitor_tasks():
    while True:
        if task_manager.is_all_completed():
            # Make a request to the shutdown route
            requests.post(f'http://127.0.0.1:{PORT}/shutdown')
            break


if __name__ == '__main__':
    # Create directories
    delete_and_create_folder(config['directories']['intermediate'])
    delete_and_create_folder(config['directories']['output'])

    task_manager = TaskManager(config)
    task_manager.create_tasks()

    # Start a background thread to monitor task completion
    threading.Thread(target=monitor_tasks, daemon=True).start()

    app.run(port=PORT)

