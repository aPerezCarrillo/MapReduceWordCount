import subprocess
import sys
import time
from typing import List


def start_driver() -> subprocess.Popen:
    """
    Start the driver process that coordinates the MapReduce job.

    Returns:
        subprocess.Popen: The process object for the driver.
    """
    driver_process = subprocess.Popen(['python', 'driver.py'])
    return driver_process


def start_workers(num_workers: int) -> List[subprocess.Popen]:
    """
    Start multiple worker processes that will perform the map tasks.

    Args:
        num_workers (int): The number of worker processes to start.

    Returns:
        List[subprocess.Popen]: A list of subprocess.Popen objects representing the worker processes.
    """
    workers = []
    for i in range(num_workers):
        # Launch each worker with its index as an argument.
        worker_process = subprocess.Popen(['python', 'worker.py', str(i)])
        workers.append(worker_process)
        time.sleep(1)  # Optional: stagger the start of workers to avoid overload.
    return workers


def run_launcher(n_workers: int) -> None:
    """
    Main function to run the launcher, starting the driver and workers.

    Args:
        n_workers (int): The number of worker processes to start.
    """
    driver = start_driver()  # Start the driver process.
    print("Launcher: Driver started.")

    # Allow some time for the driver to initialize before starting workers.
    time.sleep(5)

    # Start the specified number of worker processes.
    workers = start_workers(n_workers)
    print(f"Launcher: {n_workers} workers started.")

    # Wait for the driver process to finish before exiting.
    driver.wait()


if __name__ == "__main__":
    """
    Entry point of the script, handling command-line arguments.
    If number of workers is not provided, it runs 4 workers by default.
    """
    args = sys.argv[1:]  # Exclude the script name from arguments.
    n_workers = 4
    try:
        # Attempt to convert the first argument to an integer for the number of workers.
        n_workers = int(args[0])
    except:
        # Print usage information if conversion fails.
        print('usage: python launcher.py number_of_workers')
        print(f'Launching default {n_workers} workers.')

    run_launcher(n_workers)  # Run the launcher with the specified number of workers.
