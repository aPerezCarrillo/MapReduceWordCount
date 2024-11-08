import subprocess
import time

def start_driver():
    # Start the driver process
    driver_process = subprocess.Popen(['python', 'driver.py'])
    return driver_process

def start_workers(num_workers):
    workers = []
    for i in range(num_workers):
        worker_process = subprocess.Popen(['python', 'worker.py', str(i)])
        workers.append(worker_process)
        time.sleep(1)  # Optional: stagger the start of workers
    return workers

if __name__ == "__main__":
    driver = start_driver()
    print("Driver started.")

    # Allow some time for the driver to start
    time.sleep(5)

    # Start multiple workers
    num_workers = 5  # Specify the number of workers you want to start
    workers = start_workers(num_workers)
    print(f"{num_workers} workers started.")

    # Optionally, wait for the driver to finish
    driver.wait()
