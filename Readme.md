# Distributed Word Count MapReduce

This project implements a distributed MapReduce program for counting word occurrences in a set of text files using Python and Flask for REST API communication.
It consists of a launcher script, a driver script, and a worker script. The launcher runs the driver and n workers.

## File Structure

- `src/driver.py`: The server that distributes tasks to workers.
- `src/worker.py`: The client that processes tasks.
- `src/launcher.py`: The client that processes tasks.
- `files/inputs`: Directory containing input text files to process
- `files/intermediate`: intermediate (map results) folder. Re-created at every run.
- `files/out`: out (reduce results) folder. Re-created at every run.
- `config.yaml`: Config file
- `requirements.txt`: Required Python packages.

## How to install
1. **Clone the repository** (if applicable):
    ```bash
    git clone https://github.com/aPerezCarrillo/MapReduceWordCount.git
    cd MapReduceWordCount
    ```

2. **Set Up Your Environment** (optional):
   It is recommended to use a virtual environment to manage dependencies. You can create one using:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`

3. Install the required packages:
   ```bash
   pip install -r requirements.txt

## How to Run
1. Run launcher.py with one argument indicating number of workers (e.g. 4):
   ```bash
   python launcher.py 4

2. Alternatively, you can run the driver.py and several worker.py <worker_id> from different terminals
   ```bash
   python driver.py
   python worker.py 0
   ``` 
   Open another terminal
   ```bash
   python worker.py 1
   ``` 

## Program Flow: 

- **Contact**: ap.carrillo@gmail.com