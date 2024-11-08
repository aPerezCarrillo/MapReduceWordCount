# Distributed Word Count MapReduce

This project implements a distributed MapReduce program for counting word occurrences in a set of text files using Python and Flask for REST API communication.
It consists of a launcher script, a driver script, and a worker script. The launcher runs the driver and n workers.
- **Contact**: ap.carrillo@gmail.com
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
```mermaid
graph TD;
    A[Flask Server
(Driver Endpoint)] -->|1. Create Tasks
(4 Map Tasks, 2 Reduce Tasks)| B[Task Manager
(Driver Logic)];
    B -->|2. Assign Map Tasks
(Task 0, Task 1, Task 2, Task 3)| C[Worker 1
(Map Task Logic)];
    B -->|2. Assign Map Tasks
(Task 0, Task 1, Task 2, Task 3)| D[Worker 2
(Map Task Logic)];
    
    C -->|3. Process Input Files
(Tokenization, Word Count)| E[Intermediate Files
(e.g., mr-0-0, mr-0-1, mr-0-2, mr-0-3)];
    D -->|3. Process Input Files
(Tokenization, Word Count)| F[Intermediate Files
(e.g., mr-1-0, mr-1-1, mr-1-2, mr-1-3)];
    
    E -->|4. Notify Completion| A;
    F -->|4. Notify Completion| A;
    
    A -->|5. Assign Reduce Tasks
(Task 0, Task 1)| G[Worker 1
(Reduce Task Logic)];
    A -->|5. Assign Reduce Tasks
(Task 0, Task 1)| H[Worker 2
(Reduce Task Logic)];
    
    G -->|6. Read Intermediate Files
(Aggregate Word Counts)| I[Final Output
(e.g., out-0)];
    H -->|6. Read Intermediate Files
(Aggregate Word Counts)| J[Final Output
(e.g., out-1)];
    
    I -->|7. Notify Completion| A;
    J -->|7. Notify Completion| A;
```

