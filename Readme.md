# Distributed Word Count MapReduce

This project implements a distributed MapReduce program for counting word occurrences in a set of text files using Python and Flask for REST API communication.
It consists of a launcher script, a driver script, and a worker script. The launcher runs the driver and n workers.

## File Structure

- `driver.py`: The server that distributes tasks to workers.
- `worker.py`: The client that processes tasks.
- `intermediate/`: Directory containing intermediate and output files.
- `requirements.txt`: Required Python packages.

## How to Run

1. Install the required packages:
   ```bash
   pip install -r requirements.txt


