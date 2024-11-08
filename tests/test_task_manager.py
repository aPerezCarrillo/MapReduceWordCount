import unittest
import os
import shutil
import sys
from pathlib import Path

import yaml

# Add parent directory to path to import driver
# sys.path.append(str(Path(__file__).parent.parent))
from driver import TaskManager, config

 # This test suite covers:
 #
 #    Empty input directory scenario
 #    Fewer input files than map tasks
 #    More input files than map tasks
 #    Proper task structure
 #    Even distribution of files
 #    Proper handling of non-text files

N = 6
M = 4

class TestTaskManager(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create temporary input directory for tests
        self.test_input_dir = "test_input_files"
        os.makedirs(self.test_input_dir, exist_ok=True)

        # Store original input directory and temporarily replace it
        self.original_input_dir = config['directories']['input']
        config['directories']['input'] = self.test_input_dir

        # set number of map and reduce tasks
        config['mapreduce']['num_map_tasks'] = N
        config['mapreduce']['num_reduce_tasks'] = M
        self.task_manager = TaskManager(config)

    def tearDown(self):
        """Clean up after each test method."""
        # Remove temporary input directory
        shutil.rmtree(self.test_input_dir, ignore_errors=True)

        # Restore original input directory
        config['directories']['input'] = self.original_input_dir

    def create_test_files(self, num_files):
        """Helper method to create test input files."""
        for i in range(num_files):
            with open(os.path.join(self.test_input_dir, f'test_file_{i}.txt'), 'w') as f:
                f.write(f"Content for file {i}")

    def test_create_tasks_empty_directory(self):
        """Test task creation with no input files."""
        self.task_manager.create_tasks()

        self.assertEqual(len(self.task_manager.map_tasks), 0,
                         "Should create no map tasks when input directory is empty")
        self.assertEqual(len(self.task_manager.reduce_tasks), config['mapreduce']['num_reduce_tasks'],
                         "Should create M reduce tasks regardless of input files")

    def test_create_tasks_fewer_files_than_map_tasks(self):
        """Test when there are fewer input files than map tasks."""
        self.create_test_files(3)  # Create 3 files when N = 6
        self.task_manager.create_tasks()

        self.assertEqual(len(self.task_manager.map_tasks), 3,
                         "Should only create as many map tasks as there are files")

        # Verify file distribution
        files_assigned = sum(len(task['files']) for task in self.task_manager.map_tasks)
        self.assertEqual(files_assigned, 3,
                         "All files should be assigned exactly once")

    def test_create_tasks_more_files_than_map_tasks(self):
        """Test when there are more input files than map tasks."""
        self.create_test_files(10)  # Create 10 files when N = 6
        self.task_manager.create_tasks()

        self.assertEqual(len(self.task_manager.map_tasks), config['mapreduce']['num_map_tasks'],
                         "Should create N map tasks")

        # Verify file distribution
        files_assigned = sum(len(task['files']) for task in self.task_manager.map_tasks)
        self.assertEqual(files_assigned, 10,
                         "All files should be assigned exactly once")


    def test_create_correct_number_of_tasks(self):
        self.create_test_files(10)
        self.task_manager.create_tasks()

        # Check if the correct number of map tasks are created
        self.assertEqual(len(self.task_manager.map_tasks), N)
        # Check if the correct number of reduce tasks are created
        self.assertEqual(len(self.task_manager.reduce_tasks), M)


    def test_task_structure(self):
        """Test the structure of created tasks."""
        self.create_test_files(1)
        self.task_manager.create_tasks()

        # Test map task structure
        map_task = self.task_manager.map_tasks[0]
        self.assertIn('type', map_task)
        self.assertEqual(map_task['type'], 'map')
        self.assertIn('task_id', map_task)
        self.assertIn('files', map_task)

        # Test reduce task structure
        reduce_task = self.task_manager.reduce_tasks[0]
        self.assertIn('type', reduce_task)
        self.assertEqual(reduce_task['type'], 'reduce')
        self.assertIn('task_id', reduce_task)

    def test_get_next_task(self):
        self.create_test_files(10)
        self.task_manager.create_tasks()

        for n_map_task in range(0,N):
            # Get the n_map_task task
            task = self.task_manager.get_next_task()
            self.assertIsNotNone(task)
            self.assertEqual(task['type'], 'map')
            self.assertEqual(task['task_id'], n_map_task)


        # After getting all map tasks, the next task should be None
        # (No map task were completed)
        task = self.task_manager.get_next_task()
        self.assertIsNone(task)

    def test_even_file_distribution(self):
        """Test that files are distributed as evenly as possible."""
        self.create_test_files(11)  # Create 11 files for 6 map tasks
        self.task_manager.create_tasks()

        # Calculate expected distribution
        files_per_task = [len(task['files']) for task in self.task_manager.map_tasks]
        min_files = min(files_per_task)
        max_files = max(files_per_task)

        # The difference between max and min should be at most 1 for even distribution
        self.assertLessEqual(max_files - min_files, 1,
                             "Files should be distributed as evenly as possible")

    def test_non_txt_files_ignored(self):
        """Test that non-txt files are ignored."""
        # Create both .txt and non-.txt files
        self.create_test_files(3)  # Creates 3 .txt files
        with open(os.path.join(self.test_input_dir, 'not_a_text_file.pdf'), 'w') as f:
            f.write("This should be ignored")

        self.task_manager.create_tasks()

        # Count total files assigned
        total_files = sum(len(task['files']) for task in self.task_manager.map_tasks)
        self.assertEqual(total_files, 3,
                         "Only .txt files should be included in tasks")


if __name__ == '__main__':
    unittest.main()
