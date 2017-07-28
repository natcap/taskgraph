"""Tests for taskgraph."""
import os
import tempfile
import shutil
import taskgraph
import unittest
import pickle


def _create_list_on_disk(value, length, target_path):
    """Create a numpy array on disk filled with value of `size`."""
    target_list = [value] * length
    pickle.dump(target_list, open(target_path, 'wb'))


class TaskGraphTests(unittest.TestCase):
    """Tests for the taskgraph."""

    def setUp(self):
        """Overriding setUp function to create temp workspace directory."""
        # this lets us delete the workspace after its done no matter the
        # the rest result
        self.workspace_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Overriding tearDown function to remove temporary directory."""
        shutil.rmtree(self.workspace_dir)

    def test_single_task(self):
        """Test a single task."""
        task_graph = taskgraph.TaskGraph(self.workspace_dir, 0)
        target_path = os.path.join(self.workspace_dir, '1000.dat')
        value = 5
        list_len = 1000
        _ = task_graph.add_task(
            target=_create_list_on_disk,
            args=(value, list_len, target_path),
            target_path_list=[target_path])
        task_graph.join()
        result = pickle.load(open(target_path, 'rb'))
        self.assertEqual(result, [value]*list_len)
