"""Tests for taskgraph."""
import glob
import os
import tempfile
import shutil
import taskgraph
import unittest
import pickle
import logging

logging.basicConfig(level=logging.DEBUG)

def _create_list_on_disk(value, length, target_path):
    """Create a numpy array on disk filled with value of `size`."""
    target_list = [value] * length
    pickle.dump(target_list, open(target_path, 'wb'))


def _sum_lists_from_disk(list_a_path, list_b_path, target_path):
    """Read two lists, add them and save result."""
    list_a = pickle.load(open(list_a_path, 'rb'))
    list_b = pickle.load(open(list_b_path, 'rb'))
    target_list = []
    for a, b in zip(list_a, list_b):
        target_list.append(a+b)
    pickle.dump(target_list, open(target_path, 'wb'))


def _div_by_zero():
    """Divide by zero to raise an exception."""
    return 1/0


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

    def test_task_chain(self):
        """Test a task chain."""
        task_graph = taskgraph.TaskGraph(self.workspace_dir, 0)
        target_a_path = os.path.join(self.workspace_dir, 'a.dat')
        target_b_path = os.path.join(self.workspace_dir, 'b.dat')
        result_path = os.path.join(self.workspace_dir, 'result.dat')
        result_2_path = os.path.join(self.workspace_dir, 'result2.dat')
        value_a = 5
        value_b = 10
        list_len = 10
        task_a = task_graph.add_task(
            target=_create_list_on_disk,
            args=(value_a, list_len, target_a_path),
            target_path_list=[target_a_path])
        task_b = task_graph.add_task(
            target=_create_list_on_disk,
            args=(value_b, list_len, target_b_path),
            target_path_list=[target_b_path])
        sum_task = task_graph.add_task(
            target=_sum_lists_from_disk,
            args=(target_a_path, target_b_path, result_path),
            target_path_list=[result_path],
            dependent_task_list=[task_a, task_b])
        sum_task.join()

        result = pickle.load(open(result_path, 'rb'))
        self.assertEqual(result, [value_a+value_b]*list_len)

        sum_2_task = task_graph.add_task(
            target=_sum_lists_from_disk,
            args=(target_a_path, result_path, result_2_path),
            target_path_list=[result_2_path],
            dependent_task_list=[task_a, sum_task])
        sum_2_task.join()
        result2 = pickle.load(open(result_2_path, 'rb'))
        expected_result = [(value_a*2+value_b)]*list_len
        self.assertEqual(result2, expected_result)

        sum_3_task = task_graph.add_task(
            target=_sum_lists_from_disk,
            args=(target_a_path, result_path, result_2_path),
            target_path_list=[result_2_path],
            dependent_task_list=[task_a, sum_task])
        sum_3_task.join()
        result3 = pickle.load(open(result_2_path, 'rb'))
        expected_result = [(value_a*2+value_b)]*list_len
        self.assertEqual(result3, expected_result)

    def test_broken_task(self):
        """Test that a task with an exception won't crash multiprocessing."""
        task_graph = taskgraph.TaskGraph(self.workspace_dir, 1)
        _ = task_graph.add_task(target=_div_by_zero)
        with self.assertRaises(RuntimeError):
            task_graph.join()
        file_results = glob.glob(os.path.join(self.workspace_dir, '*'))
        # we should have a file in there that's the token
        self.assertEqual(len(file_results), 0)

    def test_empty_task(self):
        """Test a single task."""
        task_graph = taskgraph.TaskGraph(self.workspace_dir, 0)
        _ = task_graph.add_task()
        task_graph.join()
        file_results = glob.glob(os.path.join(self.workspace_dir, '*'))
        # we should have a file in there that's the token
        self.assertEqual(len(file_results), 1)

    def test_closed_graph(self):
        """Test a single task."""
        task_graph = taskgraph.TaskGraph(self.workspace_dir, 0)
        task_graph.close()
        target_path = os.path.join(self.workspace_dir, '1000.dat')
        value = 5
        list_len = 1000
        with self.assertRaises(ValueError):
            _ = task_graph.add_task(
                target=_create_list_on_disk,
                args=(value, list_len, target_path),
                target_path_list=[target_path])

    def test_single_task_multiprocessing(self):
        """Test a single task with multiprocessing."""
        task_graph = taskgraph.TaskGraph(self.workspace_dir, 1)
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

    def test_get_file_stats(self):
        """Test _get_file_stats subroutine."""
        from taskgraph.Task import _get_file_stats
        test_dir = os.path.join(self.workspace_dir, 'test_dir')
        test_file = os.path.join(test_dir, 'test_file.txt')
        os.mkdir(test_dir)
        with open(test_file, 'w') as f:
            f.write('\n')
        base_value = [
            'foo', test_dir, test_file, 10, {'a': {'b': test_file}},
            {'a': {'b': test_dir, 'foo': 9}}]
        ignore_dir_result = list(_get_file_stats(base_value, [], True))
        # should get two results if we ignore the directories because there's
        # only two files
        self.assertEqual(len(ignore_dir_result), 2)
        dir_result = list(_get_file_stats(base_value, [], False))
        # should get four results if we track directories because of two files
        # and two directories
        self.assertEqual(len(dir_result), 4)
