"""Tests for taskgraph."""
import glob
import os
import tempfile
import shutil
import time
import unittest
import pickle
import logging

import mock

import taskgraph

logging.basicConfig(level=logging.DEBUG)
LOGGER = logging.getLogger(__file__)


# Python 3 relocated the reload function to imp.
if 'reload' not in __builtins__:
    from imp import reload


def _long_running_function():
    """Wait for 5 seconds."""
    time.sleep(5)


def _create_list_on_disk(value, length, target_path):
    """Create a numpy array on disk filled with value of `size`."""
    target_list = [value] * length
    pickle.dump(target_list, open(target_path, 'ab'))


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

    def test_version_loaded(self):
        """TaskGraph: verify we can load the version."""
        try:
            import taskgraph
            # Verifies that there's a version attribute and it has a value.
            self.assertTrue(len(taskgraph.__version__) > 0)
        except Exception as error:
            self.fail('Could not load the taskgraph version as expected.')

    def test_version_not_loaded(self):
        """TaskGraph: verify exception when not installed."""
        from pkg_resources import DistributionNotFound
        import taskgraph

        with mock.patch('taskgraph.pkg_resources.get_distribution',
                        side_effect=DistributionNotFound('taskgraph')):
            with self.assertRaises(RuntimeError):
                # RuntimeError is a side effect of `import taskgraph`, so we
                # reload it to retrigger the metadata load.
                taskgraph = reload(taskgraph)

    def test_single_task(self):
        """TaskGraph: Test a single task."""
        task_graph = taskgraph.TaskGraph(self.workspace_dir, 0)
        target_path = os.path.join(self.workspace_dir, '1000.dat')
        value = 5
        list_len = 1000
        _ = task_graph.add_task(
            func=_create_list_on_disk,
            args=(value, list_len, target_path),
            target_path_list=[target_path])
        task_graph.close()
        task_graph.join()
        result = pickle.load(open(target_path, 'rb'))
        self.assertEqual(result, [value]*list_len)

    def test_timeout_task(self):
        """TaskGraph: Test timeout funcitonality"""
        task_graph = taskgraph.TaskGraph(self.workspace_dir, 0)
        target_path = os.path.join(self.workspace_dir, '1000.dat')
        _ = task_graph.add_task(
            func=_long_running_function,)
        task_graph.close()
        timedout = not task_graph.join(0.5)
        # this should timeout since function runs for 5 seconds
        self.assertTrue(timedout)

    def test_precomputed_task(self):
        """TaskGraph: Test that a task reuses old results."""
        task_graph = taskgraph.TaskGraph(self.workspace_dir, 0)
        target_path = os.path.join(self.workspace_dir, '1000.dat')
        value = 5
        list_len = 1000
        _ = task_graph.add_task(
            func=_create_list_on_disk,
            args=(value, list_len, target_path),
            target_path_list=[target_path])
        task_graph.close()
        task_graph.join()
        result = pickle.load(open(target_path, 'rb'))
        self.assertEqual(result, [value]*list_len)
        result_m_time = os.path.getmtime(target_path)

        del task_graph
        task_graph = taskgraph.TaskGraph(self.workspace_dir, 0)
        _ = task_graph.add_task(
            func=_create_list_on_disk,
            args=(value, list_len, target_path),
            target_path_list=[target_path])
        task_graph.close()
        task_graph.join()

        # taskgraph shouldn't have recomputed the result
        second_result_m_time = os.path.getmtime(target_path)
        self.assertEqual(result_m_time, second_result_m_time)

    def test_task_chain(self):
        """TaskGraph: Test a task chain."""
        task_graph = taskgraph.TaskGraph(self.workspace_dir, 0)
        target_a_path = os.path.join(self.workspace_dir, 'a.dat')
        target_b_path = os.path.join(self.workspace_dir, 'b.dat')
        result_path = os.path.join(self.workspace_dir, 'result.dat')
        result_2_path = os.path.join(self.workspace_dir, 'result2.dat')
        value_a = 5
        value_b = 10
        list_len = 10
        task_a = task_graph.add_task(
            func=_create_list_on_disk,
            args=(value_a, list_len, target_a_path),
            target_path_list=[target_a_path])
        task_b = task_graph.add_task(
            func=_create_list_on_disk,
            args=(value_b, list_len, target_b_path),
            target_path_list=[target_b_path])
        sum_task = task_graph.add_task(
            func=_sum_lists_from_disk,
            args=(target_a_path, target_b_path, result_path),
            target_path_list=[result_path],
            dependent_task_list=[task_a, task_b])
        sum_task.join()

        result = pickle.load(open(result_path, 'rb'))
        self.assertEqual(result, [value_a+value_b]*list_len)

        sum_2_task = task_graph.add_task(
            func=_sum_lists_from_disk,
            args=(target_a_path, result_path, result_2_path),
            target_path_list=[result_2_path],
            dependent_task_list=[task_a, sum_task])
        sum_2_task.join()
        result2 = pickle.load(open(result_2_path, 'rb'))
        expected_result = [(value_a*2+value_b)]*list_len
        self.assertEqual(result2, expected_result)

        sum_3_task = task_graph.add_task(
            func=_sum_lists_from_disk,
            args=(target_a_path, result_path, result_2_path),
            target_path_list=[result_2_path],
            dependent_task_list=[task_a, sum_task])
        task_graph.close()
        sum_3_task.join()
        result3 = pickle.load(open(result_2_path, 'rb'))
        expected_result = [(value_a*2+value_b)]*list_len
        self.assertEqual(result3, expected_result)
        task_graph.join()


    def test_task_chain_single_thread(self):
        """TaskGraph: Test a single threaded task chain."""
        task_graph = taskgraph.TaskGraph(self.workspace_dir, -1)
        target_a_path = os.path.join(self.workspace_dir, 'a.dat')
        target_b_path = os.path.join(self.workspace_dir, 'b.dat')
        result_path = os.path.join(self.workspace_dir, 'result.dat')
        result_2_path = os.path.join(self.workspace_dir, 'result2.dat')
        value_a = 5
        value_b = 10
        list_len = 10
        task_a = task_graph.add_task(
            func=_create_list_on_disk,
            args=(value_a, list_len, target_a_path),
            target_path_list=[target_a_path])
        task_b = task_graph.add_task(
            func=_create_list_on_disk,
            args=(value_b, list_len, target_b_path),
            target_path_list=[target_b_path])
        sum_task = task_graph.add_task(
            func=_sum_lists_from_disk,
            args=(target_a_path, target_b_path, result_path),
            target_path_list=[result_path],
            dependent_task_list=[task_a, task_b])
        sum_task.join()

        result = pickle.load(open(result_path, 'rb'))
        self.assertEqual(result, [value_a+value_b]*list_len)

        sum_2_task = task_graph.add_task(
            func=_sum_lists_from_disk,
            args=(target_a_path, result_path, result_2_path),
            target_path_list=[result_2_path],
            dependent_task_list=[task_a, sum_task])
        sum_2_task.join()
        result2 = pickle.load(open(result_2_path, 'rb'))
        expected_result = [(value_a*2+value_b)]*list_len
        self.assertEqual(result2, expected_result)

        sum_3_task = task_graph.add_task(
            func=_sum_lists_from_disk,
            args=(target_a_path, result_path, result_2_path),
            target_path_list=[result_2_path],
            dependent_task_list=[task_a, sum_task])
        task_graph.close()
        sum_3_task.join()
        result3 = pickle.load(open(result_2_path, 'rb'))
        expected_result = [(value_a*2+value_b)]*list_len
        self.assertEqual(result3, expected_result)
        task_graph.join()

    def test_broken_task(self):
        """TaskGraph: Test that a task with an exception won't hang."""
        task_graph = taskgraph.TaskGraph(self.workspace_dir, 1)
        _ = task_graph.add_task(
            func=_div_by_zero, task_name='test_broken_task')
        task_graph.close()
        with self.assertRaises(RuntimeError):
            task_graph.join()
        file_results = glob.glob(os.path.join(self.workspace_dir, '*'))
        # we shouldn't have a file in there that's the token
        self.assertEqual(len(file_results), 0)

    def test_broken_task_chain(self):
        """TaskGraph: test dependent tasks fail on ancestor fail."""
        task_graph = taskgraph.TaskGraph(self.workspace_dir, 1)
        base_task = task_graph.add_task(
            func=_div_by_zero, task_name='test_broken_task_chain')

        target_path = os.path.join(self.workspace_dir, '1000.dat')
        value = 5
        list_len = 1000
        dependent_task = task_graph.add_task(
            func=_create_list_on_disk,
            args=(value, list_len, target_path),
            target_path_list=[target_path],
            dependent_task_list=[base_task])
        task_graph.close()
        with self.assertRaises(RuntimeError):
            task_graph.join()
        file_results = glob.glob(os.path.join(self.workspace_dir, '*'))
        # we shouldn't have any files in there
        self.assertEqual(len(file_results), 0)

    def test_empty_task(self):
        """TaskGraph: Test an empty task."""
        task_graph = taskgraph.TaskGraph(self.workspace_dir, 0)
        _ = task_graph.add_task()
        task_graph.close()
        task_graph.join()
        file_results = glob.glob(os.path.join(self.workspace_dir, '*'))
        # we should have a file in there that's the token
        self.assertEqual(len(file_results), 1)

    def test_closed_graph(self):
        """TaskGraph: Test adding to an closed task graph fails."""
        task_graph = taskgraph.TaskGraph(self.workspace_dir, 0)
        task_graph.close()
        target_path = os.path.join(self.workspace_dir, '1000.dat')
        value = 5
        list_len = 1000
        with self.assertRaises(ValueError):
            _ = task_graph.add_task(
                func=_create_list_on_disk,
                args=(value, list_len, target_path),
                target_path_list=[target_path])
        task_graph.join()

    def test_single_task_multiprocessing(self):
        """TaskGraph: Test a single task with multiprocessing."""
        task_graph = taskgraph.TaskGraph(self.workspace_dir, 1)
        target_path = os.path.join(self.workspace_dir, '1000.dat')
        value = 5
        list_len = 1000
        t = task_graph.add_task(
            func=_create_list_on_disk,
            args=(value, list_len, target_path),
            target_path_list=[target_path])
        task_graph.close()
        task_graph.join()
        result = pickle.load(open(target_path, 'rb'))
        self.assertEqual(result, [value]*list_len)

    def test_get_file_stats(self):
        """TaskGraph: Test _get_file_stats subroutine."""
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

        result = list(_get_file_stats(u'foo', [], False))
        self.assertEqual(result, [])

    def test_encapsulatedtaskop(self):
        """TaskGraph: Test abstract closure task class."""
        from taskgraph.Task import EncapsulatedTaskOp

        class TestAbstract(EncapsulatedTaskOp):
            def __init__(self):
                pass

        # __call__ is abstract so TypeError since it's not implemented
        with self.assertRaises(TypeError):
            x = TestAbstract()

        class TestA(EncapsulatedTaskOp):
            def __call__(self, x):
                return x

        class TestB(EncapsulatedTaskOp):
            def __call__(self, x):
                return x

        # TestA and TestB should be different because of different class names
        a = TestA()
        b = TestB()
        # results of calls should be the same
        self.assertEqual(a.__call__(5), b.__call__(5))
        self.assertNotEqual(a.__name__, b.__name__)

        # two instances with same args should be the same
        self.assertEqual(TestA().__name__, TestA().__name__)

        # redefine TestA so we get a different hashed __name__
        class TestA(EncapsulatedTaskOp):
            def __call__(self, x):
                return x*x

        new_a = TestA()
        self.assertNotEqual(a.__name__, new_a.__name__)

        # change internal class constructor to get different hashes
        class TestA(EncapsulatedTaskOp):
            def __init__(self, q):
                super(TestA, self).__init__(q)
                self.q = q

            def __call__(self, x):
                return x*x

        init_new_a = TestA(1)
        self.assertNotEqual(new_a.__name__, init_new_a.__name__)

    def test_repeat_targetless_runs(self):
        """TaskGraph: ensure that repeated runs with no targets reexecute."""
        task_graph = taskgraph.TaskGraph(self.workspace_dir, -1)
        target_path = os.path.join(self.workspace_dir, '1000.dat')
        value = 5
        list_len = 1000
        _ = task_graph.add_task(
            func=_create_list_on_disk,
            args=(value, list_len, target_path))
        task_graph.close()
        task_graph.join()
        task_graph = None

        os.remove(target_path)

        task_graph2 = taskgraph.TaskGraph(self.workspace_dir, -1)
        _ = task_graph2.add_task(
            func=_create_list_on_disk,
            args=(value, list_len, target_path))
        task_graph2.close()
        task_graph2.join()

        self.assertTrue(
            os.path.exists(target_path),
            "Expected file to exist because taskgraph should have re-run.")
