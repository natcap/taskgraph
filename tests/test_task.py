"""Tests for taskgraph."""
import sqlite3
import os
import tempfile
import shutil
import time
import unittest
import pickle
import logging
import logging.handlers
import multiprocessing
import mock

import taskgraph

# Python 3 relocated the reload function to imp.
if 'reload' not in __builtins__:
    from imp import reload


def _long_running_function(delay):
    """Wait for `delay` seconds."""
    time.sleep(delay)


def _create_two_files_on_disk(value, target_a_path, target_b_path):
    """Create two files and write `value` and append if possible."""
    with open(target_a_path, 'a') as a_file:
        a_file.write(value)

    with open(target_b_path, 'a') as b_file:
        b_file.write(value)


def _merge_and_append_files(base_a_path, base_b_path, target_path):
    """Merge two files and append if possible to new file."""
    with open(target_path, 'a') as target_file:
        for base_path in [base_a_path, base_b_path]:
            with open(base_path, 'r') as base_file:
                target_file.write(base_file.read())


def _create_list_on_disk(value, length, target_path=None):
    """Create a numpy array on disk filled with value of `size`."""
    target_list = [value] * length
    pickle.dump(target_list, open(target_path, 'wb'))


def _call_it(target, *args):
    """Invoke `target` with `args`."""
    target(*args)


def _append_val(path, *val):
    """Append a `val` to file at `path`."""
    with open(path, 'a') as target_file:
        for v in val:
            target_file.write(str(v))


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


def _log_from_another_process(logger_name, log_message):
    """Write a log message to a given logger.

    Parameters:
        logger_name (string): The string logger name to which ``log_message``
            will be logged.
        log_message (string): The string log message to be logged (at INFO
            level) to the logger at ``logger_name``.

    Returns:
        ``None``

    """
    logger = logging.getLogger(logger_name)
    logger.info(log_message)


class TaskGraphTests(unittest.TestCase):
    """Tests for the taskgraph."""

    def setUp(self):
        """Create temp workspace directory."""
        # this lets us delete the workspace after its done no matter the
        # the rest result
        self.workspace_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Remove temporary directory."""
        shutil.rmtree(self.workspace_dir)

    def test_version_loaded(self):
        """TaskGraph: verify we can load the version."""
        try:
            import taskgraph
            # Verifies that there's a version attribute and it has a value.
            self.assertTrue(len(taskgraph.__version__) > 0)
        except Exception:
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
        task_graph = taskgraph.TaskGraph(self.workspace_dir, 0, 0.1)
        # forcing this one to be unicode since there shouldn't be a problem
        # with that at all...
        target_path = u'%s' % os.path.join(self.workspace_dir, '1000.dat')
        value = 5
        list_len = 1000
        _ = task_graph.add_task(
            func=_create_list_on_disk,
            args=(value, list_len),
            kwargs={
                'target_path': target_path,
            },
            target_path_list=[target_path])
        task_graph.close()
        task_graph.join()
        result = pickle.load(open(target_path, 'rb'))
        self.assertEqual(result, [value]*list_len)

    def test_timeout_task(self):
        """TaskGraph: Test timeout functionality."""
        task_graph = taskgraph.TaskGraph(self.workspace_dir, 0)
        _ = task_graph.add_task(
            func=_long_running_function,
            args=(5,))
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
            args=(value, list_len),
            kwargs={
                'target_path': target_path,
            },
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
            args=(value, list_len),
            kwargs={
                'target_path': target_path,
            },
            target_path_list=[target_path])
        task_graph.close()
        task_graph.join()
        del task_graph

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
            args=(value_a, list_len),
            kwargs={
                'target_path': target_a_path,
            },
            target_path_list=[target_a_path])
        task_b = task_graph.add_task(
            func=_create_list_on_disk,
            args=(value_b, list_len),
            kwargs={
                'target_path': target_b_path,
            },
            target_path_list=[target_b_path])
        sum_task = task_graph.add_task(
            func=_sum_lists_from_disk,
            args=(target_a_path, target_b_path),
            kwargs={
                'target_path': result_path,
            },
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
            args=(value_a, list_len),
            kwargs={
                'target_path': target_a_path,
            },
            target_path_list=[target_a_path])
        task_b = task_graph.add_task(
            func=_create_list_on_disk,
            args=(value_b, list_len),
            kwargs={
                'target_path': target_b_path,
            },
            target_path_list=[target_b_path])
        sum_task = task_graph.add_task(
            func=_sum_lists_from_disk,
            args=(target_a_path, target_b_path),
            kwargs={
                'target_path': result_path,
            },
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

        # we should have 4 completed values in the database, 5 total but one
        # was a duplicate
        database_path = os.path.join(
            self.workspace_dir, taskgraph._TASKGRAPH_DATABASE_FILENAME)
        with sqlite3.connect(database_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM taskgraph_data")
            result = cursor.fetchall()
        self.assertEqual(len(result), 4)

    def test_broken_task(self):
        """TaskGraph: Test that a task with an exception won't hang."""
        task_graph = taskgraph.TaskGraph(self.workspace_dir, 1)
        _ = task_graph.add_task(
            func=_div_by_zero, task_name='test_broken_task')
        task_graph.close()
        with self.assertRaises(ZeroDivisionError):
            task_graph.join()

    def test_n_retries(self):
        """TaskGraph: Test a task will attempt to retry after exception."""
        task_graph = taskgraph.TaskGraph(self.workspace_dir, 0)
        result_file_path = os.path.join(self.workspace_dir, 'result.txt')

        fail_task = task_graph.add_task(
            func=Fail(5, result_file_path),
            task_name='fail 5 times', n_retries=5)
        fail_task.join()
        task_graph.close()
        self.assertTrue(os.path.exists(result_file_path))

    def test_broken_task_chain(self):
        """TaskGraph: test dependent tasks fail on ancestor fail."""
        task_graph = taskgraph.TaskGraph(self.workspace_dir, 4)

        target_path = os.path.join(self.workspace_dir, '1000.dat')
        value = 5
        list_len = 1000
        for task_id in range(1):
            target_path = os.path.join(
                self.workspace_dir, '1000_%d.dat' % task_id)
            normal_task = task_graph.add_task(
                func=_create_list_on_disk,
                args=(value, list_len),
                kwargs={'target_path': target_path},
                target_path_list=[target_path],
                task_name='create list on disk %d' % task_id)
            zero_div_task = task_graph.add_task(
                func=_div_by_zero,
                dependent_task_list=[normal_task],
                task_name='test_broken_task_chain_%d' % task_id)
            target_path = os.path.join(
                self.workspace_dir, 'after_zerodiv_1000_%d.dat' % task_id)
            _ = task_graph.add_task(
                func=_create_list_on_disk,
                args=(value, list_len),
                kwargs={'target_path': target_path},
                dependent_task_list=[zero_div_task],
                target_path_list=[target_path],
                task_name='create list on disk after zero div%d' % task_id)

        task_graph.close()
        with self.assertRaises(ZeroDivisionError):
            task_graph.join()

    def test_empty_task(self):
        """TaskGraph: Test an empty task."""
        task_graph = taskgraph.TaskGraph(self.workspace_dir, 0)
        _ = task_graph.add_task()
        task_graph.close()
        task_graph.join()
        # we shouldn't have anything in the database
        database_path = os.path.join(
            self.workspace_dir, taskgraph._TASKGRAPH_DATABASE_FILENAME)
        with sqlite3.connect(database_path) as conn:
            cursor = conn.cursor()
            cursor.executescript("SELECT * FROM taskgraph_data")
            result = cursor.fetchall()
        self.assertEqual(len(result), 0)

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
                args=(value, list_len),
                kwargs={'target_path': target_path},
                target_path_list=[target_path])
        task_graph.join()

    def test_single_task_multiprocessing(self):
        """TaskGraph: Test a single task with multiprocessing."""
        task_graph = taskgraph.TaskGraph(self.workspace_dir, 1)
        target_path = os.path.join(self.workspace_dir, '1000.dat')
        value = 5
        list_len = 1000
        _ = task_graph.add_task(
            func=_create_list_on_disk,
            args=(value, list_len),
            kwargs={
                'target_path': target_path,
            },
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
            _ = TestAbstract()

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
            args=(value, list_len),
            kwargs={
                'target_path': target_path,
            })
        task_graph.close()
        task_graph.join()
        task_graph = None

        os.remove(target_path)

        task_graph2 = taskgraph.TaskGraph(self.workspace_dir, -1)
        _ = task_graph2.add_task(
            func=_create_list_on_disk,
            args=(value, list_len),
            kwargs={
                'target_path': target_path,
            })

        task_graph2.close()
        task_graph2.join()

        self.assertTrue(
            os.path.exists(target_path),
            "Expected file to exist because taskgraph should have re-run.")

    def test_repeat_targeted_runs(self):
        """TaskGraph: ensure that repeated runs with targets can join."""
        task_graph = taskgraph.TaskGraph(self.workspace_dir, -1)
        target_path = os.path.join(self.workspace_dir, '1000.dat')
        value = 5
        list_len = 1000
        _ = task_graph.add_task(
            func=_create_list_on_disk,
            args=(value, list_len),
            kwargs={
                'target_path': target_path,
            },
            target_path_list=[target_path])
        task_graph.close()
        task_graph.join()
        task_graph = None

        task_graph2 = taskgraph.TaskGraph(self.workspace_dir, -1)
        task = task_graph2.add_task(
            func=_create_list_on_disk,
            args=(value, list_len),
            kwargs={
                'target_path': target_path,
            },
            target_path_list=[target_path])
        self.assertTrue(task.join(1.0), "join failed after 1 second")
        task_graph2.close()
        task_graph2.join()

    def test_task_equality(self):
        """TaskGraph: test correctness of == and != for Tasks."""
        task_graph = taskgraph.TaskGraph(self.workspace_dir, -1)
        target_path = os.path.join(self.workspace_dir, '1000.dat')
        value = 5
        list_len = 1000
        task_a = task_graph.add_task(
            func=_create_list_on_disk,
            args=(value, list_len),
            kwargs={'target_path': target_path},
            target_path_list=[target_path])
        task_a_same = task_graph.add_task(
            func=_create_list_on_disk,
            args=(value, list_len),
            kwargs={'target_path': target_path},
            target_path_list=[target_path])
        task_b = task_graph.add_task(
            func=_create_list_on_disk,
            args=(value+1, list_len),
            kwargs={'target_path': target_path},
            target_path_list=[target_path])

        self.assertTrue(task_a == task_a)
        self.assertTrue(task_a == task_a_same)
        self.assertTrue(task_a != task_b)

    def test_async_logging(self):
        """TaskGraph: ensure async logging can execute."""
        task_graph = taskgraph.TaskGraph(
            self.workspace_dir, 0, reporting_interval=0.5)
        _ = task_graph.add_task(
            func=_long_running_function,
            args=(1.0,))
        task_graph.close()
        task_graph.join()
        timedout = not task_graph.join(5)
        # this should not timeout since function runs for 1 second
        self.assertFalse(timedout, "task timed out")

    def test_scrub(self):
        """TaskGraph: ensure scrub is not scrubbing base types."""
        task_graph = taskgraph.TaskGraph(self.workspace_dir, 0)

        target_path = os.path.join(self.workspace_dir, 'a.txt')
        first_task = task_graph.add_task(
            func=_append_val,
            args=(target_path, 1, [1], {'x': 1}),
            task_name='first append')

        second_task = task_graph.add_task(
            func=_append_val,
            args=(target_path, 1, [1], {'x': 2}),
            dependent_task_list=[first_task],
            task_name='second append')

        _ = task_graph.add_task(
            func=_append_val,
            args=(target_path, 1, [2], {'x': 1}),
            dependent_task_list=[second_task],
            task_name='third append')

        task_graph.close()
        task_graph.join()

        with open(target_path, 'r') as target_file:
            file_value = target_file.read()
        self.assertEqual("1[1]{'x': 1}1[1]{'x': 2}1[2]{'x': 1}", file_value)

    def test_target_path_order(self):
        """TaskGraph: ensure target path order doesn't matter."""
        task_graph = taskgraph.TaskGraph(self.workspace_dir, 0)
        target_a_path = os.path.join(self.workspace_dir, 'a.txt')
        target_b_path = os.path.join(self.workspace_dir, 'b.txt')

        task_graph.add_task(
            func=_create_two_files_on_disk,
            args=("word", target_a_path, target_b_path),
            target_path_list=[target_a_path, target_b_path])

        task_graph.add_task(
            func=_create_two_files_on_disk,
            args=("word", target_a_path, target_b_path),
            target_path_list=[target_b_path, target_a_path])

        task_graph.close()
        task_graph.join()

        with open(target_a_path, 'r') as a_file:
            a_value = a_file.read()

        with open(target_b_path, 'r') as b_file:
            b_value = b_file.read()

        self.assertEqual(a_value, "word")
        self.assertEqual(b_value, "word")

    def test_task_hash_when_ready(self):
        """TaskGraph: ensure tasks don't record execution info until ready."""
        task_graph = taskgraph.TaskGraph(self.workspace_dir, 0)
        target_a_path = os.path.join(self.workspace_dir, 'a.txt')
        target_b_path = os.path.join(self.workspace_dir, 'b.txt')

        create_files_task = task_graph.add_task(
            func=_create_two_files_on_disk,
            args=("word", target_a_path, target_b_path),
            target_path_list=[target_a_path, target_b_path])

        target_merged_path = os.path.join(self.workspace_dir, 'merged.txt')
        task_graph.add_task(
            func=_merge_and_append_files,
            args=(target_a_path, target_b_path, target_merged_path),
            target_path_list=[target_merged_path],
            dependent_task_list=[create_files_task])

        task_graph.join()

        # this second task shouldn't execute because it's a copy of the first
        task_graph.add_task(
            func=_merge_and_append_files,
            args=(target_a_path, target_b_path, target_merged_path),
            target_path_list=[target_merged_path],
            dependent_task_list=[create_files_task])

        task_graph.close()
        task_graph.join()

        with open(target_merged_path, 'r') as target_file:
            target_string = target_file.read()

        self.assertEqual(target_string, "wordword")

    def test_multiprocessed_logging(self):
        """TaskGraph: ensure tasks can log from multiple processes."""
        logger_name = 'foo.hello.world'
        log_message = 'This is coming from another process'
        logger = logging.getLogger(logger_name)
        handler = logging.handlers.MemoryHandler(capacity=100)
        logger.addHandler(handler)

        task_graph = taskgraph.TaskGraph(self.workspace_dir, 1)
        task_graph.add_task(_log_from_another_process,
                            args=(logger_name,
                                  log_message))
        task_graph.close()
        task_graph.join()
        del task_graph

        # There should be exactly one record in the queue, and it should be
        # from a different process, but have the message we defined above.
        self.assertEqual(len(handler.buffer), 1)
        self.assertTrue(isinstance(handler.buffer[0], logging.LogRecord))
        self.assertEqual(log_message, handler.buffer[0].message)
        self.assertNotEqual(handler.buffer[0].processName,
                            multiprocessing.current_process())

    def test_repeated_function(self):
        """TaskGraph: ensure no reruns if argument is a function."""
        global _append_val

        task_graph = taskgraph.TaskGraph(self.workspace_dir, 0)
        target_path = os.path.join(self.workspace_dir, 'testfile.txt')
        task_graph.add_task(
            func=_call_it,
            args=(_append_val, target_path, 1),
            target_path_list=[target_path],
            ignore_path_list=[target_path],
            task_name='first _call_it')
        task_graph.close()
        task_graph.join()
        del task_graph

        # this causes the address to change
        def _append_val(path, *val):
            """Append a `val` to file at `path`."""
            with open(path, 'a') as target_file:
                for v in val:
                    target_file.write(str(v))

        task_graph = taskgraph.TaskGraph(self.workspace_dir, 1)
        target_path = os.path.join(self.workspace_dir, 'testfile.txt')
        task_graph.add_task(
            func=_call_it,
            args=(_append_val, target_path, 1),
            target_path_list=[target_path],
            ignore_path_list=[target_path],
            task_name='second _call_it')
        task_graph.close()
        task_graph.join()

        with open(target_path, 'r') as target_file:
            result = target_file.read()

        # the second call shouldn't happen
        self.assertEqual(result, '1')

    def test_unix_path_repeated_function(self):
        """TaskGraph: ensure no reruns if path is unix style."""
        global _append_val

        task_graph = taskgraph.TaskGraph(self.workspace_dir, -1)
        target_dir = self.workspace_dir + '/foo/bar/rad/'
        os.makedirs(target_dir)
        target_path = target_dir + '/testfile.txt'
        task_graph.add_task(
            func=_call_it,
            args=(_append_val, target_path, 1),
            target_path_list=[target_path],
            task_name='first _call_it')
        task_graph.close()
        task_graph.join()
        del task_graph

        task_graph = taskgraph.TaskGraph(self.workspace_dir, -1)
        task_graph.add_task(
            func=_call_it,
            args=(_append_val, target_path, 1),
            target_path_list=[target_path],
            task_name='second _call_it')
        task_graph.close()
        task_graph.join()

        with open(target_path, 'r') as target_file:
            result = target_file.read()

        # the second call shouldn't happen
        self.assertEqual(result, '1')

    def test_very_long_string(self):
        """TaskGraph: ensure that long strings don't case an OSError."""
        from taskgraph.Task import _get_file_stats
        # this is a list with two super long strings to try to trick some
        # os function into thinking it's a path.
        base_value = [
            'c:' + r'\\\\\\\\x\\\\\\\\'*2**10 + 'foo',
            'wfeji3223j8923j9' * 2**10]
        self.assertEqual(
            list(_get_file_stats(base_value, [], True)), [])


def Fail(n_tries, result_path):
    """Create a function that fails after `n_tries`."""
    def fail_func():
        fail_func._n_tries -= 1
        if fail_func._n_tries > 0:
            raise ValueError("Fail %d more times", fail_func._n_tries)
        with open(result_path, 'w') as result_file:
            result_file.write("finished!")
    fail_func._n_tries = n_tries

    return fail_func
