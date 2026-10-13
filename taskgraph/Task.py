"""Task graph framework."""
import collections
import hashlib
import inspect
import logging
import logging.handlers
import multiprocessing
import multiprocessing.pool
import os
import pathlib
import pickle
import pprint
import queue
import sqlite3
import threading
import time
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version

import retrying

import parsl
import os
from parsl.app.app import python_app
from parsl.concurrent import ParslPoolExecutor
from parsl.configs.local_threads import config
from parsl.utils import get_all_checkpoints
from parsl.dataflow.memoization import BasicMemoizer, id_for_memo
from parsl.dataflow.futures import AppFuture
from parsl.executors import HighThroughputExecutor
from parsl.monitoring.monitoring import MonitoringHub
from parsl.addresses import address_by_hostname


LOGGER = logging.getLogger(__name__)


# @id_for_memo.register(object)
# def id_for_memo_pickle(obj, output_ref=False):
#     print('hashing', obj)
#     return pickle.dumps(obj)


try:
    __version__ = version('taskgraph')
except PackageNotFoundError:
    # package is not installed; no metadata available
    pass


_VALID_PATH_TYPES = (str, pathlib.PurePath)
_TASKGRAPH_DATABASE_FILENAME = 'taskgraph_data.db'

try:
    import psutil
    HAS_PSUTIL = True
    if psutil.WINDOWS:
        # Windows' scheduler doesn't use POSIX niceness.
        PROCESS_LOW_PRIORITY = psutil.BELOW_NORMAL_PRIORITY_CLASS
    else:
        # On POSIX, use system niceness.
        # -20 is high priority, 0 is normal priority, 19 is low priority.
        # 10 here is an arbitrary selection that's probably nice enough.
        PROCESS_LOW_PRIORITY = 10
except ImportError:
    HAS_PSUTIL = False

LOGGER = logging.getLogger(__name__)
_MAX_TIMEOUT = 5.0  # amount of time to wait for threads to terminate


@python_app(cache=True)
def wrap_func(func, *args, inputs=[], **kwargs):
    func(*args, **kwargs)


# We want our processing pool to be nondeamonic so that workers could use
# multiprocessing if desired (deamonic processes cannot start new processes)
# the following bit of code to do this was taken from
# https://stackoverflow.com/a/8963618/42897
class NoDaemonProcess(multiprocessing.Process):
    """Make 'daemon' attribute always return False."""

    @property
    def daemon(self):
        """Return False indicating not a daemon process."""
        return False

    @daemon.setter
    def daemon(self, value):
        """Do not allow daemon value to be overriden."""
        pass


class NoDaemonContext(type(multiprocessing.get_context('spawn'))):
    """From https://stackoverflow.com/a/8963618/42897.

    "As the current implementation of multiprocessing [3.7+] has been
    extensively refactored to be based on contexts, we need to provide a
    NoDaemonContext class that has our NoDaemonProcess as attribute.
    [NonDaemonicPool] will then use that context instead of the default
    one." "spawn" is chosen as default since that is the default and only
    context option for Windows and is the default option for Mac OS as
    well since 3.8.

    """

    Process = NoDaemonProcess


class NonDaemonicPool(multiprocessing.pool.Pool):
    """NonDaemonic Process Pool."""

    def __init__(self, *args, **kwargs):
        """Invoke super to set the context of Pool class explicitly."""
        kwargs['context'] = NoDaemonContext()
        super(NonDaemonicPool, self).__init__(*args, **kwargs)


def _null_func():
    """Use when func=None on add_task."""
    return None


def _initialize_logging_to_queue(logging_queue):
    """Add a synchronized queue to a new process.

    This is intended to be called as an initialization function to
    ``multiprocessing.Pool`` to establish logging from a Pool worker to the
    main python process via a multiprocessing Queue.

    Args:
        logging_queue (multiprocessing.Queue): The queue to use for passing
            log records back to the main process.

    Returns:
        None

    """
    root_logger = logging.getLogger()

    # By the time this function is called, `root_logger` has a copy of all of
    # the logging handlers registered to it within the parent process, which
    # leads to duplicate logging in some cases.  By removing all of the
    # handlers here, we ensure that log messages can only be passed back to the
    # parent process by the `logging_queue`, where they will be handled.
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    root_logger.setLevel(logging.NOTSET)
    handler = logging.handlers.QueueHandler(logging_queue)
    root_logger.addHandler(handler)


def _logging_queue_monitor(logging_queue):
    """Monitor ``logging_queue`` for message and pass to ``logger``."""
    LOGGER.debug('Starting logging worker')
    while True:
        record = logging_queue.get()
        if record is None:
            break
        logger = logging.getLogger(record.name)
        logger.handle(record)
    LOGGER.debug('_logging_queue_monitor shutting down')



class TaskGraph(object):
    """Encapsulates the worker and tasks states for parallel processing."""

    def __init__(
            self, taskgraph_cache_dir_path, n_workers,
            reporting_interval=None):
        """Create a task graph.

        Creates an object for building task graphs, executing them,
        parallelizing independent work notes, and avoiding repeated calls.

        Args:
            taskgraph_cache_dir_path (string): path to a directory that
                either contains a taskgraph cache from a previous instance or
                will create a new one if none exists.
            n_workers (int): number of parallel *subprocess* workers to allow
                during task graph execution.  If set to 0, don't use
                subprocesses.  If set to <0, use only the main thread for any
                execution and scheduling. In the case of the latter,
                ``add_task`` will be a blocking call.
            reporting_interval (scalar): if not None, report status of task
                graph every ``reporting_interval`` seconds.

        """
        try:
            os.makedirs(taskgraph_cache_dir_path)
        except OSError:
            LOGGER.debug(
                "%s already exists, no need to make it",
                taskgraph_cache_dir_path)

        self._taskgraph_cache_dir_path = taskgraph_cache_dir_path

        # this variable is used to print accurate representation of how many
        # tasks have been completed in the logging output.
        self._added_task_count = 0

        # use this to keep track of all the tasks added to the graph by their
        # task hashes. Used to determine if an identical task has been added
        # to the taskgraph during `add_task`
        self._task_hash_map = dict()

        # use this to keep track of all the tasks added to the graph by their
        # task names. Used to map a unique task name to the task object it
        # represents
        self._task_name_map = dict()

        # used to remember if task_graph has been closed
        self._closed = False

        # keep track if the task graph has been forcibly terminated
        self._terminated = False

        # If n_workers > 0 this will be a threading.Thread used to propagate
        # log records from another process into the current process.
        self._logging_monitor_thread = None

        # If n_workers > 0, this will be a multiprocessing.Queue used to pass
        # log records from the process pool to the parent process.
        self._logging_queue = None

        # keeps track of the tasks currently being processed for logging.
        self._active_task_list = []

        # keeps track of how many tasks have all their dependencies satisfied
        # and are waiting for a worker
        self._task_waiting_count = 0

        # executor threads wait on this event that gets set when new tasks are
        # added to the queue. If the queue is empty an executor will clear
        # the event to halt other executors
        self._executor_ready_event = threading.Event()

        # tasks that have all their dependencies satisfied go in this queue
        # and can be executed immediately
        self._task_ready_priority_queue = queue.PriorityQueue()

        # maps a list of task names that need to be executed before the key
        # task can
        self._task_dependent_map = collections.defaultdict(set)

        # maps a list of task names that are dependent to a task
        self._dependent_task_map = collections.defaultdict(set)

        # tasks that complete are added to this set
        self._completed_task_names = set()

        print('loading config')
        config.app_cache = True
        config.memoizer = BasicMemoizer(
            checkpoint_mode='task_exit',
            checkpoint_files=get_all_checkpoints(
                self._taskgraph_cache_dir_path))
        config.run_dir = taskgraph_cache_dir_path
        config.initialize_logging = False
        config.monitoring = MonitoringHub(
            hub_address='127.0.0.1',
            monitoring_debug=True,
            resource_monitoring_interval=10
        )
        print('create pool')
        self.pool = ParslPoolExecutor(
            config=config,
            executors=[
                HighThroughputExecutor(
                    label="local_htex",
                    cores_per_worker=1,
                    max_workers_per_node=4,
                    address='127.0.0.1'
                )
            ]
        )
        print('config loaded')
        print('pool', self.pool, type(self.pool))

        # no need to set up schedulers if n_workers is single threaded
        self._n_workers = n_workers
        if n_workers < 0:
            return

        # start concurrent reporting of taskgraph if reporting interval is set
        self._reporting_interval = reporting_interval
        if reporting_interval is not None:
            self._execution_monitor_wait_event = threading.Event()
            self._execution_monitor_thread = threading.Thread(
                target=self._execution_monitor,
                args=(self._execution_monitor_wait_event,),
                name='_execution_monitor')
            # make it a daemon so we don't have to figure out how to
            # close it when execution complete
            self._execution_monitor_thread.daemon = True
            self._execution_monitor_thread.start()

        # set up multiprocessing if n_workers > 0
        if n_workers > 0:
            self._logging_queue = multiprocessing.Queue()
            self._logging_monitor_thread = threading.Thread(
                target=_logging_queue_monitor,
                args=(self._logging_queue,))

            self._logging_monitor_thread.daemon = True
            self._logging_monitor_thread.start()
            if HAS_PSUTIL:
                parent = psutil.Process()
                parent.nice(PROCESS_LOW_PRIORITY)
                for child in parent.children():
                    try:
                        child.nice(PROCESS_LOW_PRIORITY)
                    except psutil.NoSuchProcess:
                        LOGGER.warning(
                            "NoSuchProcess exception encountered when trying "
                            "to nice %s. This might be a bug in `psutil` so "
                            "it should be okay to ignore.")\

    def __del__(self):
        """Ensure all threads have been joined for cleanup."""
        print('__del__')
        self._terminate()

    def add_task(
            self, func=None, args=None, kwargs=None, task_name=None,
            target_path_list=None, ignore_path_list=None,
            hash_target_files=True, dependent_task_list=None,
            ignore_directories=True, priority=0,
            hash_algorithm='sizetimestamp', transient_run=False,
            store_result=False):
        """Add a task to the task graph.

        Args:
            func (callable): target function
            args (list): argument list for ``func``
            kwargs (dict): keyword arguments for ``func``
            target_path_list (list): if not None, a list of file paths that
                are expected to be output by ``func``.  If any of these paths
                don't exist, or their timestamp is earlier than an input
                arg or work token, func will be executed.

                If ``None``, any identical calls to ``add_task`` will be
                skipped for the TaskGraph object. A future TaskGraph object
                will re-run an exact call once for its lifetime. The reasoning
                is that it is likely the user wishes to run a target-less task
                once for the lifetime of a task-graph, but would otherwise not
                have a transient result that could be re-used in a future
                instantiation of a TaskGraph object.

            task_name (string): if not None, this value is used to identify
                the task in logging messages.
            ignore_path_list (list): list of file paths that could be in
                args/kwargs that should be ignored when considering timestamp
                hashes.
            hash_target_files (bool): If True, the hash value of the target
                files will be recorded to determine if a future run of this
                function is precalculated. If False, this function only notes
                the existence of the target files before determining if
                a function call is precalculated.
            dependent_task_list (list): list of ``Task``s that this task must
                ``join`` before executing.
            ignore_directories (boolean): if the existence/timestamp of any
                directories discovered in args or kwargs is used as part
                of the work token hash.
            priority (numeric): the priority of a task is considered when
                there is more than one task whose dependencies have been
                met and are ready for scheduling. Tasks are inserted into the
                work queue in order of decreasing priority value
                (priority 10 is higher than priority 1). This value can be
                positive, negative, and/or floating point.
            hash_algorithm (string): either a hash function id that
                exists in hashlib.algorithms_available, 'sizetimestamp',
                or 'exists'. Any paths to actual files in the arguments will
                be digested with this algorithm. If value is 'sizetimestamp'
                the digest will only use the normed path, size, and timestamp
                of any files found in the arguments. This value is used when
                determining whether a task is precalculated or its target
                files can be copied to an equivalent task. Note if
                ``hash_algorithm`` is 'sizetimestamp' the task will require the
                same base path files to determine equality. If it is a
                ``hashlib`` algorithm only file contents will be considered.
                If the value is 'exists' the only test for file equivalence
                will be if it exists on disk (True) or not (False).
            transient_run (bool): if True, this Task will be reexecuted
                even if it was successfully executed in a previous TaskGraph
                instance. If False, this Task will be skipped if it was
                executed successfully in a previous TaskGraph instance. One
                might wish to set `transient_run` to True on a Task that does
                some sort of initialization that's needed every time a
                TaskGraph is instantiated. Perhaps to acquire dynamic resources
                or authenticate permissions.
            store_result (bool): If True, the result of ``func`` will be stored
                in the TaskGraph database and retrievable with a call to
                ``.get()`` on a ``Task`` object.

        Returns:
            Task which was just added to the graph or an existing Task that
            has the same signature and has already been added to the
            TaskGraph.

        Raises:
            ValueError if objects are passed to the dependent task list that
                are not Tasks.
            ValueError if ``add_task`` is invoked after the ``TaskGraph`` is
                closed.
            RuntimeError if ``add_task`` is invoked after ``TaskGraph`` has
                reached a terminate state.

        """
        try:
            if self._terminated:
                raise RuntimeError(
                    "add_task when Taskgraph is terminated.")
            if self._closed:
                raise ValueError(
                    "The task graph is closed and cannot accept more "
                    "tasks.")
            self._added_task_count += 1
            if args is None:
                args = []
            if kwargs is None:
                kwargs = {}
            if task_name is None:
                task_name = 'UNNAMED TASK'
            if dependent_task_list is None:
                dependent_task_list = []
            if target_path_list is None:
                target_path_list = []
            if ignore_path_list is None:
                ignore_path_list = []
            if func is None:
                func = _null_func

            # this is a pretty common error to accidentally not pass a
            # Task to the dependent task list.
            if any(not isinstance(task, Task)
                   for task in dependent_task_list):
                raise ValueError(
                    "Objects passed to dependent task list that are not "
                    "tasks: %s", dependent_task_list)

            task_name = '%s (%d)' % (task_name, len(self._task_hash_map))
            new_task = Task(
                task_name, func, args, kwargs, target_path_list,
                ignore_path_list, hash_target_files, ignore_directories,
                transient_run,
                priority, hash_algorithm, store_result,
                dependent_task_list)

            self._task_name_map[new_task.task_name] = new_task
            # it may be this task was already created in an earlier call,
            # use that object in its place
            if new_task in self._task_hash_map:
                duplicate_task = self._task_hash_map[new_task]
                new_task_target_set = set(new_task._target_path_list)
                duplicate_task_target_set = set(
                    duplicate_task._target_path_list)
                if new_task_target_set == duplicate_task_target_set:
                    LOGGER.warning(
                        "A duplicate task was submitted: %s original: %s",
                        new_task, self._task_hash_map[new_task])
                    self._added_task_count -= 1
                    return duplicate_task
                disjoint_target_set = (
                    new_task_target_set.symmetric_difference(
                        duplicate_task_target_set))
                if len(disjoint_target_set) == (
                        len(new_task_target_set) +
                        len(duplicate_task_target_set)):
                    if duplicate_task not in dependent_task_list:
                        LOGGER.info(
                            "A task was created that had an identical "
                            "args signature sans target paths, but a "
                            "different target_path_list of the same "
                            "length. To avoid recomputation, dynamically "
                            "adding previous Task (%s) as a dependent "
                            "task to this one (%s).",
                            duplicate_task.task_name, task_name)
                        dependent_task_list = (
                            dependent_task_list + [duplicate_task])
                else:
                    raise RuntimeError(
                        "A task was created that has the same arguments "
                        "as another task, but only partially different "
                        "expected target paths. This runs the risk of "
                        "unpredictably overwriting output so treating as "
                        "a runtime error: submitted task: %s, existing "
                        "task: %s" % (new_task, duplicate_task))
            self._task_hash_map[new_task] = new_task
            if self._n_workers < 0:
                # call directly if single threaded
                new_task._call()
            else:
                # determine if task is ready or is dependent on other
                # tasks
                LOGGER.debug(
                    "multithreaded: %s sending to new task queue.",
                    task_name)
                outstanding_dep_task_name_list = [
                    dep_task.task_name for dep_task in dependent_task_list
                    if dep_task.task_name
                    not in self._completed_task_names]
                if not outstanding_dep_task_name_list:
                    LOGGER.debug(
                        "sending task %s right away", new_task.task_name)
                    self._task_ready_priority_queue.put(new_task)
                    self._task_waiting_count += 1
                    self._executor_ready_event.set()
                else:
                    # there are unresolved tasks that the waiting
                    # process scheduler has not been notified of.
                    # Record dependencies.
                    for dep_task_name in outstanding_dep_task_name_list:
                        # record tasks that are dependent on dep_task_name
                        self._task_dependent_map[dep_task_name].add(
                            new_task.task_name)
                        # record tasks that new_task depends on
                        self._dependent_task_map[new_task.task_name].add(
                            dep_task_name)
            return new_task

        except Exception:
            # something went wrong, shut down the taskgraph
            LOGGER.exception(
                "Something went wrong when adding task %s, "
                "terminating taskgraph.", task_name)
            self._terminate()
            raise


        except Exception:
            # something went wrong, shut down the taskgraph
            LOGGER.exception(
                "Something went wrong when adding task %s, "
                "terminating taskgraph.", task_name)
            self._terminate()
            raise

    def _execution_monitor(self, monitor_wait_event):
        """Log state of taskgraph every ``self._reporting_interval`` seconds.

        Args:
            monitor_wait_event (threading.Event): used to sleep the monitor
                for``self._reporting_interval`` seconds, or to wake up to
                terminate for shutdown.

        Returns:
            None.

        """
        start_time = time.time()
        while True:
            if self._terminated:
                break
            active_task_count = len(self._active_task_list)
            queue_length = self._task_ready_priority_queue.qsize()
            active_task_message = '\n'.join(
                ['\t%s: executing for %.2fs' % (
                    task_name, time.time() - task_time)
                 for task_name, task_time in self._active_task_list])

            completed_tasks = len(self._completed_task_names)
            percent_complete = 0.0
            if self._added_task_count > 0:
                percent_complete = 100.0 * (
                    float(completed_tasks) / self._added_task_count)

            LOGGER.info(
                "\n\ttaskgraph execution status: tasks added: %d \n"
                "\ttasks complete: %d (%.1f%%) \n"
                "\ttasks waiting for a free worker: %d (qsize: %d)\n"
                "\ttasks executing (%d): graph is %s\n%s",
                self._added_task_count, completed_tasks, percent_complete,
                self._task_waiting_count, queue_length, active_task_count,
                'closed' if self._closed else 'open',
                active_task_message)

            monitor_wait_event.wait(
                timeout=self._reporting_interval - (
                    (time.time() - start_time)) % self._reporting_interval)
        LOGGER.debug("_execution monitor shutting down")

    def join(self, timeout=None):
        """Join all threads in the graph.

        Args:
            timeout (float): if not none will attempt to join subtasks with
                this value. If a subtask times out, the whole function will
                timeout.

        Returns:
            True if successful join, False if timed out.

        """
        print('joining taskgraph')
        LOGGER.debug("joining taskgraph")
        try:
            LOGGER.debug("attempting to join threads")

            for task in self._task_hash_map.values():
                try:
                    print('computing task', task)
                    task.join(timeout=timeout)
                except TimeoutError:
                    LOGGER.info(
                        "task %s timed out in graph join", task.task_name)
                    return False

            if self._closed:
                # Close down the taskgraph; ok if already terminated
                self._executor_ready_event.set()
                self._terminate()
            return True

        except Exception:
            # If there's an exception on a join it means that a task failed
            # to execute correctly. Print a helpful message then terminate the
            # taskgraph object.
            LOGGER.exception(
                "Exception raised when joining task %s. It's possible "
                "that this task did not cause the exception, rather another "
                "exception terminated the task_graph. Check the log to see "
                "if there are other exceptions.", task)
            self._terminate()
            raise

    def close(self):
        """Prevent future tasks from being added to the work queue."""
        print('closing task graph')
        LOGGER.debug("Closing taskgraph.")
        if self._closed:
            return
        self._closed = True
        # this wakes up all the executors and any that wouldn't otherwise
        # have work to do will see there are no tasks left and terminate
        self._executor_ready_event.set()
        LOGGER.debug("taskgraph closed")

    def _terminate(self):
        """Immediately terminate remaining task graph computation."""
        print('terminating task graph')
        LOGGER.debug(
            "Invoking terminate. already terminated? %s", self._terminated)
        if self._terminated:
            return
        print('shut down pool')
        print(self.pool)
        self.pool.shutdown()
        try:
            # it's possible the global state is not well defined, so just in
            # case we'll wrap it all up in a try/except
            self._terminated = True
            if self._executor_ready_event is not None:
                # alert executors to check that _terminated is True
                self._executor_ready_event.set()

            # This will terminate the logging worker
            if self._logging_queue is not None:
                self._logging_queue.put(None)

            LOGGER.debug('taskgraph terminated')
        except Exception:
            LOGGER.exception(
                'ignoring an exception that occurred during _terminate')


class Task(object):
    """Encapsulates work/task state for multiprocessing."""

    def __init__(
            self, task_name, func, args, kwargs, target_path_list,
            ignore_path_list, hash_target_files, ignore_directories,
            transient_run, priority, hash_algorithm,
            store_result, dependent_task_list):
        """Make a Task.

        Args:
            task_name (int): unique task id from the task graph.
            func (function): a function that takes the argument list
               ``args``
            args (tuple): a list of arguments to pass to ``func``.  Can be
                None.
            kwargs (dict): keyword arguments to pass to ``func``.  Can be
                None.
            target_path_list (list): a list of filepaths that this task
                should generate.
            ignore_path_list (list): list of file paths that could be in
                args/kwargs that should be ignored when considering timestamp
                hashes.
            hash_target_files (bool): If True, the hash value of the target
                files will be recorded to determine if a future run of this
                function is precalculated. If False, this function only notes
                the existence of the target files before determining if
                a function call is precalculated.
            ignore_directories (bool): if the existence/timestamp of any
                directories discovered in args or kwargs is used as part
                of the work token hash.
            transient_run (bool): if True a call with an identical execution
                hash will be reexecuted on a subsequent instantiation of a
                future TaskGraph object. If a duplicate task is submitted
                to the same object it will not be re-run in any scenario.
                Otherwise if False, subsequent tasks with an identical
                execution hash will be skipped.
            worker_pool (multiprocessing.Pool): if not None, is a
                multiprocessing pool that can be used for ``_call`` execution.
            priority (numeric): the priority of a task is considered when
                there is more than one task whose dependencies have been
                met and are ready for scheduling. Tasks are inserted into the
                work queue in order of decreasing priority. This value can be
                positive, negative, and/or floating point.
            hash_algorithm (string): either a hash function id that
                exists in hashlib.algorithms_available, 'sizetimestamp',
                or 'exists'. Any paths to actual files in the arguments will
                be digested with this algorithm. If value is 'sizetimestamp'
                the digest will only use the normed path, size, and timestamp
                of any files found in the arguments. If 'exists' will be
                considered the same file only if a file with the same filename
                exists on disk.
            store_result (bool): If true, the result of ``func`` will be
                stored in the TaskGraph database and retrievable with a call
                to ``.get()`` on the Task object.
            task_database_path (str): path to an SQLITE database that has
                table named "taskgraph_data" with the three fields:
                    task_hash TEXT NOT NULL,
                    target_path_stats BLOB NOT NULL
                    result BLOB NOT NULL
                If a call is successful its hash is inserted/updated in the
                table, the target_path_stats stores the base/target stats
                for the target files created by the call and listed in
                ``target_path_list``, and the result of ``func`` is stored in
                ``result``.

        """
        # it is a common error to accidentally pass a non string as to the
        # target path list, this terminates early if so
        if any([not (isinstance(path, _VALID_PATH_TYPES))
                for path in target_path_list]):
            raise ValueError(
                "Values passed to target_path_list are not strings: %s",
                target_path_list)

        # sort the target path list because the order doesn't matter for
        # a result, but it would cause a task to be reexecuted if the only
        # difference was a different order.
        self._target_path_list = sorted([
            _normalize_path(path) for path in target_path_list])
        self.task_name = task_name
        self._func = func
        self._args = args
        self._kwargs = kwargs
        self._ignore_path_list = [
            _normalize_path(path) for path in ignore_path_list]
        self._hash_target_files = hash_target_files
        self._ignore_directories = ignore_directories
        self._transient_run = transient_run
        self._hash_algorithm = hash_algorithm
        self._store_result = store_result
        self.exception_object = None

        # invert the priority since sorting goes smallest to largest and we
        # want more positive priority values to be executed first.
        self._priority = -priority

        # Used to ensure only one attempt at executing and also a mechanism
        # to see when Task is complete. This can be set if a Task finishes
        # a _call and there are no more attempts at reexecution.
        self.task_done_executing_event = threading.Event()

        # These are used to store and later access the result of the call.
        self._result = None

        # Calculate a hash based only on argument inputs.
        try:
            if not hasattr(Task, 'func_source_map'):
                Task.func_source_map = {}
            # memoize func source code because it's likely we'll import
            # the same func many times and reflection is slow
            if self._func not in Task.func_source_map:
                Task.func_source_map[self._func] = (
                    inspect.getsource(self._func))
            source_code = Task.func_source_map[self._func]
        except (IOError, TypeError):
            # many reasons for this, for example, frozen Python code won't
            # have source code, so just leave blank
            source_code = ''

        if not hasattr(self._func, '__name__'):
            LOGGER.warning(
                "function does not have a __name__ which means it will not "
                "be considered when calculating a successive input has "
                "been changed with another function without __name__.")
            self._func.__name__ = ''

        args_clean = []
        for index, arg in enumerate(self._args):
            try:
                scrubbed_value = _scrub_task_args(arg, self._target_path_list)
                _ = pickle.dumps(scrubbed_value)
                args_clean.append(scrubbed_value)
            except TypeError:
                LOGGER.warning(
                    "could not pickle argument at index %d (%s). "
                    "Skipping argument which means it will not be considered "
                    "when calculating whether inputs have been changed "
                    "on a successive run.", index, arg)

        kwargs_clean = {}
        # iterate through sorted order so we get the same hash result with the
        # same set of kwargs irrespective of the item dict order.
        for key, arg in sorted(self._kwargs.items()):
            try:
                scrubbed_value = _scrub_task_args(arg, self._target_path_list)
                _ = pickle.dumps(scrubbed_value)
                kwargs_clean[key] = scrubbed_value
            except TypeError:
                LOGGER.warning(
                    "could not pickle kw argument %s (%s) scrubbed to %s. "
                    "Skipping argument which means it will not be considered "
                    "when calculating whether inputs have been changed "
                    "on a successive run.", key, arg, scrubbed_value)

        self._reexecution_info = {
            'func_name': self._func.__name__,
            'args_clean': args_clean,
            'kwargs_clean': kwargs_clean,
            'source_code_hash': hashlib.sha1(
                source_code.encode('utf-8')).hexdigest(),
        }

        argument_hash_string = ':'.join([
            repr(self._reexecution_info[key])
            for key in sorted(self._reexecution_info.keys())])

        self._task_id_hash = hashlib.sha1(
            argument_hash_string.encode('utf-8')).hexdigest()

        # this will get calculated when ``is_precalculated`` is invoked.
        self._task_reexecution_hash = None

        self.future = wrap_func(
            func, *args, **kwargs,
            inputs=[task.future for task in dependent_task_list])

    def __eq__(self, other):
        """Two tasks are equal if their hashes are equal."""
        return (
            isinstance(self, other.__class__) and
            (self._task_id_hash == other._task_id_hash))

    def __hash__(self):
        """Return the base-16 integer hash of this hash string."""
        return int(self._task_id_hash, 16)

    def __ne__(self, other):
        """Inverse of __eq__."""
        return not self.__eq__(other)

    def __lt__(self, other):
        """Less than based on priority."""
        return self._priority < other._priority

    def __repr__(self):
        """Create a string representation of a Task."""
        return "Task object %s:\n\n" % (id(self)) + pprint.pformat(
            {
                "task_name": self.task_name,
                "priority": self._priority,
                "ignore_path_list": self._ignore_path_list,
                "ignore_directories": self._ignore_directories,
                "target_path_list": self._target_path_list,
                "task_id_hash": self._task_id_hash,
                "task_reexecution_hash": self._task_reexecution_hash,
                "exception_object": self.exception_object,
                "self._reexecution_info": self._reexecution_info,
                "self._result": self._result,
            })

    def _call(self, timeout=None):
        """Invoke this method to execute task.

        Precondition is that the Task dependencies are satisfied.

        Sets the ``self.task_done_executing_event`` flag if execution is
        successful.

        Raises:
            RuntimeError if any target paths are not generated after the
                function call is complete.

        """
        self.future.result(timeout=timeout)

        # check that the target paths exist and record stats for later
        if not self._hash_target_files:
            target_hash_algorithm = 'exists'
        else:
            target_hash_algorithm = self._hash_algorithm
        result_target_path_stats = list(
            _get_file_stats(
                self._target_path_list, target_hash_algorithm, [], False))
        result_target_path_set = set(
            [x[0] for x in result_target_path_stats])
        target_path_set = set(self._target_path_list)
        if target_path_set != result_target_path_set:
            raise RuntimeError(
                "In Task: %s\nMissing expected target path results.\n"
                "Expected: %s\nObserved: %s\n" % (
                    self.task_name, self._target_path_list,
                    result_target_path_set))

        self.task_done_executing_event.set()
        LOGGER.debug("successful run on task %s", self.task_name)

    def join(self, timeout=None):
        """Block until task is complete, raise exception if runtime failed."""
        LOGGER.debug(
            "joining %s done executing: %s", self.task_name,
            self.task_done_executing_event)
        self._call(timeout=timeout)
        successful_wait = self.task_done_executing_event.wait(timeout)
        if self.exception_object:
            raise self.exception_object
        return successful_wait

    def get(self, timeout=None):
        """Return the result of the ``func`` once it is ready.

        If ``timeout`` is None, this call blocks until the task is complete
        determined by a call to ``.join()``. Otherwise will wait up to
        ``timeout`` seconds before raising a``RuntimeError`` if exceeded.

        Args:
            timeout (float): if not None this parameter is a floating point
                number specifying a timeout for the operation in seconds.

        Returns:
            value of the result

        Raises:
            RuntimeError when ``timeout`` exceeded.
            ValueError if ``store_result`` was set to ``False`` when the task
                was created.

        """
        if not self._store_result:
            raise ValueError(
                'must set `store_result` to True in `add_task` to invoke this '
                'function')
        timeout = not self.join(timeout)
        if timeout:
            raise RuntimeError('call to get timed out')
        return self._result


@retrying.retry(
    wait_exponential_multiplier=500, wait_exponential_max=3200,
    stop_max_attempt_number=100)
def _execute_sqlite(
        sqlite_command, database_path, argument_list=None,
        mode='read_only', execute='execute', fetch=None):
    """Execute SQLite command and attempt retries on a failure.

    Args:
        sqlite_command (str): a well formatted SQLite command.
        database_path (str): path to the SQLite database to operate on.
        argument_list (list): ``execute == 'execute'`` then this list is passed
            to the internal sqlite3 ``execute`` call.
        mode (str): must be either 'read_only' or 'modify'.
        execute (str): must be either 'execute' or 'script'.
        fetch (str): if not ``None`` can be either 'all' or 'one'.
            If not None the result of a fetch will be returned by this
            function.

    Returns:
        result of fetch if ``fetch`` is not None.

    """
    cursor = None
    connection = None
    try:
        if mode == 'read_only':
            ro_uri = r'%s?mode=ro' % pathlib.Path(
                os.path.abspath(database_path)).as_uri()
            LOGGER.debug(
                '%s exists: %s', ro_uri, os.path.exists(os.path.abspath(
                    database_path)))
            connection = sqlite3.connect(ro_uri, uri=True)
        elif mode == 'modify':
            connection = sqlite3.connect(database_path)
        else:
            raise ValueError('Unknown mode: %s' % mode)

        if execute == 'execute':
            if argument_list is None:
                cursor = connection.execute(sqlite_command)
            else:
                cursor = connection.execute(sqlite_command, argument_list)
        elif execute == 'script':
            cursor = connection.executescript(sqlite_command)
        else:
            raise ValueError('Unknown execute mode: %s' % execute)

        result = None
        payload = None
        if fetch == 'all':
            payload = (cursor.fetchall())
        elif fetch == 'one':
            payload = (cursor.fetchone())
        elif fetch is not None:
            raise ValueError('Unknown fetch mode: %s' % fetch)
        if payload is not None:
            result = list(payload)
        cursor.close()
        connection.commit()
        connection.close()
        cursor = None
        connection = None
        return result
    except sqlite3.OperationalError:
        LOGGER.warning(
            'TaskGraph database is locked because another process is using '
            'it, waiting for a bit of time to try again')
        raise
    except Exception:
        LOGGER.exception('Exception on _execute_sqlite: %s', sqlite_command)
        raise
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.commit()
            connection.close()

def _get_file_stats(
        base_value, hash_algorithm, ignore_list,
        ignore_directories):
    """Return fingerprints of any filepaths in ``base_value``.

    Args:
        base_value: any python value. Any file paths in ``base_value``
            should be processed with `_normalize_path`.
        hash_algorithm (string): either a hash function id that
            exists in hashlib.algorithms_available, 'exists', or
            'sizetimestamp'. Any paths to actual files in the arguments will be
            digested with this algorithm. If value is 'sizetimestamp' the
            digest will only use the normed path, size, and timestamp of any
            files found in the arguments. This value is used when
            determining whether a task is precalculated or its target
            files can be copied to an equivalent task. Note if
            ``hash_algorithm`` is 'sizetimestamp' the task will require the
            same base path files to determine equality. If it is a
            ``hashlib`` algorithm only file contents will be considered. If
            this value is 'exists' the value of the hash will be 'exists'.
        ignore_list (list): any paths found in this list are not included
            as part of the file stats. All paths in this list should be
            "os.path.norm"ed.
        ignore_directories (boolean): If True directories are not
            considered for filestats.


    Return:
        list of (path, digest) tuples for any filepaths found in
            base_value or nested in base value that are not otherwise
            ignored by the input parameters where digest is created by
            the hash algorithm specified in ``hash_algorithm``.

    """
    if isinstance(base_value, _VALID_PATH_TYPES):
        try:
            norm_path = _normalize_path(base_value)
            if norm_path not in ignore_list and (
                    not os.path.isdir(norm_path) or
                    not ignore_directories) and os.path.exists(norm_path):
                if hash_algorithm == 'exists':
                    yield (norm_path, 'exists')
                else:
                    yield (
                        norm_path, _hash_file(norm_path, hash_algorithm))
        except (OSError, ValueError):
            # I ran across a ValueError when one of the os.path functions
            # interpreted the value as a path that was too long.
            # OSErrors could happen if there's coincidentally a directory we
            # can't read or it's not a file or something else out of our
            # control
            LOGGER.exception(
                "base_value couldn't be analyzed somehow '%s'", base_value)
    elif isinstance(base_value, dict):
        for key in base_value.keys():
            value = base_value[key]
            for stat in _get_file_stats(
                    value, hash_algorithm, ignore_list, ignore_directories):
                yield stat
    elif isinstance(base_value, (list, set, tuple)):
        for value in base_value:
            for stat in _get_file_stats(
                    value, hash_algorithm, ignore_list, ignore_directories):
                yield stat


def _filter_non_files(
        base_value, keep_list, ignore_list, keep_directories):
    """Remove any values that are files not in ignore list or directories.

    Args:
        base_value: any python value. Any file paths in ``base_value``
            should be "os.path.norm"ed before this function is called.
            contains filepaths in any nested structure.
        keep_list (list): any paths found in this list are not filtered.
            All paths in this list should be "os.path.norm"ed.
        ignore_list (list): any paths found in this list are filtered.
        keep_directories (boolean): If True directories are not filtered
            out.

    Return:
        original ``base_value`` with any nested file paths for files that
        exist in the os.exists set to ``None``.

    """
    if isinstance(base_value, _VALID_PATH_TYPES):
        try:
            norm_path = _normalize_path(base_value)
            if norm_path not in ignore_list and (
                    norm_path in keep_list or ((
                        os.path.isdir(norm_path) and keep_directories) or (
                        not os.path.isfile(norm_path) and
                        not os.path.isdir(norm_path)))):
                return norm_path
            return None
        except (OSError, ValueError):
            # I ran across a ValueError when one of the os.path functions
            # interpreted the value as a path that was too long.
            # OSErrors could happen if there's coincidentally a directory we
            # can't read or it's not a file or something else out of our
            # control
            LOGGER.exception(
                "base_value couldn't be analyzed somehow '%s'", base_value)
    elif isinstance(base_value, dict):
        return {
            key: _filter_non_files(
                value, keep_list, ignore_list, keep_directories)
            for key, value in base_value.items()
        }
    elif isinstance(base_value, (list, set, tuple)):
        return type(base_value)([
            _filter_non_files(
                value, keep_list, ignore_list, keep_directories)
            for value in base_value])
    else:
        return base_value


def _scrub_task_args(base_value, target_path_list):
    """Attempt to convert ``base_value`` to canonical values.

    Any paths in ``base_value`` are normalized, any paths that are also in
    the``target_path_list`` are replaced with a placeholder so that if
    all other arguments are the same in ``base_value`` except target path
    name the function will hash to the same.

    This function can be called before the Task dependencies are satisfied
    since it doesn't inspect any file stats on disk.

    Args:
        base_value: any python value
        target_path_list (list): a list of strings that if found in
            ``base_value`` should be replaced with 'in_target_path' so

    Returns:
        base_value with any functions replaced as strings and paths in
            ``target_path_list`` with a 'target_path_list[n]' placeholder.

    """
    if callable(base_value):
        try:
            if not hasattr(Task, 'func_source_map'):
                Task.func_source_map = {}
            # memoize func source code because it's likely we'll import
            # the same func many times and reflection is slow
            if base_value not in Task.func_source_map:
                Task.func_source_map[base_value] = (
                    inspect.getsource(base_value)).replace(
                        ' ', '').replace('\t', '')
            source_code = Task.func_source_map[base_value]
        except (IOError, TypeError):
            # many reasons for this, for example, frozen Python code won't
            # have source code, so just leave blank
            source_code = ''
        return '%s:%s' % (base_value.__name__, source_code)
    elif isinstance(base_value, dict):
        result_dict = {}
        for key in base_value.keys():
            result_dict[key] = _scrub_task_args(
                base_value[key], target_path_list)
        return result_dict
    elif isinstance(base_value, (list, set, tuple)):
        result_list = []
        for value in base_value:
            result_list.append(_scrub_task_args(value, target_path_list))
        return type(base_value)(result_list)
    elif isinstance(base_value, _VALID_PATH_TYPES):
        normalized_path = _normalize_path(base_value)
        if normalized_path in target_path_list:
            return 'in_target_path_list'
        else:
            return normalized_path
    else:
        return base_value


def _hash_file(file_path, hash_algorithm, buf_size=2**20):
    """Return a hex digest of ``file_path``.

    Args:
        file_path (string): path to file to hash.
        hash_algorithm (string): a hash function id that exists in
            hashlib.algorithms_available or 'sizetimestamp'. If function id
            is in hashlib.algorithms_available, the file contents are hashed
            with that function and the fingerprint is returned. If value is
            'sizetimestamp' the size and timestamp of the file are returned
            in a string of the form
            '[sizeinbytes]:[lastmodifiedtime]'.
        buf_size (int): number of bytes to read from ``file_path`` at a time
            for digesting.

    Returns:
        a hash hex digest computed with hash algorithm ``hash_algorithm``
        of the binary contents of the file located at ``file_path``.

    """
    if hash_algorithm == 'sizetimestamp':
        norm_path = _normalize_path(file_path)
        return '%d::%i::%s' % (
            os.path.getsize(norm_path), os.stat(norm_path).st_mtime_ns,
            norm_path)
    hash_func = hashlib.new(hash_algorithm)
    with open(file_path, 'rb') as f:
        binary_data = f.read(buf_size)
        while binary_data:
            hash_func.update(binary_data)
            binary_data = f.read(buf_size)
    return hash_func.hexdigest()


def _normalize_path(path):
    """Convert ``path`` into normalized, normcase, absolute filepath."""
    norm_path = os.path.normpath(path)
    try:
        abs_path = os.path.abspath(norm_path)
    except TypeError:
        # this occurs when encountering VERY long strings that might be
        # interpreted as paths
        LOGGER.warning(
            "failed to abspath %s so returning normalized path instead")
        abs_path = norm_path
    return os.path.normcase(abs_path)

