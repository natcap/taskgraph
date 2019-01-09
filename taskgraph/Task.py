"""Task graph framework."""
from __future__ import absolute_import

import shutil
import time
import pprint
import collections
import hashlib
import pickle
import os
import logging
import multiprocessing
import multiprocessing.pool
import threading
import sqlite3
import math
try:
    import Queue as queue
    # In python 2 basestring is superclass of str and unicode.
    _VALID_PATH_TYPES = (basestring,)

    def _isclose(a, b, rel_tol=1e-9, abs_tol=0.0):
        """Define isclose function for Python 2.7, >3.6 has math.isclose."""
        return abs(a-b) < max(rel_tol * max(abs(a), abs(b)), abs_tol)
    math.isclose = _isclose
except ImportError:
    # Python3 renamed queue as queue
    import queue
    import pathlib
    # pathlib only exists in Python3
    _VALID_PATH_TYPES = (str, pathlib.Path)
import inspect
import abc

from . import queuehandler

# Superclass for ABCs, compatible with python 2.7+ that replaces __metaclass__
# usage that is no longer clearly documented in python 3 (if it's even present
# at all ... __metaclass__ has been removed from the python data model docs)
# Taken from https://stackoverflow.com/a/38668373/299084
ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()})

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


# We want our processing pool to be nondeamonic so that workers could use
# multiprocessing if desired (deamonic processes cannot start new processes)
# the following bit of code to do this was taken from
# https://stackoverflow.com/a/8963618/42897
class NoDaemonProcess(multiprocessing.Process):
    """Make 'daemon' attribute always return False."""

    def _get_daemon(self):
        return False

    def _set_daemon(self, value):
        pass
    daemon = property(_get_daemon, _set_daemon)


class NonDaemonicPool(multiprocessing.pool.Pool):
    """NonDaemonic Process Pool."""

    Process = NoDaemonProcess


def _initialize_logging_to_queue(logging_queue):
    """Add a synchronized queue to a new process.

    This is intended to be called as an initialization function to
    ``multiprocessing.Pool`` to establish logging from a Pool worker to the
    main python process via a multiprocessing Queue.

    Parameters:
        logging_queue (multiprocessing.Queue): The queue to use for passing
            log records back to the main process.

    Returns:
        ``None``

    """
    root_logger = logging.getLogger()

    # By the time this function is called, ``root_logger`` has a copy of all of
    # the logging handlers registered to it within the parent process, which
    # leads to duplicate logging in some cases.  By removing all of the
    # handlers here, we ensure that log messages can only be passed back to the
    # parent process by the ``logging_queue``, where they will be handled.
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    root_logger.setLevel(logging.NOTSET)
    handler = queuehandler.QueueHandler(logging_queue)
    root_logger.addHandler(handler)


class TaskGraph(object):
    """Encapsulates the worker and tasks states for parallel processing."""

    def __init__(
            self, taskgraph_cache_dir_path, n_workers,
            reporting_interval=None):
        """Create a task graph.

        Creates an object for building task graphs, executing them,
        parallelizing independent work notes, and avoiding repeated calls.

        Parameters:
            taskgraph_cache_dir_path (string): path to a directory that
                either contains a taskgraph cache from a previous instance or
                will create a new one if none exists.
            n_workers (int): number of parallel *subprocess* workers to allow
                during task graph execution.  If set to 0, don't use
                subprocesses.  If set to <0, use only the main thread for any
                execution and scheduling. In the case of the latter,
                `add_task` will be a blocking call.
            reporting_interval (scalar): if not None, report status of task
                graph every `reporting_interval` seconds.

        """
        try:
            os.makedirs(taskgraph_cache_dir_path)
        except OSError:
            LOGGER.debug(
                "%s already exists, no need to make it",
                taskgraph_cache_dir_path)

        self._taskgraph_cache_dir_path = taskgraph_cache_dir_path

        self._taskgraph_started_event = threading.Event()

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

        # if n_workers > 0 this will be a multiprocessing pool used to execute
        # the __call__ functions in Tasks
        self._worker_pool = None

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

        # Synchronization objects:
        # this lock is used to synchronize the following objects
        self._taskgraph_lock = threading.RLock()

        # this is used to guard multiple connections to the same database
        self._task_database_lock = threading.Lock()

        # this might hold the threads to execute tasks if n_workers >= 0
        self._task_executor_thread_list = []

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

        self._task_database_path = os.path.join(
            self._taskgraph_cache_dir_path, _TASKGRAPH_DATABASE_FILENAME)
        sql_create_projects_table = (
            """
            CREATE TABLE IF NOT EXISTS taskgraph_data (
                task_reexecution_hash TEXT NOT NULL,
                target_path_stats BLOB NOT NULL,
                PRIMARY KEY (task_reexecution_hash)
            );
            CREATE UNIQUE INDEX IF NOT EXISTS task_reexecution_hash_index
            ON taskgraph_data (task_reexecution_hash);
            """)

        with sqlite3.connect(self._task_database_path) as conn:
            cursor = conn.cursor()
            cursor.executescript(sql_create_projects_table)

        # no need to set up schedulers if n_workers is single threaded
        self._n_workers = n_workers
        if n_workers < 0:
            return

        # start concurrent reporting of taskgraph if reporting interval is set
        self._reporting_interval = reporting_interval
        if reporting_interval is not None:
            self._monitor_thread = threading.Thread(
                target=self._execution_monitor,
                name='_execution_monitor')
            # make it a daemon so we don't have to figure out how to
            # close it when execution complete
            self._monitor_thread.daemon = True
            self._monitor_thread.start()

        # launch executor threads
        for thread_id in range(max(1, n_workers)):
            task_executor_thread = threading.Thread(
                target=self._task_executor,
                name='task_executor_%s' % thread_id)
            # make daemons in case there's a catastrophic error the main
            # thread won't hang
            task_executor_thread.daemon = True
            task_executor_thread.start()
            self._task_executor_thread_list.append(task_executor_thread)

        # set up multiprocessing if n_workers > 0
        if n_workers > 0:
            self._logging_queue = multiprocessing.Queue()
            self._worker_pool = NonDaemonicPool(
                n_workers, initializer=_initialize_logging_to_queue,
                initargs=(self._logging_queue,))
            self._logging_monitor_thread = threading.Thread(
                target=self._handle_logs_from_processes,
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
                            "it should be okay to ignore.")

    def __del__(self):
        """Ensure all threads have been joined for cleanup."""
        try:
            # it's possible the global state is not well defined, so just in
            # case we'll wrap it all up in a try/except
            if self._n_workers > 0:
                LOGGER.debug("shutting down workers")
                self._worker_pool.terminate()
                # close down the log monitor thread
                self._logging_queue.put(None)
                timedout = not self._logging_monitor_thread.join(_MAX_TIMEOUT)
                if timedout:
                    LOGGER.warning(
                        '_logging_monitor_thread %s timed out',
                        self._logging_monitor_thread)

            if self._logging_queue:
                # Close down the logging monitor thread.
                self._logging_queue.put(None)
                self._logging_monitor_thread.join(_MAX_TIMEOUT)
                # drain the queue if anything is left
                while True:
                    try:
                        x = self._logging_queue.get_nowait()
                        LOGGER.error("the logging queue had this in it: %s", x)
                    except queue.Empty:
                        break

            self._taskgraph_started_event.set()
            if self._n_workers >= 0:
                self._executor_ready_event.set()
                for executor_thread in self._task_executor_thread_list:
                    try:
                        timedout = not executor_thread.join(_MAX_TIMEOUT)
                        if timedout:
                            LOGGER.warning(
                                'task executor thread timed out %s',
                                executor_thread)
                    except Exception:
                        LOGGER.exception(
                            "Exception when joining %s", executor_thread)
                if self._reporting_interval is not None:
                    LOGGER.debug("joining _monitor_thread.")
                    timedout = not self._monitor_thread.join(_MAX_TIMEOUT)
                    if timedout:
                        LOGGER.warning(
                            '_monitor_thread %s timed out', self._monitor_thread)
                    for task in self._task_hash_map.values():
                        # this is a shortcut to get the tasks to mark as joined
                        task.task_done_executing_event.set()

            # drain the task ready queue if there's anything left
            while True:
                try:
                    x = self._task_ready_priority_queue.get_nowait()
                    LOGGER.error(
                        "task_ready_priority_queue not empty contains: %s", x)
                except queue.Empty:
                    break
            LOGGER.debug('taskgraph terminated')
        except:
            pass

    def _task_executor(self):
        """Worker that executes Tasks that have satisfied dependencies."""
        # this event blocks until the TaskGraph has signaled ready to execute
        self._taskgraph_started_event.wait()
        while True:
            # this event blocks until the task graph has signaled it wants
            # the executors to read the state of the queue or a stop event
            self._executor_ready_event.wait()
            # this lock synchronizes changes between the queue and
            # executor_ready_event
            if self._terminated:
                LOGGER.debug(
                    "taskgraph is terminated, ending %s",
                    threading.currentThread())
                break
            task = None
            self._taskgraph_lock.acquire()
            try:
                task = self._task_ready_priority_queue.get_nowait()
                self._task_waiting_count -= 1
                task_name_time_tuple = (task.task_name, time.time())
                self._active_task_list.append(task_name_time_tuple)
            except queue.Empty:
                # no tasks are waiting could be because the taskgraph is
                # closed or because the queue is just empty.
                if self._closed and not self._task_dependent_map:
                    self._taskgraph_lock.release()
                    # the task graph is signaling executors to stop,
                    # since the self._task_dependent_map is empty the
                    # executor can terminate.
                    LOGGER.debug(
                        "no tasks are pending and taskgraph closed, normally "
                        "terminating executor %s." %
                        threading.currentThread())
                    break
                else:
                    self._executor_ready_event.clear()
            self._taskgraph_lock.release()
            if task:
                try:
                    task._call()
                    task.task_done_executing_event.set()
                except Exception as e:
                    # An error occurred on a call, terminate the taskgraph
                    if task.n_retries == 0:
                        task.exception_object = e
                        LOGGER.exception(
                            'A taskgraph _task_executor failed on Task '
                            '%s. Terminating taskgraph.', task.task_name)
                        self._terminate()
                        break
                    else:
                        LOGGER.warning(
                            'A taskgraph _task_executor failed on Task '
                            '%s attempting no more than %d retries. original '
                            'exception %s',
                            task.task_name, task.n_retries, repr(e))
                        task.n_retries -= 1
                        with self._taskgraph_lock:
                            self._active_task_list.remove(task_name_time_tuple)
                            self._task_ready_priority_queue.put(task)
                            self._task_waiting_count += 1
                            self._executor_ready_event.set()
                        continue

                LOGGER.debug(
                    "task %s is complete, checking to see if any dependent "
                    "tasks can be executed now", task.task_name)
                with self._taskgraph_lock:
                    self._completed_task_names.add(task.task_name)
                    self._active_task_list.remove(task_name_time_tuple)
                    for waiting_task_name in (
                            self._task_dependent_map[task.task_name]):
                        # remove `task` from the set of tasks that
                        # `waiting_task` was waiting on.
                        self._dependent_task_map[waiting_task_name].remove(
                            task.task_name)
                        # if there aren't any left, we can push `waiting_task`
                        # to the work queue
                        if not self._dependent_task_map[waiting_task_name]:
                            # if we removed the last task we can put it to the
                            # work queue
                            LOGGER.debug(
                                "Task %s is ready for processing, sending to "
                                "task_ready_priority_queue",
                                waiting_task_name)
                            del self._dependent_task_map[waiting_task_name]
                            self._task_ready_priority_queue.put(
                                self._task_name_map[waiting_task_name])
                            self._task_waiting_count += 1
                            # indicate to executors there is work to do
                            self._executor_ready_event.set()
                    del self._task_dependent_map[task.task_name]
                LOGGER.debug("task %s done processing", task.task_name)
        LOGGER.debug("task executor shutting down")

    def add_task(
            self, func=None, args=None, kwargs=None, task_name=None,
            target_path_list=None, ignore_path_list=None,
            dependent_task_list=None, ignore_directories=True, priority=0,
            n_retries=0, hash_algorithm='sizetimestamp',
            copy_duplicate_artifact=False):
        """Add a task to the task graph.

        Parameters:
            func (callable): target function
            args (list): argument list for `func`
            kwargs (dict): keyword arguments for `func`
            target_path_list (list): if not None, a list of file paths that
                are expected to be output by `func`.  If any of these paths
                don't exist, or their timestamp is earlier than an input
                arg or work token, func will be executed.

                If `None`, any identical calls to `add_task` will be skipped
                for the TaskGraph object. A future TaskGraph object will
                re-run an exact call once for its lifetime. The reasoning is
                that it is likely the user wishes to run a target-less task
                once for the lifetime of a task-graph, but would otherwise
                not have a transient result that could be re-used in a
                future instantiation of a TaskGraph object.
            task_name (string): if not None, this value is used to identify
                the task in logging messages.
            ignore_path_list (list): list of file paths that could be in
                args/kwargs that should be ignored when considering timestamp
                hashes.
            dependent_task_list (list): list of `Task`s that this task must
                `join` before executing.
            ignore_directories (boolean): if the existence/timestamp of any
                directories discovered in args or kwargs is used as part
                of the work token hash.
            priority (numeric): the priority of a task is considered when
                there is more than one task whose dependencies have been
                met and are ready for scheduling. Tasks are inserted into the
                work queue in order of decreasing priority value
                (priority 10 is higher than priority 1). This value can be
                positive, negative, and/or floating point.
            n_retries (int): if > 0, this task will attempt to reexecute up to
                `n_retries` times if an exception occurs during execution of
                the task. The process to attempt reexecution involves
                re-inserting the task on the "work ready" queue which will be
                processed by the taskgraph scheduler. This means the task may
                attempt to reexecute immediately, or after some other tasks
                are cleared.
            hash_algorithm (string): either a hash function id that
                exists in hashlib.algorithms_available or 'sizetimestamp'.
                Any paths to actual files in the arguments will be digested
                with this algorithm. If value is 'sizetimestamp' the digest
                will only use the normed path, size, and timestamp of any
                files found in the arguments. This value is used when
                determining whether a task is precalculated or its target
                files can be copied to an equivalent task. Note if
                `hash_algorithm` is 'sizetimestamp' the task will require the
                same base path files to determine equality. If it is a
                `hashlib` algorithm only file contents will be considered.
            copy_duplicate_artifact (bool): if true and the Tasks'
                argument signature matches a previous Tasks without direct
                comparison of the target path files in the arguments other
                than their positions in the target path list, the target
                artifacts from a previously successful Task execution will
                be copied to the new one.

        Returns:
            Task which was just added to the graph or an existing Task that
            has the same signature and has already been added to the
            TaskGraph.

        Raises:
            ValueError if objects are passed to the dependent task list that
                are not Tasks.
            ValueError if `add_task` is invoked after the `TaskGraph` is
                closed.
            RuntimeError if `add_task` is invoked after `TaskGraph` has
                reached a terminate state.

        """
        with self._taskgraph_lock:
            try:
                if self._terminated:
                    raise RuntimeError(
                        "add_task when Taskgraph is terminated.")
                if self._closed:
                    raise ValueError(
                        "The task graph is closed and cannot accept more "
                        "tasks.")
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
                    def func(): return None

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
                    ignore_path_list, ignore_directories,
                    self._worker_pool, self._taskgraph_cache_dir_path,
                    priority, n_retries, hash_algorithm,
                    copy_duplicate_artifact, self._taskgraph_started_event,
                    self._task_database_path, self._task_database_lock)

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
                                "adding previous Task (%s) as a dependent task "
                                "to this one (%s).", duplicate_task.task_name,
                                task_name)
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
                    outstanding_dependent_task_name_list = [
                        dep_task.task_name for dep_task in dependent_task_list
                        if dep_task.task_name
                        not in self._completed_task_names]
                    if not outstanding_dependent_task_name_list:
                        LOGGER.debug(
                            "sending task %s right away", new_task.task_name)
                        self._task_ready_priority_queue.put(new_task)
                        self._task_waiting_count += 1
                        self._executor_ready_event.set()
                    else:
                        # there are unresolved tasks that the waiting
                        # process scheduler has not been notified of.
                        # Record dependencies.
                        for dep_task_name in outstanding_dependent_task_name_list:
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

    def _handle_logs_from_processes(self, queue_):
        LOGGER.debug('Starting logging worker')
        while True:
            record = queue_.get()
            if record is None:
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)
        LOGGER.debug('_handle_logs_from_processes shutting down')

    def _execution_monitor(self):
        """Log state of taskgraph every `self._reporting_interval` seconds."""
        start_time = time.time()
        while True:
            if self._terminated:
                break
            with self._taskgraph_lock:
                active_task_count = len(self._active_task_list)
                queue_length = self._task_ready_priority_queue.qsize()
                active_task_message = '\n'.join(
                    ['\t%s: executing for %.2fs' % (
                        task_name, time.time() - task_time)
                     for task_name, task_time in self._active_task_list])

            total_tasks = len(self._task_hash_map)
            completed_tasks = len(self._completed_task_names)
            percent_complete = 0.0
            if total_tasks > 0:
                percent_complete = 100.0 * (
                    float(completed_tasks) / total_tasks)

            LOGGER.info(
                "\n\ttaskgraph execution status: tasks added: %d \n"
                "\ttasks complete: %d (%.1f%%) \n"
                "\ttasks waiting for a free worker: %d (qsize: %d)\n"
                "\ttasks executing (%d): graph is %s\n%s", total_tasks,
                completed_tasks, percent_complete, self._task_waiting_count,
                queue_length, active_task_count,
                'closed' if self._closed else 'open',
                active_task_message)

            time.sleep(
                self._reporting_interval - (
                    (time.time() - start_time)) % self._reporting_interval)
        LOGGER.debug("_execution monitor shutting down")

    def join(self, timeout=None):
        """Join all threads in the graph.

        Parameters:
            timeout (float): if not none will attempt to join subtasks with
                this value. If a subtask times out, the whole function will
                timeout.

        Returns:
            True if successful join, False if timed out.

        """
        LOGGER.debug("joining taskgraph")
        # start delayed execution if necessary:
        self._taskgraph_started_event.set()
        # if single threaded, nothing to join.
        if self._n_workers < 0 or self._terminated:
            return True
        try:
            LOGGER.debug("attempting to join threads")
            timedout = False
            for task in self._task_hash_map.values():
                LOGGER.debug("attempting to join task %s", task.task_name)
                timedout = not task.join(timeout)
                LOGGER.debug("task %s was joined", task.task_name)
                # if the last task timed out then we want to timeout for all
                # of the task graph
                if timedout:
                    LOGGER.info(
                        "task %s timed out in graph join", task.task_name)
                    return False
            if self._closed and self._n_workers >= 0:
                with self._taskgraph_lock:
                    # we only have a task_manager if running in threaded mode
                    # wake executors so they can process that the taskgraph is
                    # closed and can shut down if there is no pending work
                    self._executor_ready_event.set()
                    self._terminated = True
                if self._logging_queue:
                    self._logging_queue.put(None)
                    self._logging_monitor_thread.join(timeout)
                if self._reporting_interval is not None:
                    LOGGER.debug("joining _monitor_thread.")
                    self._monitor_thread.join(timeout)
                for executor_task in self._task_executor_thread_list:
                    executor_task.join(timeout)
            LOGGER.debug('taskgraph terminated')
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
        LOGGER.debug("Closing taskgraph.")
        if self._closed:
            return
        with self._taskgraph_lock:
            self._closed = True
        LOGGER.debug("taskgraph closed")

    def _terminate(self):
        """Immediately terminate remaining task graph computation."""
        LOGGER.debug(
            "Invoking terminate. already terminated? %s", self._terminated)
        if self._terminated:
            return
        with self._taskgraph_lock:
            self._terminated = True

            for task in self._task_hash_map.values():
                LOGGER.debug("setting task done for %s", task.task_name)
                task.task_done_executing_event.set()

            if self._worker_pool:
                self._worker_pool.terminate()

        self._taskgraph_started_event.set()
        self._executor_ready_event.set()


class Task(object):
    """Encapsulates work/task state for multiprocessing."""

    def __init__(
            self, task_name, func, args, kwargs, target_path_list,
            ignore_path_list, ignore_directories,
            worker_pool, cache_dir, priority, n_retries, hash_algorithm,
            copy_duplicate_artifact, taskgraph_started_event,
            task_database_path, task_database_lock):
        """Make a Task.

        Parameters:
            task_name (int): unique task id from the task graph.
            func (function): a function that takes the argument list
                `args`
            args (tuple): a list of arguments to pass to `func`.  Can be
                None.
            kwargs (dict): keyword arguments to pass to `func`.  Can be
                None.
            target_path_list (list): a list of filepaths that this task
                should generate.
            ignore_path_list (list): list of file paths that could be in
                args/kwargs that should be ignored when considering timestamp
                hashes.
            ignore_directories (bool): if the existence/timestamp of any
                directories discovered in args or kwargs is used as part
                of the work token hash.
            worker_pool (multiprocessing.Pool): if not None, is a
                multiprocessing pool that can be used for `_call` execution.
            cache_dir (string): path to a directory to both write and expect
                data recorded from a previous Taskgraph run.
            priority (numeric): the priority of a task is considered when
                there is more than one task whose dependencies have been
                met and are ready for scheduling. Tasks are inserted into the
                work queue in order of decreasing priority. This value can be
                positive, negative, and/or floating point.
            n_retries (int): if > 0, this task will attempt to reexecute up to
                `n_retries` times if an exception occurs during execution of
                the task. The process to attempt reexecution involves
                re-inserting the task on the "work ready" queue which will be
                processed by the taskgraph scheduler. This means the task may
                attempt to reexecute immediately, or after some othre tasks
                are cleared. If <= 0, the task will fail on the first
                unhandled exception the Task encounters.
            hash_algorithm (string): either a hash function id that
                exists in hashlib.algorithms_available or 'sizetimestamp'.
                Any paths to actual files in the arguments will be digested
                with this algorithm. If value is 'sizetimestamp' the digest
                will only use the normed path, size, and timestamp of any
                files found in the arguments.
            copy_duplicate_artifact (bool): if true and the Tasks'
                argument signature matches a previous Tasks without direct
                comparison of the target path files in the arguments other
                than their positions in the target path list, the target
                artifacts from a previously successful Task execution will
                be copied to the new one.
            taskgraph_started_event (Event): can be used to start the main
                TaskGraph if it has not yet started in case a Task is joined.
            task_database_path (str): path to an SQLITE database that has
                table named "taskgraph_data" with the two fields:
                    task_hash TEXT NOT NULL,
                    target_path_stats BLOB NOT NULL
                If a call is successful its hash is inserted/updated in the
                table and the target_path_stats stores the base/target stats
                for the target files created by the call and listed in
                `target_path_list`.
            task_database_lock (threading.Lock): used to lock the task
                database before a .connect.

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
        self._target_path_list = sorted(
            [os.path.normpath(os.path.normcase(path))
             for path in target_path_list])
        self.task_name = task_name
        self._func = func
        self._args = args
        self._kwargs = kwargs
        self._cache_dir = cache_dir
        self._ignore_path_list = [
            os.path.normpath(os.path.normcase(path))
            for path in ignore_path_list]
        self._ignore_directories = ignore_directories
        self._worker_pool = worker_pool
        self._taskgraph_started_event = taskgraph_started_event
        self.n_retries = n_retries
        self._task_database_path = task_database_path
        self._hash_algorithm = hash_algorithm
        self._copy_duplicate_artifact = copy_duplicate_artifact
        self._task_database_lock = task_database_lock
        self.exception_object = None

        # This flag is used to avoid repeated calls to "is_precalculated"
        self._precalculated = None

        # invert the priority since sorting goes smallest to largest and we
        # want more positive priority values to be executed first.
        self._priority = -priority

        # Used to ensure only one attempt at executing and also a mechanism
        # to see when Task is complete. This can be set if a Task finishes
        # a _call and there are no more attempts at reexecution.
        self.task_done_executing_event = threading.Event()

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
                kwargs_clean[arg] = scrubbed_value
            except TypeError:
                LOGGER.warning(
                    "could not pickle kw argument %s (%s). "
                    "Skipping argument which means it will not be considered "
                    "when calculating whether inputs have been changed "
                    "on a successive run.", key, arg)

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

        # this will get calculated when `is_precalculated` is invoked.
        self._task_reexecution_hash = None

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
                "self._reexecution_info": self._reexecution_info
            })

    def _call(self):
        """Invoke this method to execute task.

        Precondition is that the Task dependencies are satisfied.

        Sets the `self.task_done_executing_event` flag if execution is
        successful.

        Raises:
            RuntimeError if any target paths are not generated after the
                function call is complete.

        """
        LOGGER.debug("_call check if precalculated %s", self.task_name)
        if self.is_precalculated():
            self.task_done_executing_event.set()
            return
        LOGGER.debug("not precalculated %s", self.task_name)
        result_calculated = False
        if self._copy_duplicate_artifact:
            # try to see if we can copy old files
            with self._task_database_lock:
                with sqlite3.connect(self._task_database_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        """
                        SELECT target_path_stats from taskgraph_data
                        WHERE (task_reexecution_hash == ?)
                        """, (self._task_reexecution_hash,))
                    database_result = cursor.fetchone()
            if database_result:
                result_target_path_stats = pickle.loads(database_result[0])
                if (len(result_target_path_stats) ==
                        len(self._target_path_list)):
                    if all([
                        file_fingerprint == _hash_file(path, hash_algorithm)
                        for path, hash_algorithm, file_fingerprint in (
                            result_target_path_stats)]):
                        LOGGER.debug(
                            "copying stored artifacts to target path list. \n"
                            "\tstored artifacts: %s\n\t"
                            "target_path_list: %s\n",
                            [x[0] for x in result_target_path_stats],
                            self._target_path_list)
                        for artifact_target, new_target in zip(
                                result_target_path_stats,
                                self._target_path_list):
                            if artifact_target != new_target:
                                shutil.copyfile(
                                    artifact_target[0], new_target)
                            else:
                                # This is a bug if this ever happens, but so
                                # bad if it does I want to stop and report a
                                # helpful error message
                                raise RuntimeError(
                                    "duplicate copy artifact and target "
                                    "path: %s, result_path_stats: %s, "
                                    "target_path_list: %s" % (
                                        artifact_target,
                                        result_target_path_stats,
                                        self._target_path_list))
                        result_calculated = True
        if not result_calculated:
            if self._worker_pool is not None:
                result = self._worker_pool.apply_async(
                    func=self._func, args=self._args, kwds=self._kwargs)
                # the following blocks and raises an exception if result
                # raised an exception
                LOGGER.debug("apply_async for task %s", self.task_name)
                result.get()
            else:
                LOGGER.debug("direct _func for task %s", self.task_name)
                self._func(*self._args, **self._kwargs)

        # check that the target paths exist and record stats for later
        result_target_path_stats = list(
            _get_file_stats(
                self._target_path_list, self._hash_algorithm, [], False))
        result_target_path_set = set(
            [x[0] for x in result_target_path_stats])
        target_path_set = set(self._target_path_list)
        if target_path_set != result_target_path_set:
            raise RuntimeError(
                "In Task: %s\nMissing expected target path results.\n"
                "Expected: %s\nObserved: %s\n" % (
                    self.task_name, self._target_path_list,
                    result_target_path_set))

        # this step will only record the run if there is an expected
        # target file. Otherwise we infer the result of this call is
        # transient between taskgraph executions and we should expect to
        # run it again.
        if self._target_path_list:
            with self._task_database_lock:
                with sqlite3.connect(self._task_database_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        """INSERT OR REPLACE INTO taskgraph_data VALUES
                           (?, ?)""", (
                            self._task_reexecution_hash, pickle.dumps(
                                result_target_path_stats)))
                    conn.commit()
        self._precalculated = True
        self.task_done_executing_event.set()
        LOGGER.debug("successful run on task %s", self.task_name)

    def is_precalculated(self):
        """Return true if _call need not be invoked.

        Returns:
            True if the Task's target paths exist in the same state as the
            last recorded run at the time this function is called. It is
            possible this value could change without running the Task if
            input parameter file stats change. False otherwise.

        """
        # This gets a list of the files and their file stats that can be found
        # in args and kwargs but ignores anything specifically targeted or
        # an expected result. This will allow a task to change its hash in
        # case a different version of a file was passed in.
        if self._precalculated is not None:
            return self._precalculated

        # these are the stats of the files that exist that aren't ignored
        file_stat_list = list(_get_file_stats(
            [self._args, self._kwargs],
            self._hash_algorithm,
            self._target_path_list+self._ignore_path_list,
            self._ignore_directories))

        other_arguments = list(_filter_non_files(
            [self._reexecution_info['args_clean'],
             self._reexecution_info['kwargs_clean']],
            self._target_path_list+self._ignore_path_list,
            self._ignore_directories))

        LOGGER.debug("file_stat_list: %s", file_stat_list)
        LOGGER.debug("other_arguments: %s", other_arguments)

        # add the file stat list to the already existing reexecution info
        # dictionary that contains stats that should not change whether
        # files have been created/updated/or not.
        self._reexecution_info['file_stat_list'] = file_stat_list
        self._reexecution_info['other_arguments'] = other_arguments

        reexecution_string = '%s:%s:%s:%s' % (
            self._reexecution_info['func_name'],
            self._reexecution_info['source_code_hash'],
            self._reexecution_info['other_arguments'],
            # the x[2] is to only take the *hash* part of the 'file_stat'
            str([x[2] for x in file_stat_list]))

        self._task_reexecution_hash = hashlib.sha1(
            reexecution_string.encode('utf-8')).hexdigest()
        try:
            with self._task_database_lock:
                with sqlite3.connect(self._task_database_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        """
                            SELECT target_path_stats from taskgraph_data
                            WHERE (task_reexecution_hash == ?)
                        """, (self._task_reexecution_hash,))
                    database_result = cursor.fetchone()
            if database_result is None:
                LOGGER.info(
                    "not precalculated, Task hash does not "
                    "exist (%s)", self.task_name)
                LOGGER.debug("is_precalculated full task info: %s", self)
                self._precalculated = False
                return False
            result_target_path_stats = pickle.loads(database_result[0])
            mismatched_target_file_list = []
            for path, hash_algorithm, hash_string in result_target_path_stats:
                if path not in self._target_path_list:
                    mismatched_target_file_list.append(
                        'Recorded path not in target path list %s' % path)
                if not os.path.exists(path):
                    mismatched_target_file_list.append(
                        'Path not found: %s' % path)
                    continue
                if hash_algorithm == 'sizetimestamp':
                    size, modified_time = [
                        float(x) for x in hash_string.split(':')]
                    target_modified_time = os.path.getmtime(path)
                    if not math.isclose(modified_time, target_modified_time):
                        mismatched_target_file_list.append(
                            "Modified times don't match "
                            "cached: (%f) actual: (%f)" % (
                                modified_time, target_modified_time))
                        continue
                    target_size = os.path.getsize(path)
                    if size != target_size:
                        mismatched_target_file_list.append(
                            "File sizes don't match "
                            "cached: (%s) actual: (%s)" % (
                                size, target_size))
                else:
                    target_hash = _hash_file(path, hash_algorithm)
                    if hash_string != target_hash:
                        mismatched_target_file_list.append(
                            "File hashes are different. cached: (%s) "
                            "actual: (%s)" % (hash_string, target_hash))
            if mismatched_target_file_list:
                LOGGER.warning(
                    "not precalculated (%s), Task hash exists, "
                    "but there are these mismatches: %s",
                    self.task_name, '\n'.join(mismatched_target_file_list))
                self._precalculated = False
                return False
            LOGGER.info("precalculated (%s)" % self)
            self._precalculated = True
            return True
        except EOFError:
            LOGGER.exception("not precalculated %s, EOFError", self.task_name)
            self._precalculated = False
            return False

    def join(self, timeout=None):
        """Block until task is complete, raise exception if runtime failed."""
        self._taskgraph_started_event.set()
        LOGGER.debug('started taskgraph %s', self._taskgraph_started_event.isSet())
        LOGGER.debug(
            "joining %s done executing: %s", self.task_name,
            self.task_done_executing_event)
        timed_out = self.task_done_executing_event.wait(timeout)
        if self.exception_object:
            raise self.exception_object
        return timed_out


class EncapsulatedTaskOp(ABC):
    """Used as a superclass for Task operations that need closures.

    This class will automatically hash the subclass's __call__ method source
    as well as the arguments to its __init__ function to calculate the
    Task's unique hash.

    """

    def __init__(self, *args, **kwargs):
        """Attempt to get the source code of __call__."""
        args_as_str = str([args, kwargs]).encode('utf-8')
        try:
            # hash the args plus source code of __call__
            id_hash = hashlib.sha1(args_as_str + inspect.getsource(
                self.__class__.__call__).encode('utf-8')).hexdigest()
        except IOError:
            # this will fail if the code is compiled, that's okay just do
            # the args
            id_hash = hashlib.sha1(args_as_str)
        # prefix the classname
        self.__name__ = '%s_%s' % (self.__class__.__name__, id_hash)

    @abc.abstractmethod
    def __call__(self, *args, **kwargs):
        """Empty method meant to be overridden by inheritor."""
        pass


def _get_file_stats(
        base_value, hash_algorithm, ignore_list, ignore_directories):
    """Return fingerprints of any filepaths in `base_value`.

    Parameters:
        base_value: any python value. Any file paths in `base_value`
            should be "os.path.norm"ed before this function is called.
            contains filepaths in any nested structure.
        ignore_list (list): any paths found in this list are not included
            as part of the file stats. All paths in this list should be
            "os.path.norm"ed.
        ignore_directories (boolean): If True directories are not
            considered for filestats.


    Return:
        list of (path, hash_algorithm, hash) tuples for any filepaths found in
            base_value or nested in base value that are not otherwise
            ignored by the input parameters.

    """
    if isinstance(base_value, _VALID_PATH_TYPES):
        try:
            norm_path = os.path.normpath(os.path.normcase(base_value))
            if norm_path not in ignore_list and (
                    not os.path.isdir(norm_path) or
                    not ignore_directories) and os.path.exists(norm_path):
                yield (
                    norm_path, hash_algorithm,
                    _hash_file(norm_path, hash_algorithm))
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
        base_value, keep_list, keep_directories):
    """Remove any values that are files not in ignore list or directories.

    Parameters:
        base_value: any python value. Any file paths in `base_value`
            should be "os.path.norm"ed before this function is called.
            contains filepaths in any nested structure.
        keep_list (list): any paths found in this list are not filtered.
            All paths in this list should be "os.path.norm"ed.
        keep_directories (boolean): If True directories are not filtered
            out.

    Return:
        original `base_value` with any nested file paths for files that
        exist in the os.exists removed.

    """
    if isinstance(base_value, _VALID_PATH_TYPES):
        try:
            norm_path = os.path.normpath(os.path.normcase(base_value))
            if (norm_path in keep_list or (
                    os.path.isdir(norm_path) and keep_directories) or
                    not os.path.isfile(norm_path)):
                yield norm_path
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
            for filter_value in _filter_non_files(
                    value, keep_list, keep_directories):
                yield (value, filter_value)
    elif isinstance(base_value, (list, set, tuple)):
        for value in base_value:
            for filter_value in _filter_non_files(
                    value, keep_list, keep_directories):
                yield filter_value
    else:
        yield base_value


def _scrub_task_args(base_value, target_path_list):
    """Attempt to convert `base_value` to canonical values.

    Any paths in `base_value` are normalized, any paths that are also in
    the `target_path_list` are replaced with a placeholder so that if
    all other arguments are the same in `base_value` except target path
    name the function will hash to the same.

    This function can be called before the Task dependencies are satisfied
    since it doesn't inspect any file stats on disk.

    Parameters:
        base_value: any python value
        target_path_list (list): a list of strings that if found in
            `base_value` should be replaced with 'in_target_path' so

    Returns:
        base_value with any functions replaced as strings and paths in
            `target_path_list` with a 'target_path_list[n]' placeholder.

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
        normalized_path = os.path.normpath(os.path.normcase(base_value))
        if normalized_path in target_path_list:
            return 'target_path_list[%d]' % target_path_list.index(
                normalized_path)
        else:
            return normalized_path
    else:
        return base_value


def _hash_file(file_path, hash_algorithm, buf_size=2**20):
    """Return a hex digest of `file_path`.

    Parameters:
        file_path (string): path to file to hash.
        hash_algorithm (string): a hash function id that exists in
            hashlib.algorithms_available or 'sizetimestamp'. If function id
            is in hashlib.algorithms_available, the file contents are hashed
            with that function and the fingerprint is returned. If value is
            'sizetimestamp' the size and timestamp of the file are returned
            in a string of the form
            '[sizeinbytes]:[lastmodifiedtime]'.
        buf_size (int): number of bytes to read from `file_path` at a time
            for digesting.

    Returns:
        a hash hex digest computed with hash algorithm `hash_algorithm`
        of the binary contents of the file located at `file_path`.

    """
    if hash_algorithm == 'sizetimestamp':
        norm_path = os.path.normpath(os.path.normcase(file_path))
        return '%d:%f' % (
            os.path.getsize(norm_path), os.path.getmtime(norm_path))
    hash_func = hashlib.new(hash_algorithm)
    with open(file_path, 'rb') as f:
        binary_data = f.read(buf_size)
        while binary_data:
            hash_func.update(binary_data)
            binary_data = f.read(buf_size)
    return hash_func.hexdigest()
