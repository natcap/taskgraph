"""Task graph framework."""
from __future__ import absolute_import

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
import errno
try:
    import Queue as queue
except ImportError:
    # Python3 renamed queue as queue
    import queue
    # Python 3, in python 2 basestring is superclass of str and unicode.
    basestring = str
import inspect
import abc

from . import queuehandler

# Superclass for ABCs, compatible with python 2.7+ that replaces __metaclass__
# usage that is no longer clearly documented in python 3 (if it's even present
# at all ... __metaclass__ has been removed from the python data model docs)
# Taken from https://stackoverflow.com/a/38668373/299084
ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()})

try:
    import psutil
    HAS_PSUTIL = True
    if psutil.WINDOWS:
        # Windows' scheduler doesn't use POSIX niceness.
        PROCESS_LOW_PRIORITY = psutil.BELOW_NORMAL_PRIORITY_CLASS
    else:
        # On POSIX, use system niceness.
        # -20 is high priority, 0 is normal priority, 19 is low priority.
        # 10 here is an abritrary selection that's probably nice enough.
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
            reporting_interval=None, delayed_start=False):
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
            delayed_start (bool): if true, taskgraph does not start executing
                tasks as `add_task` is called. Instead no execution occurs
                until `join` is invoked. A value of `True` is incompatible
                with `n_workers` < 0 and will raise a ValueError on
                construction.
            reporting_interval (scalar): if not None, report status of task
                graph every `reporting_interval` seconds.

        Raises:
            ValueError: `delayed_start` is set to `True` but `n_workers` is
                set < 0 indicating single process/thread mode. `ValueError`
                makes sense that `TaskGraph` can't delay the start if only
                the main thread is executing.

        """
        self.n_workers = n_workers

        if delayed_start and n_workers < 0:
            raise ValueError(
                "`n_workers` cannot be set single process mode while "
                "`delayed_start` is enabled.")

        self.taskgraph_cache_dir_path = taskgraph_cache_dir_path
        self.taskgraph_started_event = threading.Event()
        if not delayed_start:
            # not a delayed start, so set the event immediately
            self.taskgraph_started_event.set()

        # use this to keep track of all the tasks added to the graph by their
        # task ids. Used to determine if an identical task has been added
        # to the taskgraph during `add_task`
        self.task_map = dict()

        # used to remember if task_graph has been closed
        self.closed = False

        # keep track if the task graph has been forcibly terminated
        self.terminated = False

        # if n_workers > 0 this will be a multiprocessing pool used to execute
        # the __call__ functions in Tasks
        self.worker_pool = None

        # If n_workers > 0 this will be a threading.Thread used to propagate
        # log records from another process into the current process.
        self.logging_monitor_thread = None

        # If n_workers > 0, this will be a multiprocessing.Queue used to pass
        # log records from the process pool to the parent process.
        self.logging_queue = None

        # keeps track of the tasks currently being processed for logging.
        self.active_task_list = []

        # keeps track of how many tasks have all their dependencies satisfied
        # and are waiting for a worker
        self.task_waiting_count = 0

        # no need to set up schedulers if n_workers is single threaded
        if n_workers < 0:
            return

        # Synchronization objects:
        # this lock is used to synchronize the following objects
        self.taskgraph_lock = threading.Lock()
        # tasks that have all their dependencies satisfied go in this queue
        # and can be executed immediately
        self.task_ready_priority_queue = queue.PriorityQueue()
        # maps a list of tasks that need to be executed to a task
        self.task_dependent_map = collections.defaultdict(set)
        # maps a list of tasks that are dependent to a task
        self.dependent_task_map = collections.defaultdict(set)
        # tasks that complete are added to this set
        self.completed_tasks = set()
        # if this is set to true, executors will terminate when the
        # task_ready_priority_queue is empty
        self.ready_to_stop = False
        # executor threads wait on this event that gets set when new tasks are
        # added to the queue. If the queue is empty an executor will clear
        # the event to halt other executors
        self.executor_ready_event = threading.Event()

        # start concurrent reporting of taskgraph if reporting interval is set
        if reporting_interval is not None:
            self.reporting_interval = reporting_interval
            monitor_thread = threading.Thread(
                target=self._execution_monitor,
                name='_execution_monitor')
            # make it a daemon so we don't have to figure out how to
            # close it when execution complete
            monitor_thread.daemon = True
            monitor_thread.start()

        # launch executor threads
        self.task_executor_thread_list = []
        for thread_id in range(max(1, n_workers)):
            task_executor_thread = threading.Thread(
                target=self._task_executor,
                name='task_executor_%s' % thread_id)
            # make daemons in case there's a catastrophic error the main
            # thread won't hang
            task_executor_thread.daemon = True
            task_executor_thread.start()
            self.task_executor_thread_list.append(task_executor_thread)

        # set up multiprocessing if n_workers > 0
        if n_workers > 0:
            self.logging_queue = multiprocessing.Queue()
            self.worker_pool = NonDaemonicPool(
                n_workers, initializer=_initialize_logging_to_queue,
                initargs=(self.logging_queue,))
            self.logging_monitor_thread = threading.Thread(
                target=self._handle_logs_from_processes,
                args=(self.logging_queue,))
            self.logging_monitor_thread.daemon = True
            self.logging_monitor_thread.start()
            if HAS_PSUTIL:
                parent = psutil.Process()
                parent.nice(PROCESS_LOW_PRIORITY)
                for child in parent.children():
                    try:
                        child.nice(PROCESS_LOW_PRIORITY)
                    except psutil.NoSuchProcess:
                        LOGGER.warn(
                            "NoSuchProcess exception encountered when trying "
                            "to nice %s. This might be a bug in `psutil` so "
                            "it should be okay to ignore.")

    def __del__(self):
        """Ensure all threads have been joined for cleanup."""
        self.close()
        self.join()

        if self.logging_queue:
            # Close down the logging monitor thread.
            self.logging_queue.put(None)
            self.logging_monitor_thread.join(_MAX_TIMEOUT)

    def _task_executor(self):
        """Worker that executes Tasks that have satisfied dependencies."""
        # this event blocks until the TaskGraph has signaled ready to execute
        self.taskgraph_started_event.wait()
        while True:
            # this event blocks until the task graph has signaled it wants
            # the executors to read the state of the queue or a stop event
            self.executor_ready_event.wait()
            # this lock synchronizes changes between the queue and
            # executor_ready_event
            self.taskgraph_lock.acquire()
            if self.task_waiting_count > 0:
                task = self.task_ready_priority_queue.get()
                self.task_waiting_count -= 1
                task_name_time_tuple = (task.task_name, time.time())
                self.active_task_list.append(task_name_time_tuple)
                # we can release the lock because we got a Task that we can
                # process
                self.taskgraph_lock.release()
                if not task.is_precalculated():
                    try:
                        task._call()
                    except Exception:
                        # An error occurred on a call, terminate the taskgraph
                        LOGGER.exception(
                            'A taskgraph _task_executor failed on Task '
                            '%s. Terminating taskgraph.', task)
                        self._terminate()
                        raise
                else:
                    LOGGER.debug(
                        "executing task: %s is precalculated", task)

                LOGGER.debug(
                    "task %s is complete, checking to see if any dependent "
                    "tasks can be executed now", task.task_name)
                with self.taskgraph_lock:
                    self.completed_tasks.add(task)
                    self.active_task_list.remove(task_name_time_tuple)
                    for waiting_task in self.task_dependent_map[task]:
                        # remove `task` from the set of tasks that
                        # `waiting_task` was waiting on.
                        self.dependent_task_map[waiting_task].remove(task)
                        # if there aren't any left, we can push `waiting_task`
                        # to the work queue
                        if not self.dependent_task_map[waiting_task]:
                            # if we removed the last task we can put it to the
                            # work queue
                            LOGGER.debug(
                                "Task %s is ready for processing, sending to "
                                "task_ready_priority_queue",
                                waiting_task.task_name)
                            self.task_ready_priority_queue.put(waiting_task)
                            self.task_waiting_count += 1
                            # indicate to executors there is work to do
                            self.executor_ready_event.set()
                    del self.task_dependent_map[task]
                LOGGER.debug("tasks %s done processing", task)
            else:
                # no tasks are waiting could be because the taskgraph is
                # closed or because the queue is just empty.
                if self.closed and not self.task_dependent_map:
                    self.taskgraph_lock.release()
                    # the task graph is signaling executors to stop,
                    # since the self.task_dependent_map is empty the executor
                    # can terminate.
                    break
                else:
                    # task graph is still locked, so it's safe to clear
                    # the executor event since there is no chance a Task
                    # could have been added while the lock was acquired
                    self.executor_ready_event.clear()
                    self.taskgraph_lock.release()

    def add_task(
            self, func=None, args=None, kwargs=None, task_name=None,
            target_path_list=None, ignore_path_list=None,
            dependent_task_list=None, ignore_directories=True,
            priority=0):
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

        Returns:
            Task which was just added to the graph or an existing Task that
            has the same signature and has already been added to the
            TaskGraph.

        """
        try:
            if self.closed:
                raise ValueError(
                    "The task graph is closed and cannot accept more tasks.")
            if args is None:
                args = []
            if kwargs is None:
                kwargs = {}
            if task_name is None:
                task_name = 'unnamed_task'
            if dependent_task_list is None:
                dependent_task_list = []
            if target_path_list is None:
                target_path_list = []
            if ignore_path_list is None:
                ignore_path_list = []
            if func is None:
                def func(): return None

            task_name = '%s_%d' % (task_name, len(self.task_map))
            new_task = Task(
                task_name, func, args, kwargs, target_path_list,
                ignore_path_list, dependent_task_list, ignore_directories,
                self.worker_pool, self.taskgraph_cache_dir_path, priority,
                self.taskgraph_started_event)

            # it may be this task was already created in an earlier call,
            # use that object in its place
            if new_task in self.task_map:
                LOGGER.warn(
                    "A duplicate task was submitted: %s original: %s",
                    new_task, self.task_map[new_task])
                return self.task_map[new_task]

            self.task_map[new_task] = new_task

            if self.n_workers < 0:
                # call directly if single threaded
                if not new_task.is_precalculated():
                    LOGGER.debug(
                        "single thread: %s is not precalculated, "
                        "invoking call", task_name)
                    new_task._call()
                else:
                    LOGGER.debug(
                        "single thread: %s is precalculated, "
                        "skipping call\n(detailed info): %s", task_name,
                        new_task)
            else:
                # determine if task is ready or is dependent on other tasks
                LOGGER.debug(
                    "multithreaded: %s sending to new task queue.", task_name)
                with self.taskgraph_lock:
                    outstanding_dependent_task_list = [
                        dep_task for dep_task in
                        new_task.dependent_task_list
                        if dep_task not in self.completed_tasks]
                    if not outstanding_dependent_task_list:
                        if new_task.is_precalculated():
                            LOGGER.debug(
                                "multiprocess: %s is precalculated, "
                                "and dependent tasks are satisified, "
                                "skipping call", task_name)
                            self.completed_tasks.add(new_task)
                        else:
                            LOGGER.debug(
                                "task %s has all dependent tasks pre-"
                                "satisfied, sending to "
                                "task_ready_priority_queue.",
                                new_task.task_name)
                            self.task_ready_priority_queue.put(new_task)
                            self.task_waiting_count += 1
                            self.executor_ready_event.set()
                    else:
                        # there are unresolved tasks that the waiting
                        # process scheduler has not been notified of.
                        # Record dependencies.
                        for dep_task in outstanding_dependent_task_list:
                            # record tasks that are dependent on dep_task
                            self.task_dependent_map[dep_task].add(
                                new_task)
                            # record tasks that new_task depends on
                            self.dependent_task_map[new_task].add(
                                dep_task)

            return new_task

        except Exception:
            # something went wrong, shut down the taskgraph
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
        LOGGER.debug('Stopping logging worker')

    def _execution_monitor(self):
        """Log state of taskgraph every `self.reporting_interval` seconds."""
        start_time = time.time()
        while True:
            if self.terminated:
                break
            with self.taskgraph_lock:
                active_task_count = len(self.active_task_list)
                queue_length = self.task_ready_priority_queue.qsize()
                active_task_message = '\n'.join(
                    ['\t%s: executing for %.2fs' % (
                        task_name, time.time() - task_time)
                     for task_name, task_time in self.active_task_list])

            total_tasks = len(self.task_map)
            completed_tasks = len(self.completed_tasks)
            percent_complete = 0.0
            if total_tasks > 0:
                percent_complete = 100.0 * (
                    float(completed_tasks) / total_tasks)

            LOGGER.info(
                "\n\ttaskgraph execution status: tasks added: %d \n"
                "\ttasks complete: %d (%.1f%%) \n"
                "\ttasks waiting for a free worker: %d (qsize: %d)\n"
                "\ttasks executing (%d): graph is %s\n%s", total_tasks,
                completed_tasks, percent_complete, self.task_waiting_count,
                queue_length, active_task_count,
                'closed' if self.closed else 'open',
                active_task_message)

            time.sleep(
                self.reporting_interval - (
                    (time.time() - start_time)) % self.reporting_interval)

    def join(self, timeout=None):
        """Join all threads in the graph.

        Parameters:
            timeout (float): if not none will attempt to join subtasks with
                this value. If a subtask times out, the whole function will
                timeout.

        Returns:
            True if successful join, False if timed out.

        """
        # if single threaded, nothing to join.
        if self.terminated:
            return True
        if self.n_workers < 0:
            return True
        # start delayed execution if necessary:
        self.taskgraph_started_event.set()
        try:
            timedout = False
            for task in self.task_map.values():
                timedout = not task.join(timeout)
                # if the last task timed out then we want to timeout for all
                # of the task graph
                if timedout:
                    break
            if not timedout and self.closed:
                # join all the workers and the worker pool
                if self.n_workers >= 0:
                    # there's only something to clean up if there's a worker
                    LOGGER.info(
                        "TaskGraph closed, joining all the task executor "
                        "threads.")
                    for task_executor_thread in (
                            self.task_executor_thread_list):
                        try:
                            task_executor_thread.join(timeout)
                            LOGGER.debug(
                                "task_executor_thread joined (%s)",
                                task_executor_thread)
                        except RuntimeError:
                            LOGGER.warn(
                                "task_executor_thread already complete (%s)",
                                task_executor_thread)
                    # run through any leftover tasks that are still running
                    # after executor is dead
                    for task in self.task_map.values():
                        timedout = not task.join(timeout)
                        if timedout:
                            break
                    if self.worker_pool and not timedout:
                        LOGGER.info(
                            "TaskGraph closed, joining the worker_pool")
                        self.worker_pool.close()
                        self.worker_pool.join()
                        self.worker_pool.terminate()
            return not timedout
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
        if self.closed:
            return
        self.closed = True
        if self.n_workers >= 0:
            # we only have a task_manager if running in threaded mode
            with self.taskgraph_lock:
                # wake executors so they can process that the taskgraph is
                # closed and can shut down if there is no pending work
                self.executor_ready_event.set()

    def _terminate(self):
        """Immediately terminate remaining task graph computation."""
        if self.terminated:
            return
        self.close()
        if self.n_workers > 0:
            self.worker_pool.terminate()
            self.logging_queue.put(None)  # close down the log monitor thread
            self.logging_monitor_thread.join(_MAX_TIMEOUT)
        for task in self.task_map.values():
            task._terminate()
        self.terminated = True


class Task(object):
    """Encapsulates work/task state for multiprocessing."""

    def __init__(
            self, task_name, func, args, kwargs, target_path_list,
            ignore_path_list, dependent_task_list, ignore_directories,
            worker_pool, cache_dir, priority, taskgraph_started_event):
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
            dependent_task_list (list of Task): a list of other
                `Task`s that are to be invoked before `func(args)` is
                invoked.
            ignore_path_list (list): list of file paths that could be in
                args/kwargs that should be ignored when considering timestamp
                hashes.
            ignore_directories (boolean): if the existence/timestamp of any
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
            taskgraph_started_event (Event): if set, `Task`s can execute, but
                if not, an exception will be raised if `join` is invoked.

        """
        self.task_name = task_name
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.cache_dir = cache_dir
        self.task_cache_path = None

        # sort the target path list because the order doesn't matter for
        # a result, but it would cause a task to be reexecuted if the only
        # difference was a different order.
        self.target_path_list = sorted(target_path_list)
        self.dependent_task_list = sorted(dependent_task_list)

        self.ignore_path_list = ignore_path_list
        self.ignore_directories = ignore_directories
        self.worker_pool = worker_pool

        # invert the priority since sorting goes smallest to largest and we
        # want more positive priority values to be executed first.
        self.priority = -priority

        self.taskgraph_started_event = taskgraph_started_event
        self.terminated = False
        self.exception_object = None

        # Used to ensure only one attempt at executing and also a mechanism
        # to see when Task is complete. This can be set if a Task finishes
        # a _call, tests True on is_precomputed, or after a call to _terminate
        self.task_complete_event = threading.Event()

        # it's possible for multiple threads to attempt to determine if a
        # task has been completed. the reexecution hash may change depending
        # on whether input parameter files have changed. This lock ensures
        # there's not a race condition when calculating the reexecution hash
        self.deep_hash_lock = threading.Lock()

        # Calculate a hash based only on argument inputs.
        try:
            if not hasattr(Task, 'func_source_map'):
                Task.func_source_map = {}
            # memoize func source code because it's likely we'll import
            # the same func many times and reflection is slow
            if self.func not in Task.func_source_map:
                Task.func_source_map[self.func] = (
                    inspect.getsource(self.func))
            source_code = Task.func_source_map[self.func]
        except (IOError, TypeError):
            # many reasons for this, for example, frozen Python code won't
            # have source code, so just leave blank
            source_code = ''

        if not hasattr(self.func, '__name__'):
            LOGGER.warn(
                "function does not have a __name__ which means it will not "
                "be considered when calculating a successive input has "
                "been changed with another function without __name__.")
            self.func.__name__ = ''

        args_clean = []
        for index, arg in enumerate(self.args):
            try:
                scrubbed_value = _scrub_functions(arg)
                _ = pickle.dumps(scrubbed_value)
                args_clean.append(scrubbed_value)
            except TypeError:
                LOGGER.warn(
                    "could not pickle argument at index %d (%s). "
                    "Skipping argument which means it will not be considered "
                    "when calculating whether inputs have been changed "
                    "on a successive run.", index, arg)

        kwargs_clean = {}
        # iterate through sorted order so we get the same hash result with the
        # same set of kwargs irrespect of the item dict order.
        for key, value in sorted(self.kwargs.items()):
            try:
                scrubbed_value = _scrub_functions(arg)
                _ = pickle.dumps(scrubbed_value)
                kwargs_clean[key] = scrubbed_value
            except TypeError:
                LOGGER.warn(
                    "could not pickle kw argument %s (%s). "
                    "Skipping argument which means it will not be considered "
                    "when calculating whether inputs have been changed "
                    "on a successive run.", key, arg)

        self.reexecution_info = {
            'func_name': self.func.__name__,
            'args': pprint.pformat(args_clean),
            'kwargs': pprint.pformat(kwargs_clean),
            'source_code_hash': hashlib.sha1(
                source_code.encode('utf-8')).hexdigest(),
            'target_path_list': pprint.pformat(self.target_path_list),
        }

        argument_hash_string = ':'.join([
            self.reexecution_info[key]
            for key in sorted(self.reexecution_info.keys())])

        self.task_id_hash = hashlib.sha1(
            argument_hash_string.encode('utf-8')).hexdigest()

        # this will get calculated once dependent tasks are complete and
        # _calculate_deep_hash is invoked.
        self.task_reexecution_hash = None

    def _calculate_deep_hash(self):
        """Calculate a hash that accounts for file size objects at runtime.

        It only makes sense to call this function if all dependent tasks have
        executed. After this call, self.task_cache_path has a valid path
        defined and self.reexecution_info['file_stat_list'] is defined.

        Returns:
            None

        """
        # This gets a list of the files and their file stats that can be found
        # in args and kwargs but ignores anything specifically targeted or
        # an expected result. This will allow a task to change its hash in
        # case a different version of a file was passed in.
        file_stat_list = list(_get_file_stats(
            [self.args, self.kwargs],
            self.target_path_list+self.ignore_path_list,
            self.ignore_directories))

        # add the file stat list to the already existing reexecution info
        # dictionary that contains stats that should not change whether
        # files have been created/updated/or not.
        self.reexecution_info['file_stat_list'] = pprint.pformat(
            file_stat_list)
        reexecution_string = '%s:%s' % (
            self.task_id_hash, self.reexecution_info['file_stat_list'])
        self.task_reexecution_hash = hashlib.sha1(
            reexecution_string.encode('utf-8')).hexdigest()

        # make a directory and target based on hashname
        # take the first 3 characters of the hash and make a subdirectory
        # for each so we don't blowup the filesystem with a bunch of files in
        # one directory
        self.task_cache_path = os.path.join(
            self.cache_dir, *(
                [x for x in self.task_reexecution_hash[0:3]] +
                [self.task_reexecution_hash + '.json']))

    def __eq__(self, other):
        """Two tasks are equal if their hashes are equal."""
        if isinstance(self, other.__class__):
            return self.task_id_hash == other.task_id_hash
        return False

    def __hash__(self):
        """Return the base-16 integer hash of this hash string."""
        return int(self.task_id_hash, 16)

    def __ne__(self, other):
        """Inverse of __eq__."""
        return not self.__eq__(other)

    def __lt__(self, other):
        """Less than based on priority."""
        return self.priority < other.priority

    def __str__(self):
        """Create a string representation of a Task."""
        return "Task object %s:\n\n" % (id(self)) + pprint.pformat(
            {
                "task_name": self.task_name,
                "priority": self.priority,
                "dependent_task_list": self.dependent_task_list,
                "ignore_path_list": self.ignore_path_list,
                "ignore_directories": self.ignore_directories,
                "task_id_hash": self.task_id_hash,
                "task_reexecution_hash": self.task_reexecution_hash,
                "terminated": self.terminated,
                "exception_object": self.exception_object,
                "self.reexecution_info": self.reexecution_info
            })

    def _call(self):
        """TaskGraph should invoke this method when ready to execute task.

        Precondition is that the Task dependencies are satisfied.

        Raises:
            RuntimeError if any target paths are not generated after the
                function call is complete.

        Returns:
            A list of file parameters of the target path list.

        """
        try:
            if self.terminated:
                raise RuntimeError(
                    "Task %s was called after termination.", self.task_name)

            if self.worker_pool is not None:
                result = self.worker_pool.apply_async(
                    func=self.func, args=self.args, kwds=self.kwargs)
                result.get()
            else:
                self.func(*self.args, **self.kwargs)

            missing_target_paths = [
                target_path for target_path in self.target_path_list
                if not os.path.exists(target_path)]
            if missing_target_paths:
                raise RuntimeError(
                    "The following paths were expected but not found "
                    "after the function call: %s" % missing_target_paths)

            # check that the target paths exist
            result_target_path_stats = list(
                _get_file_stats([self.target_path_list], [], False))
            target_path_set = set(self.target_path_list)
            result_target_path_set = set(
                [x[0] for x in result_target_path_stats])
            if target_path_set != result_target_path_set:
                raise RuntimeError(
                    "In Task: %s\nMissing expected target path results.\n"
                    "Expected: %s\nObserved: %s\n" % (
                        self.task_name, self.target_path_list,
                        result_target_path_set))

            # otherwise record target path stats in a file located at
            # self.task_cache_path
            try:
                os.makedirs(os.path.dirname(self.task_cache_path))
            except OSError as exception:
                if exception.errno != errno.EEXIST:
                    raise
            # this step will only record the run for future processes if
            # there is a concrete expected file on disk. Otherwise results
            # must be transient between runs and we'd expect to run again.
            if self.target_path_list:
                with open(self.task_cache_path, 'wb') as task_cache_file:
                    pickle.dump(result_target_path_stats, task_cache_file)

            LOGGER.debug("successful run on task %s", self.task_name)
            # successful run, return target path stats
            return result_target_path_stats
        except Exception as e:
            LOGGER.exception("Exception Task: %s", self)
            self._terminate(e)
            raise
        finally:
            LOGGER.debug(
                "setting _task_complete_event on task %s", self.task_name)
            self.task_complete_event.set()

    def is_complete(self):
        """Test to determine if Task is complete.

        Returns:
            False if task thread is still running.
            True if task thread is stopped and the completion token was
                created correctly.

        Raises:
            RuntimeError if the task thread is stopped but no completion
            token

        """
        if self.terminated:
            raise RuntimeError(
                "is_complete invoked on a terminated task %s" % str(self))
        if self.task_complete_event.isSet() or self.is_precalculated():
            return True
        return False

    def is_precalculated(self):
        """Return true if Task can be skipped.

        Returns:
            True if the Task's target paths exist in the same state as the
            last recorded run at the time this function is called. It is
            possible this value could change without running the Task if
            input parameter file stats change. False otherwise.

        """
        with self.deep_hash_lock:
            self._calculate_deep_hash()
        try:
            if not os.path.exists(self.task_cache_path):
                LOGGER.info(
                    "not precalculated, Task Cache file does not "
                    "exist (%s)", self.task_name)
                LOGGER.debug("is_precalculated full task info: %s", self)
                return False
            with open(self.task_cache_path, 'rb') as task_cache_file:
                result_target_path_stats = pickle.load(task_cache_file)
            mismatched_target_file_list = []
            for path, modified_time, size in result_target_path_stats:
                if not os.path.exists(path):
                    mismatched_target_file_list.append(
                        'Path not found: %s' % path)
                    continue
                target_modified_time = os.path.getmtime(path)
                if modified_time != target_modified_time:
                    mismatched_target_file_list.append(
                        "Modified times don't match "
                        "desired: (%s) target: (%s)" % (
                            modified_time, target_modified_time))
                    continue
                target_size = os.path.getsize(path)
                if size != target_size:
                    mismatched_target_file_list.append(
                        "File sizes don't match "
                        "desired: (%s) target: (%s)" % (
                            size, target_size))
            if mismatched_target_file_list:
                LOGGER.warn(
                    "not precalculated (%s), Task Cache file exists, "
                    "but there are these mismatches: %s",
                    self.task_name, '\n'.join(mismatched_target_file_list))
                return False
            LOGGER.info(
                "precalculated (%s)" % self.task_name)
            self.task_complete_event.set()
            return True
        except EOFError:
            return False

    def join(self, timeout=None):
        """Block until task is complete, raise exception if runtime failed."""
        if not self.taskgraph_started_event.is_set():
            raise RuntimeError(
                "Task joined even though taskgraph has delayed start "
                "enabled: %s" % self)
        if self.is_precalculated():
            return True
        self.task_complete_event.wait(timeout)
        return self.is_complete()

    def _terminate(self, exception_object=None):
        """Invoke to terminate the Task."""
        self.terminated = True
        self.exception_object = exception_object
        self.task_complete_event.set()


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


def _get_file_stats(base_value, ignore_list, ignore_directories):
    """Iterate over any values that are filepaths by getting filestats.

    Parameters:
        base_value: any python value.
        ignore_list (list): any paths found in this list are not included
            as part of the file stats
        ignore_directories (boolean): If True directories are not
            considered for filestats.

    Return:
        list of (path, timestamp, filesize) tuples for any filepaths found in
            base_value or nested in base value that are not otherwise
            ignored by the input parameters.

    """
    if isinstance(base_value, basestring):
        try:
            if base_value not in ignore_list and (
                    not os.path.isdir(base_value) or
                    not ignore_directories):
                yield (base_value, os.path.getmtime(base_value),
                       os.path.getsize(base_value))
        except OSError:
            pass
    elif isinstance(base_value, dict):
        for key in sorted(base_value.keys()):
            value = base_value[key]
            for stat in _get_file_stats(
                    value, ignore_list, ignore_directories):
                yield stat
    elif isinstance(base_value, (list, set, tuple)):
        for value in base_value:
            for stat in _get_file_stats(
                    value, ignore_list, ignore_directories):
                yield stat


def _scrub_functions(base_value):
    """Replace functions with stable string representations.

    Parameters:
        base_value: any python value

    Returns:
        base_value with any functions replaced as strings.

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
        for key in sorted(base_value.keys()):
            result_dict[key] = _scrub_functions(base_value[key])
        return result_dict
    elif isinstance(base_value, (list, set, tuple)):
        result_list = []
        for value in base_value:
            result_list.append(_scrub_functions(value))
        return type(base_value)(result_list)
    return base_value
