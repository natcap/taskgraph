"""Task graph framework."""
import types
import collections
import traceback
import datetime
import hashlib
import json
import pickle
import os
import logging
import multiprocessing
import threading
import errno
import Queue
import inspect
import pkg_resources

try:
    pkg_resources.get_distribution('psutil')
except pkg_resources.DistributionNotFound:
    HAS_PSUTIL = False
else:
    import psutil
    HAS_PSUTIL = True

LOGGER = logging.getLogger('Task')


class TaskGraph(object):
    """Encapsulates the worker and tasks states for parallel processing."""

    def __init__(self, token_storage_path, n_workers):
        """Create a task graph.

        Creates an object for building task graphs, executing them,
        parallelizing independent work notes, and avoiding repeated calls.

        Parameters:
            token_storage_path (string): path to a directory where work tokens
                (files) can be stored.  Task graph checks this directory to
                see if a task has already been completed.
            n_workers (int): number of parallel workers to allow during
                task graph execution.  If set to 0, use current process.
        """
        # https://stackoverflow.com/questions/273192/how-can-i-create-a-directory-if-it-does-not-exist
        try:
            os.makedirs(token_storage_path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise
        self.token_storage_path = token_storage_path

        # the work queue is the feeder to active worker threads
        self.work_queue = Queue.Queue()
        self.n_workers = n_workers

        # used to synchronize a pass through potential tasks to add to the
        # work queue
        self.process_pending_tasks_condition = threading.Condition(
            threading.Lock())

        # tasks that haven't been evaluated for dependencies go in here
        # to start making it a set so adds and removes are relatively fast
        self.pending_task_set = set()

        if n_workers > 0:
            self.worker_pool = multiprocessing.Pool(n_workers)
            if HAS_PSUTIL:
                parent = psutil.Process()
                parent.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
                for child in parent.children():
                    child.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
        else:
            self.worker_pool = None

        # use this to keep track of all the tasks added to the graph
        self.task_set = set()

        # used to remember if task_graph has been closed
        self.closed = False

        # used to synchronize on a terminate command
        self.terminating_event = threading.Event()
        self.terminating_event.set()

        # launch threads to manage the workers
        for thread_id in xrange(n_workers):
            worker_thread = threading.Thread(
                target=self.worker, args=(self.work_queue,),
                name=thread_id)
            worker_thread.daemon = True
            worker_thread.start()

        # launch thread to monitor the pending task set
        pending_task_thread = threading.Thread(
            target=self.process_pending_tasks,
            name='process_pending_tasks')
        pending_task_thread.daemon = True
        pending_task_thread.start()

    def __del__(self):
        """Clean up task graph by injecting STOP sentinels."""
        self.close()

    def worker(self, work_queue):
        """Worker taking (func, args, kwargs) tuple from `work_queue`."""
        for func, args, kwargs in iter(work_queue.get, 'STOP'):
            LOGGER.debug("Worker start")
            self.terminating_event.wait()
            try:
                func(*args, **kwargs)
            except Exception as subprocess_exception:
                LOGGER.debug("Worker error")
                LOGGER.error(traceback.format_exc())
                LOGGER.error(subprocess_exception)
                self._terminate()
                LOGGER.debug("Worker error return")
                return

    def _terminate(self):
        """Used to terminate remaining task graph computation on an error."""
        self.terminating_event.clear()
        # clear the working queue
        while not self.work_queue.empty():
            _ = self.work_queue.get()
        # clear the pending task set
        self.pending_task_set.clear()
        self.close()
        self.terminating_event.set()

    def close(self):
        """Prevent future tasks from being added to the work queue."""
        if not self.closed:
            LOGGER.debug("closing taskgraph")
            self.process_pending_tasks_condition.acquire()
            self.closed = True
            for _ in xrange(self.n_workers):
                self.work_queue.put('STOP')
            self.pending_task_set.add('STOP')
            self.process_pending_tasks_condition.notify()
            self.process_pending_tasks_condition.release()
            LOGGER.debug("taskgraph closed")

    def add_task(
            self, func=None, args=None, kwargs=None, task_name=None,
            target_path_list=None, ignore_path_list=None,
            dependent_task_list=None, ignore_directories=True):
        """Add a task to the task graph.

        See the docstring for Task.__call__ to determine how it determines
        if it should execute.

        Parameters:
            func (callable): target function
            args (list): argument list for `func`
            kwargs (dict): keyword arguments for `func`
            target_path_list (list): if not None, a list of file paths that
                are expected to be output by `func`.  If any of these paths
                don't exist, or their timestamp is earlier than an input
                arg or work token, func will be executed.  If None, not
                considered when scheduling task.
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

        Returns:
            Task which was just added to the graph.
        """
        self.terminating_event.wait()
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
                func = lambda: None

            task_id = '%s_%d' % (task_name, len(self.task_set))
            task = Task(
                task_id, func, args, kwargs, target_path_list,
                ignore_path_list, dependent_task_list, ignore_directories,
                self.token_storage_path)
            self.task_set.add(task)

            if self.n_workers > 0:
                self.process_pending_tasks_condition.acquire()
                self.pending_task_set.add(task)
                self.process_pending_tasks_condition.notify()
                self.process_pending_tasks_condition.release()
            else:
                task(self.worker_pool)
            return task
        except Exception:
            # something went wrong, shut down the taskgraph
            self.close()
            raise

    def process_pending_tasks(self):
        """Search pending task list for free tasks to work queue."""
        try:
            self.process_pending_tasks_condition.acquire()
            while True:
                self.process_pending_tasks_condition.wait()
                self.terminating_event.wait()
                queued_task_set = set()
                LOGGER.debug(
                    "process pending task set %s", self.pending_task_set)
                for task in self.pending_task_set:
                    if task == 'STOP':
                        return
                    try:
                        if all([task.is_complete()
                                for task in task.dependent_task_list]):
                            self.work_queue.put((task, (self.worker_pool,), {}))
                            queued_task_set.add(task)
                    except Exception:
                        # a dependent task failed, stop execution and raise
                        # exception
                        self.close()
                        raise
                self.pending_task_set = self.pending_task_set.difference(
                    queued_task_set)
                LOGGER.debug(
                    "process pending task set after call %s", self.pending_task_set)
        finally:
            LOGGER.debug("process pending task quitting")
            self.process_pending_tasks_condition.release()

    def join(self):
        """Join all threads in the graph."""
        LOGGER.debug("Joining taskgraph")
        self.terminating_event.wait()
        try:
            for task in self.task_set:
                LOGGER.debug("taskgraph join task set %s", self.task_set)
                task.join()
            LOGGER.debug("done Joining taskgraph")
        except Exception:
            self.close()
            raise


class Task(object):
    """Encapsulates work/task state for multiprocessing."""

    def __init__(
            self, task_id, func, args, kwargs, target_path_list,
            ignore_path_list, dependent_task_list, ignore_directories,
            token_storage_path):
        """Make a Task.

        Parameters:
            task_id (int): unique task id from the task graph.
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
            target_path_list (list): list of filepaths expected
                to be generated by this func and args/kwargs.
            ignore_path_list (list): list of file paths that could be in
                args/kwargs that should be ignored when considering timestamp
                hashes.
            ignore_directories (boolean): if the existence/timestamp of any
                directories discovered in args or kwargs is used as part
                of the work token hash.
            token_storage_path (string): path to a directory that exists
                where task can store a file to indicate completion of task.
        """
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.target_path_list = target_path_list
        self.dependent_task_list = dependent_task_list
        self.target_path_list = target_path_list
        self.ignore_path_list = ignore_path_list
        self.ignore_directories = ignore_directories
        self.token_storage_path = token_storage_path
        self.token_path = None  # not set until dependencies are blocked
        self.task_id = task_id

        # Used to ensure only one attempt at executing and also a mechanism
        # to see when Task is complete
        self.lock = threading.Lock()
        self.lock.acquire()  # the only release is at the end of __call__

        # https://stackoverflow.com/questions/273192/how-can-i-create-a-directory-if-it-does-not-exist
        try:
            os.makedirs(token_storage_path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

    def _calculate_token(self):
        """Make a unique hash of the call. Standalone so it can be threaded."""
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
            # many reasons for this, so just leave blank
            source_code = ''

        file_stat_list = list(_get_file_stats(
            [self.args, self.kwargs],
            self.target_path_list+self.ignore_path_list,
            self.ignore_directories))

        task_string = '%s:%s:%s:%s:%s:%s' % (
            self.func.__name__, pickle.dumps(self.args),
            json.dumps(self.kwargs, sort_keys=True), source_code,
            self.target_path_list, str(file_stat_list))

        return hashlib.sha1(task_string).hexdigest()

    def __call__(
            self, global_worker_pool):
        """Invoke this method when ready to execute task.

        This function will execute `func` on `args`/`kwargs` under the
        following circumstances:
            * if no work token exists (a work token is a hashed combination
                of the source code, arguments, target files, and associated
                time stamps).
            * if any input filepath arguments have a newer timestamp than the
              work token or any path in `target_path_list`.
            * AND all the tasks in `dependant_task_list` have been joined.


        Parameters:
            global_worker_pool (multiprocessing.Pool): a process pool used to
                execute subprocesses.  If None, use current process.

        Returns:
            None
        """
        try:
            if len(self.dependent_task_list) > 0:
                for task in self.dependent_task_list:
                    task.join()

            token_id = self._calculate_token()
            self.token_path = os.path.join(self.token_storage_path, token_id)
            if self.is_complete():
                LOGGER.debug(
                    "Completion token exists for %s so not executing",
                    self.task_id)
                return

            if global_worker_pool is not None:
                result = global_worker_pool.apply_async(
                    func=self.func, args=self.args, kwds=self.kwargs)
                result.get()
            else:
                self.func(*self.args, **self.kwargs)
            with open(self.token_path, 'w') as token_file:
                # write out json string as target paths, file modified, file size
                file_stat_list = list(
                    _get_file_stats([self.target_path_list], [], False))
                token_file.write(json.dumps(file_stat_list))
        finally:
            LOGGER.debug("task is quitting %s", self.lock)
            self.lock.release()
            LOGGER.debug("task lock released %s", self.lock)

    def is_complete(self):
        """Return true if target files are the same as recorded in token."""
        if not self.lock.acquire(False):
            # lock is still acquired, so it's not done yet.
            return False
        try:
            with open(self.token_path, 'r') as token_file:
                for path, modified_time, size in json.loads(token_file.read()):
                    if not (os.path.exists(path) and
                            modified_time == os.path.getmtime(path) and
                            size == os.path.getsize(path)):
                        raise RuntimeError()
        except Exception:
            raise RuntimeError(
                "Task %s didn't complete correctly" % self.task_id)
        return True

    def join(self):
        """Block until task is complete, raise exception if runtime failed."""
        LOGGER.debug("joining task")
        LOGGER.debug("task lock %s", self.lock)
        with self.lock:
            pass
        LOGGER.debug("task checking is_complete")
        return self.is_complete()


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
    if isinstance(base_value, types.StringTypes):
        try:
            if base_value not in ignore_list and (
                    not os.path.isdir(base_value) or
                    not ignore_directories):
                yield (base_value, os.path.getmtime(base_value),
                       os.path.getsize(base_value))
        except OSError:
            pass
    elif isinstance(base_value, collections.Mapping):
        for key in sorted(base_value.iterkeys()):
            value = base_value[key]
            for stat in _get_file_stats(
                    value, ignore_list, ignore_directories):
                yield stat
    elif isinstance(base_value, collections.Iterable):
        for value in base_value:
            for stat in _get_file_stats(
                    value, ignore_list, ignore_directories):
                yield stat
