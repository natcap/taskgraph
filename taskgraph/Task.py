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
        self.work_queue = Queue.Queue()
        self.n_workers = n_workers
        for thread_id in xrange(n_workers):
            threading.Thread(
                target=TaskGraph.worker, args=(self.work_queue,),
                name=thread_id).start()

        if n_workers > 0:
            self.worker_pool = multiprocessing.Pool(n_workers)
            if HAS_PSUTIL:
                parent = psutil.Process()
                parent.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
                for child in parent.children():
                    child.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
        else:
            self.worker_pool = None

        # used to lock global resources
        self.global_lock = threading.Lock()

        # if a Task is in here, it's been previously created
        self.global_working_task_set = set()

        self.closed = False

    def __del__(self):
        """Clean up task graph by injecting STOP sentinels."""
        self.close()

    @staticmethod
    def worker(work_queue):
        """Worker taking (func, args, kwargs) tuple from `work_queue`."""
        for func, args, kwargs in iter(work_queue.get, 'STOP'):
            try:
                func(*args, **kwargs)
            except Exception as subprocess_exception:
                LOGGER.error(traceback.format_exc())
                LOGGER.error(subprocess_exception)

    def close(self):
        """Prevent future tasks from being added to the work queue."""
        self.closed = True
        for _ in xrange(self.n_workers):
            self.work_queue.put('STOP')

    def add_task(
            self, func=None, args=None, kwargs=None,
            target_path_list=None,
            ignore_path_list=None,
            dependent_task_list=None,
            ignore_directories=True):
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
        if self.closed:
            raise ValueError(
                "The task graph is closed and cannot accept more tasks.")
        if args is None:
            args = []
        if kwargs is None:
            kwargs = {}
        if dependent_task_list is None:
            dependent_task_list = []
        if target_path_list is None:
            target_path_list = []
        if ignore_path_list is None:
            ignore_path_list = []
        if func is None:
            func = lambda: None
        with self.global_lock:
            task_id = len(self.global_working_task_set)
            task = Task(
                task_id, func, args, kwargs, target_path_list,
                ignore_path_list, dependent_task_list, ignore_directories,
                self.token_storage_path)
            self.global_working_task_set.add(task)
        if self.n_workers > 0:
            self.work_queue.put(
                (task,
                 (self.global_lock,
                  self.global_working_task_set,
                  self.worker_pool),
                 {}))
        else:
            task(
                self.global_lock, self.global_working_task_set,
                self.worker_pool)
        return task

    def join(self):
        """Join all threads in the graph."""
        for task in self.global_working_task_set:
            task.join()


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
            self, global_lock, global_working_task_dict,
            global_worker_pool):
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
            global_lock (threading.Lock): use this to lock global
                the global resources to the task graph.
            global_working_task_dict (dict): contains a dictionary of task_ids
                to Tasks that are currently executing.  Global resource and
                should acquire lock before modifying it.
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
            self.lock.release()

    def is_complete(self):
        """Return true if target files are the same as recorded in token."""
        try:
            with open(self.token_path, 'r') as token_file:
                for path, modified_time, size in json.loads(token_file.read()):
                    if not (os.path.exists(path) and
                            modified_time == os.path.getmtime(path) and
                            size == os.path.getsize(path)):
                        return False
            return True
        except (IOError, ValueError, TypeError):
            # file might not exist or be a JSON object, not complete then.
            return False

    def join(self):
        """Block until task is complete, raise exception if not complete."""
        with self.lock:
            pass
        if not self.is_complete():
            raise RuntimeError(
                "Task %s didn't complete, discontinuing "
                "execution of %s" % (
                    self.task_id, self.func.__name__))


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
