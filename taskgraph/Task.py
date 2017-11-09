"""Task graph framework."""
import pprint
import types
import collections
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

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

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
        self.process_pending_tasks_condition = threading.Condition()

        # keep track if the task graph has been forcibly terminated
        self.terminated = False

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

        # this is the set of threads created by taskgraph
        self.thread_set = set()

        # launch threads to manage the workers
        for thread_id in xrange(n_workers):
            worker_thread = threading.Thread(
                target=self.worker, args=(self.work_queue,),
                name=thread_id)
            worker_thread.daemon = True
            worker_thread.start()
            self.thread_set.add(worker_thread)

        self.pending_process_task_ready = threading.Event()

        # launch thread to monitor the pending task set
        pending_task_thread = threading.Thread(
            target=self.process_pending_tasks,
            name='process_pending_tasks')
        pending_task_thread.daemon = True
        pending_task_thread.start()
        self.thread_set.add(pending_task_thread)

        # wait for the process pending task to be set up
        self.pending_process_task_ready.wait()

    def __del__(self):
        """Clean up task graph by injecting STOP sentinels."""
        self.close()

    def worker(self, work_queue):
        """Worker taking (func, args, kwargs) tuple from `work_queue`."""
        for func, args, kwargs in iter(work_queue.get, 'STOP'):
            try:
                func(*args, **kwargs)
            except Exception as subprocess_exception:
                # An error occurred on a call, terminate the taskgraph
                LOGGER.error(
                    "A taskgraph worker failed on function \"%s(%s, %s)\" "
                    "with exception \"%s\". "
                    "Terminating taskgraph.", func, args, kwargs,
                    subprocess_exception)
                self._terminate()
                return

    def _terminate(self):
        """Used to terminate remaining task graph computation on an error."""
        if self.terminated:
            return
        with self.process_pending_tasks_condition:
            self.pending_task_set.clear()
            try:
                while True:
                    self.work_queue.get_nowait()
            except Queue.Empty:
                self.close()
            for task in self.task_set:
                task.terminate()
            if self.n_workers > 0:
                self.worker_pool.terminate()
            self.terminated = True

    def close(self):
        """Prevent future tasks from being added to the work queue."""
        if self.closed:
            return
        with self.process_pending_tasks_condition:
            self.closed = True
            for _ in xrange(self.n_workers):
                self.work_queue.put('STOP')
            self.pending_task_set.add('STOP')
            self.process_pending_tasks_condition.notify()

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
        try:
            self.process_pending_tasks_condition.acquire()
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
                self.token_storage_path, self.process_pending_tasks_condition)
            self.task_set.add(task)

            if self.n_workers > 0:
                self.pending_task_set.add(task)
                self.process_pending_tasks_condition.notify()
            else:
                task(self.worker_pool)
            return task
        except Exception:
            # something went wrong, shut down the taskgraph
            self.close()
            raise
        finally:
            self.process_pending_tasks_condition.release()

    def process_pending_tasks(self):
        """Search pending task list for free tasks to work queue."""
        try:
            self.process_pending_tasks_condition.acquire()
            # set the flag to alert the init thread this process is ready
            # to be consistent on the process_pending_tasks_condition
            # condition
            self.pending_process_task_ready.set()
            while True:
                # There is a race condition where the taskgraph could finish
                # all adds and join before this function first acquires a
                # lock. In that case there is nothing left to notify the
                # thread.
                self.process_pending_tasks_condition.wait()
                queued_task_set = set()
                for task in self.pending_task_set:
                    if task == 'STOP':
                        return
                    try:
                        if all([d_task.is_complete()
                                for d_task in task.dependent_task_list]):
                            self.work_queue.put(
                                (task, (self.worker_pool,), {}))
                            queued_task_set.add(task)
                    except Exception:
                        # a dependent task failed, stop execution and raise
                        # exception
                        self.close()
                        raise
                self.pending_task_set = self.pending_task_set.difference(
                    queued_task_set)
        finally:
            self.process_pending_tasks_condition.release()

    def join(self):
        """Join all threads in the graph."""
        try:
            for task in self.task_set:
                task.join()
        except Exception as e:
            # If there's an exception on a join it means that a task failed
            # to execute correctly. Print a helpful message then terminate the
            # taskgraph object.
            LOGGER.error(
                "Exception \"%s\" raised when joining task %s. It's possible "
                "that this task did not cause the exception, rather another "
                "exception terminated the task_graph. Check the log to see "
                "if there are other exceptions.", e, task)
            self._terminate()
            raise


class Task(object):
    """Encapsulates work/task state for multiprocessing."""

    def __init__(
            self, task_id, func, args, kwargs, target_path_list,
            ignore_path_list, dependent_task_list, ignore_directories,
            token_storage_path, completion_condition):
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
            completion_condition (threading.Condition): this condition should
                be .notified() when the task successfully completes.
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
        self.completion_condition = completion_condition
        self.terminated = False

        # Used to ensure only one attempt at executing and also a mechanism
        # to see when Task is complete
        self.task_complete_event = threading.Event()

        # https://stackoverflow.com/questions/273192/how-can-i-create-a-directory-if-it-does-not-exist
        try:
            os.makedirs(token_storage_path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

    def __str__(self):
        return "Task object %s:\n" % (id(self)) + pprint.pformat(
            {
                "target_path_list": self.target_path_list,
                "dependent_task_list": self.dependent_task_list,
                "ignore_path_list": self.ignore_path_list,
                "ignore_directories": self.ignore_directories,
                "token_storage_path": self.token_storage_path,
                "token_path": self.token_path,
                "task_id": self.task_id,
                "completion_condition": self.completion_condition,
                "terminated": self.terminated,
            })

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
            if self.terminated:
                raise RuntimeError(
                    "Task %s was called after termination.", self.task_id)
            if self.dependent_task_list:
                for task in self.dependent_task_list:
                    task.join()

            token_id = self._calculate_token()
            self.token_path = os.path.join(self.token_storage_path, token_id)
            if self.is_complete():
                LOGGER.info(
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
                # write json string as target paths, file modified, file size
                file_stat_list = list(
                    _get_file_stats([self.target_path_list], [], False))
                token_file.write(json.dumps(file_stat_list))
        except Exception:
            self.terminate()
            raise
        finally:
            self.task_complete_event.set()
            with self.completion_condition:
                self.completion_condition.notify()

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
        if not self.task_complete_event.isSet():
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
        self.task_complete_event.wait()
        return self.is_complete()

    def terminate(self):
        """Invoke to terminate the Task."""
        self.terminated = True
        self.task_complete_event.set()


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
