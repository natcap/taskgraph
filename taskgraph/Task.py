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
import sqlite3

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

LOGGER = logging.getLogger('Task')


class TaskGraph(object):
    """Encapsulates the worker and tasks states for parallel processing."""

    def __init__(self, taskgraph_cache_path, n_workers):
        """Create a task graph.

        Creates an object for building task graphs, executing them,
        parallelizing independent work notes, and avoiding repeated calls.

        Parameters:
            taskgraph_cache_path (string): path to a file that either contains
                a taskgraph cache from a previous instance or will create
                one if none exists.
            n_workers (int): number of parallel workers to allow during
                task graph execution.  If set to 0, use current process.
        """
        # https://stackoverflow.com/questions/273192/how-can-i-create-a-directory-if-it-does-not-exist
        try:
            os.makedirs(taskgraph_cache_path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

        # the work queue is the feeder to active worker threads
        self.work_queue = Queue.Queue()
        self.n_workers = n_workers

        # used to synchronize a pass through potential tasks to add to the
        # work queue
        self.process_pending_tasks_event = threading.Event()

        # keep track if the task graph has been forcibly terminated
        self.terminated = False

        # if n_workers > 0 this will be a multiprocessing pool used to execute
        # the __call__ functions in Tasks
        self.worker_pool = None
        if n_workers > 0:
            self.worker_pool = multiprocessing.Pool(n_workers)
            if HAS_PSUTIL:
                parent = psutil.Process()
                parent.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
                for child in parent.children():
                    try:
                        child.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
                    except psutil.NoSuchProcess:
                        LOGGER.warn(
                            "NoSuchProcess exception encountered when trying "
                            "to nice %s. This might be a bug in `psutil` so "
                            "it should be okay to ignore.")

        # use this to keep track of all the tasks added to the graph by their
        # task ids. Used to determine if an identical task has been added
        # to the taskgraph during `add_task`
        self.task_id_map = dict()

        # used to remember if task_graph has been closed
        self.closed = False

        # this is the set of threads created by taskgraph
        self.thread_set = set()

        # launch threads to manage the workers
        for thread_id in xrange(n_workers):
            worker_thread = threading.Thread(
                target=self._task_worker, args=(self.work_queue,),
                name='taskgraph_worker_thread_%d' % thread_id)
            worker_thread.daemon = True
            worker_thread.start()
            self.thread_set.add(worker_thread)

        # tasks that get passed right to add_task get put in this queue for
        # scheduling
        self.pending_task_queue = Queue.Queue()

        # launch thread to monitor the pending task set
        pending_task_scheduler = threading.Thread(
            target=self._process_pending_tasks,
            name='_process_pending_task_scheduler')
        pending_task_scheduler.daemon = True
        pending_task_scheduler.start()
        self.thread_set.add(pending_task_scheduler)

    def _task_worker(self):
        """Execute and manage Task objects.

        This worker extracts (task object, args, kwargs) tuples from
        `self.work_queue`, processes the return value to ensure either a
        successful completion OR handle an error.  On successful completion
        the task's hash and dependent files are recorded in TaskGraph's
        cache structure to test and prevent future-re-executions.
        """
        for task_object, args, kwargs in iter(self.work_queue.get, 'STOP'):
            try:
                task_object._call(*args, **kwargs)
            except Exception as subprocess_exception:
                # An error occurred on a call, terminate the taskgraph
                LOGGER.error(
                    "A taskgraph _task_worker failed on function "
                    "\"%s(%s, %s)\" with exception \"%s\". "
                    "Terminating taskgraph.", task_object, args, kwargs,
                    subprocess_exception)
                self._terminate()
                return

    def add_task(
            self, func=None, args=None, kwargs=None, task_name=None,
            target_path_list=None, ignore_path_list=None,
            dependent_task_list=None, ignore_directories=True):
        """Add a task to the task graph.

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
                func = lambda: None

            task_name = '%s_%d' % (task_name, len(self.task_set))
            new_task = Task(
                task_name, func, args, kwargs, target_path_list,
                ignore_path_list, dependent_task_list, ignore_directories,
                self.process_pending_tasks_event)
            task_hash = new_task.task_hash

            # it may be this task was already created in an earlier call,
            # use that object in its place
            if task_hash in self.task_id_map:
                return self.task_id_map[task_hash]

            self.task_id_map[task_hash] = new_task
            self.pending_task_queue.put(new_task)
            self.process_pending_tasks_event.set()
            return new_task

        except Exception:
            # something went wrong, shut down the taskgraph
            self._terminate()
            raise

    def _process_pending_tasks(self):
        """Drain work queue and iterate through pending task set for tasks."""

        # this will hold all the tasks whose dependencies have not been
        # satisfied on a previous iteration, but may now be
        pending_task_set = set()

        # this will remember if we've encountered a STOP sentinel
        stop_work = False
        # this is the main processing loop
        while True:
            # this will remember the tasks that were queued to the work queue
            # on this iteration
            queued_task_set = set()
            while not stop_work:
                # drain the pending task queue into the pending task set
                try:
                    task = self.pending_task_queue.get(False)
                    if task == 'STOP':
                        stop_work = True
                        break
                    pending_task_set.add(task)
                except Queue.Empty:
                    break

            # Process all pending tasks and put them on the work
            # queue if their dependencies are satisfied
            for task in pending_task_set:
                try:
                    if all([dep_task.is_complete()
                            for dep_task in task.dependent_task_list]):
                        self.work_queue.put(
                            (task, (self.worker_pool,), {}))
                        queued_task_set.add(task)
                except Exception as e:
                    LOGGER.error("Dependent task failed. Exception: %s", e)
                    self._terminate()
                    return

            pending_task_set = pending_task_set.difference(
                queued_task_set)
            if not pending_task_set and stop_work:
                return
            self.process_pending_tasks_event.wait()
            self.process_pending_tasks_event.clear()

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

    def close(self):
        """Prevent future tasks from being added to the work queue."""
        if self.closed:
            return
        self.closed = True
        for _ in xrange(self.n_workers):
            self.work_queue.put('STOP')
        self.pending_task_queue.put('STOP')

    def _terminate(self):
        """Forcefully terminate remaining task graph computation."""
        if self.terminated:
            return
        self.close()
        if self.n_workers > 0:
            self.worker_pool.terminate()
        self.terminated = True


class Task(object):
    """Encapsulates work/task state for multiprocessing."""

    def __init__(
            self, task_name, func, args, kwargs, target_path_list,
            ignore_path_list, dependent_task_list, ignore_directories):
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
            target_path_list (list): list of filepaths expected
                to be generated by this func and args/kwargs.
            ignore_path_list (list): list of file paths that could be in
                args/kwargs that should be ignored when considering timestamp
                hashes.
            ignore_directories (boolean): if the existence/timestamp of any
                directories discovered in args or kwargs is used as part
                of the work token hash.
        """
        self.task_name = task_name
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.target_path_list = target_path_list
        self.dependent_task_list = dependent_task_list
        self.target_path_list = target_path_list
        self.ignore_path_list = ignore_path_list
        self.ignore_directories = ignore_directories

        self.terminated = False
        self.exception_object = None

        # Used to ensure only one attempt at executing and also a mechanism
        # to see when Task is complete
        self.task_complete_event = threading.Event()

        # calculate the unique hash of the Task
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

        # This gets a list of the files and their file stats that can be found
        # in args and kwargs but ignores anything specifically targeted or
        # an expected result. This will allow a task to change its hash in
        # case a different version of a file was passed in.
        file_stat_list = list(_get_file_stats(
            [self.args, self.kwargs],
            self.target_path_list+self.ignore_path_list,
            self.ignore_directories))

        task_string = '%s:%s:%s:%s:%s:%s' % (
            self.func.__name__, pickle.dumps(self.args),
            json.dumps(self.kwargs, sort_keys=True), source_code,
            self.target_path_list, str(file_stat_list))

        self.task_hash = hashlib.sha1(task_string).hexdigest()

    def __str__(self):
        return "Task object %s:\n\n" % (id(self)) + pprint.pformat(
            {
                "task_name": self.task_name,
                "target_path_list": self.target_path_list,
                "dependent_task_list": self.dependent_task_list,
                "ignore_path_list": self.ignore_path_list,
                "ignore_directories": self.ignore_directories,
                "task_hash": self.task_hash,
                "terminated": self.terminated,
                "exception_object": self.exception_object,
            })

    @profile
    def _call(
            self, global_worker_pool):
        """TaskGraph should invoke this method when ready to execute task.

        Precondition is that the Task dependencies are satisfied.

        Parameters:
            global_worker_pool (multiprocessing.Pool): a process pool used to
                execute subprocesses.  If None, use current process.

        Raises:
            RuntimeError if any target paths are not generated after the
                function call is complete.

        Returns:
            A list of file parameters of the target path list.


        """
        try:
            if self.terminated:
                raise RuntimeError(
                    "Task %s was called after termination.", self.task_id)

            if global_worker_pool is not None:
                result = global_worker_pool.apply_async(
                    func=self.func, args=self.args, kwds=self.kwargs)
                result.get()
            else:
                self.func(*self.args, **self.kwargs)

            missing_target_paths = [
                target_path for target_path in self.target_path_list
                if not os.path.exists(path)]
            if len(missing_target_paths) > 0:
                raise RuntimeError(
                        "The following paths were expected but not found "
                        "after the function call: %s" % missing_target_paths)
            # otherwise successful run, return the target path expected stats.
            return list(
                _get_file_stats([self.target_path_list], [], False))
        finally:
            self.task_complete_event.set()

    @profile
    def _valid_token(self):
        """Determine if `self.token_path` represents a valid token.

        Returns:
            True if files referenced in json file at `self.token_path` exist,
            modified times are equal to the modified times recorded in the
            token record, and the size is the same as in the recorded record.
        """
        try:
            db_connection = sqlite3.connect(self.taskgraph_cache_path)
            cursor = db_connection.cursor()
            cursor.execute(
                """SELECT json_data FROM task_tokens WHERE hash=?;""",
                (self.task_hash,))
            result = cursor.fetchone()
            if result is None:
                return False

            for path, modified_time, size in json.loads(result[0]):
                if not (os.path.exists(path) and
                        modified_time == os.path.getmtime(path) and
                        size == os.path.getsize(path)):
                    return False
        except Exception as e:
            print e
            return False
        finally:
            db_connection.close()

        return True

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
        return True

    def join(self):
        """Block until task is complete, raise exception if runtime failed."""
        self.task_complete_event.wait()
        return self.is_complete()

    def terminate(self, exception_object=None):
        """Invoke to terminate the Task."""
        self.terminated = True
        self.exception_object = exception_object
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
