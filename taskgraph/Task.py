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

    def __init__(self, taskgraph_cache_dir_path, n_workers):
        """Create a task graph.

        Creates an object for building task graphs, executing them,
        parallelizing independent work notes, and avoiding repeated calls.

        Parameters:
            taskgraph_cache_dir_path (string): path to a directory that
                either contains a taskgraph cache from a previous instance or
                will create a new one if none exists.
            n_workers (int): number of parallel workers to allow during
                task graph execution.  If set to 0, use current process.
        """
        # the work queue is the feeder to active worker threads
        self.taskgraph_cache_dir_path = taskgraph_cache_dir_path
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
        for thread_id in xrange(max(1, n_workers)):
            worker_thread = threading.Thread(
                target=self._task_worker,
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
            name='_pending_task_scheduler')
        pending_task_scheduler.daemon = True
        pending_task_scheduler.start()
        self.thread_set.add(pending_task_scheduler)

        self.waiting_task_queue = Queue.Queue()

        waiting_task_scheduler = threading.Thread(
            target=self._process_waiting_tasks,
            name='_waiting_task_scheduler')
        waiting_task_scheduler.daemon = True
        waiting_task_scheduler.start()
        self.thread_set.add(waiting_task_scheduler)

    def _task_worker(self):
        """Execute and manage Task objects.

        This worker extracts (task object, args, kwargs) tuples from
        `self.work_queue`, processes the return value to ensure either a
        successful completion OR handle an error.  On successful completion
        the task's hash and dependent files are recorded in TaskGraph's
        cache structure to test and prevent future-re-executions.
        """
        for task in iter(self.work_queue.get, 'STOP'):
            try:
                if not task.is_precalculated():
                    target_path_stats = task._call()
                else:
                    task._task_complete_event.set()
                # task complete, signal to pending task scheduler that this
                # task is complete
                self.waiting_task_queue.put((task, 'done'))
            except Exception as subprocess_exception:
                # An error occurred on a call, terminate the taskgraph
                LOGGER.exception(
                    'A taskgraph _task_worker failed on Task '
                    '%s with exception "%s". '
                    'Terminating taskgraph.', task, subprocess_exception)
                self._terminate()
                raise

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

            task_name = '%s_%d' % (task_name, len(self.task_id_map))
            new_task = Task(
                task_name, func, args, kwargs, target_path_list,
                ignore_path_list, dependent_task_list, ignore_directories,
                self.worker_pool, self.taskgraph_cache_dir_path)
            task_hash = new_task.task_hash

            # it may be this task was already created in an earlier call,
            # use that object in its place
            if task_hash in self.task_id_map:
                return self.task_id_map[task_hash]

            self.task_id_map[task_hash] = new_task
            self.pending_task_queue.put(new_task)
            return new_task

        except Exception:
            # something went wrong, shut down the taskgraph
            self._terminate()
            raise

    def _process_pending_tasks(self):
        """Process pending task queue, send ready tasks to work queue.

        There are two reasons a task will be on the pending_task_queue. One
        is to potentially process it for work. The other is to alert that
        the task is complete and any tasks that were dependent on it may
        now be processed.
        """
        tasks_sent_to_work = set()
        for task in iter(self.pending_task_queue.get, 'STOP'):
            # invariant: a task coming in was put in the queue before it was
            #   complete and because it was a dependent task of another task
            #   that completed. OR a task is complete and alerting that any
            #   tasks that were dependent on it can be processed.

            # it's a new task, check and see if its dependencies are complete
            outstanding_dependent_task_list = [
                dep_task for dep_task in task.dependent_task_list
                if not dep_task.is_complete()]

            if outstanding_dependent_task_list:
                # if outstanding tasks, delay execution and put a reminder
                # that this task is dependent on another
                self.waiting_task_queue.put((task, 'wait'))
            elif task.task_hash not in tasks_sent_to_work:
                # otherwise if not already sent to work, put in the work queue
                # and record it was sent
                tasks_sent_to_work.add(task.task_hash)
                self.work_queue.put(task)

    def _process_waiting_tasks(self):
        """Process any tasks that are waiting on dependencies.

        This worker monitors the self.waiting_task_queue Queue and looks for
        (task, 'wait'), or (task, 'done') tuples.

            If mode is 'wait' the task is indexed locally with reference to
            its incomplete tasks. If its depedent tasks are complete, the
            task is sent to the work queue. If mode is 'done' this signals the
            worker to re-'wait' any task that was dependent on the one that
            arrived in the queue.
        """
        task_dependent_map = collections.defaultdict(set)
        dependent_task_map = collections.defaultdict(set)
        completed_tasks = set()
        for task, mode in iter(self.waiting_task_queue.get, 'STOP'):
            if mode == 'wait':
                # invariant: task has come directly from `add_task` and has
                # been determined that is has at least one unsatisfied
                # dependency

                outstanding_dependent_task_list = [
                    dep_task for dep_task in task.dependent_task_list
                    if dep_task not in completed_tasks]
                # possible a dependency has been satisfied since `add_task`
                # was able to add this task to the waiting queue.
                if not outstanding_dependent_task_list:
                    # if nothing is outstanding, send to work queue
                    self.work_queue.put(task)
                    continue

                # there are unresolved tasks that the waiting process
                # scheduler has not been notified of. Record dependencies.
                for dep_task in outstanding_dependent_task_list:
                    # keep track of the tasks that are dependent on dep_task
                    task_dependent_map[dep_task].add(task)
                    # keep track of the tasks that prevent this one from
                    # executing
                    dependent_task_map[task].add(dep_task)
            elif mode == 'done':
                # invariant: task has not previously been sent as a 'done'
                # notification and task is done.
                completed_tasks.add(task)
                for waiting_task in task_dependent_map[task]:
                    # remove `task` from the set of tasks that `waiting_task`
                    # was waiting on.
                    dependent_task_map[waiting_task].remove(task)
                    # if there aren't any left, we can push `waiting_task`
                    # to the work queue
                    if not dependent_task_map[waiting_task]:
                        # if we removed the last task we can put it to the
                        # work queue
                        self.work_queue.put(waiting_task)
                del task_dependent_map[task]
        # if we got here, the waiting task queue is shut down, pass signal
        # to the workers
        for _ in xrange(max(1, self.n_workers)):
            self.work_queue.put('STOP')

    def join(self, timeout=None):
        """Join all threads in the graph.

        Parameters:
            timeout (float): if not none will attempt to join subtasks with
                this value. If a subtask times out, the whole function will
                timeout.

        Returns:
            True if successful join, False if timed out.
        """
        try:
            timedout = False
            for task in self.task_id_map.itervalues():
                timedout = not task.join(timeout)
                # if the last task timed out then we want to timeout for all
                # of the task graph
                if timedout:
                    break
            if self.closed:
                # inject sentinels to the queues
                self.waiting_task_queue.put('STOP')
                for _ in xrange(max(1, self.n_workers)):
                    self.work_queue.put('STOP')
            return not timedout
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
        self.pending_task_queue.put('STOP')

    def _terminate(self):
        """Forcefully terminate remaining task graph computation."""
        LOGGER.debug("********* calling _terminate")
        if self.terminated:
            return
        self.close()
        if self.n_workers > 0:
            self.worker_pool.terminate()
        for task in self.task_id_map.itervalues():
            task._terminate()
        self.terminated = True


class Task(object):
    """Encapsulates work/task state for multiprocessing."""

    def __init__(
            self, task_name, func, args, kwargs, target_path_list,
            ignore_path_list, dependent_task_list, ignore_directories,
            worker_pool, cache_dir):
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
            worker_pool (multiprocessing.Pool): if not None, is a
                multiprocessing pool that can be used for `_call` execution.
            cache_dir (string): path to a directory to both write and expect
                data recorded from a previous Taskgraph run.
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
        self.worker_pool = worker_pool

        self.terminated = False
        self.exception_object = None

        # Used to ensure only one attempt at executing and also a mechanism
        # to see when Task is complete
        self._task_complete_event = threading.Event()

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

        # get ready to make a directory and target based on hashname
        # take the first 3 characters of the hash and make a subdirectory
        # for each so we don't blowup the filesystem with a bunch of files in
        # one directory
        self.task_cache_path = os.path.join(
            cache_dir, *(
                [x for x in self.task_hash[0:3]] +
                [self.task_hash + '.json']))

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
                    "Task %s was called after termination.", self.task_hash)

            if self.worker_pool is not None:
                result = self.worker_pool.apply_async(
                    func=self.func, args=self.args, kwds=self.kwargs)
                result.get()
            else:
                self.func(*self.args, **self.kwargs)

            missing_target_paths = [
                target_path for target_path in self.target_path_list
                if not os.path.exists(target_path)]
            if len(missing_target_paths) > 0:
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
                        self.task_name, target_path_list,
                        result_target_path_set))

            # otherwise record target path stats in a file located at
            # self.task_cache_path
            try:
                os.makedirs(os.path.dirname(self.task_cache_path))
            except OSError as exception:
                if exception.errno != errno.EEXIST:
                    raise
            with open(self.task_cache_path, 'wb') as task_cache_file:
                pickle.dump(result_target_path_stats, task_cache_file)

            # successful run, return target path stats
            return result_target_path_stats
        except Exception as e:
            LOGGER.error("Exception %s in Task: %s" % (e, self))
            self._terminate(e)
            raise
        finally:
            self._task_complete_event.set()

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
        if not self._task_complete_event.isSet():
            # lock is still acquired, so it's not done yet.
            return False
        return True

    def is_precalculated(self):
        """Return true Task can be skipped.

        Returns:
            True if the Task's target paths exist in the same state as the
            last recorded run. False otherwise.
        """
        if not os.path.exists(self.task_cache_path):
            return False
        with open(self.task_cache_path, 'rb') as task_cache_file:
            result_target_path_stats = pickle.load(task_cache_file)
        for path, modified_time, size in result_target_path_stats:
            if not (os.path.exists(path) and
                    modified_time == os.path.getmtime(path) and
                    size == os.path.getsize(path)):
                return False
        return True

    def join(self, timeout=None):
        """Block until task is complete, raise exception if runtime failed."""
        self._task_complete_event.wait(timeout)
        return self.is_complete()

    def _terminate(self, exception_object=None):
        """Invoke to terminate the Task."""
        self.terminated = True
        self.exception_object = exception_object
        self._task_complete_event.set()


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
