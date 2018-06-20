"""Task graph framework."""
import heapq
import pprint
import collections
import hashlib
import json
import pickle
import os
import logging
import multiprocessing
import threading
import errno
try:
    import Queue as queue
except ImportError:
    # Python3 renamed queue as queue
    import queue
import inspect
import abc

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

LOGGER = logging.getLogger('Task')


try:
    dict.itervalues
except AttributeError:
    # Python 3
    # range is an iterator in python3.
    xrange = range
    # In python2, basestring is the common superclass of str and unicode.  In
    # python3, we'll probably only be dealing with str objects.
    basestring = str
    def itervalues(d):
        """Python 2/3 compatibility iterator over d.values()"""
        return iter(d.values())
else:
    # Python 2
    def itervalues(d):
        """Python 2/3 compatibility alias for d.itervalues()"""
        return d.itervalues()


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
            n_workers (int): number of parallel *subprocess* workers to allow
                during task graph execution.  If set to 0, don't use
                subprocesses.  If set to <0, use only the main thread for any
                execution and scheduling. In the case of the latter,
                `add_task` will be a blocking call.

        """
        # the work queue is the feeder to active worker threads
        self.taskgraph_cache_dir_path = taskgraph_cache_dir_path
        self.n_workers = n_workers

        # keep track if the task graph has been forcibly terminated
        self.terminated = False

        # use this to keep track of all the tasks added to the graph by their
        # task ids. Used to determine if an identical task has been added
        # to the taskgraph during `add_task`
        self.task_id_map = dict()

        # used to remember if task_graph has been closed
        self.closed = False

        # if n_workers > 0 this will be a multiprocessing pool used to execute
        # the __call__ functions in Tasks
        self.worker_pool = None

        # no need to set up schedulers if n_workers is single threaded
        if n_workers < 0:
            return

        # set up multrpocessing if n_workers > 0
        if n_workers > 0:
            self.worker_pool = multiprocessing.Pool(n_workers)
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

        # used to synchronize a pass through potential tasks to add to the
        # work queue
        self.work_queue = queue.Queue()
        self.worker_semaphore = threading.Semaphore(max(1, n_workers))
        # launch threads to manage the workers
        for thread_id in xrange(max(1, n_workers)):
            worker_thread = threading.Thread(
                target=self._task_worker,
                name='taskgraph_worker_thread_%d' % thread_id)
            worker_thread.daemon = True
            worker_thread.start()

        # tasks that get passed add_task get put in this queue for scheduling
        self.waiting_task_queue = queue.Queue()
        waiting_task_scheduler = threading.Thread(
            target=self._process_waiting_tasks,
            name='_waiting_task_scheduler')
        waiting_task_scheduler.daemon = True
        waiting_task_scheduler.start()

        # tasks in the work ready queue have dependencies satisfied but need
        # priority scheduling
        self.work_ready_queue = queue.Queue()
        priority_task_scheduler = threading.Thread(
            target=self._schedule_priority_tasks,
            name='_priority_task_scheduler')
        priority_task_scheduler.daemon = True
        priority_task_scheduler.start()

    def _task_worker(self):
        """Execute and manage Task objects."""
        for task in iter(self.work_queue.get, 'STOP'):
            try:
                # precondition: task wouldn't be in queue if it were
                # precalculated
                task._call()
                self.worker_semaphore.release()
                self.waiting_task_queue.put((task, 'done'))
            except Exception:
                # An error occurred on a call, terminate the taskgraph
                LOGGER.exception(
                    'A taskgraph _task_worker failed on Task '
                    '%s. Terminating taskgraph.', task)
                self._terminate()
                raise

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
                work queue in order of decreasing priority. This value can be
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
                func = lambda: None

            task_name = '%s_%d' % (task_name, len(self.task_id_map))
            new_task = Task(
                task_name, func, args, kwargs, target_path_list,
                ignore_path_list, dependent_task_list, ignore_directories,
                self.worker_pool, self.taskgraph_cache_dir_path, priority)
            task_hash = new_task.task_hash

            # it may be this task was already created in an earlier call,
            # use that object in its place
            if task_hash in self.task_id_map:
                return self.task_id_map[task_hash]

            self.task_id_map[task_hash] = new_task

            if self.n_workers < 0:
                # call directly if single threaded
                if not new_task.is_precalculated():
                    new_task._call()
            else:
                # send to scheduler
                if not new_task.is_precalculated():
                    self.waiting_task_queue.put((new_task, 'wait'))
                else:
                    # this is a shortcut to clear pre-calculated tasks
                    new_task._task_complete_event.set()
                    self.waiting_task_queue.put((new_task, 'done'))

            return new_task

        except Exception:
            # something went wrong, shut down the taskgraph
            self._terminate()
            raise

    def _schedule_priority_tasks(self):
        """Priority schedules the `self.work_ready` queue.

        Reads the `self.work_ready` queue and feeds in highest priority tasks
        when the self.work_queue is ready for them.
        """
        stopped = False
        priority_queue = []
        while not stopped:
            while True:
                try:
                    # only block if the priority queque is empty
                    task = self.work_ready_queue.get(not priority_queue)
                    if task == 'STOP':
                        # encounter STOP so break and don't get more elements
                        stopped = True
                        break
                    # push task to priority queue
                    heapq.heappush(priority_queue, task)
                except queue.Empty:
                    # this triggers when work_ready_queue is empty and
                    # there's something in the work_ready_queue
                    break
            # only put elements if there are workers available
            self.worker_semaphore.acquire()
            while priority_queue:
                # push high priority on the queue until queue is full
                # or if thread is stopped, drain the priority queue
                self.work_queue.put(priority_queue[0])
                heapq.heappop(priority_queue)
                if not stopped:
                    # by stopping after one put, we can give the chance for
                    # other higher priority tasks to flow in
                    break
        # got a 'STOP' so signal worker threads to stop too
        for _ in xrange(max(1, self.n_workers)):
            self.work_queue.put('STOP')

    def _process_waiting_tasks(self):
        """Process any tasks that are waiting on dependencies.

        This worker monitors the self.waiting_task_queue queue and looks for
        (task, 'wait'), or (task, 'done') tuples.

        If mode is 'wait' the task is indexed locally with reference to
        its incomplete tasks. If its dependent tasks are complete, the
        task is sent to the work queue. If mode is 'done' this signals the
        worker to re-'wait' any task that was dependent on the one that
        arrived in the queue.
        """
        task_dependent_map = collections.defaultdict(set)
        dependent_task_map = collections.defaultdict(set)
        completed_tasks = set()
        for task, mode in iter(self.waiting_task_queue.get, 'STOP'):
            tasks_ready_to_work = set()
            if mode == 'wait':
                # see if this task's dependencies are satisfied, if so send
                # to work.
                outstanding_dependent_task_list = [
                    dep_task for dep_task in task.dependent_task_list
                    if dep_task not in completed_tasks]
                if not outstanding_dependent_task_list:
                    # if nothing is outstanding, send to work queue
                    tasks_ready_to_work.add(task)

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
                if task not in task_dependent_map:
                    # this can occur if add_task identifies task is complete
                    # before any other analysis.
                    continue
                for waiting_task in task_dependent_map[task]:
                    # remove `task` from the set of tasks that `waiting_task`
                    # was waiting on.
                    dependent_task_map[waiting_task].remove(task)
                    # if there aren't any left, we can push `waiting_task`
                    # to the work queue
                    if not dependent_task_map[waiting_task]:
                        # if we removed the last task we can put it to the
                        # work queue
                        tasks_ready_to_work.add(waiting_task)
                del task_dependent_map[task]
            for ready_task in sorted(
                    tasks_ready_to_work, key=lambda x: x.priority):
                self.work_ready_queue.put(ready_task)
            tasks_ready_to_work = None
        # if we got here, the waiting task queue is shut down, pass signal
        # to the lower queue
        self.work_ready_queue.put('STOP')

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
        if self.n_workers < 0:
            return True
        try:
            timedout = False
            for task in itervalues(self.task_id_map):
                timedout = not task.join(timeout)
                # if the last task timed out then we want to timeout for all
                # of the task graph
                if timedout:
                    break
            if self.closed:
                # inject sentinels to the queues
                self.waiting_task_queue.put('STOP')
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

    def _terminate(self):
        """Forcefully terminate remaining task graph computation."""
        if self.terminated:
            return
        self.close()
        if self.n_workers > 0:
            self.worker_pool.terminate()
        for task in itervalues(self.task_id_map):
            task._terminate()
        self.terminated = True


class Task(object):
    """Encapsulates work/task state for multiprocessing."""

    def __init__(
            self, task_name, func, args, kwargs, target_path_list,
            ignore_path_list, dependent_task_list, ignore_directories,
            worker_pool, cache_dir, priority):
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
            priority (numeric): the priority of a task is considered when
                there is more than one task whose dependencies have been
                met and are ready for scheduling. Tasks are inserted into the
                work queue in order of decreasing priority. This value can be
                positive, negative, and/or floating point.

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
        # invert the priority since heapq goes smallest to largest
        self.priority = -priority

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

        self.task_hash = hashlib.sha1(task_string.encode('utf-8')).hexdigest()

        # get ready to make a directory and target based on hashname
        # take the first 3 characters of the hash and make a subdirectory
        # for each so we don't blowup the filesystem with a bunch of files in
        # one directory
        self.task_cache_path = os.path.join(
            cache_dir, *(
                [x for x in self.task_hash[0:3]] +
                [self.task_hash + '.json']))

    def __eq__(self, other):
        """Two tasks are equal if their hashes are equal."""
        if isinstance(self, other.__class__):
            return self.task_hash == other.task_hash
        return False

    def __hash__(self):
        """Return the base-16 integer hash of this hash string."""
        return int(self.task_hash, 16)

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
        try:
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
        except EOFError:
            return False

    def join(self, timeout=None):
        """Block until task is complete, raise exception if runtime failed."""
        self._task_complete_event.wait(timeout)
        return self.is_complete()

    def _terminate(self, exception_object=None):
        """Invoke to terminate the Task."""
        self.terminated = True
        self.exception_object = exception_object
        self._task_complete_event.set()


class EncapsulatedTaskOp(ABC):
    """Used as a superclass for Task operations that need closures.

    This class will automatically hash the subclass's __call__ method source
    as well as the arguments to its __init__ function to calculate the
    Task's unique hash.

    """
    def __init__(self, *args, **kwargs):
        # try to get the source code of __call__ so task graph will recompute
        # if the function has changed
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
    elif isinstance(base_value, collections.Mapping):
        for key in sorted(base_value.keys()):
            value = base_value[key]
            for stat in _get_file_stats(
                    value, ignore_list, ignore_directories):
                yield stat
    elif isinstance(base_value, collections.Iterable):
        for value in base_value:
            for stat in _get_file_stats(
                    value, ignore_list, ignore_directories):
                yield stat
