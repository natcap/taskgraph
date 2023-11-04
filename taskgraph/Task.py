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
try:
    from importlib.metadata import PackageNotFoundError
    from importlib.metadata import version
except ImportError:
    # importlib.metadata added to stdlib in 3.8
    from importlib_metadata import PackageNotFoundError
    from importlib_metadata import version

import retrying

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


def _create_taskgraph_table_schema(taskgraph_database_path):
    """Create database exists and/or ensures it is compatible and recreate.

    Args:
        taskgraph_database_path (str): path to an existing database or desired
            location of a new database.

    Returns:
        None.

    """
    sql_create_projects_table_script = (
        """
        CREATE TABLE taskgraph_data (
            task_reexecution_hash TEXT NOT NULL,
            target_path_stats BLOB NOT NULL,
            result BLOB NOT NULL,
            PRIMARY KEY (task_reexecution_hash)
        );
        CREATE TABLE global_variables (
            key TEXT NOT NULL,
            value BLOB,
            PRIMARY KEY (key)
        );
        """)

    table_valid = True
    expected_table_column_name_map = {
        'taskgraph_data': [
            'task_reexecution_hash', 'target_path_stats', 'result'],
        'global_variables': ['key', 'value']}
    if os.path.exists(taskgraph_database_path):
        try:
            # check that the tables exist and the column names are as expected
            for expected_table_name in expected_table_column_name_map:
                table_result = _execute_sqlite(
                    '''
                    SELECT name
                    FROM sqlite_master
                    WHERE type='table' AND name=?
                    ''', taskgraph_database_path,
                    argument_list=[expected_table_name],
                    mode='read_only', execute='execute', fetch='all')
                if not table_result:
                    raise ValueError(f'missing table {expected_table_name}')

                # this query returns a list of results of the form
                # [(0, 'task_reexecution_hash', 'TEXT', 1, None, 1), ... ]
                # we'll just check that the header names are the same, no
                # need to be super aggressive, also need to construct the
                # PRAGMA string directly since it doesn't take arguments
                table_info_result = _execute_sqlite(
                    f'PRAGMA table_info({expected_table_name})',
                    taskgraph_database_path, mode='read_only',
                    execute='execute', fetch='all')

                expected_column_names = expected_table_column_name_map[
                    expected_table_name]
                header_count = 0
                for header_line in table_info_result:
                    column_name = header_line[1]
                    if column_name not in expected_column_names:
                        raise ValueError(
                            f'expected {column_name} in table '
                            f'{expected_table_name} but not found')
                    header_count += 1
                if header_count < len(expected_column_names):
                    raise ValueError(
                        f'found only {header_count} of an expected '
                        f'{len(expected_column_names)} columns in table '
                        f'{expected_table_name}')
                if not table_info_result:
                    raise ValueError(f'missing table {expected_table_name}')
        except Exception:
            # catch all "Exception"s because anything that goes wrong while
            # checking the database should be considered a bad database and we
            # should make a new one.
            LOGGER.exception(
                f'{taskgraph_database_path} exists, but is incompatible '
                'somehow. Deleting and making a new one.')
            os.remove(taskgraph_database_path)
            table_valid = False
    else:
        # table does not exist
        table_valid = False

    if not table_valid:
        # create the base table
        _execute_sqlite(
            sql_create_projects_table_script, taskgraph_database_path,
            mode='modify', execute='script')
        # set the database version
        _execute_sqlite(
            '''
            INSERT OR REPLACE INTO global_variables
            VALUES ("version", ?)
            ''', taskgraph_database_path, mode='modify',
            argument_list=(__version__,))


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

        # create new table if needed
        _create_taskgraph_table_schema(self._task_database_path)

        # check the version of the database and warn if a problem
        local_version = _execute_sqlite(
            '''
            SELECT value
            FROM global_variables
            WHERE key=?
            ''', self._task_database_path, mode='read_only',
            fetch='one', argument_list=['version'])[0]
        if local_version != __version__:
            LOGGER.warning(
                f'the database located at {self._task_database_path} was '
                f'created with TaskGraph version {local_version} but the '
                f'current version is {__version__}')

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

        # launch executor threads
        self._executor_thread_count = max(0, n_workers)
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
                            "it should be okay to ignore.")

    def __del__(self):
        """Ensure all threads have been joined for cleanup."""
        self._terminate()

    def _task_executor(self):
        """Worker that executes Tasks that have satisfied dependencies."""
        while True:
            # this event blocks until the task graph has signaled it wants
            # the executors to read the state of the queue or a stop event or
            # a timeout exceeded just to protect against a worst case deadlock
            self._executor_ready_event.wait(_MAX_TIMEOUT)
            # this lock synchronizes changes between the queue and
            # executor_ready_event
            if self._terminated:
                LOGGER.debug(
                    "taskgraph is terminated, ending %s",
                    threading.current_thread())
                break
            task = None
            try:
                task = self._task_ready_priority_queue.get_nowait()
                self._task_waiting_count -= 1
                task_name_time_tuple = (task.task_name, time.time())
                self._active_task_list.append(task_name_time_tuple)
            except queue.Empty:
                # no tasks are waiting could be because the taskgraph is
                # closed or because the queue is just empty.
                if (self._closed and len(self._completed_task_names) ==
                        self._added_task_count):
                    # the graph is closed and there are as many completed tasks
                    # as there are added tasks, so none left. The executor can
                    # terminate.
                    self._executor_thread_count -= 1
                    if self._executor_thread_count == 0 and self._worker_pool:
                        # only the last executor should terminate the worker
                        # pool, because otherwise who knows if it's still
                        # executing anything
                        try:
                            self._worker_pool.close()
                            self._worker_pool.terminate()
                            self._worker_pool = None
                            self._terminate()
                        except Exception:
                            # there's the possibility for a race condition here
                            # where another thread already closed the worker
                            # pool, so just guard against it
                            LOGGER.warning('worker pool was already closed')
                    LOGGER.debug(
                        "no tasks are pending and taskgraph closed, normally "
                        "terminating executor %s." % threading.current_thread())
                    break
                else:
                    # there's still the possibility for work to be added or
                    # still work in the pipeline
                    self._executor_ready_event.clear()
            if task is None:
                continue
            try:
                task._call()
                task.task_done_executing_event.set()
            except Exception as e:
                # An error occurred on a call, terminate the taskgraph
                task.exception_object = e
                LOGGER.exception(
                    'A taskgraph _task_executor failed on Task '
                    '%s. Terminating taskgraph.', task.task_name)
                self._terminate()
                break

            LOGGER.debug(
                "task %s is complete, checking to see if any dependent "
                "tasks can be executed now", task.task_name)
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
            # this extra set ensures that recently emptied map won't get
            # ignored by the executor if no work is left to do and the graph is
            # closed
            self._executor_ready_event.set()
            LOGGER.debug("task %s done processing", task.task_name)
        LOGGER.debug("task executor shutting down")

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
                transient_run, self._worker_pool,
                priority, hash_algorithm, store_result,
                self._task_database_path)

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
        LOGGER.debug("joining taskgraph")
        if self._n_workers < 0:
            # Join() is meaningless since tasks execute synchronously.
            LOGGER.debug(
                'n_workers: %s; join is vacuously true' % self._n_workers)
            return True

        try:
            LOGGER.debug("attempting to join threads")
            timedout = False
            for task in self._task_hash_map.values():
                LOGGER.debug("attempting to join task %s", task.task_name)
                # task.join() will raise any exception that resulted from the
                # task's execution.
                timedout = not task.join(timeout)
                LOGGER.debug("task %s was joined", task.task_name)
                # if the last task timed out then we want to timeout for all
                # of the task graph
                if timedout:
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
        LOGGER.debug(
            "Invoking terminate. already terminated? %s", self._terminated)
        if self._terminated:
            return
        try:
            # it's possible the global state is not well defined, so just in
            # case we'll wrap it all up in a try/except
            self._terminated = True
            if self._executor_ready_event is not None:
                # alert executors to check that _terminated is True
                self._executor_ready_event.set()
            LOGGER.debug("shutting down workers")
            if self._worker_pool is not None:
                self._worker_pool.close()
                self._worker_pool.terminate()
                self._worker_pool = None

            # This will terminate the logging worker
            if self._logging_queue is not None:
                self._logging_queue.put(None)

            # This will cause all 'join'ed Tasks to join.
            if self._n_workers >= 0:
                self._executor_ready_event.set()
                if self._reporting_interval is not None:
                    self._execution_monitor_wait_event.set()
                for task in self._task_hash_map.values():
                    # shortcut to get the tasks to mark as joined
                    task.task_done_executing_event.set()

            LOGGER.debug('taskgraph terminated')
        except Exception:
            LOGGER.exception(
                'ignoring an exception that occurred during _terminate')


class Task(object):
    """Encapsulates work/task state for multiprocessing."""

    def __init__(
            self, task_name, func, args, kwargs, target_path_list,
            ignore_path_list, hash_target_files, ignore_directories,
            transient_run, worker_pool, priority, hash_algorithm,
            store_result, task_database_path):
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
        self._worker_pool = worker_pool
        self._task_database_path = task_database_path
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

    def _call(self):
        """Invoke this method to execute task.

        Precondition is that the Task dependencies are satisfied.

        Sets the ``self.task_done_executing_event`` flag if execution is
        successful.

        Raises:
            RuntimeError if any target paths are not generated after the
                function call is complete.

        """
        LOGGER.debug("_call check if precalculated %s", self.task_name)
        if not self._transient_run and self.is_precalculated():
            self.task_done_executing_event.set()
            return
        LOGGER.debug("not precalculated %s", self.task_name)

        if self._worker_pool is not None:
            result = self._worker_pool.apply_async(
                func=self._func, args=self._args, kwds=self._kwargs)
            # the following blocks and raises an exception if result
            # raised an exception
            LOGGER.debug("apply_async for task %s", self.task_name)
            payload = result.get()
        else:
            LOGGER.debug("direct _func for task %s", self.task_name)
            payload = self._func(*self._args, **self._kwargs)
        if self._store_result:
            self._result = payload

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

        # this step will only record the run if there is an expected
        # target file. Otherwise we infer the result of this call is
        # transient between taskgraph executions and we should expect to
        # run it again.
        if not self._transient_run:
            _execute_sqlite(
                "INSERT OR REPLACE INTO taskgraph_data VALUES (?, ?, ?)",
                self._task_database_path, mode='modify',
                argument_list=(
                    self._task_reexecution_hash,
                    pickle.dumps(result_target_path_stats),
                    pickle.dumps(self._result)))
        self.task_done_executing_event.set()
        LOGGER.debug("successful run on task %s", self.task_name)

    def is_precalculated(self):
        """Return true if _call need not be invoked.

        If the task has been precalculated it will fetch the return result from
        the previous run.

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
        # these are the stats of the files that exist that aren't ignored
        if not self._hash_target_files:
            target_hash_algorithm = 'exists'
        else:
            target_hash_algorithm = self._hash_algorithm
        file_stat_list = list(_get_file_stats(
            [self._args, self._kwargs],
            target_hash_algorithm,
            self._target_path_list+self._ignore_path_list,
            self._ignore_directories))

        other_arguments = _filter_non_files(
            [self._reexecution_info['args_clean'],
             self._reexecution_info['kwargs_clean']],
            self._target_path_list,
            self._ignore_path_list,
            self._ignore_directories)

        LOGGER.debug("file_stat_list: %s", file_stat_list)
        LOGGER.debug("other_arguments: %s", other_arguments)

        # add the file stat list to the already existing reexecution info
        # dictionary that contains stats that should not change whether
        # files have been created/updated/or not.
        self._reexecution_info['file_stat_list'] = file_stat_list
        self._reexecution_info['other_arguments'] = other_arguments

        reexecution_string = '%s:%s:%s:%s:%s' % (
            self._reexecution_info['func_name'],
            self._reexecution_info['source_code_hash'],
            self._reexecution_info['other_arguments'],
            self._store_result,
            # the x[1] is to only take the digest part of the 'file_stat'
            str([x[1] for x in file_stat_list]))

        self._task_reexecution_hash = hashlib.sha1(
            reexecution_string.encode('utf-8')).hexdigest()
        try:
            database_result = _execute_sqlite(
                """SELECT target_path_stats, result from taskgraph_data
                    WHERE (task_reexecution_hash == ?)""",
                self._task_database_path, mode='read_only',
                argument_list=(self._task_reexecution_hash,), fetch='one')
            if database_result is None:
                LOGGER.debug(
                    "not precalculated, Task hash does not "
                    "exist (%s)", self.task_name)
                LOGGER.debug("is_precalculated full task info: %s", self)
                return False
            result_target_path_stats = pickle.loads(database_result[0])
            mismatched_target_file_list = []
            for path, hash_string in result_target_path_stats:
                if path not in self._target_path_list:
                    mismatched_target_file_list.append(
                        'Recorded path not in target path list %s' % path)
                if not os.path.exists(path):
                    mismatched_target_file_list.append(
                        'Path not found: %s' % path)
                    continue
                elif target_hash_algorithm == 'exists':
                    # this is the case where hash_algorithm == 'exists' but
                    # we already know the file exists so we do nothing
                    continue
                if target_hash_algorithm == 'sizetimestamp':
                    size, modified_time, actual_path = [
                        x for x in hash_string.split('::')]
                    if actual_path != path:
                        mismatched_target_file_list.append(
                            "Path names don't match\n"
                            "cached: (%s)\nactual (%s)" % (path, actual_path))

                    # Using nanosecond resolution for mtime (instead of the
                    # usual float result of os.path.getmtime()) allows us to
                    # precisely compare modification time because we're
                    # comparing ints: st_mtime_ns always returns an int.
                    #
                    # Timestamp resolution: the python docs note that "many
                    # filesystems do not provide nanosecond precision".
                    # This is true (e.g. FAT, FAT32 timestamps are only
                    # accurate to within 2 seconds), but the data read from the
                    # filesystem will be consistent. This lets us know
                    # whether the timestamp changed.  This also means that, on
                    # FAT filesystems, if a file is changed within 2s of its
                    # creation time, we might not be able to detect it.  This
                    # is a weakness of FAT, not taskgraph.
                    target_modified_time = os.stat(path).st_mtime_ns
                    if not int(modified_time) == target_modified_time:
                        mismatched_target_file_list.append(
                            "Modified times don't match "
                            "cached: (%f) actual: (%f)" % (
                                float(modified_time), target_modified_time))
                        continue
                    target_size = os.path.getsize(path)
                    if float(size) != target_size:
                        mismatched_target_file_list.append(
                            "File sizes don't match "
                            "cached: (%s) actual: (%s)" % (
                                size, target_size))
                else:
                    target_hash = _hash_file(path, target_hash_algorithm)
                    if hash_string != target_hash:
                        mismatched_target_file_list.append(
                            "File hashes are different. cached: (%s) "
                            "actual: (%s)" % (hash_string, target_hash))
            if mismatched_target_file_list:
                LOGGER.info(
                    "not precalculated (%s), Task hash exists, "
                    "but there are these mismatches: %s",
                    self.task_name, '\n'.join(mismatched_target_file_list))
                return False
            if self._store_result:
                self._result = pickle.loads(database_result[1])
            LOGGER.debug("precalculated (%s)" % self)
            return True
        except EOFError:
            LOGGER.exception("not precalculated %s, EOFError", self.task_name)
            return False

    def join(self, timeout=None):
        """Block until task is complete, raise exception if runtime failed."""
        LOGGER.debug(
            "joining %s done executing: %s", self.task_name,
            self.task_done_executing_event)
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
