"""Task graph framework."""
import collections
from concurrent.futures import Future
from functools import lru_cache
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
import threading
import time
import types
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
from parsl.executors import ThreadPoolExecutor
from parsl.monitoring.monitoring import MonitoringHub
from parsl.addresses import address_by_hostname


LOGGER = logging.getLogger(__name__)

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


def create_parsl_app(
        cache, executor_label, ignore_for_cache=[],
        parsl_resource_specification={}):

    @python_app(
        cache=cache,
        ignore_for_cache=ignore_for_cache,
        executors=[executor_label])
    def wrap_func(func, *args, inputs=[],
                  parsl_resource_specification=parsl_resource_specification,
                  **kwargs):
        return func(*args, **kwargs)

    return wrap_func


def _null_func():
    """Use when func=None on add_task."""
    return None


FUNC_SOURCE_MAP = {}
def get_task_hash_info(func, args, kwargs, target_path_list):
    """Return hash used to compare equality of task executions.

    Data used in the hash:

    - the __name__ of func, or '' if it has no __name__ attribute
    - the source code of func as returned by inspect.getsource, or
      '' if inspecting source isn't possible
    - 'cleaned' args and kwargs:

    """
    try:
        # memoize func source code because it's likely we'll import
        # the same func many times and reflection is slow
        if func not in FUNC_SOURCE_MAP:
            FUNC_SOURCE_MAP[func] = inspect.getsource(func)
        source_code = FUNC_SOURCE_MAP[func]
    except (IOError, TypeError):
        # many reasons for this, for example, frozen Python code won't
        # have source code, so just leave blank
        source_code = ''

    args_clean = []
    for index, arg in enumerate(args):
        try:
            scrubbed_value = _scrub_task_args(arg, target_path_list)
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
    for key, arg in sorted(kwargs.items()):
        try:
            scrubbed_value = _scrub_task_args(arg, target_path_list)
            _ = pickle.dumps(scrubbed_value)
            kwargs_clean[key] = scrubbed_value
        except TypeError:
            LOGGER.warning(
                "could not pickle kw argument %s (%s) scrubbed to %s. "
                "Skipping argument which means it will not be considered "
                "when calculating whether inputs have been changed "
                "on a successive run.", key, arg, scrubbed_value)

    return {
        'func_name': getattr(func, '__name__', ''),
        'args_clean': args_clean,
        'kwargs_clean': kwargs_clean,
        'source_code_hash': hashlib.sha1(
            source_code.encode('utf-8')).hexdigest(),
    }


def get_task_hash(hash_info):
    argument_hash_string = ':'.join([
        repr(hash_info[key])
        for key in sorted(hash_info.keys())])
    return hashlib.sha1(
        argument_hash_string.encode('utf-8')).hexdigest()


# overwrite parsl's default function serializer to include the source code
#
# the LRU cache decorator must be applied closer to the id_for_memo_function call
# that the .register() call, so that the cache-decorated version is registered.
@id_for_memo.register(types.FunctionType)
@lru_cache()
def id_for_memo_function(f: types.FunctionType, output_ref: bool = False) -> bytes:
    """Checkpoint a function based on its name, module, and source code."""
    try:
        # memoize func source code because it's likely we'll import
        # the same func many times and reflection is slow
        if f not in FUNC_SOURCE_MAP:
            FUNC_SOURCE_MAP[f] = inspect.getsource(f)
        source_code = FUNC_SOURCE_MAP[f]
    except (IOError, TypeError):
        # many reasons for this, for example, frozen Python code won't
        # have source code, so just leave blank
        source_code = ''
    return pickle.dumps(["types.FunctionType", f.__name__, f.__module__, source_code])


class TaskGraphMemoizer(BasicMemoizer):
    """Memoizer is responsible for ensuring that identical work is not repeated.

    When a task is repeated, i.e., the same function is called with the same exact arguments, the
    result from a previous execution is reused. `wiki <https://en.wikipedia.org/wiki/Memoization>`_

    The memoizer implementation here does not collapse duplicate calls
    at call time, but works **only** when the result of a previous
    call is available at the time the duplicate call is made.

    For instance::

       No advantage from                 Memoization helps
       memoization here:                 here:

        TaskA                            TaskB
          |   TaskA                        |
          |     |   TaskA                done  (TaskB)
          |     |     |                                (TaskB)
        done    |     |
              done    |
                    done

    The memoizer creates a lookup table by hashing the function name
    and its inputs, and storing the results of the function.

    When a task is ready for launch, i.e., all of its arguments
    have resolved, we add its hash to the task datastructure.

    """

    hash_algorithm: str = 'sizetimestamp'

    def __init__(self, *,
                 checkpoint_period: Optional[str] = None,
                 checkpoint_mode: Literal['task_exit', 'periodic', 'dfk_exit', 'manual'] | None = None,
                 memoize: bool = True):
        """Initialize the memoizer.

        KWargs:

            - checkpoint_files : sequence of str, optional
                  List of paths to checkpoint files to load. By default, all checkpoints from the
                  run directory will be restored. This is usually the right behaviour, but this
                  parameter allows that behaviour to be overridden.
                  See :func:`parsl.utils.get_all_checkpoints` and :func:`parsl.utils.get_last_checkpoint`
                  for helpers.
            - checkpoint_period : str, optional
                  Time interval (in "HH:MM:SS") at which to checkpoint completed tasks. Only has an effect if
                  ``checkpoint_mode='periodic'``.
            - checkpoint_mode : str, optional
                  Checkpoint mode to use, can be ``'dfk_exit'``, ``'task_exit'``, ``'periodic'`` or ``'manual'``.
                  If set to `None`, checkpointing will be disabled. Default is None.
            - memoize : str, enable memoization or not.

        """
        self.checkpointed_tasks = 0

        # this lock must be held when:
        # * writing to any checkpoint files
        # * interacting with self.checkpointable_tasks
        self._checkpoint_lock = threading.Lock()

        self.checkpoint_mode = checkpoint_mode
        self.checkpoint_period = checkpoint_period

        self.checkpointable_tasks: List[CheckpointCommand] = []

        self._checkpoint_timer: Timer | None = None
        self.memoize = memoize

    def is_precalculated(
            self, task, target_hash_algorithm, func, args, kwargs,
            target_path_list, ignore_path_list, ignore_directories):
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
        try:
            # memoize func source code because it's likely we'll import
            # the same func many times and reflection is slow
            if func not in FUNC_SOURCE_MAP:
                FUNC_SOURCE_MAP[func] = inspect.getsource(func)
            source_code = FUNC_SOURCE_MAP[func]
        except (IOError, TypeError):
            # many reasons for this, for example, frozen Python code won't
            # have source code, so just leave blank
            source_code = ''

        args_clean = []
        for index, arg in enumerate(args):
            try:
                scrubbed_value = _scrub_task_args(arg, target_path_list)
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
        for key, arg in sorted(kwargs.items()):
            try:
                scrubbed_value = _scrub_task_args(arg, target_path_list)
                _ = pickle.dumps(scrubbed_value)
                kwargs_clean[key] = scrubbed_value
            except TypeError:
                LOGGER.warning(
                    "could not pickle kw argument %s (%s) scrubbed to %s. "
                    "Skipping argument which means it will not be considered "
                    "when calculating whether inputs have been changed "
                    "on a successive run.", key, arg, scrubbed_value)

        for target_path in target_path_list:
            if not os.path.exists(_normalize_path(target_path)):
                LOGGER.debug(f'Could not create a hash because {target_path} does not exist.')
                return False

        target_file_stat_list = list(_get_file_stats(
            base_value=target_path_list,
            hash_algorithm=target_hash_algorithm,
            ignore_list=[],
            ignore_directories=ignore_directories))

        non_target_file_stat_list = list(_get_file_stats(
            base_value=[args, kwargs],
            hash_algorithm=target_hash_algorithm,
            ignore_list=target_path_list + ignore_path_list,
            ignore_directories=ignore_directories))

        other_arguments = _filter_non_files(
            [args_clean, kwargs_clean],
            target_path_list,
            ignore_path_list,
            ignore_directories)

        try:
            mismatched_target_file_list = []
            # for path, hash_string in result_target_path_stats:
            #     if path not in target_path_list:
            #         mismatched_target_file_list.append(
            #             'Recorded path not in target path list %s' % path)
            #     if not os.path.exists(path):
            #         mismatched_target_file_list.append(
            #             'Path not found: %s' % path)
            #         continue
            #     elif target_hash_algorithm == 'exists':
            #         # this is the case where hash_algorithm == 'exists' but
            #         # we already know the file exists so we do nothing
            #         continue
            #     if target_hash_algorithm == 'sizetimestamp':
            #         size, modified_time, actual_path = [
            #             x for x in hash_string.split('::')]
            #         if actual_path != path:
            #             mismatched_target_file_list.append(
            #                 "Path names don't match\n"
            #                 "cached: (%s)\nactual (%s)" % (path, actual_path))

            #         # Using nanosecond resolution for mtime (instead of the
            #         # usual float result of os.path.getmtime()) allows us to
            #         # precisely compare modification time because we're
            #         # comparing ints: st_mtime_ns always returns an int.
            #         #
            #         # Timestamp resolution: the python docs note that "many
            #         # filesystems do not provide nanosecond precision".
            #         # This is true (e.g. FAT, FAT32 timestamps are only
            #         # accurate to within 2 seconds), but the data read from the
            #         # filesystem will be consistent. This lets us know
            #         # whether the timestamp changed.  This also means that, on
            #         # FAT filesystems, if a file is changed within 2s of its
            #         # creation time, we might not be able to detect it.  This
            #         # is a weakness of FAT, not taskgraph.
            #         target_modified_time = os.stat(path).st_mtime_ns
            #         if not int(modified_time) == target_modified_time:
            #             mismatched_target_file_list.append(
            #                 "Modified times don't match "
            #                 "cached: (%f) actual: (%f)" % (
            #                     float(modified_time), target_modified_time))
            #             continue
            #         target_size = os.path.getsize(path)
            #         if float(size) != target_size:
            #             mismatched_target_file_list.append(
            #                 "File sizes don't match "
            #                 "cached: (%s) actual: (%s)" % (
            #                     size, target_size))
            #     else:
            #         target_hash = _hash_file(path, target_hash_algorithm)
            #         if hash_string != target_hash:
            #             mismatched_target_file_list.append(
            #                 "File hashes are different. cached: (%s) "
            #                 "actual: (%s)" % (hash_string, target_hash))
            if mismatched_target_file_list:
                LOGGER.info(
                    "Task hash exists, but there are these mismatches: %s",
                    '\n'.join(mismatched_target_file_list))
                return False

            # # if kwargs contains an outputs parameter, that parameter is removed
            # # and normalised differently - with output_ref set to True.
            # # kwargs listed in ignore_for_cache will also be removed
            filtered_kw = kwargs.copy()
            # ignore_list = task['ignore_for_cache']

            # LOGGER.debug("Ignoring these kwargs for checkpointing: %s", ignore_list)
            # for k in ignore_list:
            #     LOGGER.debug("Ignoring kwarg %s", k)
            #     del filtered_kw[k]

            # add the file stat list to the already existing reexecution info
            # dictionary that contains stats that should not change whether
            # files have been created/updated/or not.
            reexecution_info = {
                'func_name': getattr(func, '__name__', ''),
                'args_clean': args_clean,
                'kwargs_clean': kwargs_clean,
                'source_code_hash': hashlib.sha1(
                    source_code.encode('utf-8')).hexdigest(),
                'target_file_stat_list': target_file_stat_list,
                'non_target_file_stat_list': non_target_file_stat_list,
                'other_arguments': other_arguments,
                'non_outputs': get_task_hash_info(
                    task['func'],
                    task['args'],
                    filtered_kw,
                    task['kwargs']['_taskgraph_task_info']['target_path_list']),
                'filtered_kw': filtered_kw,
                'func': task['func'],
                'args': task['args']
            }

            binary_reexecution_info = id_for_memo(reexecution_info)

            if 'outputs' in task['kwargs']:
                del filtered_kw['outputs']
                reexecution_info['outputs'] = task['kwargs']['outputs']
                binary_reexecution_info += id_for_memo(
                    task['kwargs']['outputs'], output_ref=True)

            hashsum = hashlib.md5(binary_reexecution_info).hexdigest()
            LOGGER.debug("Task {} has memoization hash {}".format(task['id'], hashsum))
            task['hashsum'] = hashsum

            checkpoint_dir = task['kwargs']['_taskgraph_task_info']['checkpoint_dir']
            if checkpoint_dir in self.memo_lookup_table:
                if hashsum in self.memo_lookup_table[checkpoint_dir]:
                    return True
                else:
                    return False
            else:
                LOGGER.info('task checkpoint dir has not been loaded')
                return False
        except EOFError:
            LOGGER.exception("EOFError")
            return False

    def check_memo(self, task: TaskRecord) -> Optional[Future[Any]]:
        """Create a hash of the task and its inputs and check the lookup table for this hash.

        If present, the results are returned.

        Args:
            - task(task) : task from the dfk.tasks table

        Returns:
            - Result of the function if present in table, wrapped in a Future

        This call will also set task['hashsum'] to the unique hashsum for the func+inputs.
        """
        task_id = task['id']

        if not self.memoize or not task['memoize']:
            task['hashsum'] = None
            LOGGER.debug("Task {} will not be memoized".format(task_id))
            return None

        result = None
        if self.is_precalculated(
                task=task,
                target_hash_algorithm=task['kwargs']['_taskgraph_task_info']['target_hash_algorithm'],
                func=task['kwargs']['_taskgraph_task_info']['func'],
                args=task['kwargs']['_taskgraph_task_info']['args'],
                kwargs=task['kwargs']['_taskgraph_task_info']['kwargs'],
                target_path_list=task['kwargs']['_taskgraph_task_info']['target_path_list'],
                ignore_path_list=task['kwargs']['_taskgraph_task_info']['ignore_path_list'],
                ignore_directories=task['kwargs']['_taskgraph_task_info']['ignore_directories']):
            checkpoint_dir = task['kwargs']['_taskgraph_task_info']['checkpoint_dir']
            result = self.memo_lookup_table[checkpoint_dir][task['hashsum']]
            LOGGER.info("Task %s using result from cache", task_id)
        else:
            LOGGER.info("Task %s had no result in cache", task_id)

        assert isinstance(result, Future) or result is None
        return result

    def add_checkpoint_dir(self, checkpoint_dir):
        self.memo_lookup_table[checkpoint_dir] = self.load_checkpoints(
            get_all_checkpoints(checkpoint_dir))

    def start(self, *, run_dir: str, config_run_dir: str) -> None:

        self.run_dir = run_dir
        self.config_run_dir = config_run_dir

        if self.memoize:
            LOGGER.info("App caching enabled")
            self.memo_lookup_table = {}
        else:
            LOGGER.info("App caching disabled for all apps")

        if self.checkpoint_mode == "periodic":
            if self.checkpoint_period is None:
                raise ConfigurationError("Checkpoint period must be specified with periodic checkpoint mode")
            else:
                try:
                    h, m, s = map(int, self.checkpoint_period.split(':'))
                except Exception:
                    raise ConfigurationError("invalid checkpoint_period provided: {0} expected HH:MM:SS".format(self.checkpoint_period))
                checkpoint_period = (h * 3600) + (m * 60) + s
                self._checkpoint_timer = Timer(self.checkpoint_queue, interval=checkpoint_period, name="Checkpoint")

    def _load_checkpoints(self, checkpointDirs: Sequence[str]) -> Dict[str, Future[Any]]:
        """Load a checkpoint file into a lookup table.

        The data being loaded from the pickle file mostly contains input
        attributes of the task: func, args, kwargs, env...
        To simplify the check of whether the exact task has been completed
        in the checkpoint, we hash these input params and use it as the key
        for the memoized lookup table.

        Args:
            - checkpointDirs (list) : List of filepaths to checkpoints
              Eg. ['runinfo/001', 'runinfo/002']

        Returns:
            - memoized_lookup_table (dict)
        """
        memo_lookup_table = {}

        for checkpoint_dir in checkpointDirs:
            LOGGER.info("Loading checkpoints from {}".format(checkpoint_dir))
            checkpoint_file = os.path.join(checkpoint_dir, 'tasks.pkl')
            try:
                with open(checkpoint_file, 'rb') as f:
                    while True:
                        try:
                            data = pickle.load(f)
                            # Copy and hash only the input attributes
                            memo_fu: Future = Future()
                            assert data['exception'] is None
                            memo_fu.set_result(data['result'])
                            memo_lookup_table[data['hash']] = memo_fu

                        except EOFError:
                            # Done with the checkpoint file
                            break
            except FileNotFoundError:
                reason = "Checkpoint file was not found: {}".format(
                    checkpoint_file)
                LOGGER.error(reason)
                raise BadCheckpoint(reason)
            except Exception as ex:
                reason = "Failed to load checkpoint: {}".format(
                    checkpoint_file)
                LOGGER.error(reason)
                raise BadCheckpoint(reason)

            LOGGER.info("Completed loading checkpoint: {0} with {1} tasks".format(checkpoint_file,
                                                                                  len(memo_lookup_table.keys())))
        return memo_lookup_table

    def _update_memo(self, task: TaskRecord) -> None:
        """Updates the memoization lookup table with the result from a task.
        This doesn't move any values around but associates the memoization
        hashsum with the completed (by success or failure) AppFuture.

        Args:
             - task (TaskRecord) : A task record from dfk.tasks
        """
        task_id = task['id']
        self.is_precalculated(
            task=task,
            target_hash_algorithm=task['kwargs']['_taskgraph_task_info']['target_hash_algorithm'],
            func=task['kwargs']['_taskgraph_task_info']['func'],
            args=task['kwargs']['_taskgraph_task_info']['args'],
            kwargs=task['kwargs']['_taskgraph_task_info']['kwargs'],
            target_path_list=task['kwargs']['_taskgraph_task_info']['target_path_list'],
            ignore_path_list=task['kwargs']['_taskgraph_task_info']['ignore_path_list'],
            ignore_directories=task['kwargs']['_taskgraph_task_info']['ignore_directories'])

        if not self.memoize or not task['memoize'] or 'hashsum' not in task:
            return

        if not isinstance(task['hashsum'], str):
            LOGGER.error("Attempting to update app cache entry but hashsum is not a string key")
            return

        checkpoint_dir = task['kwargs']['_taskgraph_task_info']['checkpoint_dir']
        if task['hashsum'] in self.memo_lookup_table[checkpoint_dir]:
            LOGGER.info(f"Replacing app cache entry {task['hashsum']} with result from task {task_id}")
        else:
            LOGGER.debug(f"Storing app cache entry {task['hashsum']} with result from task {task_id}")
        self.memo_lookup_table[checkpoint_dir][task['hashsum']] = task['app_fu']

    def _checkpoint_these_tasks(self, checkpoint_queue: List[CheckpointCommand]) -> None:
        """Play a sequence of CheckpointCommands into a checkpoint file.

        The checkpoint lock must be held when invoking this method.
        """

        # This checks that the lock is held, at least, but does not check that
        # it is held by the current thread - threading.Lock does not have a
        # concept of locking thread for threading.Lock.
        assert self._checkpoint_lock.locked(), "checkpoint system should be locked"

        count = 0

        for cc in checkpoint_queue:
            if cc.exception is None:
                checkpoint_dir = pathlib.Path(cc.task_record['kwargs'][
                    '_taskgraph_task_info']['checkpoint_dir']
                ) / 'all_runs' / 'checkpoint'
                if not os.path.exists(checkpoint_dir):
                    os.makedirs(checkpoint_dir, exist_ok=True)
                checkpoint_filepath = checkpoint_dir / 'tasks.pkl'
                with open(checkpoint_filepath, 'ab') as f:
                    hashsum = cc.task_record['hashsum']
                    if not hashsum:
                        continue
                    t = {'hash': hashsum, 'exception': None, 'result': cc.result}

                    # We are using pickle here since pickle dumps to a file in 'ab'
                    # mode behave like a incremental log.
                    pickle.dump(t, f)
                    count += 1
                    LOGGER.debug("Task %s checkpointed", cc.task_record['id'])

        self.checkpointed_tasks += count

        if count == 0:
            if self.checkpointed_tasks == 0:
                LOGGER.warning("No tasks checkpointed so far in this run. Please ensure caching is enabled")
            else:
                LOGGER.debug("No tasks checkpointed in this pass.")
        else:
            LOGGER.info("Done checkpointing {} tasks".format(count))


config.app_cache = True
config.memoizer = TaskGraphMemoizer(checkpoint_mode='task_exit')
config.initialize_logging = False
config.monitoring = MonitoringHub(
    hub_address='127.0.0.1',
    monitoring_debug=True,
    resource_monitoring_interval=10
)
_DFK = parsl.load(config)


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

        # keeps track of the tasks currently being processed for logging.
        self._active_task_list = []

        # maps a list of task names that need to be executed before the key
        # task can
        self._task_dependent_map = collections.defaultdict(set)

        # maps a list of task names that are dependent to a task
        self._dependent_task_map = collections.defaultdict(set)

        # tasks that complete are added to this set
        self._completed_task_names = set()

        _DFK.config.memoizer.add_checkpoint_dir(self._taskgraph_cache_dir_path)

        if n_workers > 0:
            self.executor_label = "htex"
            executor = HighThroughputExecutor(
                label=self.executor_label,
                cores_per_worker=1,
                max_workers_per_node=n_workers,
                address='127.0.0.1')
        elif n_workers == 0:
            self.executor_label = "threaded"
            executor = ThreadPoolExecutor(
                label=self.executor_label,
                max_threads=2)
        else:
            self.executor_label = "threaded"
            executor = ThreadPoolExecutor(
                label=self.executor_label,
                max_threads=1)

        _DFK.add_executors([executor])
        self.pool = ParslPoolExecutor(dfk=_DFK)

        print('executors:', _DFK.executors)

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

    def add_task(
            self, func=None, args=None, kwargs=None, task_name=None,
            target_path_list=None, ignore_path_list=None,
            hash_target_files=True, dependent_task_list=None,
            ignore_directories=True, priority=0,
            hash_algorithm='sizetimestamp', transient_run=False):
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

            hash = get_task_hash(get_task_hash_info(
                func, args, kwargs, target_path_list))

            # it may be this task was already created in an earlier call,
            # use that object in its place
            if hash in self._task_hash_map:
                duplicate_task = self._task_hash_map[hash]
                new_task_target_set = set(sorted([
                    _normalize_path(path) for path in target_path_list]))
                duplicate_task_target_set = set(
                    duplicate_task._target_path_list)
                if new_task_target_set == duplicate_task_target_set:
                    LOGGER.warning(
                        "A duplicate task of %s was submitted",
                        self._task_hash_map[hash])
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
                        "a runtime error. Existing task: %s" % (duplicate_task))

            task_name = '%s (%d)' % (task_name, len(self._task_hash_map))
            new_task = Task(
                task_name, func, args, kwargs, target_path_list,
                ignore_path_list, hash_target_files, ignore_directories,
                transient_run,
                priority, hash_algorithm,
                dependent_task_list,
                checkpoint_dir=self._taskgraph_cache_dir_path,
                executor_label=self.executor_label)

            self._task_name_map[new_task.task_name] = new_task
            self._task_hash_map[hash] = new_task
            if self._n_workers < 0:
                # call directly if single threaded
                new_task._call()
            else:
                print('checking status')
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
            print('returning', new_task)
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
                "\ttasks executing (%d): graph is %s\n%s",
                self._added_task_count, completed_tasks, percent_complete,
                active_task_count,
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
        print('join')
        LOGGER.debug("joining taskgraph")
        try:
            LOGGER.debug("attempting to join threads")

            for task in self._task_hash_map.values():
                try:
                    task.join(timeout=timeout)
                except TimeoutError:
                    LOGGER.info(
                        "task %s timed out in graph join", task.task_name)
                    return False

            if self._closed:
                # Close down the taskgraph; ok if already terminated
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
        LOGGER.debug("taskgraph closed")

    def _terminate(self):
        """Immediately terminate remaining task graph computation."""
        LOGGER.debug(
            "Invoking terminate. already terminated? %s", self._terminated)
        if self._terminated:
            return
        self.pool.shutdown()
        try:
            # it's possible the global state is not well defined, so just in
            # case we'll wrap it all up in a try/except
            self._terminated = True

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
            dependent_task_list, checkpoint_dir, executor_label):
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
        self._ignore_directories = ignore_directories
        self._hash_algorithm = hash_algorithm
        if not hash_target_files:
            self._target_hash_algorithm = 'exists'
        else:
            self._target_hash_algorithm = self._hash_algorithm

        self._taskgraph_task_info = {
            'target_hash_algorithm': self._target_hash_algorithm,
            'func': self._func,
            'args': self._args,
            'kwargs': self._kwargs,
            'target_path_list': self._target_path_list,
            'ignore_path_list': self._ignore_path_list,
            'ignore_directories': self._ignore_directories,
            'checkpoint_dir': checkpoint_dir
        }

        # These are used to store and later access the result of the call.
        self._result = None

        self._reexecution_info = get_task_hash_info(
            self._func, self._args, self._kwargs, self._target_path_list)

        self._task_id_hash = get_task_hash(self._reexecution_info)

        parsl_resource_specification = {}
        if executor_label == 'htex':
            parsl_resource_specification['priority'] = priority

        wrap_func = create_parsl_app(
            cache=not transient_run,
            executor_label=executor_label,
            ignore_for_cache=self._ignore_path_list,
            parsl_resource_specification=parsl_resource_specification)

        def inner_wrap_func(func, *args, _taskgraph_task_info=None, **kwargs):
            return func(*args, **kwargs)

        self.future = wrap_func(
            inner_wrap_func, func, *args, **kwargs,
            _taskgraph_task_info=self._taskgraph_task_info,
            inputs=[task.future for task in dependent_task_list])
        print('got future', self.future)

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

    def __repr__(self):
        """Create a string representation of a Task."""
        return "Task object %s:\n\n" % (id(self)) + pprint.pformat(
            {
                "task_name": self.task_name,
                "ignore_path_list": self._ignore_path_list,
                "ignore_directories": self._ignore_directories,
                "target_path_list": self._target_path_list,
                "task_id_hash": self._task_id_hash,
                "self._reexecution_info": self._reexecution_info,
                "self._result": self._result,
            })

    def _call(self, timeout=None):
        """Invoke this method to execute task.

        Precondition is that the Task dependencies are satisfied.

        Raises:
            RuntimeError if any target paths are not generated after the
                function call is complete.

        """
        print('calling result with timeout', timeout)
        self._result = self.future.result(timeout=timeout)

        # check that the target paths exist
        result_target_path_stats = list(
            _get_file_stats(
                self._target_path_list, self._target_hash_algorithm, [], False))
        result_target_path_set = set(
            [x[0] for x in result_target_path_stats])
        target_path_set = set(self._target_path_list)
        if target_path_set != result_target_path_set:
            raise RuntimeError(
                "In Task: %s\nMissing expected target path results.\n"
                "Expected: %s\nObserved: %s\n" % (
                    self.task_name, self._target_path_list,
                    result_target_path_set))

        LOGGER.debug("successful run on task %s", self.task_name)

    def join(self, timeout=None):
        """Block until task is complete, raise exception if runtime failed."""
        LOGGER.debug("joining %s done executing: %s", self.task_name)
        self._call(timeout=timeout)

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

        """
        self.join(timeout)
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

    Attempts to standardize task arguments according to these rules:
    - if value is callable, return a string containing the __name__ and
      source code as returned by inspect.getsource
    - if value is a dict, return a copy of the dict where each value is
      replaced with the result of calling _scrub_task_args on the original
      value
    - if value is a list, set, or tuple, return a copy of where each value is
      replaced with the result of calling _scrub_task_args on the original
      value
    - if value is a string or pathlib.PurePath, attempt to convert into
      a normalized, normcase, absolute filepath string. if this normalized
      string exists in `target_path_list`, return 'in_target_path_list'
    - otherwise, return the value unchanged

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
            # memoize func source code because it's likely we'll import
            # the same func many times and reflection is slow
            if base_value not in FUNC_SOURCE_MAP:
                FUNC_SOURCE_MAP[base_value] = (
                    inspect.getsource(base_value)).replace(
                        ' ', '').replace('\t', '')
            source_code = FUNC_SOURCE_MAP[base_value]
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

