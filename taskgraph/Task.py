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
import traceback
import glob

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

LOGGER = logging.getLogger('Task')


class TaskGraph(object):
    """Encapsulates the worker and tasks states for parallel processing."""

    def __init__(self, db_storage_path, n_workers):
        """Create a task graph.

        Creates an object for building task graphs, executing them,
        parallelizing independent work notes, and avoiding repeated calls.

        Parameters:
            db_storage_path (string): path to a file that either contains
                an existing SQLite database, or will create one if none
                exists.
            n_workers (int): number of parallel workers to allow during
                task graph execution.  If set to 0, use current process.
        """
        # https://stackoverflow.com/questions/273192/how-can-i-create-a-directory-if-it-does-not-exist
        try:
            os.makedirs(os.path.dirname(db_storage_path))
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise
        self.db_storage_path = db_storage_path
        db_connection = sqlite3.connect(self.db_storage_path)
        db_connection.execute('PRAGMA synchronous=OFF')
        db_connection.execute('PRAGMA journal_mode=WA')
        #print list(db_connection.execute(
        #    """SELECT * FROM task_tokens"""))
        with db_connection:
            db_connection.execute(
                'CREATE TABLE IF NOT EXISTS task_tokens '
                '(hash text PRIMARY KEY, json_data text)')
        db_connection.close()

        # the work queue is the feeder to active worker threads
        self.work_queue = Queue.Queue()
        self.n_workers = n_workers

        # used to synchronize a pass through potential tasks to add to the
        # work queue
        self.process_pending_tasks_event = threading.Event()

        # keep track if the task graph has been forcibly terminated
        self.terminated = False

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
                name='worker_thread_%d' % thread_id)
            worker_thread.daemon = True
            worker_thread.start()
            self.thread_set.add(worker_thread)

        # tasks that haven't been evaluated for dependencies go in here
        # to be processed by the process_pending_tasks thread
        self.pending_task_queue = Queue.Queue()

        # Tasks will send their completed tokens along this queue. Another
        # worker will read it and write to the database.
        self.completed_tokens_queue = multiprocessing.Queue()
        completed_tokens_worker = threading.Thread(
            target=self.process_completed_tokens,
            name='process_completed_tokens')
        completed_tokens_worker.daemon = True
        completed_tokens_worker.start()
        self.thread_set.add(completed_tokens_worker)

        # launch thread to monitor the pending task set
        pending_task_worker = threading.Thread(
            target=self.process_pending_tasks,
            name='process_pending_tasks')
        pending_task_worker.daemon = True
        pending_task_worker.start()
        self.thread_set.add(pending_task_worker)

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

    def process_completed_tokens(self):
        """A worker that inserts finished tokens into the database.

            Results are stored in the `task_tokens` table in
            self.db_storage_path.

            self.completed_tokens_queue is a queue of (hash, json_txt) tuples
            that come from completed Tasks. If this tuple is the queue it
            means the Task successfully completed. All tuples will be
            inserted into the `task_tokens` table. A sentinel of 'STOP' will
            indicate that the worker should insert any remaining tuples, close
            the database, and exit.

        Returns:
            None.
        """
        db_connection = sqlite3.connect(self.db_storage_path)
        try:
            stop_work = False
            while not stop_work:
                try:
                    # drain the queue until its empty
                    token_list = []
                    token_tuple = self.completed_tokens_queue.get()
                    while True:
                        if token_tuple == 'STOP':
                            stop_work = True
                            break
                        token_list.append(token_tuple)
                        token_tuple = self.completed_tokens_queue.get(False)
                except Queue.Empty:
                    pass
                finally:
                    db_connection.executemany(
                        'INSERT into task_tokens (hash, json_data) '
                        'VALUES (?,?)', token_list)
                    db_connection.commit()
        finally:
            db_connection.close()


    def _terminate(self):
        """Forcefully terminate remaining task graph computation."""
        if self.terminated:
            return
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
        self.closed = True
        for _ in xrange(self.n_workers):
            self.work_queue.put('STOP')
        self.pending_task_queue.put('STOP')
        self.completed_tokens_queue.put('STOP')

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
                self.completed_tokens_queue, self.db_storage_path,
                self.process_pending_tasks_event)
            self.task_set.add(task)

            if self.n_workers > 0:
                self.pending_task_queue.put(task)
                self.process_pending_tasks_event.set()
            else:
                task(self.worker_pool)
            return task
        except Exception:
            # something went wrong, shut down the taskgraph
            self._terminate()
            raise

    def process_pending_tasks(self):
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


class Task(object):
    """Encapsulates work/task state for multiprocessing."""

    def __init__(
            self, task_id, func, args, kwargs, target_path_list,
            ignore_path_list, dependent_task_list, ignore_directories,
            completed_tokens_queue, db_storage_path, completion_event):
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
            completed_tokens_queue (multiprocessing.Queue): completed tokens
                are put in this queue to signal a successful run.
            db_storage_path (string): path to an SQLite database file that
                as a table of the form

                    task_tokens (hash text PRIMARY KEY, json_data text)

            completion_event (threading.Event): this Event should
                be .set() when the task successfully completes.
        """
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.target_path_list = target_path_list
        self.dependent_task_list = dependent_task_list
        self.target_path_list = target_path_list
        self.ignore_path_list = ignore_path_list
        self.ignore_directories = ignore_directories
        self.completed_tokens_queue = completed_tokens_queue
        self.db_storage_path = db_storage_path
        self.token_path = None  # not set until dependencies are blocked
        self.task_id = task_id
        self.completion_event = completion_event
        self.terminated = False
        self.exception_object = None

        # Used to ensure only one attempt at executing and also a mechanism
        # to see when Task is complete
        self.task_complete_event = threading.Event()

        self.token_id = self._calculate_token()

    def __str__(self):
        return "Task object %s:\n\n" % (id(self)) + pprint.pformat(
            {
                "target_path_list": self.target_path_list,
                "dependent_task_list": self.dependent_task_list,
                "ignore_path_list": self.ignore_path_list,
                "ignore_directories": self.ignore_directories,
                "db_storage_path": self.db_storage_path,
                "token_path": self.token_path,
                "task_id": self.task_id,
                "completion_event": self.completion_event,
                "terminated": self.terminated,
                "exception_object": self.exception_object,
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

    @profile
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

            if self._valid_token():
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
            # write json string as target paths, file modified, file size
            file_stat_list = list(
                _get_file_stats([self.target_path_list], [], False))
            json_data = json.dumps(file_stat_list)
            self.completed_tokens_queue.put((self.token_id, json_data))
            # we can shortcut _valid_token since we just evaluated it
            self._valid_token = lambda: True
        except Exception as e:
            self.terminate(e)
            raise
        finally:
            self.task_complete_event.set()
            self.completion_event.set()

    @profile
    def _valid_token(self):
        """Determine if `self.token_path` represents a valid token.

        Returns:
            True if files referenced in json file at `self.token_path` exist,
            modified times are equal to the modified times recorded in the
            token record, and the size is the same as in the recorded record.
        """
        try:
            db_connection = sqlite3.connect(self.db_storage_path)
            cursor = db_connection.cursor()
            cursor.execute(
                'SELECT json_data FROM task_tokens WHERE hash=?;',
                (self.token_id,))
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
        # if we made it this far we don't need to check again
        self._valid_token = lambda: True
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
        if self._valid_token():
            return True

        # If the thread is done and the token is not valid, there was an error
        raise RuntimeError("Task %s didn't complete correctly" % self.task_id)

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
