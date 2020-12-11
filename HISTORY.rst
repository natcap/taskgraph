.. :changelog:

=========================
TaskGraph Release History
=========================

0.10.1 (2020-12-11)
-------------------
* Fixed an issue that would ignore the state of a ``transient_run`` flag if
  a previous Task run had run it with that flag set to False.
* Removed a limit on the number of times ``TaskGraph`` can attempt to update
  its database up to 5 minutes of continuous failures. This is to address
  expected issues when many parallel threads may compete for an update.
  Relevant information about why the database update fails is logged.
* Fixed an issue where the logging queue would always report an exception
  even if the logging thread shut down correctly.

0.10.0 (2020-08-25)
-------------------
* Fixed several race conditions that could cause the ``TaskGraph`` object to
  hang on an otherwise ordinary termination.
* Changed logging level to "INFO" on cases where the taskgraph was not
  precalculated since it's an expected path of execution in ``TaskGraph``.
* Adding a ``hardlink_allowed`` parameter to ``add_task`` that allows the
  attempt to hardlink a file in a case where a ``copy_artifact=True`` may
  permit one. This will save on disk space as well as computation time
  if large files are not needed to copy.
* Adding a ``store_result`` flag to ``add_task`` that conditionally stores
  the ``func`` result in the database for later ``.get``. This was added to
  guard against return types that were not picklable and would otherwise
  cause an exception when being executed normally.
* Fixed issue that would cause the logger thread to continue reporting status
  after all tasks were complete and the graph was closed.

0.9.1 (2020-06-04)
------------------
* Fixed issue that would cause an infinite loop if a ``TaskGraph`` object were
  created with a database from an incompatible previous version. Behavior now
  is to log the issue, delete the old database, and create a new compatible
  one.
* Fixed issue that would cause some rare infinite loops if ``TaskGraph`` were
  to fail due to some kinds of task exceptions.
* Adding open source BSD-3-Clause license.

0.9.0 (2020-03-05)
------------------
* Updating primary repository URL to GitHub.
* Adding support for Python 3.8.
* Removing the ``EncapsulatedOp`` abstract class. In practice the development
  loop that encouraged the use of ``EncapsulatedOp`` is flawed and can lead to
  design errors.
* Removing unnecessary internal locks which will improve runtime performance of
  processing many small Tasks.
* Refactor to support separate TaskGraph objects that use the same database.
* Removed the ``n_retries`` parameter from ``add_task``. Users are recommended
  to handle retries within functions themselves.
* Added a ``hash_target_files`` flag to ``add_task`` that when set to False,
  causes TaskGraph to only note the existence of target files after execution
  or as part of an evaluation to determine if the Task was precalculated.
  This is useful for operations that initialize a file but subsequent runs of
  the program modify it such as a new database or a downloaded file.
* Fixed an issue on the monitor execution thread that caused shutdown of a
  TaskGraph object to be delayed up to the amount of delay in the monitor
  reporting update.
* Added a ``.get()`` function for ``Task`` objects that returns the result of
  the respective ``func`` call. This value is cached in the TaskGraph database
  and hence can be used to avoid repeated execution. Note the addition of this
  function changes the functionality of calling ``add_task`` with no target
  path list. In previous versions the Task would execute once per TaskGraph
  instance, now successive ``Task`` objects with the same execution signature
  will use cached results.
* To support the addition of the ``.get()`` function a ``transient_run``
  parameter is added to ``add_task`` that causes TaskGraph to avoid
  recording a completed ``Task`` even if the execution hash would have been
  identical to a previously completed run where the target artifacts still
  existed.

0.8.5 (2019-09-11)
------------------
* Dropped support for Python 2.7.
* Fixed an issue where paths in ``ignore_paths`` were not getting ignored in
  the case of ``copy_duplicate_artifact=True``.
* Fixed an issue where the "percent completed" in the logging monitor would
  sometimes exceed 100%. This occurred when a duplicate task was added to
  the TaskGraph object.
* Fixed an issue where a relative path set as a target path would always cause
  TaskGraph to raise an exception after the task was complete.
* Fixed an issue where kwargs that were unhashable were not considered when
  determining if a Task should be re-run.
* Fixed an issue where files with almost identical modified times and sizes
  would hash equal in cases even when the filenames were different.

0.8.4 (2019-05-23)
------------------
* Fixed an exception that occurred when two tasks were constructed that
  targeted the same file but one path was relative and the other was absolute.

0.8.3 (2019-02-26)
------------------
* Fixed an issue that would cause TaskGraph to raise an IOError if an
  ``add_task`` call was marked for ``copy_duplicate_artifact`` but the
  base target file was missing.
* Fixed an issue that would prevent the source distribution from
  installing.
* Taskgraph is now tested against python versions 2.7, 3.6 and 3.7.

0.8.2 (2019-01-31)
------------------
* Adjusted logging levels so most chatty information is lowered to debug and
  oddness in ``__del__`` shutdown are degraded from ``error`` to ``debug`` so
  as not to cause alarm.

0.8.1 (2019-01-09)
------------------
* Fixed an issue that would cause a deadlock if two tasks were added that had
  the same function signature except different target paths.

0.8.0 (2019-01-07)
------------------
* Fixed a race condition that would sometimes cause an exception when multiple
  threads attempted to read or write to the completed Task Database.
* Fixed an issue that could cause an exception in ``__del__`` to print to
  stderr during Python interpreter shutdown.
* Added a ``hash_algorithm`` parameter to ``add_task`` that is a string of
  either 'sizetimestamp' or anything in ``hashlib.algorithms_available``. This
  option tells TaskGraph how to fingerprint input and target files to
  determine the need for recomputation.
* Added a ``copy_duplicate_artifact`` parameter to ``add_task`` that when True
  tells TaskGraph to copy duplicate target results to a new target so long as
  all the parameters and base/target files fingerprint to the same value.
  This can save significant computation time when use in scenarios where
  there are small changes in a workflow, but otherwise significant changes
  in filenames. This often occurs when putting timestamps or other suffixes
  on files that otherwise have identical content.

0.7.2 (2018-11-21)
------------------
* TaskGraph now stores all task completion information in a single SQLite
  database stored in its cache directory. In previous versions
  TaskGraph would write a small text file for each task in a highly branching
  directory tree. This structure made removal of those directory trees
  computationally difficult.
* Fixed an issue that would cause TaskGraph to reexecute if the target path
  was included in the argument list and that path was not normalized to the
  operating system's path style.
* Fixed a deadlock in some cases where Tasks failed while other tasks checked
  for pre-execution clauses.

0.7.0 (2018-10-22)
------------------
* Fixed an issue where very long strings might be interpreted as paths and
  Windows crashes because the path is too long.
* Fixed a deadlock issue where a Task might raise an unhandled exception as a
  new task was added to the TaskGraph.
* Fixed the occasional ``BrokenPipeError`` that could occur when a Task
  encountered an unhandled exception.
* Added an ``n_retries`` parameter to ``add_task`` that lets TaskGraph attempt
  to reexecute a failing Task up to ``n_retries`` times before terminating
  the TaskGraph.
* Removed the ``delayed_start`` option.

0.6.1 (2018-08-14)
------------------
* Resolving an issue with duplicate logging being printed to stdout when
  ``n_workers > 0``.  Logging is now only handled in the process that contains
  the TaskGraph instance.
* Updated main logging message to indicate which tasks, by task name, are
  currently active and how many tasks are ready to execute but can't because
  there is not an open worker.
* Attempted to fix an issue where processes in the process pool were not
  terminating on a Linux system by aggressively joining all threads and
  processes when possible.
* Fixed an issue that would cause tasks that had been previously calculated to
  prematurely trigger children tasks even if the parent tasks of the current
  task needed to be reexecuted.

0.6.0 (2018-07-24)
------------------
* Added a ``delayed_start`` flag to TaskGraph to allow for delayed execution
  of taskgraph tasks. If enabled on threaded or multiprocess mode, calls to
  ``add_task`` will not execute tasks until the ``join`` method is invoked on
  ``taskgraph``. This allows for finer control over execution order when tasks
  are passed non-equivalent ``priority`` levels.
* Fixing an issue where a non-JSON serializeable object would cause
  ``add_task`` to crash. Now TaskGraph is more tolerant of non-JSON
  serializeable objects and will log warnings when parameters cannot be
  serialized.
* TaskGraph constructor has an option to report a ongoing logging message
  at a set interval. The message reports how many tasks have been committed
  and completed.
* Fixed a bug that would cause TaskGraph to needlessly reexecute a task if
  the only change was the order of the ``target_path_list`` or
  ``dependent_task_list`` variables.
* Fixed a bug that would cause a task to reexecute between runs if input
  argument was a file that would be generated by a task that had not yet
  executed.
* Made a code change that makes it very likely that tasks will be executed in
  priority order if added to a TaskGraph in delayed execution mode.
* Refactored internal TaskGraph scheduling to fix a design error that made it
  likely tasks would be needlessly reexecuted. This also simplified TaskGraph
  flow control and cause slight performance improvements.
* Fixed an issue discovered when a ``scipy.sparse`` matrix was passed as an
  argument and ``add_task`` crashed on infinite recursion. Type checking of
  arguments has been simplified and now iteration only occurs on the Python
  ``set``, ``dict``, ``list``, and ``tuple`` types.
* Fixed an issue where the ``TaskGraph`` was not ``join``\ing the worker
  process pool on a closed/join TaskGraph, or when the ``TaskGraph`` object
  was being deconstructed. This would occasionally cause a race condition
  where the TaskGraph may still have a cache ``.json`` file open. Discovered
  through a flaky build test.
* Added functionality to the ``TaskGraph`` object to propagate log messages
  from workers back to the parent process.  This only applies for cases where
  a ``TaskGraph`` instance is started with ``n_workers > 0``.
* Fixed an issue where a function that was passed as an argument would cause
  a reexecution on a separate run because the ``__repr__`` of a function
  includes its pointer address.
* Adjusted logging levels so that detailed task information is shown on DEBUG
  but basic status updates are shown in INFO.

0.5.2 (2018-06-20)
------------------
* Fixing an issue where a Task would hang on a ``join`` if the number of
  workers in TaskGraph was -1 and a call to ``add_task`` has a non-``None``
  passed to ``target_path_list`` and the resulting task was ``\.join``\ed
  after a second run of the same program.

0.5.1 (2018-06-20)
------------------
* Fixing an issue where TaskGraph would hang on a ``join`` if the number of
  workers was -1 and a call to ``add_task`` has ``None`` passed to
  ``target_path_list``.

0.5.0 (2018-05-04)
------------------
* Taskgraph now supports python versions 2 and 3 (tested with python 2.7,
  3.6).
* Fixed an issue with ``taskgraph.TaskGraph`` that prevented a multiprocessed
  graph from executing on POSIX systems when ``psutil`` was installed.
* Adding matrix-based test automation (python 2.7, python 3.6, with/without
  ``psutil``) via ``tox``.
* Updating repository path to ``https://bitbucket.org/natcap/taskgraph``.

0.4.0 (2018-04-18)
------------------
* Auto-versioning now happens via ``setuptools_scm``, replacing previous calls
  to ``natcap.versioner``.
* Added an option to ``TaskGraph`` constructor to allow negative values in the
  ``n_workers`` argument to indicate that the entire object should run in the
  main thread. A value of 0 will indicate that no multiprocessing will be used
  but concurrency will be allowed for non-blocking ``add_task``.
* Added an abstract class ``task.EncapsulatedTaskOp`` that can be used to
  instance a class that needs scope in order to be used as an operation passed
  to a process. The advantage of using ``EncapsulatedTaskOp`` is that the
  ``__name__`` hash used by ``TaskGraph`` to determine if a task is unique is
  calculated in the superclass and the subclass need only worry about
  implementation of ``__call__``.
* Added a ``priority`` optional scalar argument to ``TaskGraph.add_task`` to
  indicates the priority preference of the task to be executed. A higher
  priority task whose dependencies are satisfied will executed before one with
  a lower priority.

0.3.0 (2017-11-17)
------------------
* Refactor of core scheduler. Old scheduler used asynchronicity to attempt to
  test if a Task was complete, occasionally testing all Tasks in potential
  work queue per task completion. Scheduler now uses bookkeeping to keep track
  of all dependencies and submits tasks for work only when all dependencies
  are satisfied.
* TaskGraph and Task ``.join`` methods now have a timeout parameter.
  Additionally ``join`` now also returns False if ``join`` terminates because
  of a timeout.
* More robust error reporting and shutdown of TaskGraph if any tasks fail
  during execution using pure threading or multiprocessing.


0.2.7 (2017-11-09)
------------------
* Fixed a critical error from the last hotfix that prevented ``taskgraph``
  from avoiding recomputation of already completed tasks.

0.2.6 (2017-11-07)
------------------
* Fixed an issue from the previous hotfix that could cause ``taskgraph`` to
  exceed the number of available threads if enough tasks were added with long
  running dependencies.
* Additional error checking and flow control ensures that a TaskGraph will
  catastrophically fail and report useful exception logging a task fails
  during runtime.
* Fixed a deadlock issue where a failure on a subtask would occasionally cause
  a TaskGraph to hang.
* ``Task.is_complete`` raises a RuntimeError if the task is complete but
  failed.
* More efficient handling of topological progression of task execution to
  attempt to maximize total possible CPU load.
* Fixing an issue from the last release that caused the test cases to fail.
  (Don't use 0.2.5 at all).

0.2.5 (2017-10-11)
------------------
* Fixed a bug where tasks with satisfied dependencies or no dependencies were
  blocked on dependent tasks added to the task graph earlier in the main
  thread execution.
* Indicating that ``psutil`` is an optional dependency through the ``setup``
  function.

0.2.4 (2017-09-19)
------------------
* Empty release.  Possible bug with PyPI release, so re-releasing with a
  bumped up version.

0.2.3 (2017-09-18)
------------------
* More robust testing on a chain of tasks that might fail because an ancestor
  failed.

0.2.2 (2017-08-15)
------------------
* Changed how TaskGraph determines of work is complete.  Now records target
  paths in file token with modified time and file size.  When checking if work
  is complete, the token is loaded and the target file stats are compared for
  each file.

0.2.1 (2017-08-11)
------------------
* Handling cases where a function might be an object or something else that
  can't import source code.
* Using natcap.versioner for versioning.

0.2.0 (2017-07-31)
------------------
* Fixing an issue where ``types.StringType`` is not the same as
  ``types.StringTypes``.
* Redefined ``target`` in ``add_task`` to ``func`` to avoid naming collision
  with ``target_path_list`` in the same function.

0.1.1 (2017-07-31)
------------------
* Fixing a TYPO on ``__version__`` number scheme.
* Importing ``psutil`` if it exists.

0.1.0 (2017-07-29)
------------------
* Initial release.
