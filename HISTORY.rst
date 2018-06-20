.. :changelog:

.. Unreleased Changes

0.5.1 (2018-06-20)
------------------
* Fixing an issue where TaskGraph would hang on a `join` if the number of
  workers was -1 and a call to `add_task` has `None` passed to
  `target_path_list`.

0.5.0 (2018-05-04)
------------------
* Taskgraph now supports python versions 2 and 3 (tested with python 2.7, 3.6).
* Fixed an issue with ``taskgraph.TaskGraph`` that prevented a multiprocessed
  graph from executing on POSIX systems when ``psutil`` was installed.
* Adding matrix-based test automation (python 2.7, python 3.6, with/without
  ``psutil``) via ``tox``.
* Updating repository path to ``https://bitbucket.org/natcap/taskgraph``.

0.4.0 (2018-04-18)
------------------
* Auto-versioning now happens via ``setuptools_scm``, replacing previous calls to ``natcap.versioner``.
* Added an option to `TaskGraph` constructor to allow negative values in the `n_workers` argument to indicate that the entire object should run in the main thread. A value of 0 will indicate that no multiprocessing will be used but concurrency will be allowed for non-blocking `add_task`.
* Added an abstract class `task.EncapsulatedTaskOp` that can be used to instance a class that needs scope in order to be used as an operation passed to a process. The advantage of using `EncapsulatedTaskOp` is that the `__name__` hash used by `TaskGraph` to determine if a task is unique is calculated in the superclass and the subclass need only worry about implementation of `__call__`.
* Added a `priority` optional scalar argument to `TaskGraph.add_task` to indicates the priority preference of the task to be executed. A higher priority task whose dependencies are satisfied will executed before one with a lower priority.

0.3.0 (2017-11-17)
------------------
* Refactor of core scheduler. Old scheduler used asynchronicity to attempt to test if a Task was complete, occasionally testing all Tasks in potential work queue per task completion. Scheduler now uses bookkeeping to keep track of all dependencies and submits tasks for work only when all dependencies are satisfied.
* TaskGraph and Task `.join` methods now have a timeout parameter. Additionally `join` now also returns False if `join` terminates because of a timeout.
* More robust error reporting and shutdown of TaskGraph if any tasks fail during execution using pure threading or multiprocessing.


0.2.7 (2017-11-09)
------------------
* Fixed a critical error from the last hotfix that prevented `taskgraph` from avoiding recomputation of already completed tasks.

0.2.6 (2017-11-07)
------------------
* Fixed an issue from the previous hotfix that could cause `taskgraph` to exceed the number of available threads if enough tasks were added with long running dependencies.
* Additional error checking and flow control ensures that a TaskGraph will catastrophically fail and report useful exception logging a task fails during runtime.
* Fixed a deadlock issue where a failure on a subtask would occasionally cause a TaskGraph to hang.
* `Task.is_complete` raises a RuntimeError if the task is complete but failed.
* More efficient handling of topological progression of task execution to attempt to maximize total possible CPU load.
* Fixing an issue from the last release that caused the test cases to fail. (Don't use 0.2.5 at all).

0.2.5 (2017-10-11)
------------------
* Fixed a bug where tasks with satisfied dependencies or no dependencies were blocked on dependent tasks added to the task graph earlier in the main thread execution.
* Indicating that `psutil` is an optional dependency through the `setup` function.

0.2.4 (2017-09-19)
------------------
* Empty release.  Possible bug with PyPI release, so re-releasing with a bumped up version.

0.2.3 (2017-09-18)
------------------
* More robust testing on a chain of tasks that might fail because an ancestor failed.

0.2.2 (2017-08-15)
------------------
* Changed how TaskGraph determines of work is complete.  Now records target paths in file token with modified time and file size.  When checking if work is complete, the token is loaded and the target file stats are compared for each file.

0.2.1 (2017-08-11)
------------------
* Handling cases where a function might be an object or something else that can't import source code.
* Using natcap.versioner for versioning.

0.2.0 (2017-07-31)
------------------
* Fixing an issue where `types.StringType` is not the same as `types.StringTypes`.
* Redefined `target` in `add_task` to `func` to avoid naming collision with `target_path_list` in the same function.

0.1.1 (2017-07-31)
------------------
* Fixing a TYPO on __version__ number scheme.
* Importing `psutil` if it exists.

0.1.0 (2017-07-29)
------------------
* Initial release.
