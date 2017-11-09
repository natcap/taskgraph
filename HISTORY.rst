.. :changelog:

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
