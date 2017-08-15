.. :changelog:

.. Unreleased Changes

Unreleased Changes
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
