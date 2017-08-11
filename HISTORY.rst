.. :changelog:

.. Unreleased Changes

Unreleased Changes
------------------

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
