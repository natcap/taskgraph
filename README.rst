===============
About TaskGraph
===============

``TaskGraph`` is a library that was developed to help manage complicated
computational software pipelines consisting of long running individual tasks.
Many of these tasks could be executed in parallel, almost all of them wrote
results to disk, and many times results could be reused from part of the
pipeline. TaskGraph manages all of this for you. With it you can schedule
tasks with dependencies, avoid recomputing results that have already been
computed, and allot multiple CPU cores to execute tasks in parallel if
desired.

TaskGraph Dependencies
----------------------

Task Graph is written in pure Python, but if the ``psutils`` package is
installed the distributed multiprocessing processes will be ``nice``\d.

Example Use
-----------

Install ``TaskGraph`` with

``pip install taskgraph``

Then

.. code-block:: python

  import os
  import pickle
  import logging

  import taskgraph

  logging.basicConfig(level=logging.DEBUG)

  def _create_list_on_disk(value, length, target_path):
      """Create a numpy array on disk filled with value of `size`."""
      target_list = [value] * length
      pickle.dump(target_list, open(target_path, 'wb'))


  def _sum_lists_from_disk(list_a_path, list_b_path, target_path):
      """Read two lists, add them and save result."""
      list_a = pickle.load(open(list_a_path, 'rb'))
      list_b = pickle.load(open(list_b_path, 'rb'))
      target_list = []
      for a, b in zip(list_a, list_b):
          target_list.append(a+b)
      pickle.dump(target_list, open(target_path, 'wb'))

  # create a taskgraph that uses 4 multiprocessing subprocesses when possible
  if __name__ == '__main__':
      workspace_dir = 'workspace'
      task_graph = taskgraph.TaskGraph(workspace_dir, 4)
      target_a_path = os.path.join(workspace_dir, 'a.dat')
      target_b_path = os.path.join(workspace_dir, 'b.dat')
      result_path = os.path.join(workspace_dir, 'result.dat')
      result_2_path = os.path.join(workspace_dir, 'result2.dat')
      value_a = 5
      value_b = 10
      list_len = 10
      task_a = task_graph.add_task(
          func=_create_list_on_disk,
          args=(value_a, list_len, target_a_path),
          target_path_list=[target_a_path])
      task_b = task_graph.add_task(
          func=_create_list_on_disk,
          args=(value_b, list_len, target_b_path),
          target_path_list=[target_b_path])
      sum_task = task_graph.add_task(
          func=_sum_lists_from_disk,
          args=(target_a_path, target_b_path, result_path),
          target_path_list=[result_path],
          dependent_task_list=[task_a, task_b])

      task_graph.close()
      task_graph.join()

      # expect that result is a list `list_len` long with `value_a+value_b` in it
      result = pickle.load(open(result_path, 'rb'))


Caveats
-------

* Taskgraph's default method of checking whether a file has changed
  (``hash_algorithm='sizetimestamp'``) uses the filesystem's modification
  timestamp, interpreted in integer nanoseconds.  This check is only as
  accurate as the filesystem's timestamp.  For example:

  * FAT and FAT32 timestamps have a 2-second modification timestamp resolution
  * exFAT has a 10 millisecond timestamp resolution
  * NTFS has a 100 nanosecond timestamp resolution
  * HFS+ has a 1 second timestamp resolution
  * APFS has a 1 nanosecond timestamp resolution
  * ext3 has a 1 second timestamp resolution
  * ext4 has a 1 nanosecond timestamp resolution

  If you suspect timestamp resolution to be an issue on your filesystem, you
  may wish to store your files on a filesystem with more accurate timestamps or
  else consider using a different ``hash_algorithm``.


Running Tests
-------------

Taskgraph includes a ``tox`` configuration for automating builds across
multiple python versions and whether ``psutil`` is installed.  To execute all
tests on all platforms, run:

    $ tox

Alternatively, if you're only trying to run tests on a single configuration
(say, python 3.7 without ``psutil``), you'd run::

    $ tox -e py37

Or if you'd like to run the tests for the combination of Python 3.7 with
``psutil``, you'd run::

    $ tox -e py37-psutil

If you don't have multiple python installations already available on your system,
an easy way to accomplish this is to use ``tox-conda``
(https://github.com/tox-dev/tox-conda) which will use conda environments to manage
the versions of python available::

    $ pip install tox-conda
    $ tox
