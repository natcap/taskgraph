TaskGraph:
=================================================

About TaskGraph
===============

TaskGraph is great.

TaskGraph Dependencies
======================

Task Graph is written in pure Python, but if the ``psutils`` package is
installed the distributed multiprocessing processes will be ``nice``\d.

Example Use
===========

Install taskgraph with

`pip install taskgraph`

Then

.. code-block:: python

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
  task_graph = taskgraph.TaskGraph(self.workspace_dir, 4)
  target_a_path = os.path.join(self.workspace_dir, 'a.dat')
  target_b_path = os.path.join(self.workspace_dir, 'b.dat')
  result_path = os.path.join(self.workspace_dir, 'result.dat')
  result_2_path = os.path.join(self.workspace_dir, 'result2.dat')
  value_a = 5
  value_b = 10
  list_len = 10
  task_a = task_graph.add_task(
      target=_create_list_on_disk,
      args=(value_a, list_len, target_a_path),
      target_path_list=[target_a_path])
  task_b = task_graph.add_task(
      target=_create_list_on_disk,
      args=(value_b, list_len, target_b_path),
      target_path_list=[target_b_path])
  sum_task = task_graph.add_task(
      target=_sum_lists_from_disk,
      args=(target_a_path, target_b_path, result_path),
      target_path_list=[result_path],
      dependent_task_list=[task_a, task_b])
  sum_task.join()

  # expect that result is a list `list_len` long with `value_a+value_b` in it
  result = pickle.load(open(result_path, 'rb'))
