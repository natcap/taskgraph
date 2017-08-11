"""taskgraph module."""
from taskgraph.Task import TaskGraph
from taskgraph.Task import Task

import natcap.versioner

__version__ = natcap.versioner.get_version('taskgraph')
