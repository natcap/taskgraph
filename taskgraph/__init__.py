"""TaskGraph init module."""
from pkg_resources import get_distribution

from .Task import TaskGraph
from .Task import Task
from .Task import _TASKGRAPH_DATABASE_FILENAME

__all__ = ['TaskGraph', 'Task', '_TASKGRAPH_DATABASE_FILENAME']
__version__ = get_distribution(__name__).version
