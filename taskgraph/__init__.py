"""taskgraph module."""
import pkg_resources
from taskgraph.Task import *

try:
    __version__ = pkg_resources.get_distribution(__name__).version
except pkg_resources.DistributionNotFound:
    # package is not installed.
    pass
