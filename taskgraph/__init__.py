"""taskgraph module."""
from __future__ import unicode_literals
from __future__ import absolute_import
import pkg_resources
from .Task import TaskGraph, Task, EncapsulatedTaskOp


__all__ = ['TaskGraph', 'Task', 'EncapsulatedTaskOp']


try:
    __version__ = pkg_resources.get_distribution(__name__).version
except pkg_resources.DistributionNotFound:
    # Package is not installed, so the package metadata is not available.
    # This should only happen if a package is importable but the package
    # metadata is not, as might happen if someone copied files into their
    # system site-packages or they're importing this package from the CWD.
    raise RuntimeError(
        "Could not load version from installed metadata.\n\n"
        "This is often because the package was not installed properly. "
        "Ensure that the package is installed in a way that the metadata is "
        "maintained.  Calls to ``pip`` and this package's ``setup.py`` "
        "maintain metadata.  Examples include:\n"
        "  * python setup.py install\n"
        "  * python setup.py develop\n"
        "  * pip install <distribution>")
