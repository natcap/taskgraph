import logging
import pickle
import shutil
import time


def _return_value_once(value):
    """Return the value passed to it only once."""
    if hasattr(_return_value_once, 'executed'):
        raise RuntimeError("this function was called twice")
    _return_value_once.executed = True
    return value


def _noop_function(**kwargs):
    """Do nothing except allow kwargs to be passed."""
    pass


def _long_running_function(delay):
    """Wait for ``delay`` seconds."""
    print('start')
    time.sleep(delay)
    print('done')


def _create_two_files_on_disk(value, target_a_path, target_b_path):
    """Create two files and write ``value`` and append if possible."""
    with open(target_a_path, 'a') as a_file:
        a_file.write(value)

    with open(target_b_path, 'a') as b_file:
        b_file.write(value)


def _merge_and_append_files(base_a_path, base_b_path, target_path):
    """Merge two files and append if possible to new file."""
    with open(target_path, 'a') as target_file:
        for base_path in [base_a_path, base_b_path]:
            with open(base_path, 'r') as base_file:
                target_file.write(base_file.read())


def _create_list_on_disk(value, length, target_path=None):
    """Create a numpy array on disk filled with value of ``size``."""
    target_list = [value] * length
    pickle.dump(target_list, open(target_path, 'wb'))


def _call_it(target, *args):
    """Invoke ``target`` with ``args``."""
    target(*args)


def _append_val(path, *val):
    """Append a ``val`` to file at ``path``."""
    with open(path, 'a') as target_file:
        for v in val:
            target_file.write(str(v))


_append_val_2 = _append_val


def _sum_lists_from_disk(list_a_path, list_b_path, target_path):
    """Read two lists, add them and save result."""
    list_a = pickle.load(open(list_a_path, 'rb'))
    list_b = pickle.load(open(list_b_path, 'rb'))
    target_list = []
    for a, b in zip(list_a, list_b):
        target_list.append(a+b)
    pickle.dump(target_list, open(target_path, 'wb'))


def _div_by_zero():
    """Divide by zero to raise an exception."""
    return 1/0


def _create_file(target_path, content):
    """Create a file with contents."""
    with open(target_path, 'w') as target_file:
        target_file.write(content)


def _create_file_once(target_path, content):
    """Create a file on the first call, raise an exception on the second."""
    if hasattr(_create_file_once, 'executed'):
        raise RuntimeError("this function was called twice")
    _create_file_once.executed = True
    with open(target_path, 'w') as target_file:
        target_file.write(content)


def _copy_file_once(base_path, target_path):
    """Copy base to target on the first call, raise exception on second."""
    if hasattr(_copy_file_once, 'executed'):
        raise RuntimeError("this function was called twice")
    _copy_file_once.executed = True
    shutil.copyfile(base_path, target_path)


def _copy_two_files_once(base_path, target_a_path, target_b_path):
    """Copy base to target a/b on first call, raise exception on second."""
    if hasattr(_copy_two_files_once, 'executed'):
        raise RuntimeError("this function was called twice")
    _copy_two_files_once.executed = True
    shutil.copyfile(base_path, target_a_path)
    shutil.copyfile(base_path, target_b_path)


def _log_from_another_process(logger_name, log_message):
    """Write a log message to a given logger.

    Args:
        logger_name (string): The string logger name to which ``log_message``
            will be logged.
        log_message (string): The string log message to be logged (at INFO
            level) to the logger at ``logger_name``.

    Returns:
        ``None``

    """
    print('log from another')
    logger = logging.getLogger(logger_name)
    print('logging message', log_message)
    logger.info(log_message)
    print('logged')
