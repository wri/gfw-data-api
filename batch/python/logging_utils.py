import logging
import sys
from logging.handlers import QueueHandler


def listener_configurer():
    """Run this in the parent process to configure logger."""
    root = logging.getLogger()
    h = logging.StreamHandler(stream=sys.stdout)
    root.addHandler(h)


def log_listener(queue, configurer):
    """Run this in the parent process to listen for log messages from
    children."""
    configurer()
    while True:
        try:
            record = queue.get()
            if (
                record is None
            ):  # We send this as a sentinel to tell the listener to quit.
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)  # No level or filter logic applied - just do it!
        except Exception:
            import traceback

            print("Encountered a problem in the log listener!", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            raise


def log_client_configurer(queue):
    """Run this in child processes to configure sending logs to parent."""
    h = QueueHandler(queue)
    root = logging.getLogger()
    root.addHandler(h)
    root.setLevel(logging.INFO)
