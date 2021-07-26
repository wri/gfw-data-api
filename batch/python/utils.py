import sys
import traceback
from functools import wraps
from multiprocessing import Process, Queue


class SubprocessKilledError(Exception):
    pass


def processify(func):
    """Decorator to run a function as a process.

    Be sure that every argument and the return value
    is *picklable*.
    The created process is joined, so the code is
    synchronous.
    Modified from original to not hang when subprocess
    gets killed (such as from OOM).
    Credits: https://gist.github.com/schlamar/2311116
    """

    def process_func(q, *args, **kwargs):
        try:
            ret = func(*args, **kwargs)
        except Exception:
            ex_type, ex_value, tb = sys.exc_info()
            error = ex_type, ex_value, "".join(traceback.format_tb(tb))
            ret = None
        else:
            error = None

        q.put((ret, error))

    # register original function with different name
    # in sys.modules so it is picklable
    process_func.__name__ = func.__name__ + "processify_func"
    setattr(sys.modules[__name__], process_func.__name__, process_func)

    @wraps(func)
    def wrapper(*args, **kwargs):
        q = Queue()
        p = Process(target=process_func, args=(q, *args), kwargs=kwargs)

        error = None
        ret = None
        untimely_death = False

        p.start()

        while p.is_alive():
            p.join(timeout=60)  # TODO: Make configurable
            if p.exitcode is None:
                continue
            elif p.exitcode < 0:
                untimely_death = True
                break
            ret, error = q.get()

        if untimely_death:
            raise SubprocessKilledError("Process was killed")
        elif error:
            ex_type, ex_value, tb_str = error
            message = "%s (in subprocess)\n%s" % (str(ex_value), tb_str)
            raise ex_type(message)

        return ret

    return wrapper
