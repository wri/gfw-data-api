import string

ALLOWABLE_CHARS = set(string.ascii_letters + string.digits + "-" + "_")


class RingOfLists:
    def __init__(self, size):
        self._size = size
        self._lists = [list() for i in range(0, size)]
        self._idx = -1

    def __next__(self):
        self._idx += 1
        if self._idx >= self._size:
            self._idx = 0
        return self._lists[self._idx]

    def __iter__(self):
        self.idx = -1
        return self

    def all(self):
        return self._lists


def sanitize_batch_job_name(proposed_name: str) -> str:
    """Make a string acceptable as an AWS Batch job name According to AWS docs,
    the first character must be alphanumeric, the name can be up to 128
    characters, and ASCII uppercase + lowercase letters, numbers, hyphens, and
    underscores are allowed."""
    short_name: str = proposed_name[:125]

    if not str.isalnum(short_name[0]):
        short_name = "x_" + proposed_name[:]

    filtered_name = ""
    for char in short_name:
        if char in ALLOWABLE_CHARS:
            filtered_name += char
        else:
            filtered_name += "_"

    return filtered_name
