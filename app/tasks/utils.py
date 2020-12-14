import string

ALLOWABLE_CHARS = set(string.ascii_letters + string.digits + "-" + "_")


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
