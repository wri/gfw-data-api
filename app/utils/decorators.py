import functools


# See https://stackoverflow.com/questions/6358481/using-functools-lru-cache-with-dictionary-arguments
# Required to use a dict argument with @alru_cache, since it needs to be hasheable/immutable
def hash_dict(func):
    """Transform mutable dictionnary Into immutable Useful to be compatible
    with cache."""

    class HDict(dict):
        def __hash__(self):
            return hash(frozenset(self.items()))

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        args = tuple([HDict(arg) if isinstance(arg, dict) else arg for arg in args])
        kwargs = {k: HDict(v) if isinstance(v, dict) else v for k, v in kwargs.items()}
        return func(*args, **kwargs)

    return wrapped
