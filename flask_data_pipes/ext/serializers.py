from datetime import datetime
from functools import singledispatch

__all__ = [dict, list, tuple, str, int, float, bool, type(None), datetime]


@singledispatch
def serialize(val):
    """Used by default."""
    return str(val)


@serialize.register(datetime)
def ts_datetime(val):
    """Used if *val* is an instance of datetime."""
    return val.isoformat() + "Z"
