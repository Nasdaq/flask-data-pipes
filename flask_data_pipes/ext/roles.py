from enum import IntEnum
from flask import g, abort
from functools import wraps
import warnings


class Role(IntEnum):
    admin = 3
    superuser = 2
    readonly = 1


def require_role(*role):
    def wrapper(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            try:
                if g.user.role >= role.value:
                    return f(*args, **kwargs)
                abort(403)
            except AttributeError:
                warnings.warn('Roles not implemented on users, permissions not evaluated.')
                return f(*args, **kwargs)
        return wrapped
    return wrapper
