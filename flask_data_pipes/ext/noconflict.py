def noconflict(*args, **kwargs):
    """Use this as an explicit metaclass to fix metaclass conflicts
    You can use this as `class c3(c1, c2, metaclass=noconflict):` to
    automatically fix metaclass conflicts between c1 and c2, or as
    `class c3(c1, c2, metaclass=noconflict(othermeta))` to explicitly
    add in another metaclass in addition to any used by c1 and c2.

    From a 2015 gist by PJ Eby (pjeby).
    """
    explicit_meta = None

    def make_class(name, bases, attrs, **kwargs):
        meta = metaclass_for_bases(bases, explicit_meta)
        return meta(name, bases, attrs, **kwargs)

    if len(args) == 1 and not kwargs:
        explicit_meta, = args
        return make_class
    else:
        return make_class(*args, **kwargs)


class sentinel:
    """Marker for detecting incompatible root metaclasses"""


def metaclass_for_bases(bases, explicit_mc=None):
    """Determine metaclass from 1+ bases and optional explicit metaclass"""

    meta = [getattr(b, '__class__', type(b)) for b in bases]
    if explicit_mc is not None:
        # The explicit metaclass needs to be verified for compatibility
        # as well, and allowed to resolve the incompatible bases, if any
        meta.insert(0, explicit_mc)

    candidates = normalized_bases(meta)

    if not candidates:
        # No bases, use type
        return type

    elif len(candidates) > 1:
        return derived_meta(tuple(candidates))

    # Just one, return it
    return candidates[0]


def normalized_bases(classes):
    """Remove redundant base classes from `classes`"""
    candidates = []
    for m in classes:
        for n in classes:
            if issubclass(n, m) and m is not n:
                break
        else:
            # m has no subclasses in 'classes'
            if m in candidates:
                candidates.remove(m)  # ensure that we're later in the list
            candidates.append(m)
    return candidates


# Weak registry, so unused derived metaclasses will die off
from weakref import WeakValueDictionary

meta_reg = WeakValueDictionary()


def derived_meta(metaclasses):
    """Synthesize a new metaclass that mixes the given `metaclasses`"""

    derived = meta_reg.get(metaclasses)

    if derived is sentinel:
        # We should only get here if you have a metaclass that
        # doesn't inherit from `type` -- in Python 2, this is
        # possible if you use ExtensionClass or some other exotic
        # metaclass implemented in C, whose metaclass isn't `type`.
        # In practice, this isn't likely to be a problem in Python 3,
        # or even in Python 2, but we shouldn't just crash with
        # unbounded recursion here, so we give a better error message.
        raise TypeError("Incompatible root metatypes", metaclasses)

    elif derived is None:
        # prevent unbounded recursion
        meta_reg[metaclasses] = sentinel

        # get the common meta-metaclass of the metaclasses (usually `type`)
        metameta = metaclass_for_bases(metaclasses)

        # create a new metaclass
        meta_reg[metaclasses] = derived = metameta(
            '_'.join(m.__name__ for m in metaclasses), metaclasses, {}
        )

    return derived
