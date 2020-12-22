"""
    Helper functions related to basic Python objects
"""

import sys
from numbers import Number
from collections import deque
from collections.abc import Set, Mapping

# All 'getsize' functionality is shamelessly stolen from here:
#  https://stackoverflow.com/questions/449560/how-do-i-determine-the-size-of-an-object-in-python

try:
    # Python 2
    zero_depth_bases = (basestring, Number, xrange, bytearray)
    iteritems = 'iteritems'
except NameError:
    # Python 3
    zero_depth_bases = (str, bytes, Number, range, bytearray)
    iteritems = 'items'


def get_size(obj_0):
    """Recursively iterate to sum size of object & members."""
    _seen_ids = set()

    def inner(obj):
        obj_id = id(obj)
        if obj_id in _seen_ids:
            return 0
        _seen_ids.add(obj_id)
        size = sys.getsizeof(obj)
        if isinstance(obj, zero_depth_bases):
            pass  # bypass remaining control flow and return
        elif isinstance(obj, (tuple, list, Set, deque)):
            size += sum(inner(i) for i in obj)
        elif isinstance(obj, Mapping) or hasattr(obj, iteritems):
            size += sum(inner(k) + inner(v) for k, v in getattr(obj, iteritems)())
        # Check for custom object instances - may subclass above too
        if hasattr(obj, '__dict__'):
            size += inner(vars(obj))
        if hasattr(obj, '__slots__'):  # can have __slots__ with __dict__
            size += sum(inner(getattr(obj, s)) for s in obj.__slots__ if hasattr(obj, s))
        return size

    return inner(obj_0)
