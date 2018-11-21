# -*- coding: utf-8 -*-

import time
import datetime
import functools
import re

from bson import Timestamp


def timeit(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        t0_ = time.time()
        ret = func(*args, **kwargs)
        print('%s in %.6f secs' % (
            func.__name__, time.time() - t0_))
        return ret
    return wrapper


def ts_to_slice_name(ts):
    return '{}_{}'.format(ts.time, ts.inc)


def slice_name_to_ts(name):
    time, inc = name.split('_')
    ts = Timestamp(time=int(time), inc=int(inc))
    return ts


def dt2ts(dt):
    return Timestamp(int(dt.timestamp()), 0)


def namespace_to_regex(namespace):
    """Create a RegexObject from a wildcard namespace."""
    db_name, coll_name = namespace.split('.', 1)
    # A database name cannot contain a '.' character
    db_regex = re.escape(db_name).replace('\*', '([^.]*)')
    # But a collection name can.
    coll_regex = re.escape(coll_name).replace('\*', '(.*)')
    return re.compile(r'\A' + db_regex + r'\.' + coll_regex + r'\Z')
