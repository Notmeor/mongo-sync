# -*- coding: utf-8 -*-

import time
import datetime
import functools
import re

from pymongo import IndexModel
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


def ts2localtime(ts):
    return ts.as_datetime().astimezone()


def namespace_to_regex(namespace):
    """Create a RegexObject from a wildcard namespace."""
    db_name, coll_name = namespace.split('.', 1)
    # A database name cannot contain a '.' character
    db_regex = re.escape(db_name).replace('\*', '([^.]*)')
    # But a collection name can.
    coll_regex = re.escape(coll_name).replace('\*', '(.*)')
    return re.compile(r'\A' + db_regex + r'\.' + coll_regex + r'\Z')


def copy_index(src_coll, dst_coll):
    """
    Copy collection index settings
    """
    ind_settings = src_coll.index_information()
    models = []
    
    def _to_legal_ind_direction(val):
        if isinstance(val, str):
            return val
        return int(val)
    
    for ind_name in ind_settings:
        if ind_name == '_id_':
            continue
        ind = ind_settings[ind_name]
        print(ind)
        
        keys = [(e[0], _to_legal_ind_direction(e[1])) for e in ind['key']]

        models.append(IndexModel(
            keys,
            unique=ind.get('unique', False),
            background=ind.get('background', False)))

    if len(models) > 0:
        dst_coll.create_indexes(models)
