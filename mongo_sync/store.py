# -*- coding: utf-8 -*-

import os
import datetime
import logging
import pickle

from bson import Timestamp

from mongo_sync.mongo_store import MongoStore

from mongo_sync.utils import (timeit, dt2ts, ts_to_slice_name,
                              slice_name_to_ts, namespace_to_regex)

from mongo_sync.config import conf


class OplogStore(object):
    """
    继承此类实现oplog存取操作

    NOTE
    ----------
    oplog切片命名规则：Timstamp.time_Timstamp.inc (切片末尾时间戳)
    """

    def list_names(self):
        raise NotImplementedError

    def remove(self):
        raise NotImplementedError

    def get_last_saved_ts(self):
        raise NotImplementedError

    def load_oplog(self, last_ts):
        raise NotImplementedError

    def dump_oplog(self, ts):
        raise NotImplementedError


store_url = conf['oplog_store_url']

oplog_store_db = conf['oplog_store_db']


class MongoOplogStore(OplogStore):

    def list_names(self):
        with MongoStore(store_url, oplog_store_db) as store:
            slice_names = store.list()
        return slice_names

    def remove(self, slice_name):
        with MongoStore(store_url, oplog_store_db) as store:
            store.delete(slice_name)

    def get_last_saved_ts(self):

        with MongoStore(store_url, oplog_store_db) as store:
            slices = store.list()
            if not slices:
                return Timestamp(
                    int(datetime.datetime(1970, 1, 2).timestamp()), 0)

            last_slice = max(slices)
            time, inc = last_slice.split('_')
            last_ts = Timestamp(int(time), int(inc))

        return last_ts

    def load_oplog(self, last_ts):

        with MongoStore(store_url, oplog_store_db) as store:

            last_name = ts_to_slice_name(last_ts)
            names = sorted(store.list())
            cur_name = None

            for n in names:
                if n > last_name:
                    cur_name = n
                    break

            if cur_name is None:
                cur_slice = None
            else:
                cur_slice = store.read(cur_name)

        return cur_slice

    def dump_oplog(self, last_ts, oplog):

        with MongoStore(store_url, oplog_store_db) as store:
            name = '{}_{}'.format(last_ts.time, last_ts.inc)
            store.write(name, oplog)


class LocalOplogStore(OplogStore):
    
    def __init__(self):
        self.store_path = conf['local_store_path']
    
    def list_names(self):
        return sorted(os.listdir(self.store_path))

    def remove(self, slice_name):
        raise NotImplementedError

    def get_last_saved_ts(self):

        slices = self.list_names()
        if not slices:
            return Timestamp(
                int(datetime.datetime(1970, 1, 2).timestamp()), 0)

        last_slice = max(slices)
        time, inc = last_slice.split('_')
        last_ts = Timestamp(int(time), int(inc))

        return last_ts

    def load_oplog(self, last_ts):

        last_name = ts_to_slice_name(last_ts)
        names = self.list_names()
        cur_name = None

        for n in names:
            if n > last_name:
                cur_name = n
                break

        if cur_name is None:
            cur_slice = None
        else:
            with open(os.path.join(self.store_path, last_name)) as f:
                cur_slice = pickle.load(f)

        return cur_slice

    def dump_oplog(self, last_ts, oplog):
        raise NotImplementedError


store_type = conf['oplog_store_type']

OplogStore = {
    'MongoOplogStore': MongoOplogStore,
    'LocalOplogStore': LocalOplogStore,
}[store_type]
