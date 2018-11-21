# -*- coding: utf-8 -*-

import datetime
import logging

from bson import Timestamp

from framemongo import SimpleFrameMongo

from mongo_sync.utils import (timeit, dt2ts, ts_to_slice_name,
                              slice_name_to_ts, namespace_to_regex)

from mongo_sync.config import conf

LOG = logging.getLogger(__file__)

src_url = conf['src_url']


class OplogStore(object):
    """
    继承此类实现oplog存取操作

    NOTE
    ----------
    oplog切片命名规则：Timstamp.time_Timstamp.inc
    """

    def get_last_saved_ts(self):
        raise NotImplementedError

    def load_oplog(self, last_ts):
        raise NotImplementedError

    def dump_oplog(self, ts):
        raise NotImplementedError


class MongoOplogStore(OplogStore):

    def get_last_saved_ts(self):

        with SimpleFrameMongo(src_url, 'oplog_slices') as store:
            slices = store.list()
            if not slices:
                return Timestamp(
                    int(datetime.datetime(1970, 1, 2).timestamp()), 0)

            last_slice = max(slices)
            time, inc = last_slice.split('_')
            last_ts = Timestamp(int(time), int(inc))

        return last_ts

    def load_oplog(self, last_ts):

        with SimpleFrameMongo(src_url, 'oplog_slices') as store:

            last_name = ts_to_slice_name(last_ts)
            names = store.list()
            cur_name = None

            for n in names:
                if n > last_name:
                    cur_name = n
                    break

            if cur_name is None:
                LOG.warning('No more docs')
                cur_slice = None
            else:
                LOG.info('Loading slice {}'.format(cur_name))
                cur_slice = store.read(cur_name)

        return cur_slice

    def dump_oplog(self, last_ts, oplog):

        with SimpleFrameMongo(src_url, 'oplog_slices') as store:
            name = '{}_{}'.format(last_ts.time, last_ts.inc)
            store.write(name, oplog)
