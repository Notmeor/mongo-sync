# -*- coding: utf-8 -*-

import os
import datetime
import time
import threading
import logging

import pymongo

from mongo_sync.utils import timeit, dt2ts, slice_name_to_ts, ts2localtime
from mongo_sync.store import MongoOplogStore as OplogStore
from mongo_sync.config import conf

LOG = logging.getLogger(__file__)

src_url = conf['src_url']

keep_days = conf['keep_days']

# TODO: non-intrusive logging


class OplogManager(object):

    def __init__(self, start=None, interval=None):

        self._oplog_store = OplogStore()

        self._client = pymongo.MongoClient(src_url)
        self._oplog = self._client['local']['oplog.rs']

        self._initialize_slice_range(start, interval)

        self._hungry = False
        self._running = False

    def _initialize_slice_range(self, start, interval):

        _start = self.get_first_ts()

        start = start or conf['oplog_start_time']

        if start:
            self._start_ts = max(dt2ts(start), _start)
        else:
            self._start_ts = _start

        # incrementing
        last_saved_ts = self.get_last_saved_ts()

        LOG.info('Last saved ts={}'.format(last_saved_ts))

        if self._start_ts < last_saved_ts:
            self._start_ts = last_saved_ts

        self._last_ts = None

        interval = interval or conf['oplog_dump_interval']
        self._slice_interval = datetime.timedelta(minutes=interval)

        LOG.info('Initial ts={}, interval={}'.format(self._start_ts, interval))

    @property
    def is_running(self):
        if not self._running:
            return False
        if not self._thread.is_alive():
            return False
        return True

    def remove_expiration(self):
        slice_names = self._oplog_store.list_names()

        if slice_names:
            first_name = min(slice_names)
            first_dt = ts2localtime(slice_name_to_ts(first_name))

            if (datetime.date.today() - first_dt.date()).days > keep_days:
                self._oplog_store.remove(first_name)
                LOG.info('Removed slice {}'.format(first_name))

    def save_sliced(self, sliced):
        self._oplog_store.dump_oplog(self._last_ts, sliced)

    def get_last_saved_ts(self):
        return self._oplog_store.get_last_saved_ts()

    def get_first_ts(self):
        return self._oplog.find_one(
            {'op': {'$ne': 'n'}}, sort=[('$natural', pymongo.ASCENDING)]
        )['ts']

    def get_latest_ts(self):
        return self._oplog.find_one(
            {'op': {'$ne': 'n'}}, sort=[('$natural', pymongo.DESCENDING)]
        )['ts']

    def slice_oplog(self):

        def get_cursor():
            query = {'op': {'$ne': 'n'},
                     'ts': {'$gt': self._last_ts, '$lte': self._next_ts}}
            cursor = self._oplog.find(
                query,
                cursor_type=pymongo.CursorType.TAILABLE_AWAIT,
                oplog_replay=True)
            return cursor

        cursor = get_cursor()
        sliced = list(cursor)

        if not sliced:
            time.sleep(10)

        self._last_ts = sliced[-1]['ts']
        self.save_sliced(sliced)

        LOG.info('Dumped size={}, ts={}'.format(len(sliced), self._last_ts))

    def run_dumping(self):

        while self._running:
            self.remove_expiration()

            if self._last_ts is None:
                self._last_ts = self._start_ts

            self._next_ts = dt2ts(self._last_ts.as_datetime() +
                                  self._slice_interval)

            latest_ts = self.get_latest_ts()

            if latest_ts < self._next_ts:
                self._hungry = True
                LOG.info('Hungry, waiting feed...')
                time.sleep(self._next_ts.time - latest_ts.time)
            else:
                if self._hungry:
                    self._next_ts = latest_ts
                    self._hungry = False
                self.slice_oplog()

        LOG.warning('Oplog dumping stopped.')

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self.run_dumping)
        self._thread.start()
        LOG.warning('Started pid={}, dumping thread={}'.format(
                os.getpid(), self._thread.ident))

    def safe_stop(self):
        self._running = False
        LOG.warning('Would stop as soon as current slice dumping completes.')
