# -*- coding: utf-8 -*-

import datetime
import time
import threading
import re
import functools

import pymongo
from bson import Timestamp, ObjectId
from pandas.io.pickle import pkl

from framemongo import SimpleFrameMongo


src_url = 'mongodb://pftz:Pftz8888@192.168.211.190:27017'
# dst_url = 'mongodb://pftz:Pftz8888@192.168.211.190:27017'

up, mongo_host = src_url.split('@')
__, username, password = re.split('://|:', up)
SimpleFrameMongo.config_settings = {
    'name': 'oplog_slices',
    'mongo_host': mongo_host,
    'username': username,
    'password': password
}

def timeit(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        t0_ = time.time()
        ret = func(*args, **kwargs)
        print('%s in %.6f secs' % (
            func.__name__, time.time() - t0_))
        return ret
    return wrapper

def dt2ts(dt):
    return Timestamp(int(dt.timestamp()), 0)

class OplogManager(object):

    def __init__(self, start=None):
        self._client = pymongo.MongoClient(src_url)
        self._oplog = self._client['local']['oplog.rs']

        _start = self._oplog.find_one(
            sort=[('$natural', pymongo.ASCENDING)])['ts']
        
        if start is None:
            self._start_ts = _start
        else:
            self._start_ts = max(dt2ts(start), _start)
            
        # incrementing
        last_saved_ts = self.get_last_saved_ts()
        if self._start_ts < last_saved_ts:
            self._start_ts = last_saved_ts

        self._last_ts = None
        self._slice_interval = datetime.timedelta(minutes=5)

        self._hungry = False
        self._running = False

    @timeit
    def save_sliced(self, sliced):
        with SimpleFrameMongo() as conn:
            last_dt = self._last_ts
            name = '{}_{}'.format(last_dt.time, last_dt.inc)
            conn.write(name, sliced)

    def get_last_saved_ts(self):
        with SimpleFrameMongo() as conn:
            slices = conn.list()
            if not slices:
                return Timestamp(
                    int(datetime.datetime(1970, 1, 1, 8).timestamp()), 0)

            last_slice = max(slices)
            time, inc = last_slice.split('_')
            last_ts = Timestamp(int(time), int(inc))
        return last_ts
            
    def get_latest_ts(self):
        return self._oplog.find_one(
            {'op': {'$ne': 'n'}}, sort=[('$natural', pymongo.DESCENDING)]
        )['ts']

    def slice_oplog(self):
        @timeit
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

        

    def run_dumping(self):

        while self._running:
            
            if self._last_ts is None:
                self._last_ts = self._start_ts
    
            self._next_ts = dt2ts(self._last_ts.as_datetime() + 
                                  self._slice_interval)
        
            latest_ts = self.get_latest_ts()
            
            if latest_ts < self._next_ts:
                self._hungry = True
                time.sleep(self._next_ts.time - latest_ts.time)
            else:
                if self._hungry:
                    self._next_ts = latest_ts
                self.slice_oplog()

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self.run_dumping)
        self._thread.start()
    
    def stop(self):
        self._running = False


