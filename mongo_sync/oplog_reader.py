# -*- coding: utf-8 -*-

import os

import logging
import pymongo

import threading
import datetime
import dateutil
import time

from bson import SON, Timestamp

from mongo_sync.utils import (timeit, dt2ts, ts2localtime, ts_to_slice_name,
                              slice_name_to_ts, namespace_to_regex)
from mongo_sync.store import MongoOplogStore as OplogStore
from mongo_sync.config import conf

LOG = logging.getLogger(__file__)

src_url = conf['src_url']
dst_url = conf['dst_url']


class OplogReader(object):

    def __init__(self, start=None):

        self._oplog_store = OplogStore()

        self._running = False

        self._initialize_start_time(start)

        self.docman = DocManager()

    def _initialize_start_time(self, start):
        start = start or conf['sync_start_time']
        if not isinstance(start, datetime.datetime):
            raise TypeError('Expect datetime.datetime, got {}'.format(
                    type(start)))

        self._last_ts = dt2ts(start)
        sync_tag = self.read_tag_file()
        if sync_tag == -1:
            err_msg = 'Sync process crashed last time, ' +\
                'have to manually restore db state'
            LOG.warning(err_msg)
            raise Exception(err_msg)
        elif sync_tag is not None:
            if sync_tag >= self._last_ts:
                self._last_ts = sync_tag
                LOG.warning('Read start timestamp from tag file')
            else:
                err_msg = 'Start timestamp larger than sync tag, delete tag file first'
                LOG.warning(err_msg)
                raise Exception(err_msg)

    @property
    def is_running(self):
        return self._running

    def load_oplog(self):
        oplog = self._oplog_store.load_oplog(self._last_ts)
        if oplog:
            if oplog[0]['ts'] < self._last_ts:
                for n, entry in enumerate(oplog):
                    if entry['ts'] >= self._last_ts:
                        break
                oplog = oplog[n:]
            return oplog
        else:
            return None

    def set_tag_file(self, ts=None):
        if ts is None:
            tag = '-1'
        else:
            tag = '{}_{}={}'.format(ts.time, ts.inc, ts2localtime(ts))
        with open('last_timestamp', 'w') as f:
            f.write(tag)
    
    def read_tag_file(self):
        fname = 'last_timestamp'
        if os.path.exists(fname):
            with open('last_timestamp', 'r') as f:
                tag = f.read()
            
            if tag == '-1':
                return -1
            _time, _inc = tag.split('=')[0].split('_')
            ts = Timestamp(int(_time), int(_inc))
            return ts
        return None

    def replay(self, oplog):

        self.set_tag_file()

        for entry in oplog:
            # TODO: log excep
            self.docman.process(entry)

        self._last_ts = entry['ts']
        self.set_tag_file(self._last_ts)

        LOG.info('Current progress={}'.format(ts2localtime(self._last_ts)))

    def run(self):
        try:
            while self._running:
                LOG.info('Loading ts={}, last progress={}'.format(
                    self._last_ts, ts2localtime(self._last_ts)))
                oplog = self.load_oplog()

                if oplog is None:
                    LOG.info('Loaded None. No more oplog to sync.')
                    time.sleep(10)
                else:
                    LOG.info('Loaded ts={}, size={}'.format(
                        self._last_ts, len(oplog)))
                    self.replay(oplog)
        except Exception as e:
            LOG.error(str(e), exc_info=True)

        LOG.warning('Oplog syncing stopped.')

    def start(self):
        LOG.warning('Oplog syncing starting...')
        self._running = True
        self._thread = threading.Thread(target=self.run)
        self._thread.start()
        LOG.warning('Started pid={}, syncing thread={}'.format(
            os.getpid(), self._thread.ident))

    def safe_stop(self):
        self._running = False
        LOG.warning('Would stop as soon as current slice syncing completes.')


class DocManager(object):

    def __init__(self):
        self.mongo = pymongo.MongoClient(dst_url)
        self._whitelist = conf['whitelist']
        self._blacklist = conf['blacklist']

        if self._whitelist:
            self._whitelist_regex = []
            for ns in self._whitelist:
                ns_regex = namespace_to_regex(ns)
                self._whitelist_regex.append(ns_regex)

            self._should_sync = self.is_in_whitelist
        elif self._blacklist:
            self._should_sync = self.skip_blacklist
            self._blacklist_regex = []
            for ns in self._blacklist:
                ns_regex = namespace_to_regex(ns)
                self._blacklist_regex.append(ns_regex)
        else:
            self.should_sync = lambda x: True

        LOG.info('Whitelist: {}, blacklist: {}'.format(
            self._whitelist, self._blacklist))

    def is_in_whitelist(self, entry):
        for regex in self._whitelist_regex:
            if regex.match(entry['ns']):
                return True
        return False

    def skip_blacklist(self, entry):
        for regex in self._blacklist_regex:
            if regex.match(entry['ns']):
                return True
        return True

    def should_sync(self, entry):
        db, coll = entry['ns'].split('.', 1)
        # ignore system.indexes
        if coll.startswith("system."):
            return False
        if coll == "$cmd":
            return True
        return self._should_sync(entry)

    def process(self, entry):

        if not self.should_sync(entry):
            return

        operation = entry['op']

        # Remove
        if operation == 'd':
            self.remove(entry)

        # Insert
        elif operation == 'i':
            self.insert(entry)

        # Update
        elif operation == 'u':
            self.update(entry)

        # Command
        elif operation == 'c':
            try:
                self.handle_command(entry)
            except pymongo.errors.OperationFailure as e:
                LOG.warning('Command failed: {}'.format(e))

    def insert(self, entry):
        doc = entry['o']
        ns = entry['ns']
        db_name, coll_name = ns.split('.', 1)
        self.mongo[db_name][coll_name].replace_one(
            {'_id': doc['_id']},
            doc, upsert=True)

    def update(self, entry):

        _id = entry['o2']['_id']
        doc = entry['o']
        ns = entry['ns']
        db_name, coll_name = ns.split('.', 1)

        no_obj_error = "No matching object found"
        updated = self.mongo[db_name].command(
            SON([('findAndModify', coll_name),
                 ('query', {'_id': _id}),
                 ('update', doc),
                 ('new', True)]),
            allowable_errors=[no_obj_error])['value']
        return updated

    def remove(self, entry):

        doc = entry['o']
        ns = entry['ns']
        db_name, coll_name = ns.split('.', 1)

        self.mongo[db_name][coll_name].delete_one(
            {'_id': doc['_id']})

    def handle_command(self, entry):

        # TODO: filter by blacklist/whitelist
        doc = entry['o']
        if doc.get('dropDatabase'):
            db_name = entry['ns'].split('.', 1)[0]
            self.mongo.drop_database(db_name)

        if doc.get('renameCollection'):
            a = doc['renameCollection']
            b = doc['to']
            if a and b:
                self.mongo.admin.command(
                    "renameCollection", a, to=b)

        if doc.get('create'):
            new_db, coll = doc['idIndex']['ns'].split('.', 1)
            if new_db:
                self.mongo[new_db].command(SON(doc))

        if doc.get('drop'):
            db_name = entry['ns'].split('.', 1)[0]
            coll = doc['drop']
            if db_name:
                self.mongo[db_name].drop_collection(coll)
