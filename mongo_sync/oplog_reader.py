# -*- coding: utf-8 -*-

import os

import logging
import pymongo

import threading
import datetime
import dateutil
import time

from bson import SON

from mongo_sync.utils import (timeit, dt2ts, ts2localtime, ts_to_slice_name,
                              slice_name_to_ts, namespace_to_regex)
from mongo_sync.store import MongoOplogStore as OplogStore
from mongo_sync.config import conf

LOG = logging.getLogger(__file__)

src_url = conf['src_url']
dst_url = conf['dst_url']


class OplogReader(object):

    def __init__(self, start):

        self._oplog_store = OplogStore()

        self._running = False
        _dt = dateutil.parser.parse(start)
        self._last_ts = dt2ts(_dt)

        self.docman = DocManager()

    def load_oplog(self):
        return self._oplog_store.load_oplog(self._last_ts)

    def replay(self, oplog):
        for n, entry in enumerate(oplog):
            # TODO: log excep
            self.docman.process(entry)
        self._last_ts = entry['ts']
        LOG.info('Current progress={}'.format(ts2localtime(self._last_ts)))

    def run(self):
        while self._running:
            LOG.info('Loading ts={}'.format(self._last_ts))
            oplog = self.load_oplog()
            
            if oplog is None:
                time.sleep(60)
                LOG.info('Loaded None')
            else:
                LOG.info('Loaded ts={}, size={}'.format(
                    self._last_ts, len(oplog)))
                self.replay(oplog)

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self.run)
        self._thread.start()
        LOG.info('Started process={}, syncing thread={}'.format(
            os.getpid(), self._thread.ident))


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
            self.handle_command(entry)

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
            new_db, coll = doc['idIndex']['ns'].split('.', 1)
            if new_db:
                self.mongo[new_db].drop_collection(coll)
