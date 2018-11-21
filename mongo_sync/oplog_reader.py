# -*- coding: utf-8 -*-

import logging
import pymongo

import threading
import datetime
import dateutil
import time
import re

from bson import SON, Timestamp, ObjectId

from framemongo import SimpleFrameMongo

logging.basicConfig(level='DEBUG',
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='parser_result.log',
                    filemode='w')

LOG = logging.getLogger(__file__)

docman = None

src_url = 'mongodb://pftz:Pftz8888@192.168.211.190:27017'
dst_url = 'mongodb://192.168.211.169:27017'

up, mongo_host = src_url.split('@')
__, username, password = re.split('://|:', up)
SimpleFrameMongo.config_settings = {
    'name': 'oplog_slices',
    'mongo_host': mongo_host,
    'username': username,
    'password': password
}

whilelist = [
    'fund_report_manager.heartbeat'
]

blacklist = [
]

conf = {
    'src_url': src_url,
    'dst_url': dst_url,
    'whitelist': whilelist,
    'blacklist': blacklist
}

# TODO: gridfs


def ts_to_slice_name(ts):
    return '{}_{}'.format(ts.time, ts.inc)


def slice_name_to_ts(name):
    time, inc = name.split('_')
    ts = Timestamp(time=int(time), inc=int(inc))
    return ts


def dt2ts(dt):
    return Timestamp(int(dt.timestamp()), 0)


class OplogReader(object):

    def __init__(self, start):
        self._running = False
        _dt = dateutil.parser.parse(start)
        self._last_ts = dt2ts(_dt)

        self.docman = DocManager()

    def load_oplog(self):

        with SimpleFrameMongo() as conn:
            last_name = ts_to_slice_name(self._last_ts)
            names = conn.list()
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
                print('Loading slice {}'.format(cur_name))
                self._oplog = cur_slice = conn.read(cur_name)
                print('Loaded slice {}'.format(cur_name))

        return cur_slice

    def replay(self, oplog):
        for n, entry in enumerate(oplog):
            self.docman.process(entry)
        self._last_ts = entry['ts']

    def run(self):
        while self._running:
            oplog = self.load_oplog()
            if oplog is None:
                time.sleep(60)
            else:
                self.replay(oplog)

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self.run)
        self._thread.start()

commands = []
class DocManager(object):

    def __init__(self):
        self.mongo = pymongo.MongoClient(dst_url)
        self._whitelist = conf['whitelist']
        self._blacklist = conf['blacklist']

        if self._whitelist:
            self._should_sync = self.is_in_whitelist
        elif self._blacklist:
            self._should_sync = self.skip_blacklist
        else:
            self.should_sync = lambda x: True

    def is_in_whitelist(self, entry):
        if entry['ns'] in self._whitelist:
            return True
        return False

    def skip_blacklist(self, entry):
        if entry['ns'] in self._blacklist:
            return False
        return True

    def should_sync(self, entry):
        db, coll = entry['ns'].split('.', 1)
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
        print(entry)
        commands.append(entry)
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
                self.mongo[new_db].create_collection(coll)

        if doc.get('drop'):
            new_db, coll = doc['idIndex']['ns'].split('.', 1)
            if new_db:
                self.mongo[new_db].drop_collection(coll)
