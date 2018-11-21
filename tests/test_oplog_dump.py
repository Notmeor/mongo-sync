# -*- coding: utf-8 -*-

import os
print(os.getcwd())
os.environ['MONGOSYNC_CONF'] = os.path.realpath('./config.yaml')

from mongo_sync import oplog_manager

from mongo_sync import oplog_reader

op = op = oplog_reader.OplogReader('20181119 10:30:00')