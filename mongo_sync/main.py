
import sys
import argparse
import time
import datetime

from mongo_sync import oplog_dump
from mongo_sync import oplog_replay
from mongo_sync.config import conf
from mongo_sync.emails import send_email


def alert(message):
    send_email(
        conf['email']['smtp_mail_to'], 
        'mongo-sync failed',
         f'{datetime.datetime.now()} {message}'
    )


def dump_oplog():
    om = oplog_dump.OplogDump()
    om.start()

    # while om.is_running():
    #     time.sleep(5)
    
    alert('oplog dump stopped')


def replay_oplog():
    op = oplog_replay.OplogReplay()
    op.start()

    while True:
        time.sleep(5)
    
    alert('oplog dump stopped')


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--dump', dest='dump', action='store_true', default=False,
        help='dump mongodb oplog')
    parser.add_argument(
        '--replay', dest='replay', action='store_true', default=False,
        help='load and repaly mongodb oplog')

    options = parser.parse_args()

    if not(options.dump or options.replay):
        parser.print_help(sys.stderr)
        sys.exit(1)
    else:
        if options.dump:
            dump_oplog()
        elif options.replay:
            replay_oplog()
