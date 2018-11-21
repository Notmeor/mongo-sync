# -*- coding: utf-8 -*-


src_url = 'mongodb://pftz:Pftz8888@192.168.211.190:27017'
dst_url = 'mongodb://192.168.211.169:27017'


whilelist = [
    'fund_report_manager.heartbeat',
    'arctic_test_oplog.*',
    'oplog_slices.*'
]

blacklist = [
]

conf = {
    'src_url': src_url,
    'dst_url': dst_url,
    'whitelist': whilelist,
    'blacklist': blacklist
}
