import warnings
import time, functools

import pickle
import hashlib
import lz4.block

import pymongo
from gridfs import GridFS
from bson import ObjectId


def compress(b):
    return lz4.block.compress(b, mode='fast')


def decompress(b):
    return lz4.block.decompress(b)


class Serializer:

    @staticmethod
    def serialize(obj):
        ret = pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)
        ret = compress(ret)
        return ret

    @staticmethod
    def deserialize(b):
        try:
            b = decompress(b)
        except lz4.block.LZ4BlockError:
            pass
        ret = pickle.loads(b)
        return ret

    @classmethod
    def gen_md5(cls, b, value=False):
        bytes_ = cls.serialize(b)
        md5 = hashlib.md5(bytes_).hexdigest()
        if value:
            return md5, bytes_
        return md5

serializer = Serializer


class MongoStore(object):
    
    config_settings = {}
    
    def __init__(self, uri=None, db_name=None):

        if uri is None:
            db_name = self.config_settings['name'] 
            mongo_host = self.config_settings['mongo_host']
            username = self.config_settings['username']
            password = self.config_settings['password']
            
            self.db = pymongo.MongoClient(mongo_host)[db_name]
            self.db.authenticate(username, password)
        else:
            if db_name is None:
                raise Exception('Must provide target db name')
            self.db = pymongo.MongoClient(uri)[db_name]
        
        self.fs = GridFS(self.db)
        
    def write(self, name, df, metadata='', upsert=True):
        
        if upsert:
            self.delete(name)
            
        if name in self.fs.list():
            warnings.warn(
                'filename `{}` already exists, nothing inserted'.format(name))
            return 
                            
        return self.fs.put(
            serializer.serialize(df),
            filename=name,
            metadata=metadata
        )
    
    def delete(self, name):
        doc = self.db['fs.files'].find_one(
            {'filename': name})
        if doc:
            _id = doc.get('_id')
            self.fs.delete(_id)
        
    def read(self, name):

        def _read(name):
            return self.fs.find_one(
                {'filename': name}).read()

        sr = _read(name)
        return serializer.deserialize(sr)
    
    def read_metadata(self, name):
        return self.db['fs.files'].find_one(
            {'filename': name}).get('metadata')
    
    def list(self):
        return self.fs.list()
    
    def __enter__(self):
        return self
    
    def __exit__(self, et, ev, tb):
        self.close()
    
    def close(self):
        self.db.client.close()