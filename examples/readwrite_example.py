#!/usr/bin/env python
# -*- coding: utf-8 -*-

import redis
from pymongo import MongoClient

from redis_lru_scheduler import RedisDelegate, CollectionBase, SetField, ListField, ZsetField, DictField

class Resources(CollectionBase):
    _key_name = 'res_id'
    _key_type = str
    _col_name = "resources"
    _none_string_key_name_dict = {'type': int, 'file_size': int}
    _ignore_field_names = list('unwanted')
    tags = SetField('tags')
    exp_info = ListField('exp_info', DictField())
    owners = ZsetField('owners', 'uid', long, 'count', int)

def main():
    sync_db = MongoClient('localhost', 27017).test
    redis_conn = redis.StrictRedis()
    resources = Resources()
    redis_delegator = RedisDelegate(redis_conn, sync_db)
    redis_delegator.add_collection(resources)

    resource = redis_delegator.resources('1')
    print resource.find('1', ['owners', 'file_name'])
    print resource.file_name
    resource.owners.zadd(111, 1, 222, 0)
    print resource.owners.get_all_items()
    print resource.owners.zcard()
    resource.file_name = "xyz"
    resource.file_size = 123
    print resource.owners.zrem(111)
    print resource.find('1', ['owners', 'file_name'])
    resource.tags.sadd('x', 'y', 'z')
    resource.exp_info.rpush({'a': 1, 2: 'b'}, {3: 1, 21: 'b'})
    print resource.exp_info.llen()
    print resource.find('1', ['tags', 'exp_info'])

if __name__ == "__main__":
    main()