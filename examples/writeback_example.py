# -*- coding: utf-8 -*-

import redis
from pymongo import MongoClient

from rmlru import RedisDelegate, CollectionBase, SetField, ListField, ZsetField, DictField

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
    redis_delegator.check_overload(scheduler_dict={'21:00': ['resources']})

if __name__ == "__main__":
    main()