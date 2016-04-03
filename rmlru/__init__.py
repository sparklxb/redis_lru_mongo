# -*- coding: utf-8 -*-

"""
data structures in redis
keys_modified: set, key_names have been modified since readed from mongodb to redis
lru_queue: zset, member: key_name, score: time

common field name means field name stored in hash, complex field( subfield) name means field name stored in set, list, or zset.

NOTE: 
for names Not exists, smembers return set([])
                      lrange return []
                      zrange return []
                      hgetall return {}
                      hmget return [None,...]
                      get return None

for key Not exists, hmget return a None in a list in the same order as args
                      zrank return None
"""

import redis
import time
import uuid
import json
from bson import json_util
from functools import partial
from copy import deepcopy

KEYS_MODIFIED_SET = 'keys_modified'
LRU_QUEUE = 'lru_queue'
EVERY_ZRANGE_NUM = 1000
LOCK_TIMEOUT = 10

def make_lockname(key_name):
    return 'lock:' + key_name

def make_key_name(*args):
    return ':'.join(map(str, args))

# make complex field key name in redis
def make_sub_key_name(*args):
    return '.'.join(map(str, args))

def acquire_lock_with_timeout(conn, key_name, lock_timeout=LOCK_TIMEOUT):
    """
    Tell scheduler that I will do sth with this key in LOCK_TIMEOUT seconds, so it can't write it back to mongo
    """
    identifier = str(uuid.uuid4())
    lockname = make_lockname(key_name)
    conn.setex(lockname, lock_timeout, identifier)

class IndirectField(object):
    def set(self, val) :
        raise NotImplementedError()
    def get(self, val):
        raise NotImplementedError() 

class DictField(IndirectField):
    def set(self, val) :
        if not isinstance(val, str):
            return json.dumps(val, default=json_util.default)
        else:
            return val
    def get(self, val):
        if isinstance(val, str):
            return json.loads(val, object_hook=json_util.object_hook)
        else:
            return val

class NumberField(IndirectField):
    """
    field_type can be int, long or float
    """
    def __init__(self, field_type=int):
        self.field_type = field_type
    def set(self, val) :
        return val
    def get(self, val):
        return self.field_type(val)

class ComplexField(object):
    """
    field_type: IndirectField or None (for string)
    field_name: field_name in mongo
    key_name: [collection name]:[key in mongo].[field_name in mongo]
    """
    def __init__(self, field_name, field_type=None):
        self.field_name = field_name
        self.field_type = field_type

    def __get__(self, obj, objtype):
        self.conn = obj.redis_delegate.conn
        self.col = obj
        self.key_name = make_sub_key_name(obj._key, self.field_name)
        obj.make_data_in_redis([self.field_name])
        return self

    def __set__(self, obj, val):
        """
        only for non-transitional command using =
        """
        raise NotImplementedError()

    def get(self):
        raise NotImplementedError()

    def set(self, val):
        raise NotImplementedError()

    def _handle_members_list(self, member_score_list, is_set=True):
        if isinstance(self.field_type, IndirectField):
            tmp = list()
            if is_set:
                f = self.field_type.set
            else:
                f = self.field_type.get
            for v in member_score_list:
                tmp.append(f(v))
            return tmp
        else:
            return member_score_list

    def _handle_one_member(self, val, is_set=True):
        if isinstance(self.field_type, IndirectField):
            if is_set:
                return self.field_type.set(val)
            else:
                return self.field_type.get(val)
        else:
            return val

    def record_modify(self):
        self.conn.sadd(KEYS_MODIFIED_SET, self.key_name)
        self.conn.zadd(LRU_QUEUE, time.time(), self.key_name)

class ZsetField(ComplexField):
    """
    docstring for ZsetField
    """
    def __init__(self, field_name, member_name, member_type, score_name, score_type):
        super(ZsetField, self).__init__(field_name)
        self.member_name = member_name
        self.member_type = member_type
        self.score_name = score_name
        self.score_type = score_type

    def _handle_members_list(self, member_score_list):
        if isinstance(self.member_type, IndirectField):
            i = 1
            tmp = list()
            while i < len(member_score_list):
                tmp.append(self.member_type.set(member_score_list[i]))
                i += 2
            return tmp
        else:
            return member_score_list

    def __set__(self, obj, val):
        self.key_name = make_sub_key_name(obj._key, self.field_name)
        self.conn = obj.redis_delegate.conn.pipeline()

        if obj.need_record_modify():
            self.record_modify()

        self.conn.delete(self.key_name)
        if val:
            member_score_list = list()
            for v in val:
                if self.member_name in v and self.score_name in v:
                    member_score_list.append(v[self.score_name])
                    member_score_list.append(v[self.member_name])
                else :
                    print "Error: %s miss %s or %s" % (str(v), self.member_name, self.score_name)
            if member_score_list:
                self._handle_members_list(member_score_list)
                self.conn.zadd(self.key_name, *member_score_list)
        self.conn.execute()

        # change back to non-transactional mode
        self.conn = obj.redis_delegate.conn

    def __getattr__(self, attr):
        raise AttributeError, attr + ' not allowed currently'

    def zcard(self):
        res = self.col.get_from_just_loaded(self.field_name)
        if res:
            return len(res)
        else :
            return self.conn.zcard(self.key_name)

    def zadd(self, *values, **kwargs):
        self.record_modify()
        values = self._handle_members_list(values)
        return self.conn.zadd(self.key_name, *values, **kwargs)

    def zscore(self, member):
        """
        ignore _document_just_loaded_from_mongo, because zscore is more direct and easy
        """
        score = self.conn.zscore(self.key_name, member)
        if score and self.score_type is not float:
            score = self.score_type(score)
        return score

    def zrem(self, *values):
        self.record_modify()
        values = self._handle_members_list(values)
        return self.conn.zrem(self.key_name, *values)

    def zrange(self, start, end):
        res = self.col.get_from_just_loaded(self.field_name)
        if res:
            if -1 != end:
                return res[start: end+1]
            else:
                return res[start:]
        else:
            values = self.conn.zrange(self.key_name, start, end, withscores=True, score_cast_func=self.score_type)
            res = list()
            for v in values:
                res.append({self.member_name: self.member_type(v[0]), self.score_name: v[1]})
            return res

    def get(self):
        return self.zrange(0, -1)

class SetField(ComplexField):
    def __set__(self, obj, val):
        self.key_name = make_sub_key_name(obj._key, self.field_name)
        self.conn = obj.redis_delegate.conn.pipeline()

        if obj.need_record_modify():
            self.record_modify()

        self.conn.delete(self.key_name)
        if val:
            val = self._handle_members_list(val)
            self.conn.sadd(self.key_name, *val)
        self.conn.execute()

        # change to not transaction
        self.conn = obj.redis_delegate.conn

    def __getattr__(self, attr):
        raise AttributeError, attr + ' not allowed currently'

    def scard(self):
        res = self.col.get_from_just_loaded(self.field_name)
        if res:
            return len(res)
        else :
            return self.conn.scard(self.key_name)

    def sadd(self, *values):
        self.record_modify()
        values = self._handle_members_list(values)
        return self.conn.sadd(self.key_name, *values)

    def sismember(self, val):
        res = self.col.get_from_just_loaded(self.field_name)
        if res:
            return val in res
        else:
            val = self._handle_one_member(val)
            return self.conn.sismember(self.key_name, val)

    def smembers(self):
        val = self.col.get_from_just_loaded(self.field_name)
        if not val:
            val = self.conn.smembers(self.key_name)
        return set(val)

    get = smembers

    def srem(self, *values):
        self.record_modify()

        values = self._handle_members_list(values)
        return self.conn.srem(self.key_name, *values)

class ListField(ComplexField):
    def __set__(self, obj, val):
        self.key_name = make_sub_key_name(obj._key, self.field_name)
        self.conn = obj.redis_delegate.conn.pipeline()
        if obj.need_record_modify():
            self.record_modify()

        self.conn.delete(self.key_name)
        if val:
            val = self._handle_members_list(val)
            self.conn.rpush(self.key_name, *val)
        self.conn.execute()

        self.conn = obj.redis_delegate.conn

    def lrem(self, count, val):
        self.record_modify()

        val = self._handle_one_member(val)
        val = self.conn.lrem(self.key_name, count, val)
        return val

    def ltrim(self, start, end):
        self.record_modify()

        val = self.conn.ltrim(self.key_name, start, end)
        return val

    def llen(self):
        res = self.col.get_from_just_loaded(self.field_name)
        if res:
            return len(res)
        else:
            return self.conn.llen(self.key_name)

    def lindex(self, index):
        res = self.col.get_from_just_loaded(self.field_name)
        if res:
            if index < len(res):
                return res[index]
            else :
                return None
        else:
            val = self.conn.lindex(self.key_name, index)
            val = self._handle_one_member(val, False)
            return val

    def lpop(self):
        self.record_modify()

        val = self.conn.lpop(self.key_name)
        return val

    def rpush(self, *values):
        self.record_modify()

        if values:
            values = self._handle_members_list(values)
            return self.conn.rpush(self.key_name, *values)

    def lrange(self, start, end):
        res = self.col.get_from_just_loaded(self.field_name)
        if res:
            if -1 != end:
                return res[start: end+1]
            else:
                return res[start:]
        else:
            values = self.conn.lrange(self.key_name, start, end)
            values = self._handle_members_list(values, False)
            return values

    def get(self):
        return self.lrange(0, -1)

class CollectionMetaclass(type):
    def __new__(cls, name, bases, attrs):
        subfield_names = list()
        for k, v in attrs.iteritems():
            if not k.startswith('_'):
                if isinstance(v, ComplexField):
                    subfield_names.append(k)
                elif isinstance(v, IndirectField):
                    cls._none_string_key_name_dict[k] = v
        attrs['_subfield_names'] = subfield_names
        if '_ignore_field_names' in attrs:
            ignore_field_name_list = attrs['_ignore_field_names'] + ['_id', attrs['_key_name']]
        else:
            ignore_field_name_list = ['_id', attrs['_key_name']]
        attrs['_ignore_field_names'] = dict.fromkeys(ignore_field_name_list, 0)
        return type.__new__(cls, name, bases, attrs)


class CollectionBase(object):
    """
    Don't define vars not starting with '_' by yourself
    _key:  The key name of collection in redis
    _key_name: The key name of collection in mongo
    _key_type: The type of key in mongo, such as int, long, float or str
    _col_name: Collection name
    _none_string_key_name_dict: dict maping the name to type of field whose type is not str, list, set and zset
    _ignore_field_names: the list of fields which we never need to load into redis
    """
    __metaclass__ = CollectionMetaclass
    _mongo_key = None
    _key = None
    _key_name = ""
    _key_type = None
    _col_name = ""
    _none_string_key_name_dict = dict()
    # just parts of one document, the key and its value are not in it!
    _document_just_loaded_from_mongo = dict()
    # just a trick for find funciton
    _is_already_in_redis = False
    # just a trick for make_data_in_redis
    _need_record_modify = True
    _need_lock = True
    _ignore_field_names = list()

    redis_delegate = None

    def __init__(self, key_name=None):
        if key_name:
            self._key_name = key_name

    def record_modify(self):
        if self.need_record_modify():
            self.redis_delegate.conn.sadd(KEYS_MODIFIED_SET, self._key)
            self.redis_delegate.conn.zadd(LRU_QUEUE, time.time(), self._key)

    def get_hashes_by_dict(self, hashes_dict):
        for k, v in self._none_string_key_name_dict.iteritems():
            if k in hashes_dict:
                hashes_dict[k] = v(hashes_dict[k])
        return hashes_dict

    def need_record_modify(self):
        return self._need_record_modify

    def turn_on_record_modify(self):
        self._need_record_modify = True

    def turn_off_record_modify(self):
        self._need_record_modify = False

    def is_already_in_redis(self):
        return self._is_already_in_redis

    def turn_on_already_in_redis(self):
        self._is_already_in_redis = True

    def turn_off_already_in_redis(self):
        self._is_already_in_redis = False

    def get_all_key_names(self):
        field_names = self.get_all_class_var_names()
        sub_key_names =  map(partial(make_sub_key_name, self._key), field_names)
        return sub_key_names, field_names

    def get_from_just_loaded(self, key_name):
        return self._document_just_loaded_from_mongo.get(key_name)

    def get_all_class_var_names(self):
        return [_ for _ in self.__class__.__dict__ if not _.startswith('_')]

    def set_redis_delegate(self, redis_delegate):
        self.redis_delegate = redis_delegate

    def __call__(self, key):
        self._mongo_key = key
        self._key = make_key_name(self._col_name, key)
        return self

    def __setattr__(self, attr, value):
        """
        set one common field with =
        """
        # thanks to http://stackoverflow.com/questions/9161302/using-both-setattr-and-descriptors-for-a-python-class
        for cls in self.__class__.__mro__ + (self, ):
            if attr in cls.__dict__:
                return object.__setattr__(self, attr, value)
        # self.make_data_in_redis(need_hash=True)
        if value is None:
            mongo_col = getattr(self.redis_delegate.mongo_conn, self._col_name)
            mongo_key = self._mongo_key
            self.redis_delegate.conn.hdel(self._key, attr)
            mongo_col.update({self._key_name: mongo_key}, {"$set": {attr: None}}, True)
        else:
            self.redis_delegate.conn.hset(self._key, attr, value)
            self.record_modify()

    def __getattr__(self, attr):
        self.make_data_in_redis(need_hash=True)
        res = self.get_from_just_loaded(attr)
        if res:
            return res
        else : # not likely!
            res = self.redis_delegate.conn.hget(self._key, attr)
            if attr in self._none_string_key_name_dict:
                return self._none_string_key_name_dict[attr](res)
            else:
                return res

    def make_data_in_redis(self, field_names=(), need_hash=False):
        '''
        self: collection_self
        if field_names is () and need_hash=False, reload all data in the collection
        '''
        if self.is_already_in_redis():
            return

        self.turn_off_record_modify()

        conn = self.redis_delegate.conn
        mongo_col = getattr(self.redis_delegate.mongo_conn, self._col_name)
        key_name = self._key_name

        self._document_just_loaded_from_mongo.clear()

        field_name_not_in_redis_list = list()
        all_sub_key_names, all_field_names = self.get_all_key_names()

        if not field_names and not need_hash:
            sub_key_names, field_names = all_sub_key_names, all_field_names
            need_hash = True
        else:
            sub_key_names = map(partial(make_sub_key_name, self._key), field_names)

        for sub_key_name, field_name in zip(sub_key_names, field_names):
            if self._need_lock:
                acquire_lock_with_timeout(conn, sub_key_name)
            self.redis_delegate.conn.zadd(LRU_QUEUE, time.time(), sub_key_name)
            if not conn.exists(sub_key_name):
                ##print "try_fetch:", sub_key_name
                field_name_not_in_redis_list.append(field_name)

        if need_hash:
            if self._need_lock:
                acquire_lock_with_timeout(conn, self._key)
            self.redis_delegate.conn.zadd(LRU_QUEUE, time.time(), self._key)

            if conn.exists(self._key):
                need_hash = False
            ##else:
            ##    print "try_fetch:", self._key

        if field_name_not_in_redis_list or need_hash:
            mongo_key = self._mongo_key
            res = mongo_col.find_one({key_name: mongo_key}, self._ignore_field_names)
            if not res:
                self.turn_on_record_modify()
                return
            
            for field_name in all_field_names:
                if field_name in res:
                    val = res.pop(field_name)
                    if field_name in field_name_not_in_redis_list:
                        self.turn_on_already_in_redis()
                        getattr(self, field_name).__set__(self, val)
                        self.turn_off_already_in_redis()
                        self._document_just_loaded_from_mongo[field_name] = val

            if need_hash:
                self._document_just_loaded_from_mongo.update(res)
                # rm empty val
                to_be_pop_list = list()
                for k, v in res.iteritems():
                    if not v:
                        to_be_pop_list.append(k)
                for k in to_be_pop_list:
                        res.pop(k)

                conn.hmset(self._key, res)

        self.turn_on_record_modify()

    def _get_all_hashes(self, key):
        res = self.redis_delegate.conn.hgetall(make_key_name(self._col_name, key))
        return self.get_hashes_by_dict(res)

    def update(self, doc_dict):
        doc_dict = deepcopy(doc_dict)

        for field_name in self.get_all_class_var_names():
            if field_name in doc_dict:
                getattr(self, field_name).__set__(self, doc_dict.pop(field_name))

        if doc_dict:
            for k, v in doc_dict.items():
                if v is None:
                    doc_dict.pop(k)
                    self.redis_delegate.conn.hdel(self._key, k)
            if doc_dict:
                self.redis_delegate.conn.hmset(self._key, doc_dict)
            self.record_modify()

    def find(self, key, field_name_list=None):
        res = dict()
        if field_name_list is None:
            self.make_data_in_redis()
            if self._document_just_loaded_from_mongo:
                return self._document_just_loaded_from_mongo
            else:
                res = self._get_all_hashes(key)
                for field_name in self.get_all_class_var_names():
                    res[field_name] = getattr(self, field_name).get()
            return res
        else:
            common_field_name_list = list()
            complex_field_name_list = list()
            self._key = make_key_name(self._col_name, key)
            self._mongo_key = key

            if field_name_list:
                all_complex_field_name = self.get_all_class_var_names()
                for field_name in field_name_list:
                    if field_name in all_complex_field_name:
                        complex_field_name_list.append(field_name)
                    else:
                        common_field_name_list.append(field_name)
            self.make_data_in_redis(complex_field_name_list, bool(common_field_name_list))
            self.turn_on_already_in_redis()
            for field_name in complex_field_name_list:
                _res = self.get_from_just_loaded(field_name)
                if _res:
                    res[field_name] = _res
                else:
                    res[field_name] = getattr(self, field_name).get()

            for field_name in common_field_name_list:
                _res = self.get_from_just_loaded(field_name)
                if _res:
                    res[field_name] = _res
                else:
                    res[field_name] = getattr(self, field_name)
            self.turn_off_already_in_redis()
        return res

    def write_back(self, key, field_name=None):
        mongo_col = getattr(self.redis_delegate.mongo_conn, self._col_name)
        #conn = self.redis_delegate.conn
        if field_name:
            res_list = getattr(self(key), field_name).get()
            mongo_col.update({self._key_name: key}, {"$set": {field_name: res_list}}, True)
        else:
            res_dict = self._get_all_hashes(key)
            mongo_col.update({self._key_name: key}, {"$set": res_dict}, True)

class RedisDelegate(object):
    """docstring for RedisDelegate"""
    def __init__(self, redis_conn, sync_db):
        self.conn = redis_conn
        self.mongo_conn = sync_db
        self.col_name_list = list()

    def set_redis_conn(self, redis_conn):
        self.conn = redis_conn

    def set_mongo(self, sync_db):
        self.mongo_conn = sync_db

    def add_collection(self, collection, col_name=None):
        if col_name:
            assert (isinstance(col_name, str) and 0 == col_name.split())
        else:
            col_name = collection._col_name or collection.__class__.__name__

        if hasattr(self, col_name):
            raise AttributeError, col_name + 'already exists'

        collection.set_redis_delegate(self)
        self.__dict__[col_name] = collection
        self.col_name_list.append(col_name)

    def parse_sub_key_name(self, sub_key_name):
        col_name, others = sub_key_name.split(':', 2)
        others = others.split('.', 2)
        if 2 == len(others):
            key, field_name = others
        else:
            key = others[0]
            field_name = ""
        key = getattr(self, col_name)._key_type(key)
        return col_name, key, field_name

    def try_write_back(self, conn, key_name):
        pipe = conn.pipeline()
        identifier = str(uuid.uuid4())
        lockname = make_lockname(key_name)
        if conn.setnx(lockname, identifier):
            try:
                pipe.watch(lockname)
                pre_identifier = pipe.get(lockname)
                ismember = pipe.sismember(KEYS_MODIFIED_SET, key_name)

                # the lock isn't modified by other clients, otherwise just ignore it
                if pre_identifier != identifier:
                    pipe.unwatch()
                    return False
                else:
                    if ismember:
                        col_name, key, field_name = self.parse_sub_key_name(key_name)
                        col = getattr(self, col_name)
                        col._need_lock = False
                        col.write_back(key, field_name)
                        col._need_lock = True
                        ##print "write_back", key_name

                    pipe.multi()
                    pipe.delete(key_name)
                    if ismember:
                        pipe.srem(KEYS_MODIFIED_SET, key_name)
                    pipe.zrem(LRU_QUEUE, key_name)
                    pipe.delete(lockname)
                    res = pipe.execute()
                    return True
            except redis.exceptions.WatchError, e:
                return False

    def check_overload(self, interval=5, lru_queue_num_min=10000, lru_queue_num_max=15000, scheduler_dict=None):
        """
        scheduler_dict: time we want to write back certain collection to mongo, for example {time: col_name_list}
            when empty, it means all!
            the format of time: [hour]:[minite], such as '3:10'
        """
        from datetime import date, datetime, time as nomal_time
        for col_name in self.col_name_list:
            getattr(self, col_name).turn_on_already_in_redis()

        scheduler_list = list()
        last_write_all_back_day = date(1970, 1, 1)
        # the keys we want to write back when writing all back but are using
        left_key_list = list()
        scheduler_list_index = 0

        if scheduler_dict:
            for k, v in scheduler_dict.iteritems():
                sche_time = map(int, k.split(':'))
                sche_time = nomal_time(*sche_time)
                scheduler_list.append((sche_time, v))
            scheduler_list = sorted(scheduler_list, key=lambda x: x[0])

        half_interval = interval / 2
        while True:
            conn = self.conn
            now = datetime.now().time()
            if scheduler_list and last_write_all_back_day < date.today() and now >= scheduler_list[scheduler_list_index][0]:
                col_name_list = scheduler_list[scheduler_list_index][1]
                to_be_writeback_list = conn.zrange(LRU_QUEUE, len(left_key_list), EVERY_ZRANGE_NUM + len(left_key_list))
                while to_be_writeback_list:
                    for key_name in to_be_writeback_list:
                        if key_name.split(':', 1)[0] in col_name_list:
                            isSuccess = self.try_write_back(conn, key_name)
                            if not isSuccess:
                                left_key_list.append(key_name)
                    to_be_writeback_list = conn.zrange(LRU_QUEUE, len(left_key_list), EVERY_ZRANGE_NUM + len(left_key_list))
                    scheduler_list_index += 1
                    if scheduler_list_index == len(scheduler_list):
                        scheduler_list_index = 0
                        last_write_all_back_day = date.today()

            # try left_key_list again
            if left_key_list:
                for i, key_name in enumerate(left_key_list):
                    isSuccess = self.try_write_back(conn, key_name)
                    if isSuccess:
                        del left_key_list[i]

            num = conn.zcard(LRU_QUEUE)
            if num >= lru_queue_num_max:
                rm_num = num - lru_queue_num_min
                to_be_writeback_list = conn.zrange(LRU_QUEUE, 0, rm_num)
                for key_name in to_be_writeback_list:
                    self.try_write_back(conn, key_name)
            elif num >= lru_queue_num_min:
                time.sleep(half_interval)
            else:
                time.sleep(interval)
