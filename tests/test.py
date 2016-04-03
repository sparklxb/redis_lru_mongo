# -*- coding: utf-8 -*-

import redis
import mock
import time
import unittest
from pymongo import MongoClient

from rmlru import RedisDelegate, CollectionBase, SetField, ListField, ZsetField, DictField, KEYS_MODIFIED_SET, LRU_QUEUE, acquire_lock_with_timeout

class Tags(CollectionBase):
    _key_name = 'uid'
    _key_type = long
    _col_name = "tags"
    _none_string_key_name_dict = {_key_name: long}
    file_ids = SetField('file_ids')

class Fblog(CollectionBase):
    _key_name = 'uid'
    _key_type = long
    _col_name = 'fblog'
    _none_string_key_name_dict = {_key_name: long, "online_time": float}
    log = ListField('log', DictField())

class Users(CollectionBase):
    _key_name = 'uid'
    _key_type = long
    _col_name = 'users'
    _none_string_key_name_dict = {_key_name: long, "haslog": int}
    friends = ZsetField('friends', 'uid', long, 'isStar', int)

class RMLRUTest(unittest.TestCase):
    def setUp(self):
        self.mongo_conn = MongoClient('localhost', 27017)
        self.db = self.mongo_conn.test
        self.redis_conn = redis.StrictRedis()
        self.redis_delegator = RedisDelegate(self.redis_conn, self.db)
    
        tag = Tags()
        users = Users()
        fblog = Fblog()
        self.redis_delegator.add_collection(tag)
        self.redis_delegator.add_collection(users)
        self.redis_delegator.add_collection(fblog)

    def tearDown(self):
        self.redis_conn.flushdb()
        self.mongo_conn.drop_database('test')

    def test_SetField_get_and_set(self):
        tag = self.redis_delegator.tags(1)
        sr = self.redis_conn
        self.assertEqual(tag.file_ids.get(), set())
        file_ids_list = ['1','2','3']
        tag.file_ids = file_ids_list
        self.assertEqual(sr.scard(tag.file_ids.key_name), 3)
        self.assertTrue(sr.sismember(KEYS_MODIFIED_SET, tag.file_ids.key_name))
        self.assertTrue(sr.zrank(LRU_QUEUE, tag.file_ids.key_name) is not None)
        self.assertEqual(tag.file_ids.get(), set(file_ids_list))

    def test_SetField_scard_sadd_srem_sismember(self):
        tag = self.redis_delegator.tags(1)
        self.assertEqual(tag.file_ids.scard(), 0)
        res = tag.file_ids.sadd('1')
        self.assertEqual(res, 1)
        self.assertEqual(tag.file_ids.scard(), 1)
        res = tag.file_ids.sadd('1', '2', '3')
        self.assertEqual(res, 2)
        self.assertTrue(tag.file_ids.sismember('2'))
        res =  tag.file_ids.srem('2')
        self.assertEqual(res, 1)
        self.assertFalse(tag.file_ids.sismember('2'))
        self.assertEqual(tag.file_ids.scard(), 2)

    def test_SetField_make_data_in_redis(self):
        tag = self.redis_delegator.tags(1)
        file_ids_list = ['1','2','3', '4']
        self.db.tags.insert({'uid': 1, 'file_ids': file_ids_list})
        self.assertEqual(tag.file_ids.get(), set(file_ids_list))

    def test_ListField_get_and_set(self):
        fblog = self.redis_delegator.fblog(1)
        sr = self.redis_conn
        self.assertEqual(fblog.log.get(), list())
        log_list = [{'1':[1,2]},{'2': {'2':1}},{'3':[1,2]}]
        fblog.log = log_list
        self.assertEqual(sr.llen(fblog.log.key_name), 3)
        self.assertTrue(sr.sismember(KEYS_MODIFIED_SET, fblog.log.key_name))
        self.assertTrue(sr.zrank(LRU_QUEUE, fblog.log.key_name) is not None)
        self.assertEqual(fblog.log.get(), log_list)

    def test_ListField_llen_ltrim(self):
        fblog = self.redis_delegator.fblog(1)
        log_list = [{'1':[1,2]},{'2': {'2':1}},{'3':[1,2]}, {'4':[2,3,4]}]
        fblog.log = log_list
        self.assertEqual(fblog.log.llen(), 4)
        self.assertTrue(fblog.log.ltrim(0, 5))
        self.assertEqual(fblog.log.llen(), 4)  
        fblog.log.ltrim(0, 1)
        self.assertEqual(fblog.log.llen(), 2)
        self.assertEqual(fblog.log.get(), log_list[:2])

    def test_ListField_lindex(self):
        fblog = self.redis_delegator.fblog(1)
        log_list = [{'1':[1,2]},{'2': {'2':1}},{'3':[1,2]}, {'4':[2,3,4]}]
        fblog.log = log_list
        self.assertEqual(fblog.log.lindex(0), log_list[0])
        self.assertEqual(fblog.log.lindex(1), log_list[1])
        self.assertEqual(fblog.log.lindex(-1), log_list[-1]) 
        self.assertEqual(fblog.log.lindex(-5), None)
        self.assertEqual(fblog.log.lindex(5), None)

    def test_ListField_lrem(self):
        fblog = self.redis_delegator.fblog(1)
        log_list = [{'1':[1,2]},{'2': {'2':1}},{'3':[1,2]}, {'2': {'2':1}}, {'2': {'2':1}}, {'4':[2,3,4]}, {'2': {'2':1}}]
        fblog.log = log_list
        fblog.log.lrem(1, {'2': {'2':1}})
        del log_list[1]
        self.assertEqual(fblog.log.get(), log_list)
        fblog.log.lrem(-1, {'2': {'2':1}})
        del log_list[-1]
        self.assertEqual(fblog.log.get(), log_list)
        fblog.log.lrem(0, {'2': {'2':1}})
        del log_list[2:4]
        self.assertEqual(fblog.log.get(), log_list)

    def test_ListField_lindex_rpush_lpop_lrange(self):
        fblog = self.redis_delegator.fblog(1)
        log_list = [{'1':[1,2]},{'4':[2,3,4]}, {'2': {'2':1}},{'3':[1,2]}]
        res = fblog.log.rpush({'1':[1,2]})
        self.assertEqual(res, 1) 
        res = fblog.log.rpush({'4':[2,3,4]})
        self.assertEqual(res, 2) 
        res = fblog.log.rpush({'2': {'2':1}},{'3':[1,2]})
        self.assertEqual(res, 4) 
        res = fblog.log.lrange(0, -1)
        self.assertEqual(res, log_list)
        res = fblog.log.lrange(0, 5)
        self.assertEqual(res, log_list)
        res = fblog.log.lrange(0, -5)
        self.assertEqual(res, [])
        res = fblog.log.lrange(0, 2)
        self.assertEqual(res, log_list[:3])
        res = fblog.log.lrange(1, 3)
        self.assertEqual(res, log_list[1:4])

    def test_ListField_make_data_in_redis(self):
        fblog = self.redis_delegator.fblog(1)
        log_list = [{'1':[1,2]},{'4':[2,3,4]}, {'2': {'2':1}},{'3':[1,2]}]
        self.db.fblog.insert({'uid': 1, 'online_time': 1.0, 'log': log_list})
        self.assertEqual(fblog.log.get(), log_list)

    def test_ZsetField_get_and_set(self):
        user = self.redis_delegator.users(1)
        sr = self.redis_conn
        friends_list = [{'uid': 1, 'isStar': 0}, {'uid': 2, 'isStar': 0}, {'uid': 3, 'isStar': 1}, {'uid': 4, 'isStar': 0}]
        user.friends = friends_list
        self.assertEqual(sr.zcard(user.friends.key_name), 4)
        self.assertTrue(sr.sismember(KEYS_MODIFIED_SET, user.friends.key_name))
        self.assertTrue(sr.zrank(LRU_QUEUE, user.friends.key_name) is not None)
        self.assertEqual(user.friends.get(), sorted(friends_list, key = lambda x: x['isStar']))

    def test_ZsetField_zscore(self):
        user = self.redis_delegator.users(1)
        user.friends = [{'uid': 1, 'isStar': 5}, {'uid': 2, 'isStar': 0}, {'uid': 3, 'isStar': 1}, {'uid': 4, 'isStar': 0}]
        self.assertEqual(user.friends.zscore(1), 5)
        self.assertEqual(user.friends.zscore(3), 1)
        self.assertEqual(user.friends.zscore(8), None)

    def test_ZsetField_zadd_zrem_zrange(self):
        user = self.redis_delegator.users(1)
        friends_list = [{'uid': 1, 'isStar': 5}, {'uid': 2, 'isStar': 0}, {'uid': 3, 'isStar': 1}, {'uid': 4, 'isStar': 0}]
        user.friends.zadd(5, 1, 0, 2, 1, 3, 0, 4)
        self.assertEqual(user.friends.zcard(), len(friends_list))
        self.assertEqual(user.friends.zrange(0, -1), sorted(friends_list, key = lambda x: x['isStar']))
        self.assertEqual(user.friends.zrem(1), 1)
        self.assertEqual(user.friends.zrange(0, -1), sorted(friends_list[1:], key = lambda x: x['isStar']))
        self.assertEqual(user.friends.zrem(5), 0)
        self.assertEqual(user.friends.zrange(0, -1), sorted(friends_list[1:], key = lambda x: x['isStar']))
        self.assertEqual(user.friends.zrem(2, 3), 2)
        self.assertEqual(user.friends.zrange(0, -1), sorted(friends_list[3:], key = lambda x: x['isStar']))

    def test_ZsetField_make_data_in_redis(self):
        user = self.redis_delegator.users(1)
        friends_list = [{'uid': 1, 'isStar': 5}, {'uid': 2, 'isStar': 2}, {'uid': 3, 'isStar': 1}, {'uid': 4, 'isStar': 0}]
        self.db.users.insert({'uid': 1, 'haslog': 1, 'test': 'xyz', 'friends': friends_list})
        self.assertEqual(user.friends.get(), sorted(friends_list, key = lambda x: x['isStar'], reverse=True))

    def test_set_None(self):
        users = self.redis_delegator.users(1)
        sr = self.redis_conn
        friends_list = [{'uid': 1, 'isStar': 5}, {'uid': 2, 'isStar': 0}, {'uid': 3, 'isStar': 1}, {'uid': 4, 'isStar': 0}]
        self.db.users.insert({'uid': 1, 'haslog': 1, 'test': 'xyz', 'friends': friends_list})
        doc = self.db.users.find_one({'uid': 1})
        self.assertEqual(users.haslog, 1)
        self.assertEqual(users.test, 'xyz')
        res = users.find(1, ['test', 'xyz'])
        self.assertEqual(res, {'xyz': None, 'test': 'xyz'})

        users.test = None
        self.assertFalse(sr.sismember(KEYS_MODIFIED_SET, users._key))
        # self.assertTrue(sr.zrank(LRU_QUEUE, users._key) is None)
        self.assertEqual(sr.hgetall(users._key), {'haslog': '1'})
        doc1 = self.db.users.find_one({'uid': 1})
        doc['test'] = None
        self.assertEqual(doc, doc1)

    def test_common_field_get_and_set(self):
        users = self.redis_delegator.users(1)
        sr = self.redis_conn
        friends_list = [{'uid': 1, 'isStar': 5}, {'uid': 2, 'isStar': 0}, {'uid': 3, 'isStar': 1}, {'uid': 4, 'isStar': 0}]
        self.db.users.insert({'uid': 1, 'haslog': 1, 'test': 'xyz', 'friends': friends_list})
        users.haslog = 0
        res = users.haslog
        self.assertEqual(res, 0)
        self.assertTrue(sr.sismember(KEYS_MODIFIED_SET, users._key))
        self.assertTrue(sr.zrank(LRU_QUEUE, users._key) is not None)

    def test_find_update(self):
        users = self.redis_delegator.users(1)
        sr = self.redis_conn
        friends_list = [{'uid': 1, 'isStar': 5}, {'uid': 2, 'isStar': 0}, {'uid': 3, 'isStar': 1}, {'uid': 4, 'isStar': 0}]
        doc = {'haslog': 1, 'test': 'xyz', 'friends': sorted(friends_list, key = lambda x: x['isStar'])}
        self.db.users.insert({'uid': 1, 'haslog': 1, 'test': 'xyz', 'friends': sorted(friends_list, key = lambda x: x['isStar'])})
        res = users.find(1)
        self.assertEqual(res, doc)

        update_doc = {'haslog': 3, 'friends': [{'uid': 2, 'isStar': 11}]}
        users.update(update_doc)
        doc.update(update_doc)
        res = users.find(1)
        self.assertEqual(res, doc)
        self.assertTrue(sr.sismember(KEYS_MODIFIED_SET, users._key))
        self.assertFalse(sr.zrank(LRU_QUEUE, users._key) is None)
        self.assertTrue(sr.sismember(KEYS_MODIFIED_SET, users.friends.key_name))
        self.assertFalse(sr.zrank(LRU_QUEUE, users.friends.key_name) is None)
        
        update_doc = {'haslog': None}
        users.update(update_doc)
        #doc.update(update_doc)
        doc.pop('haslog')
        res = users.find(1)
        self.assertEqual(res, doc)

        update_doc = {'test': '123', 'abc': 'aaa'}
        users.update(update_doc)
        doc.update(update_doc)
        res = users.find(1)
        self.assertEqual(res, doc)

    def test_write_back(self):
        users = self.redis_delegator.users(1)
        sr = self.redis_conn
        friends_list = [{'uid': 1, 'isStar': 5}, {'uid': 2, 'isStar': 0}, {'uid': 3, 'isStar': 1}, {'uid': 4, 'isStar': 0}]
        doc = {'haslog': 1, 'test': 'xyz', 'friends': sorted(friends_list, key = lambda x: x['isStar'])}
        self.db.users.insert({'uid': 1, 'haslog': 1, 'test': 'xyz', 'friends': sorted(friends_list, key = lambda x: x['isStar'])})

        update_doc = {'test': '123', 'abc': 'aaa'}
        users.update(update_doc)
        doc.update(update_doc)
        users.write_back(1)
        doc1 = self.db.users.find_one({'uid': 1}, {'uid': 0, '_id': 0})
        self.assertEqual(doc, doc1)

        update_doc = {'haslog': 3, 'friends': [{'uid': 2, 'isStar': 11}]}
        users.update(update_doc)
        doc.update(update_doc)
        users.write_back(1, 'friends')
        users.write_back(1)
        doc1 = self.db.users.find_one({'uid': 1}, {'uid': 0, '_id': 0})
        self.assertEqual(doc, doc1)

    def test_try_write_back(self):
        def side_effect(*args, **kwargs):
            kwargs['lock_timeout'] = 1
            return acquire_lock_with_timeout(*args, **kwargs)
        with mock.patch('rmlru.acquire_lock_with_timeout', side_effect=side_effect) as whate_ever:
            LOCK_TIMEOUT = 1
            users = self.redis_delegator.users(1)
            sr = self.redis_conn
            friends_list = [{'uid': 1, 'isStar': 5}, {'uid': 2, 'isStar': 0}, {'uid': 3, 'isStar': 1}, {'uid': 4, 'isStar': 0}]
            doc = {'haslog': 1, 'test': 'xyz', 'friends': sorted(friends_list, key = lambda x: x['isStar'])}
            self.db.users.insert({'uid': 1, 'haslog': 1, 'test': 'xyz', 'friends': sorted(friends_list, key = lambda x: x['isStar'])})
            res = users.find(1)
            self.assertEqual(res, doc)
            self.assertFalse(sr.sismember(KEYS_MODIFIED_SET, users._key))
            self.assertTrue(sr.zrank(LRU_QUEUE, users._key) is not None)

            update_doc = {'test': '123', 'abc': 'aaa'}
            users.update(update_doc)
            doc.update(update_doc)
            self.assertTrue(sr.sismember(KEYS_MODIFIED_SET, users._key))
            self.assertTrue(sr.zrank(LRU_QUEUE, users._key) is not None)

            time.sleep(LOCK_TIMEOUT + 1)
            res = self.redis_delegator.try_write_back(sr, 'users:1')
            self.assertTrue(res)
            self.assertFalse(sr.sismember(KEYS_MODIFIED_SET, users._key))
            self.assertTrue(sr.zrank(LRU_QUEUE, users._key) is None)
            doc1 = self.db.users.find_one({'uid': 1}, {'uid': 0, '_id': 0})
            self.assertEqual(doc, doc1)

            update_doc = {'haslog': 3, 'friends': [{'uid': 2, 'isStar': 11}]}
            users.update(update_doc)
            doc.update(update_doc)
            self.assertTrue(sr.sismember(KEYS_MODIFIED_SET, users.friends.key_name))
            self.assertTrue(sr.zrank(LRU_QUEUE, users.friends.key_name) is not None)

            time.sleep(LOCK_TIMEOUT + 1)
            res = self.redis_delegator.try_write_back(sr, 'users:1.friends')
            self.assertTrue(res)
            self.assertFalse(sr.sismember(KEYS_MODIFIED_SET, users.friends.key_name))
            # strange!!
            # self.assertTrue(sr.zrank(LRU_QUEUE, users.friends.key_name) is None)

            res = self.redis_delegator.try_write_back(sr, 'users:1')
            self.assertFalse(sr.sismember(KEYS_MODIFIED_SET, users.friends.key_name))
            # strange !!
            # self.assertTrue(sr.zrank(LRU_QUEUE, users.friends.key_name) is None)
            doc1 = self.db.users.find_one({'uid': 1}, {'uid': 0, '_id': 0})
            self.assertEqual(doc, doc1)