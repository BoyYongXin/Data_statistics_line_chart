# -*- coding: utf-8 -*-
import json
import time
import traceback
import datetime
from threading import Timer
from venv import logger

import jieba
import numpy as np
import pymongo
import redis


def write_data(files, values):
    with open(files, "a", encoding="gb2312") as f:
        if isinstance(values, list):
            for i in values:
                f.write(str(i) + "\n")
        else:
            f.write(str(values) + "\n")


def deal_time(release_time):
    if len(str(release_time)) > 10:
        release_time = str(release_time)[0:10]
    release_time = int(release_time)
    release_time = time.localtime(release_time)
    release_time = time.strftime("%Y-%m-%d %H:%M:%S", release_time)
    return release_time


def debug(func):
    """
    抛出异常
    :param func:
    :return:
    """

    def wrapper(*args, **kw):
        try:
            return func(*args, **kw)
        except Exception as err:
            logger.error(err)
            traceback.print_exc()

    return wrapper


class MyTimer(object):

    def __init__(self, start_time, interval, callback_proc, args=None, kwargs=None):
        self.__timer = None
        self.__start_time = start_time
        self.__interval = interval
        self.__callback_pro = callback_proc
        self.__args = args if args is not None else []
        self.__kwargs = kwargs if kwargs is not None else {}

    def exec_callback(self, args=None, kwargs=None):
        self.__callback_pro(*self.__args, **self.__kwargs)
        self.__timer = Timer(self.__interval, self.exec_callback)
        self.__timer.start()

    def start(self):
        interval = self.__interval - (datetime.datetime.now().timestamp() - self.__start_time.timestamp())
        print(f"任务将于{deal_time(int(time.time()) + int(interval))}，开始执行")
        self.__timer = Timer(interval, self.exec_callback)
        self.__timer.start()

    def cancel(self):
        self.__timer.cancel()
        self.__timer = None


class AA:
    def __init__(self):
        self.mongo_conn = pymongo.MongoClient(
            'rs_cralwer_01.mongo.int.yidian-inc.com:27017,rs_cralwer_02.mongo.int.yidian-inc.com:27017,rs_cralwer_03.mongo.int.yidian-inc.com:27017',
            connect=False
        )
        self.redis_conn = redis.StrictRedis(host='10.126.30.6', port='6379', db=2, decode_responses=True)

    def func(self, ele):
        return ele["score_crawler"]

    def get_mongo_keys(self, db_name):
        aa = []
        db = self.mongo_conn.wemedia
        for i in db[db_name].find().limit(1):
            for a in i.keys():
                aa.append(a)
        return aa

    def get_mongo_datas(self):
        db = self.mongo_conn.wemedia
        a = db.list_collection_names()
        for db_name in a:
            if db_name in ['system.profile', 'wemedia_sync_schedule', 'wemedia', 'wx_open', 'wemedia_sync',
                           'weixin_article']:
                continue
            else:
                collection = db[db_name]
                scores_list = ['score_crawler_video', 'score_crawler', 'score_crawler_news']
                for score_type in scores_list:
                    user_info_list = collection.find({}, {'_id': 0, score_type: 1}, no_cursor_timeout=True)
                    if not user_info_list[0]:
                        continue
                    else:
                        logger.warning('get {} score'.format(db_name))
                        ll = [i.get(score_type, 0) for i in user_info_list]
                        score_list = np.sort(-np.array(ll), axis=0)
                        ll_len = len(ll)
                        score_1 = -score_list[int(ll_len * 0.01)]
                        score_2 = -score_list[int(ll_len * 0.1)]
                        score_3 = -score_list[int(ll_len * 0.3)]
                        score_4 = -score_list[int(ll_len * 0.6)]
                        self.redis_conn.hset('crawl:crawl_platform:website_score',
                                             '{}_{}_5'.format(db_name, score_type),
                                             score_1)
                        self.redis_conn.hset('crawl:crawl_platform:website_score',
                                             '{}_{}_4'.format(db_name, score_type),
                                             score_2)
                        self.redis_conn.hset('crawl:crawl_platform:website_score',
                                             '{}_{}_3'.format(db_name, score_type),
                                             score_3)
                        self.redis_conn.hset('crawl:crawl_platform:website_score',
                                             '{}_{}_2'.format(db_name, score_type),
                                             score_4)
                    user_info_list.close()

    def cut_search_words(self):
        db = self.mongo_conn.wemedia
        a = db.list_collection_names()
        self.redis_conn.delete('crawl:crawl_platform:search_user_words')
        for db_name in a:
            if db_name in ['system.profile', 'wemedia_sync_schedule', 'wemedia', 'wx_open', 'wemedia_sync',
                           'weixin_article']:
                continue
            else:
                collection = db[db_name]
                user_name_list = collection.find({}, {'_id': 1, 'author_name': 1}, no_cursor_timeout=True).limit(50000)
                s = ''
                website_id = self.redis_conn.hget('crawl:crawl_platform:website_search', db_name)
                ci_dict = dict()
                for i in user_name_list:
                    if 'author_name' in i.keys():
                        s += str(i['author_name'])
                    else:
                        continue
                user_name_list.close()
                ci_list = list(jieba.cut_for_search(s, HMM=True))
                if db_name != 'miaopai':
                    for j in s:
                        ci_list.append(j)
                for k in ci_list:
                    if len(k) > 4:
                        continue
                    s = json.dumps({"_id": str(ci_list.index(k)), "key_word": k})
                    ci_dict[s] = int(website_id)
                logger.warning('{}_{} add words: {}'.format(website_id, db_name, len(ci_dict)))
                self.redis_conn.zadd('crawl:crawl_platform:search_user_words', ci_dict)

    def mongo_2_zset(self):
        time_info = datetime.datetime.now().isocalendar()
        years = time_info[0]
        weeks = time_info[1]
        db = self.mongo_conn.wemedia
        a = db.list_collection_names()
        for db_name in a:
            if db_name in ['system.profile', 'wemedia_sync_schedule', 'wemedia', 'wx_open', 'wemedia_sync',
                           'weixin_article']:
                continue
            else:
                if 'score_crawler' in self.get_mongo_keys(db_name):
                    self.add_zset(db_name, 'score_crawler', years, weeks)
                elif 'score_crawler_video' in self.get_mongo_keys(
                        db_name) or 'score_crawler_news' in self.get_mongo_keys(db_name):
                    if 'score_crawler_video' in self.get_mongo_keys(db_name):
                        self.add_zset(db_name, 'score_crawler_video', years, weeks)
                    if 'score_crawler_news' in self.get_mongo_keys(db_name):
                        self.add_zset(db_name, 'score_crawler_news', years, weeks)
                else:
                    self.add_zset(db_name, None, years, weeks)

    def judge_redis_key(self, mongo_db, level, score_type):
        """
        判断 每一个任务，指向缓存任务的redis_key
        :param mongo_db:
        :param task_id:
        :param level:
        :param start_form: topic：0、 feed：1、 statuses：2 、Search:3 、Detail:4
        :param business: 业务类型，conventional：0、push：1、import：2、sync：3
        :return:
        """
        time_info = datetime.datetime.now().isocalendar()
        yet_time_info = (datetime.datetime.now() - datetime.timedelta(days=7)).isocalendar()
        two_yet_time_info= (datetime.datetime.now() - datetime.timedelta(days=14)).isocalendar()
        years = time_info[0]
        weeks = time_info[1]
        yet_years = yet_time_info[0]
        yet_weeks = yet_time_info[1]
        two_yet_years = two_yet_time_info[0]
        two_yet_weeks = two_yet_time_info[1]

        if not score_type:
            now_redis_key = mongo_db + '_' + str(level) + '_' + str(years) + '_' + str(weeks)
            previous_redis_key = mongo_db + '_' + str(level) + '_' + str(yet_years) + '_' + str(yet_weeks)
            previous_redis_key_1 = mongo_db + '_' + str(level) + '_' + str(two_yet_years) + '_' + str(two_yet_weeks)
        else:
            now_redis_key = mongo_db + '_' + str(level) + '_' + str(years) + '_' + str(weeks) + '_' + score_type
            previous_redis_key = mongo_db + '_' + str(level) + '_' + str(yet_years) + '_' + str(yet_weeks) + '_' + score_type
            previous_redis_key_1 = mongo_db + '_' + str(level) + '_' + str(two_yet_years) + '_' + str(
                two_yet_weeks) + '_' + score_type
        previous_redis_key_info = self.redis_conn.exists(previous_redis_key)
        previous_redis_key_info_1 = self.redis_conn.exists(previous_redis_key_1)
        if previous_redis_key_info:
            self.redis_conn.delete(previous_redis_key)
        if previous_redis_key_info_1:
            self.redis_conn.delete(previous_redis_key_1)
        redis_key_info = self.redis_conn.exists(now_redis_key)
        if redis_key_info:
            return now_redis_key, 1
        else:
            return now_redis_key, 0

    def add_zset(self, db_name, score_type, years, weeks):
        db = self.mongo_conn.wemedia
        collection = db[db_name]
        output_field = {"_id": 1, "author_name": 1, 'url': 1, "watch_count": 1, "media_id": 1, 'user_key': 1,'area':1,'label':1}
        if not score_type:
            level = 1
            query_condition = {}
            cache_db, cache_statuses = self.judge_redis_key(db_name, level, score_type)
            logger.warning(cache_db)
            if cache_statuses:
                return
            mongo_datas = collection.find(query_condition, output_field, no_cursor_timeout=True)
            # mongo_datas_count=mongo_datas.count()
            # self.redis_conn.hset('crawl:crawl_platform:websites_source','{}_{}'.format(db_name,level),mongo_datas_count)
            mongo_datas_dict = dict()
            for i in mongo_datas:
                i['_id'] = str(i['_id'])
                i_res = json.dumps(i)
                mongo_datas_dict[i_res] = 0
                if len(mongo_datas_dict) >= 100000:
                    self.redis_conn.zadd(cache_db, mongo_datas_dict)
                    mongo_datas_dict = dict()
            if mongo_datas_dict:
                self.redis_conn.zadd(cache_db, mongo_datas_dict)
            s = self.redis_conn.zcard(cache_db)
            level_dict = {"1": 280, "2": 140, '3': 70, "4": 30, "5": 10}
            num = int(s // level_dict[str(level)]) + 1
            # print(db_name, level, num)
            self.redis_conn.hset('crawl:crawl_platform:websites_source', '{}_{}'.format(db_name, level), num)
        else:
            for level in range(1, 6):
                cache_db, cache_statuses = self.judge_redis_key(db_name, level, score_type)
                logger.warning(cache_db)
                if cache_statuses:
                    continue
                if 1 < int(level) < 5:
                    score = self.redis_conn.hget('crawl:crawl_platform:website_score',
                                                 '{}_{}_{}'.format(db_name, score_type, int(level) + 1))
                    next_score = self.redis_conn.hget('crawl:crawl_platform:website_score',
                                                      '{}_{}_{}'.format(db_name, score_type, int(level)))
                    query_condition = {score_type: {'$gte': float(next_score), '$lte': float(score)}}
                elif int(level) == 5:
                    score = 1
                    next_score = self.redis_conn.hget('crawl:crawl_platform:website_score',
                                                      '{}_{}_{}'.format(db_name, score_type, int(level)))
                    query_condition = {score_type: {'$gte': float(next_score), '$lte': float(score)}}
                else:
                    next_score = 0
                    score = self.redis_conn.hget('crawl:crawl_platform:website_score',
                                                 '{}_{}_{}'.format(db_name, score_type, int(level) + 1))
                    query_condition = {"$or": [{score_type: {'$gte': float(next_score), '$lte': float(score)}},
                                               {score_type: {"$exists": False}}]}
                mongo_datas = collection.find(query_condition, output_field, no_cursor_timeout=True)
                mongo_datas_dict = dict()
                for i in mongo_datas:
                    i['_id'] = str(i['_id'])
                    i_res = json.dumps(i)
                    mongo_datas_dict[i_res] = 0
                    if len(mongo_datas_dict) >= 100000:
                        self.redis_conn.zadd(cache_db, mongo_datas_dict)
                        mongo_datas_dict = dict()
                if mongo_datas_dict:
                    self.redis_conn.zadd(cache_db, mongo_datas_dict)
                s = self.redis_conn.zcard(cache_db)
                level_dict = {"1": 1000, "2": 700, '3': 300, "4": 150, "5": 50}
                num = int(s // level_dict[str(level)]) + 1
                # print(db_name,level,num)
                self.redis_conn.hset('crawl:crawl_platform:websites_source', '{}_{}'.format(db_name, level), num)


if __name__ == "__main__":
    aa = AA()
    # aa.get_mongo_update()
    # aa.mongo_2_zset()
    # aa.cut_search_words()
    # aa.mongo_2_zset()
    # print( aa.get_mongo_keys('bilibili'))
    # aa.send_email()
    # print(datetime.now().replace(day=22,hour=0, minute=15, second=0, microsecond=0))
    start1 = datetime.datetime.now().replace(month=1,hour=12, minute=15, second=0, microsecond=0)
    start2 = datetime.datetime.now().replace(month=1,hour=10, minute=25, second=0, microsecond=0)
    start3 = datetime.datetime.now().replace(month=1,hour=0, minute=30, second=0, microsecond=0)
    tmr1 = MyTimer(start1, 30 * 24 * 60 * 60, aa.get_mongo_datas, [])
    tmr2 = MyTimer(start2, 30 * 24 * 60 * 60, aa.cut_search_words, [])
    tmr3 = MyTimer(start3, 7 * 24 * 60 * 60, aa.mongo_2_zset, [])
    tmr1.start()
    tmr2.start()
    tmr3.start()

    # tmr.cancel()
