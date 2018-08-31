# coding=utf-8

"""
scrapy pipelines for storing proxy ip infos.
"""
from scrapy.exceptions import DropItem
from twisted.internet.threads import deferToThread

from .items import (
    ProxyScoreItem, ProxyVerifiedTimeItem,
    ProxySpeedItem)
from ..config.settings import (
    REDIS_DB, DATA_ALL,
    INIT_HTTP_QUEUE, INIT_SOCKS4_QUEUE,
    INIT_SOCKS5_QUEUE)
from ..utils import get_redis_conn


class BasePipeline:
    # 当spider被开启时，这个方法被调用。
    # 这里初始化redis连接
    def open_spider(self, spider):
        self.redis_con = get_redis_conn(db=REDIS_DB)

    def process_item(self, item, spider):
        # twisted 分配到另一个线程
        return deferToThread(self._process_item, item, spider)

    def _process_item(self, item, spider):
        raise NotImplementedError


class ProxyIPPipeline(BasePipeline):
    def _process_item(self, item, spider):
        url = item.get('url', None)
        if not url:
            return item

        # 开启redis的管道
        pipeline = self.redis_con.pipeline()

        # 集合中添加元素
        not_exists = pipeline.sadd(DATA_ALL, url)

        # 把对应的数据存在各种的队列中
        if not_exists:
            if 'socks4' in url:
                pipeline.rpush(INIT_SOCKS4_QUEUE, url)
            elif 'socks5' in url:
                pipeline.rpush(INIT_SOCKS5_QUEUE, url)
            else:
                pipeline.rpush(INIT_HTTP_QUEUE, url)
        pipeline.execute()
        return item


class ProxyCommonPipeline(BasePipeline):
    def _process_item(self, item, spider):
        if isinstance(item, ProxyScoreItem):
            self._process_score_item(item, spider)
        if isinstance(item, ProxyVerifiedTimeItem):
            self._process_verified_item(item, spider)
        if isinstance(item, ProxySpeedItem):
            self._process_speed_item(item, spider)

        return item

    def _process_score_item(self, item, spider):
        # 处理url得分

        # 获取url的得分
        score = self.redis_con.zscore(item['queue'], item['url'])
        if score is None:
            # 如果没有找到，插入数据
            self.redis_con.zadd(item['queue'], item['score'], item['url'])
        else:
            # delete ip resource when score < 1 or error happens
            if item['incr'] == '-inf' or (item['incr'] < 0 and score <= 1):
                pipe = self.redis_con.pipeline(True)

                # 在数据集合中删除某些值
                pipe.srem(DATA_ALL, item['url'])

                # 在有序集合中删除数据
                pipe.zrem(item['queue'], item['url'])
                pipe.execute()
            elif item['incr'] < 0 and 1 < score:
                # 增加权重分数
                self.redis_con.zincrby(item['queue'], item['url'], -1)
            elif item['incr'] > 0 and score < 10:
                # 增加权重分数
                self.redis_con.zincrby(item['queue'], item['url'], 1)
            elif item['incr'] > 0 and score >= 10:
                incr = round(10 / score, 2)
                self.redis_con.zincrby(item['queue'], item['url'], incr)

    def _process_verified_item(self, item, spider):
        if item['incr'] == '-inf' or item['incr'] < 0:
            # 该异常由item pipeline抛出，用于停止处理item
            # 验证时间无效的话
            raise DropItem('item verification has failed')

        # 在优先队列中增加数据
        self.redis_con.zadd(item['queue'], item['verified_time'], item['url'])

    def _process_speed_item(self, item, spider):
        if item['incr'] == '-inf' or item['incr'] < 0:
            raise DropItem('item verification has failed')

        self.redis_con.zadd(item['queue'], item['response_time'], item['url'])
