# coding=utf8
"""
Scrapy items for haiproxy
"""
import scrapy


# ip 的模型类

# url
class ProxyUrlItem(scrapy.Item):
    url = scrapy.Field()


# url评分
class ProxyScoreItem(scrapy.Item):
    url = scrapy.Field()
    score = scrapy.Field()
    incr = scrapy.Field()
    queue = scrapy.Field()


# url校验时间
class ProxyVerifiedTimeItem(scrapy.Item):
    url = scrapy.Field()
    verified_time = scrapy.Field()
    incr = scrapy.Field()
    queue = scrapy.Field()


# url的响应时间
class ProxySpeedItem(scrapy.Item):
    url = scrapy.Field()
    response_time = scrapy.Field()
    incr = scrapy.Field()
    queue = scrapy.Field()
