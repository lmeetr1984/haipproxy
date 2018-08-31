# coding=utf8

"""
We use this validator to filter ip that can access mobile zhihu website.
"""
from haipproxy.config.settings import (
    TEMP_ZHIHU_QUEUE, VALIDATED_ZHIHU_QUEUE,
    TTL_ZHIHU_QUEUE, SPEED_ZHIHU_QUEUE)
from .base import BaseValidator
from ..redis_spiders import ValidatorRedisSpider


class ZhiHuValidator(BaseValidator, ValidatorRedisSpider):
    """This validator checks the liveness of zhihu proxy resources"""

    # 用zhihu来验证代理的速度

    name = 'zhihu'
    urls = [
        'https://www.zhihu.com/question/47464143'
    ]
    task_queue = TEMP_ZHIHU_QUEUE
    score_queue = VALIDATED_ZHIHU_QUEUE
    ttl_queue = TTL_ZHIHU_QUEUE
    speed_queue = SPEED_ZHIHU_QUEUE
    success_key = '安全验证'
