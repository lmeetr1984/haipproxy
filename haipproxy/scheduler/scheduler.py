# coding=utf8

"""
This module schedules all the tasks according to config.rules.
"""
import time
from multiprocessing import Pool

import click
import schedule
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor

from ..client import SquidClient
# from logger import (
#     crawler_logger, scheduler_logger,
#     client_logger)
from ..config.rules import (
    CRAWLER_TASKS, VALIDATOR_TASKS,
    CRAWLER_TASK_MAPS, TEMP_TASK_MAPS)
from ..config.settings import (
    SPIDER_COMMON_TASK, SPIDER_AJAX_TASK,
    SPIDER_GFW_TASK, SPIDER_AJAX_GFW_TASK,
    TEMP_HTTP_QUEUE, TEMP_HTTPS_QUEUE,
    TIMER_RECORDER, TTL_VALIDATED_RESOURCE)
from ..crawler.spiders import all_spiders
from ..crawler.validators import all_validators
from ..utils import (
    get_redis_conn, acquire_lock,
    release_lock)

DEFAULT_CRAWLER_TASKS = [
    SPIDER_COMMON_TASK, SPIDER_AJAX_TASK,
    SPIDER_GFW_TASK, SPIDER_AJAX_GFW_TASK]
DEFAULT_VALIDATORS_TASKS = [TEMP_HTTP_QUEUE, TEMP_HTTPS_QUEUE]

# 4种spider：
DEFAULT_CRAWLERS = all_spiders
DEFAULT_VALIDATORS = all_validators


class BaseCase:
    def __init__(self, spider):
        self.spider = spider

    def check(self, task, maps):
        task_queue = maps.get(task)
        return self.spider.task_queue == task_queue


# 调度器
# 调度器的作用就是把初始的种子url灌入到redis中
# 供 爬虫使用
class BaseScheduler:
    def __init__(self, name, tasks, task_queues=None):
        """
        init function for schedulers.
        :param name: scheduler name, generally the value is used by the scheduler
        :param tasks: tasks in config.rules
        :param task_queues: for crawler, the value is task_queue, while for validator, it's task name
        """
        self.name = name
        self.tasks = tasks  # 就是那个rule.py 中的所有的代理ip网站列表
        self.task_queues = list() if not task_queues else task_queues

    def schedule_with_delay(self):
        # 定时获取
        for task in self.tasks:
            interval = task.get('interval')
            schedule.every(interval).minutes.do(self.schedule_task_with_lock, task)
        while True:
            schedule.run_pending()
            time.sleep(1)

    def schedule_all_right_now(self):
        # 进程池处理
        with Pool() as pool:
            # 多进程调度
            pool.map(self.schedule_task_with_lock, self.tasks)

    def get_lock(self, conn, task):
        if not task.get('enable'):
            return None

        # 获取对应的task 所在的redis 队列名
        task_queue = task.get('task_queue')
        if task_queue not in self.task_queues:
            return None

        # 获取任务名称
        task_name = task.get('name')
        lock_indentifier = acquire_lock(conn, task_name)
        return lock_indentifier

    def schedule_task_with_lock(self, task):
        raise NotImplementedError


class CrawlerScheduler(BaseScheduler):
    # 爬虫的调度器
    def schedule_task_with_lock(self, task):
        """Crawler scheduler filters tasks according to task type"""
        if not task.get('enable'):
            return None
        task_queue = task.get('task_queue')
        if task_queue not in self.task_queues:
            return None

        # 获取redis的连接
        conn = get_redis_conn()
        task_name = task.get('name')
        interval = task.get('interval')
        urls = task.get('resource')

        # 获取锁定
        lock_indentifier = acquire_lock(conn, task_name)
        if not lock_indentifier:
            return False

        pipe = conn.pipeline(True)
        try:
            now = int(time.time())

            # 在name对应的hash中获取根据key获取value
            pipe.hget(TIMER_RECORDER, task_name)
            r = pipe.execute()[0]
            if not r or (now - int(r.decode('utf-8'))) >= interval * 60:
                # 在name对应的list中添加元素，每个新的元素都添加到列表的最左边
                pipe.lpush(task_queue, *urls)

                # 更新url 时间
                pipe.hset(TIMER_RECORDER, task_name, now)
                pipe.execute()
                # scheduler_logger.info('crawler task {} has been stored into redis successfully'.format(task_name))
                return True
            else:
                return None
        finally:
            release_lock(conn, task_name, lock_indentifier)


class ValidatorScheduler(BaseScheduler):
    def schedule_task_with_lock(self, task):
        """Validator scheduler filters tasks according to task name
        since its task name stands for task type"""
        if not task.get('enable'):
            return None
        task_queue = task.get('task_queue')
        if task_queue not in self.task_queues:
            return None

        conn = get_redis_conn()
        interval = task.get('interval')
        task_name = task.get('name')
        resource_queue = task.get('resource')
        lock_indentifier = acquire_lock(conn, task_name)
        if not lock_indentifier:
            return False
        pipe = conn.pipeline(True)
        try:
            now = int(time.time())
            pipe.hget(TIMER_RECORDER, task_name)
            pipe.zrevrangebyscore(resource_queue, '+inf', '-inf')
            r, proxies = pipe.execute()
            if not r or (now - int(r.decode('utf-8'))) >= interval * 60:
                if not proxies:
                    # scheduler_logger.warning('fetched no proxies from task {}'.format(task_name))
                    print('fetched no proxies from task {}'.format(task_name))
                    return None

                pipe.sadd(task_queue, *proxies)
                pipe.hset(TIMER_RECORDER, task_name, now)
                pipe.execute()
                # scheduler_logger.info('validator task {} has been stored into redis successfully'.format(task_name))
                return True
            else:
                return None
        finally:
            release_lock(conn, task_name, lock_indentifier)


@click.command()
@click.option('--usage', type=click.Choice(['crawler', 'validator']), default='crawler')
@click.argument('task_queues', nargs=-1)
def scheduler_start(usage, task_queues):
    """Start specified scheduler."""
    # scheduler_logger.info('{} scheduler is starting...'.format(usage))

    # 启动scrapy的调度器
    print('{} scheduler is starting...'.format(usage))
    if usage == 'crawler':
        # 爬虫的调度器
        default_tasks = CRAWLER_TASKS  # 默认的任务
        default_allow_tasks = DEFAULT_CRAWLER_TASKS
        maps = CRAWLER_TASK_MAPS
        SchedulerCls = CrawlerScheduler
    else:
        # 校验爬虫的调度器
        default_tasks = VALIDATOR_TASKS
        default_allow_tasks = DEFAULT_VALIDATORS_TASKS
        maps = TEMP_TASK_MAPS
        SchedulerCls = ValidatorScheduler

    # 初始化调度器
    scheduler = SchedulerCls(usage, default_tasks)

    if not task_queues:
        scheduler.task_queues = default_allow_tasks
    else:
        for task_queue in task_queues:
            allow_task_queue = maps.get(task_queue)
            if not allow_task_queue:
                # scheduler_logger.warning('scheduler task {} is an invalid task, the allowed tasks are {}'.format(
                #     task_queue, list(maps.keys())))
                print('scheduler task {} is an invalid task, the allowed tasks are {}'.format(
                    task_queue, list(maps.keys())))
                continue
            # 加入到调度器的队列中
            scheduler.task_queues.append(allow_task_queue)

    # 开始调度
    scheduler.schedule_all_right_now()
    scheduler.schedule_with_delay()


@click.command()
@click.option('--usage', type=click.Choice(['crawler', 'validator']), default='crawler')
@click.argument('tasks', nargs=-1)
def crawler_start(usage, tasks):
    """Start specified spiders or validators from cmd with scrapy core api.
    There are four kinds of spiders: common, ajax, gfw, ajax_gfw. If you don't
    assign any tasks, all these spiders will run.
    """
    # 使用了click模块来定义入口命令行cli
    if usage == 'crawler':

        # 定义了4个 redis queue 来获取任务
        maps = CRAWLER_TASK_MAPS

        # 定义spider
        origin_spiders = DEFAULT_CRAWLERS
    else:
        # 如果是验证器的话，那么启动验证器参数
        maps = TEMP_TASK_MAPS
        origin_spiders = DEFAULT_VALIDATORS

    if not tasks:
        spiders = origin_spiders
    else:
        spiders = list()

        # 对每个spider包裹成一个BaseCase类
        cases = list(map(BaseCase, origin_spiders))

        # 检查每一个tasks
        # 如果task在列表中，就把对应的spider放到spiders中
        for task in tasks:
            for case in cases:
                if case.check(task, maps):
                    spiders.append(case.spider)
                    break
            else:
                # crawler_logger.warning('spider task {} is an invalid task, the allowed tasks are {}'.format(
                #     task, list(maps.keys())))
                pass
    if not spiders:
        # crawler_logger.warning('no spider starts up, please check your task input')
        return

    # 获取整个项目的配置文件
    settings = get_project_settings()
    configure_logging(settings)

    # 生成一个执行爬虫的helper类，这个类内置了twisted reactor
    runner = CrawlerRunner(settings)
    for spider in spiders:
        runner.crawl(spider)
    d = runner.join()
    d.addBoth(lambda _: reactor.stop())
    reactor.run()


@click.command()
@click.option('--usage', default='https', help='Usage of squid')
@click.option('--interval', default=TTL_VALIDATED_RESOURCE, help='Updating frenquency of squid conf.')
def squid_conf_update(usage, interval):
    """Timertask for updating proxies for squid config file"""
    # client_logger.info('the updating task is starting...')
    client = SquidClient(usage)
    client.update_conf()
    schedule.every(interval).minutes.do(client.update_conf)
    while True:
        schedule.run_pending()
        time.sleep(1)
