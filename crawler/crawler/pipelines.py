# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

# Pipeline 监控爬虫启动跟完结
# https://blog.csdn.net/mldxs/article/details/8697020
import logging

from crawler.common import get_gp_connect, gp_execute, gp_select


class CrawlerPipeline(object):

    def __init__(self):
        self.gp_client = get_gp_connect()
        self.duplicates = {}
        self.duplicates_user = {}
        self.data = []

    def open_spider(self, spider):
        self.duplicates[spider.name] = set()
        self.duplicates_user[spider.name] = set()

    def close_spider(self, spider):
        self.save_data(spider, self.gp_client, self.duplicates[spider.name])
        self.gp_client.close()

    def process_item(self, item, spider):
        '''
        增量添加
        做单条的查询，看数据库是否已存在
        :param item:
        :param spider:
        :return:
        '''
        if 'user' == spider.name:  # 用户
            if self.judge_user_exist(item):
                # 这里并不能保存一次进来的两个数据相同
                return
        if 'attend' == spider.name:  # 考勤
            if self.judge_attend_exist(item):
                return
        if 'week_summary' == spider.name:  # 周汇总
            pass

        if len(self.duplicates[spider.name]) >= 5000:
            self.save_data(spider, self.gp_client, self.duplicates[spider.name])
            self.duplicates[spider.name] = set()

        if 'user' == spider.name and item['username'] in self.duplicates_user[spider.name]:
            return
        else:
            self.duplicates_user[spider.name].add(item['username'])

        self.duplicates[spider.name].add(item)

    def save_data(self, spider, gp_client, item_list):
        # for data in data_list:
        save_sql = ''
        value_str = ''
        if 'user' == spider.name:
            save_sql = self.get_save_user_sql()
            value_str = ','.join(self.handler_user_data(item_list))
        if 'attend' == spider.name: # 考勤
            save_sql = self.get_save_sql()
            value_str = ','.join(self.handler_attend_data(item_list))
        if 'week_summary' == spider.name:  # 周汇总
            pass

        if value_str:
            save_sql += value_str
            print(save_sql)
            try:
                res = gp_execute(gp_client, save_sql)
                spider.log("[gp save result]: %s" % res)
            except Exception as e:
                spider.log("[gp save result] sql: %s" % save_sql, logging.ERROR)
                spider.log("[gp save result]: %s" % str(e), logging.ERROR)

    def handler_attend_data(self, item_list):
        data_list = []
        for item in item_list:
            data = []
            data.append(item['username'])
            data.append(item['attend_status'])
            data.append(item['localhost'])
            data.append(item['category'])
            data.append(item['task_date'])
            data = str(tuple(data))
            data_list.append(data)
        return data_list

    def judge_user_exist(self, item):
        select_sql = self.get_select_user_sql(item['username'].strip())
        res = gp_select(self.gp_client, select_sql)
        if res:
            return True
        return False

    def judge_attend_exist(self, item):
        if gp_select(self.gp_client, self.get_select_attend_sql(item['username'], item['task_date'])):
            return True
        return False

    def handler_user_data(self, item_list):
        data_list = []
        for item in item_list:
            data = []
            data.append(item['username'])
            data.append(1) # 表示用户状态 1 为启用

            data = str(tuple(data))
            data_list.append(data)
        return data_list

    @staticmethod
    def get_save_sql():
        return '''insert into stzb."816_attend" (
                        username,
                        attend_status,
                        localhost,
                        category,
                        task_date
                        ) VALUES '''

    @staticmethod
    def get_select_attend_sql(username, task_date):
        return '''select username from stzb."816_attend" where username='%s' and task_date='%s' ''' % (username, task_date)

    @staticmethod
    def get_select_user_sql(username):
        return '''select username from stzb."816_user" where username='%s' ''' % username

    @staticmethod
    def get_save_user_sql():
        return '''insert into stzb."816_user" (
                        username,
                        status
                        ) VALUES '''

