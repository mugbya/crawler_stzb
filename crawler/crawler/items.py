# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class UserItem(scrapy.Item):
    username = scrapy.Field()
    group = scrapy.Field()
    status = scrapy.Field()


class AttendItem(scrapy.Item):
    # define the fields for your item here like:
    id = scrapy.Field()
    username = scrapy.Field()
    task_date = scrapy.Field()
    attend_status = scrapy.Field()  # 0 表示缺勤， 1表示 请假, 2表示出勤
    localhost = scrapy.Field()  # 城池 或 关卡
    category = scrapy.Field()  # 分类 1 表示灭敌上榜  2表示拆迁上榜, 3 表示双榜 0表示都没有


