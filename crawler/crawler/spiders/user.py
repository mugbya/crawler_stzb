# -*- coding: utf-8 -*-
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from crawler.items import AttendItem
from crawler.util import text_handler
import logging
from datetime import datetime, timedelta
from crawler.settings import TIEM_INTERVAL


class UserSpider(CrawlSpider):
    '''
    爬取用户
    '''
    name = "user"
    allowed_domains = ["xushengzhou.com"]
    start_urls = ['http://stzb.xushengzhou.com/forum.php?mod=forumdisplay&fid=41']
    rules = (
        Rule(LinkExtractor(allow=(r'mod=viewthread',), deny=('viewthread&tid=52&extra=page%3D1',)), callback='parse_item'),
    )

    def parse_item(self, response):
        item = AttendItem()
        username = response.css('span[id=thread_subject]::text').extract_first()
        item['username'] = username
        print(username)
        yield item
