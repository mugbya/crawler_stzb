# -*- coding: utf-8 -*-
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from crawler.items import AttendItem
from crawler.util import text_handler
import logging
from datetime import datetime, timedelta
from crawler.settings import TIEM_INTERVAL


class SummarySpider(CrawlSpider):
    name = "summary"
    allowed_domains = ["xushengzhou.com"]
    start_urls = ['http://stzb.xushengzhou.com/forum.php?mod=forumdisplay&fid=41']
    # start_urls = ['http://stzb.xushengzhou.com/forum.php?mod=forumdisplay&fid=40&page=1']
    rules = (
        Rule(LinkExtractor(allow=(r'mod=viewthread',), deny=('viewthread&tid=52&extra=page%3D1',)), callback='parse_item'),
        # Rule(LinkExtractor(allow=(r'mod=viewthread',)), callback='parse_item'),
    )

    def match_text_of_activity(self, text):
        '''
        每天的 活动的爬出匹配
        根据关键字识别 爬取得信息是否值得 保存
        :param text:
        :return:
        '''
        localhost = ''
        attend_status = 1  # 缺勤表示 没有回复贴，这里是不知道，只能后面汇总后算是否出勤
        category = 0       # 活动 上榜， 0表示没有上榜
        if '请假' in text:
            attend_status = 1
            text_list = text.split('请假')
            localhost = text_list[0].replace(',', '').replace('，', '').strip()
        if '出勤' in text:
            attend_status = 2
            text_list = text.split('出勤')
            localhost = text_list[0].replace(',', '').replace('，', '').strip()

        if '签到' in text:
            attend_status = 2
            text_list = text.split('签到')
            localhost = text_list[0].replace(',', '').replace('，', '').strip()

        if '灭敌' in text and '拆迁' not in text:
            attend_status = 2
            category = 1  # 表示灭敌上榜
        if '拆迁' in text and '灭敌' not in text:
            attend_status = 2
            category = 2  # 拆迁上榜

        if '拆迁' in text and '灭敌' in text:
            attend_status = 2
            category = 3  # 双榜

        if localhost:
            # 如果是考勤则直接返回 考勤信息
            return {'localhost': localhost, 'attend_status': attend_status, 'category': category}
        return None

    def match_text_of_week(self, text):
        '''
        每周统计的信息
        :param text:
        :return:
        '''
        text_list = text.split(',')
        if not text_list:
            text_list = text.split('，')
        for _str in text_list:
            # TODO 需要优化
            if '武勋' in _str:
                res_list = _str.split(':')
                if not res_list:
                    res_list = _str.split('：')
                try:
                    medal = int(res_list[1].strip())
                except Exception as e:
                    self.log(e, logging.ERROR)

            if '翻地' in text:
                res_list = _str.split(':')
                if not res_list:
                    res_list = _str.split('：')
                try:
                    occupy = int(res_list[1].strip())
                except Exception as e:
                    self.log(e, logging.ERROR)

            if '拆迁' in text:
                res_list = _str.split(':')
                if not res_list:
                    res_list = _str.split('：')
                try:
                    emolition = int(res_list[1].strip())
                except Exception as e:
                    self.log(e, logging.ERROR)

        # 返回周统计信息
        return

    def parse_item(self, response):
        if response.url == 'http://stzb.xushengzhou.com/forum.php?mod=viewthread&tid=24&extra=page%3D1':
            print('---')
        username = response.css('span[id=thread_subject]::text').extract_first()

        # content = response.css('//*[@id="aimg_18"]').extract()
        # content = response.xpath('//*[@id="pid16"]/tbody/tr[1]/td[2]/div[2]/div/div[1]"]').extract()

        node_list = response.css('table[class=plhin]')
        for node in node_list:
            now_time = datetime.now()
            task_date = node.css('em>span::attr(title)').extract_first()  # [:10]
            _date = datetime.strptime(task_date.strip(), '%Y-%m-%d %H:%M:%S')
            if now_time > _date + timedelta(hours=TIEM_INTERVAL):
                continue

            print(task_date)

            content = node.css('div[class=t_fsz] > table > tr > td[class=t_f]::text').extract_first()
            # for content in content_list:
            content = text_handler(content)
            if not content:
                continue
            res_dict = self.match_text_of_activity(content)
            if not res_dict or 'localhost' not in res_dict:
                # 本回复无效信息，直接继续判断
                continue
            # if res_dict.get('attend_status') == 1:
            #     pass # 请假可以直接ji

            photo = node.css('div[class=pattl]')
            if not photo and res_dict.get('attend_status') == 2:
                # 如果是正常出勤，但是没有截图，则不认为是有效数据，不抓取
                continue

            item = AttendItem()

            item['username'] = username
            item['localhost'] = res_dict.get('localhost', '')
            item['attend_status'] = res_dict.get('attend_status', 1)
            item['task_date'] = task_date
            item['category'] = res_dict.get('category', 0)
            item['id'] = task_date + username
            yield item

        # content = response.css('div[class=pcb] td::text').extract()
        # for x in content:
        #     print(x)

        # data = response.css('em>span::attr(title)').extract()
        # for o in data:
        #     print(o)

        # print(data)

