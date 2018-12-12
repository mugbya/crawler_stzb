# -*- coding: utf-8 -*-
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from crawler.items import AttendItem
from crawler.util import text_handler
from datetime import datetime, timedelta
from crawler.settings import TIEM_INTERVAL


class AttendSpider(CrawlSpider):
    name = "attend"
    allowed_domains = ["xushengzhou.com"]
    start_urls = ['http://stzb.xushengzhou.com/forum.php?mod=forumdisplay&fid=41']
    rules = (
        Rule(LinkExtractor(allow=(r'mod=viewthread',), deny=('viewthread&tid=52&extra=page%3D1',)), callback='parse_item'),
    )

    def match_text_of_activity(self, text):
        '''
        每天的 活动的爬出匹配
        根据关键字识别 爬取得信息是否值得 保存
        :param text:
        :return:
        '''
        localhost = ''
        attend_status = -1  # 缺勤表示 没有回复贴，这里是不知道，只能后面汇总后算是否出勤
        category = 0       # 活动 上榜， 0表示没有上榜
        if '请假' in text:
            text_list = text.split('请假')
            localhost = text_list[0].replace(',', '').replace('，', '').strip()

        if '签到' in text:
            attend_status = 1
            text_list = text.split('签到')
            localhost = text_list[0].replace(',', '').replace('，', '').strip()

        if '灭敌' in text and '拆迁' not in text:
            text_list = text.split('灭敌')
            localhost = text_list[0].replace(',', '').replace('，', '').strip()
            attend_status = 1
            category = 1  # 表示灭敌上榜
        if '拆迁' in text and '灭敌' not in text:
            text_list = text.split('拆迁')
            localhost = text_list[0].replace(',', '').replace('，', '').strip()
            attend_status = 1
            category = 2  # 拆迁上榜

        if '拆迁' in text and '灭敌' in text:
            text_list = text.split('灭敌')
            localhost = text_list[0].replace(',', '').replace('，', '').strip()

            attend_status = 1
            category = 3  # 双榜

        if localhost:
            # 如果是考勤则直接返回 考勤信息
            return {'localhost': localhost, 'attend_status': attend_status, 'category': category}
        return None

    def parse_item(self, response):

        username = response.css('span[id=thread_subject]::text').extract_first()

        node_list = response.css('table[class=plhin]')
        for node in node_list:
            now_time = datetime.now()
            task_date_str = node.css('em>span::attr(title)').extract_first()  # [:10]
            if not task_date_str:
                # 当没有按照上面采集到 时间格式时，表示已经至少是3天前了，而本程序每天跑，之前的数据已经有了，可以不再采集
                continue
            task_date = datetime.strptime(task_date_str.strip(), '%Y-%m-%d %H:%M:%S')
            if now_time > task_date + timedelta(hours=TIEM_INTERVAL):
                continue

            res_dict = {}
            content_list = node.css('div[class=t_fsz] > table > tr > td[class=t_f]::text').extract()
            for content in content_list:
                content = text_handler(content)
                if not content:
                    continue
                res_dict = self.match_text_of_activity(content)
                if not res_dict or 'localhost' not in res_dict:
                    # 本回复无效信息，直接继续判断
                    continue

            if res_dict:
                photo = node.css('div[class=pattl]')
                if not photo or res_dict.get('attend_status') != 1:
                    # 如果是正常出勤，但是没有截图，则不认为是有效数据，不抓取
                    continue

                localhost = res_dict.get('localhost', '')

                item = AttendItem()

                item['username'] = username
                item['localhost'] = localhost
                item['attend_status'] = res_dict.get('attend_status', 1)
                item['task_date'] = task_date_str
                item['category'] = res_dict.get('category', 0)
                item['id'] = localhost + username
                yield item
