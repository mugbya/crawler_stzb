# -*- coding: utf-8 -*-
from scrapy import cmdline

name = 'user'
cmd = 'scrapy crawl {0}'.format(name)
# cmd = 'scrapy crawl {0} -o {1}.json'.format(name, name)
cmdline.execute(cmd.split())