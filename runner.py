

from jobcrawl.spiders.alljobs import AllJobsSpider
from jobcrawl.spiders.drushim import DrushimSpider
from jobcrawl.spiders.jobmaster import JobmasterSpider
from jobcrawl.spiders.left_company_check import LeftCompany

from scrapy.utils.project import get_project_settings
from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
import time
import logging
from scrapy.utils.log import configure_logging

configure_logging(install_root_handler=False)
logging.basicConfig(
    filename="%s_%s.txt" % ('scrapy_log_output', time.strftime('%Y-%m-%d')),
    format='%(levelname)s: %(message)s',
    level=logging.INFO
)

settings = get_project_settings()

runner = CrawlerRunner(settings)


@defer.inlineCallbacks
def crawl():
    yield runner.crawl(JobmasterSpider)
    yield runner.crawl(AllJobsSpider)
    yield runner.crawl(DrushimSpider)
    yield runner.crawl(LeftCompany)
    reactor.stop()

crawl()
reactor.run()
