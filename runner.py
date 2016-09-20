

from jobcrawl.spiders.alljobs import AllJobsSpider
from jobcrawl.spiders.drushim import DrushimSpider
from jobcrawl.spiders.jobmaster import JobmasterSpider
from jobcrawl.spiders.left_company_check import LeftCompany

from scrapy.utils.project import get_project_settings
from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging

# configure_logging()
configure_logging({'LOG_FORMAT': '%(levelname)s: %(message)s'})

runner = CrawlerRunner()
runner.settings = get_project_settings()
@defer.inlineCallbacks
def crawl():
    yield runner.crawl(AllJobsSpider)
    yield runner.crawl(DrushimSpider)
    yield runner.crawl(JobmasterSpider)
    yield runner.crawl(LeftCompany)
    reactor.stop()

crawl()
reactor.run()