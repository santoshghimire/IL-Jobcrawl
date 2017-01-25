from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


process = CrawlerProcess(get_project_settings())
process.crawl('alljobs')
process.crawl('jobmaster')
process.crawl('drushim')
process.crawl('jobnet')
process.crawl('left')
process.start()
# the script will block here until the crawling is finished
process.stop()
