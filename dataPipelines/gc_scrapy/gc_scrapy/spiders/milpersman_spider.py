import scrapy
from scrapy import Selector
import re
import bs4
from dataPipelines.gc_scrapy.gc_scrapy.items import DocumentItem
from dataPipelines.gc_scrapy.gc_scrapy.data_model import Document, DownloadableItem
from dataPipelines.gc_scrapy.gc_scrapy.utils import abs_url

from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider


def remove_html_tags(text):
    import re
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)


digit_re = re.compile('\d')


def has_digit(text):
    return digit_re.match(text)


class MilpersmanSpider(GCSpider):
    name = 'milpersman'

    start_urls = ['https://www.mynavyhr.navy.mil/References/MILPERSMAN/']

    def parse(self, response):

        anchors = [a for a in response.css(
            'li[title="MILPERSMAN"] ul li a') if has_digit(a.css("::text").get())]

        for res in response.follow_all(anchors, self.parse_doc_type):
            yield res

    def parse_doc_type(self, response):
        sub_anchors = response.css("ul.afAccordionMenuSubMenu a")

        if len(sub_anchors):
            for res in response.follow_all(sub_anchors, self.parse_page):
                yield res
        else:
            for res in self.parse_page(response):
                yield res

    def parse_page(self, response):

        rows = [tr for tr in response.css(
            'div.livehtml table tbody tr') if len(tr.css('td:nth-child(1) a'))]

        for row in rows:
            doc_num_raw = "".join(row.css('td:nth-child(1) *::text').getall())
            doc_title_raw = " ".join(
                row.css('td:nth-child(2) *::text').getall())

            doc_num = self.ascii_clean(doc_num_raw)
            doc_title = self.ascii_clean(doc_title_raw)

            if not self.ascii_clean(doc_num):
                continue

            doc_name = f"MILPERSMAN {doc_num}"
            yield DocItem(
                doc_name=doc_name,
                doc_title=doc_title
            )
