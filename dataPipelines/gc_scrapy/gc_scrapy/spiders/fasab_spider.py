import scrapy
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
import datetime
import time
from dataPipelines.gc_scrapy.gc_scrapy.utils import abs_url
import re


class BrickSetSpider(scrapy.Spider):
    name = 'FASAB'
    allowed_domains = ['fasab.gov']
    file_type = "pdf"
    start_urls = ['https://fasab.gov/accounting-standards/document-by-chapter/']
    start_url = 'https://fasab.gov/accounting-standards/document-by-chapter/'

    def parse(self, response):
        SET_SELECTOR = 'li'
        for brickset in response.css(SET_SELECTOR):

            NAME_SELECTOR = 'a ::text'
            URL_SELECTOR = 'a::attr(href)'
            TITLE_SELECTOR = 'ul li::text'
            doc_name = brickset.css(NAME_SELECTOR).extract_first()
            url = brickset.css(URL_SELECTOR).extract_first()
            doc_title = brickset.css(TITLE_SELECTOR).extract_first()
            if doc_title is None:
                continue
            if doc_name is None:
                continue
            if url is None:
                continue
            if "SFFAS" not in str(doc_name) and "SFFAC" not in str(doc_name):
                doc_name = "FASAB " + str(doc_name)
            doc_num = doc_name.rsplit(' ', 1)[-1]
            doc_type = doc_name.rsplit(' ', 1)[0]
            cac_login_required = False
            if not url.startswith("http"):
                url = "https:" + url
            downloadable_items = [
                {
                    "doc_type": 'pdf',
                    "web_url": url,
                    "compression_type": None
                }
            ]
            version_hash_fields = {
                # version metadata found on pdf links
                "item_currency": url.split('/')[-1],
                "doc_name": doc_name,
            }

            yield DocItem(
                doc_name=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_name),
                doc_title=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_title),
                doc_num=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_num),
                doc_type=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_type),
                publication_date="N/A",
                cac_login_required=cac_login_required,
                downloadable_items=downloadable_items,
                version_hash_raw_data=version_hash_fields,
                access_timestamp=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
                crawler_used="FASAB Crawler",
                source_page_url='https://fasab.gov/accounting-standards/document-by-chapter/'
            )

