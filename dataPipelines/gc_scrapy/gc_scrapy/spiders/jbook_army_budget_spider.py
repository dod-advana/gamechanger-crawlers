# JBOOK CRAWLER
# Army Budget Spider

import scrapy
from scrapy import Selector
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver import Chrome
from selenium.common.exceptions import NoSuchElementException
import re
from urllib.parse import urljoin, urlparse
from datetime import datetime

from dataPipelines.gc_scrapy.gc_scrapy.middleware_utils.selenium_request import SeleniumRequest
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSeleniumSpider import GCSeleniumSpider

type_and_num_regex = re.compile(r"([a-zA-Z].*) (\d.*)") # Get 'type' (alphabetic) value and 'num' (numeric) value from 'doc_name' string

class JBOOKArmyBudgetSpider(GCSeleniumSpider):
    '''
    Class defines the behavior for crawling and extracting text-based documents from the "Army Financial Management & Comptroller" site. 
    This class inherits the 'GCSeleniumSpider' class from GCSeleniumSpider.py. The GCSeleniumSpider class applies Selenium settings to the standard
    parse method used in Scrapy crawlers in order to return a Selenium response instead of a standard Scrapy response.

    This class and its methods = the jbook_army_budget "spider".
    '''

    name = 'jbook_army_budget' # Crawler name
    display_org = "Unknown Source" # Level 1: GC app 'Source' filter for docs from this crawler
    data_source = "Unknown Source" # Level 2: GC app 'Source' metadata field for docs from this crawler
    source_title = "Unknown Source" # Level 3 filter

    cac_login_required = False
    rotate_user_agent = True
    allowed_domains = ['asafm.army.mil'] # Domains the spider is allowed to crawl
    start_urls = [
        'https://www.asafm.army.mil/Budget-Materials/'
    ] # URL where the spider begins crawling

    file_type = "pdf" # Define filetype for the spider to identify.

    @staticmethod
    def clean(text):
        '''
        This function forces text into the ASCII characters set, ignoring errors
        '''
        return text.encode('ascii', 'ignore').decode('ascii').strip()

    def parse(self, response):
        '''
        This function generates a link and metadata for each document found on the Army Reserves Publishing
        site for use by bash download script.
        '''

        content_sections = response.css('div.z-content tbody tr a')

        for content in content_sections:
            doc_url = content.css('a::attr(href)').get()
            doc_title = content.css('a::attr(title)').get()

            is_revoked = False


            # If the document is none, is neither procurement or rdte type, 
            # or does not contain Portals (this gets rid of a few Javascript headers that get pulled back as hrefs)
            # then ignore the document
            if doc_url is None or not ('Procurement' in doc_url or 'rdte' in doc_url) or not 'Portals' in doc_url:
                continue

            year = doc_url.split('/')[5]

            doc_type = 'RDTE' if 'rdte' in doc_url else 'Procurement'
            doc_name = doc_url.split('/')[-1].replace('.pdf', '')
            doc_name = f'{year} {doc_name}'

            web_url = urljoin(response.url, doc_url)
            downloadable_items = [
                {
                    "doc_type": "pdf",
                    "web_url": web_url,
                    "compression_type": None
                }
            ]

            version_hash_fields = {
                "item_currency": downloadable_items[0]["web_url"].split('/')[-1],
                "document_title": doc_title,
                "publication_date": year,
            }

            doc_item = DocItem(
                doc_name=doc_name,
                doc_title=self.ascii_clean(doc_title),
                doc_type=self.ascii_clean(doc_type),
                publication_date=year,
                source_page_url=response.url,
                downloadable_items=downloadable_items,
                version_hash_raw_data=version_hash_fields,
                is_revoked=is_revoked,
            )
            if int(year[0:4]) >= 2014:
                yield doc_item