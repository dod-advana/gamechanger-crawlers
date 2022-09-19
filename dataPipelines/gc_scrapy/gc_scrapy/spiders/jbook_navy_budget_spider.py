# JBOOK CRAWLER
# Navy Budget Spider

import scrapy
from scrapy import Selector
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver import Chrome
from selenium.common.exceptions import NoSuchElementException
import re
import json
from urllib.parse import urljoin, urlparse
from datetime import datetime

from dataPipelines.gc_scrapy.gc_scrapy.middleware_utils.selenium_request import SeleniumRequest
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSeleniumSpider import GCSeleniumSpider

class JBOOKNavyBudgetSpider(GCSeleniumSpider):
    '''
    Class defines the behavior for crawling and extracting text-based documents from the "Army Financial Management & Comptroller" site. 
    This class inherits the 'GCSeleniumSpider' class from GCSeleniumSpider.py. The GCSeleniumSpider class applies Selenium settings to the standard
    parse method used in Scrapy crawlers in order to return a Selenium response instead of a standard Scrapy response.

    This class and its methods = the jbook_navy_budget "spider".
    '''

    name = 'jbook_navy_budget' # Crawler name
    display_org = "Unknown Source" # Level 1: GC app 'Source' filter for docs from this crawler
    data_source = "Unknown Source" # Level 2: GC app 'Source' metadata field for docs from this crawler
    source_title = "Unknown Source" # Level 3 filter

    cac_login_required = False
    rotate_user_agent = True
    allowed_domains = ['secnav.navy.mil'] # Domains the spider is allowed to crawl
    start_urls = [
        'https://www.secnav.navy.mil/fmc/fmb/Documents/Forms/AllItems.aspx'
    ] # URL where the spider begins crawling

    file_type = "pdf" # Define filetype for the spider to identify.

    @staticmethod
    def clean(text):
        '''
        This function forces text into the ASCII characters set, ignoring errors
        '''
        return text.encode('ascii', 'ignore').decode('ascii').strip()

    def parse(self, response):
        year_buttons = response.css('td[class="ms-cellstyle ms-vb-title"]')
        for year_button in year_buttons:
            link = year_button.css('a::attr(href)').get()
            text = year_button.css('a::text').get()

            # If we are looking at a folder we want to parse
            if 'pres' in text:
                if text[0] == '9':
                    year = '19' + text[0:2]
                else:
                    year = '20' + text[0:2]

                if int(year) >= 2014:
                    yield response.follow(url=link, callback=self.parse_page, meta={"year": year})


    def parse_page(self, response):
        year = response.meta["year"]
        
        pattern = r'\bvar\s+WPQ2ListData\s*=\s*\{[\s\S]*?\}\n\]'
        table_data = response.css('script::text').re_first(pattern)
        try:
            table_data = table_data.split('],"FirstRow"')[0].split(': \n[')[1]
        except:
            print('ERROR: Blocked from site.')
            yield DocItem()
        for doc_string in table_data.split(',{'):
            if not doc_string[0] == '{':
                doc_string = '{' + doc_string
            doc_dict = json.loads(doc_string)
            

            doc_url = doc_dict['FileRef'].replace('\u002f', '/')
            doc_title = doc_dict['Title']

            if doc_title is '':
                doc_title = doc_dict['FileLeafRef'].replace('.pdf', '')

            is_revoked = False

            publication_date = doc_dict['Modified'].replace('\u002f', '/')

            doc_type = 'Procurement' if 'PROCUREMENT' in doc_dict["Section"] else 'RDTE'
            doc_name = doc_dict['FileLeafRef'].replace('.pdf', '')
            doc_name = f'{year};{doc_type};{doc_name}'

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
                "publication_date": publication_date,
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
            yield doc_item