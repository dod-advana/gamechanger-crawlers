# JBOOK CRAWLER
# Air Force Budget Spider

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

class JBOOKAirForceBudgetSpider(GCSeleniumSpider):
    '''
    Class defines the behavior for crawling and extracting text-based documents from the "Army Financial Management & Comptroller" site. 
    This class inherits the 'GCSeleniumSpider' class from GCSeleniumSpider.py. The GCSeleniumSpider class applies Selenium settings to the standard
    parse method used in Scrapy crawlers in order to return a Selenium response instead of a standard Scrapy response.

    This class and its methods = the jbook_navy_budget "spider".
    '''

    name = 'jbook_air_force_budget' # Crawler name
    display_org = "Unknown Source" # Level 1: GC app 'Source' filter for docs from this crawler
    data_source = "Unknown Source" # Level 2: GC app 'Source' metadata field for docs from this crawler
    source_title = "Unknown Source" # Level 3 filter

    cac_login_required = False
    rotate_user_agent = True
    allowed_domains = ['saffm.hq.af.mil'] # Domains the spider is allowed to crawl
    start_urls = [
        'https://www.saffm.hq.af.mil/FM-Resources/Budget/'
    ] # URL where the spider begins crawling

    file_type = "pdf" # Define filetype for the spider to identify.

    @staticmethod
    def clean(text):
        '''
        This function forces text into the ASCII characters set, ignoring errors
        '''
        return text.encode('ascii', 'ignore').decode('ascii').strip()

    def parse(self, response):
        year_buttons = response.css('div[id="dnn_ctr44627_View_AccordionContainer"] a')
        
        for year_button in year_buttons:
            
            link = year_button.css('a::attr(href)').get()
            text = year_button.css('a::text').get()
            print('\n\n***************')
            print(text)
            if 'pres' in text:
                print(text)
                print(text[0:1])
                if text[0] == '9':
                    year = '19' + text[0:2]
                else:
                    year = '20' + text[0:2]
                yield response.follow(url=link, callback=self.parse_page, meta={"year": year})

    def parse_page(self, response):
        year = response.meta["year"]
        
        content_sections = response.css('div[class="rightHalf wpzContainerdiv"] [class="ms-webpart-chrome ms-webpart-chrome-vertical ms-webpart-chrome-fullWidth "]')

        print('\n\n***************')
        print(year)
        #for content in content_sections:
        #    c = content.css('a > span')
        #    print(c.get())
        print('***************\n\n')
            # doc_url = content.css('a::attr(href)').get()
            # doc_title = content.css('a::attr(title)').get()

            # is_revoked = False


            # # If the document is none, is neither procurement or rdte type, 
            # # or does not contain Portals (this gets rid of a few Javascript headers that get pulled back as hrefs)
            # # then ignore the document
            # if doc_url is None or not ('Procurement' in doc_url or 'rdte' in doc_url) or not 'Portals' in doc_url:
            #     continue

            # publication_date = doc_url.split('/')[5]

            # doc_type = 'RDTE' if 'rdte' in doc_url else 'Procurement'
            # doc_name = f'{doc_type} {publication_date} - {doc_title}'

            # web_url = urljoin(response.url, doc_url)
            # downloadable_items = [
            #     {
            #         "doc_type": "pdf",
            #         "web_url": web_url,
            #         "compression_type": None
            #     }
            # ]

            # version_hash_fields = {
            #     "item_currency": downloadable_items[0]["web_url"].split('/')[-1],
            #     "document_title": doc_title,
            #     "publication_date": publication_date,
            # }

            # doc_item = DocItem(
            #     doc_name=doc_name,
            #     doc_title=self.ascii_clean(doc_title),
            #     doc_type=self.ascii_clean(doc_type),
            #     publication_date=publication_date,
            #     source_page_url=response.url,
            #     downloadable_items=downloadable_items,
            #     version_hash_raw_data=version_hash_fields,
            #     is_revoked=is_revoked,
            # )
            # yield doc_item