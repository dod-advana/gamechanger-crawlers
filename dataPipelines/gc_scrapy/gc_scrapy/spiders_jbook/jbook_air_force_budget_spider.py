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
            year = text[-4:len(text)]
            if int(year) >= 2014:
                yield response.follow(url=link.split('/')[-2], callback=self.parse_page, meta={"year": year})

    def parse_page(self, response):
        year = response.meta["year"]
        content_sections = response.css('div[class="DNNModuleContent ModICGModulesExpandableTextHtmlC"] a')#('div[id="dnn_ctr47771_ModuleContent"] a')

        for content in content_sections:
            doc_url = content.css('a::attr(href)').get()
            doc_title = content.css('a::text').get()

            # Document is not valid / an actual link if either url or title is None
            if doc_url is None or doc_title is None or 'javascript' in doc_url:
                continue
            
            is_rdte_document = ('Research, Development, Test and Evaluation' in doc_title or 'RDT&E' in doc_title or
                        'RDTE' in doc_url or 'RDT_E' in doc_url)
            is_procurement_document = ("PROCUREMENT" in doc_url or '/Proc/' in doc_url or ("Procurement" in doc_title and "Procurement" != doc_title))

            if is_procurement_document or is_rdte_document:
                doc_type = 'rdte' if is_rdte_document else "procurement"
                doc_name = doc_url.split('/')[-1].replace('.pdf', '').replace('%20', ' ')
                if '?' in doc_name:
                    doc_name = doc_name.split('?')[0]
                doc_name = f'{doc_type};{year};{doc_name}'

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
                    is_revoked=False,
                )
                yield doc_item