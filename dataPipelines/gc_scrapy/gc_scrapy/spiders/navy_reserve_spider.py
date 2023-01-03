# -*- coding: utf-8 -*-
import scrapy
from scrapy import Selector
from datetime import datetime
from dataPipelines.gc_scrapy.gc_scrapy.utils import parse_timestamp, dict_to_sha256_hex_digest
from urllib.parse import urlparse
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver import Chrome
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from itertools import zip_longest
import re

from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSeleniumSpider import GCSeleniumSpider

from time import sleep


class NavyReserveSpider(GCSeleniumSpider):
    name = "navy_reserves" # Crawler name
    
    allowed_domains = ['navyreserve.navy.mil']
    start_urls = [
        'https://www.navyreserve.navy.mil/'
    ]


    
    tables_selector = 'table.dnnGrid'

    @staticmethod
    def get_display_doc_type(doc_type):
        if doc_type.strip().lower().endswith("inst"):
            return "Instruction"
        elif doc_type.strip().lower().endswith("note"):
            return "Notice"
        else:
            return "Document"

    def parse(self, response):
        driver: Chrome = response.meta["driver"]

        anchors = driver.find_elements_by_css_selector(
            "#header > div.navbar-collapse.nav-main-collapse.collapse.otnav.nopad > div > nav > ul > li:nth-child(4) > ul > li.dm.dropdown > ul > li > a"
        )

        pages = [
            elem.get_attribute('href') for elem in anchors if 'Message' not in elem.get_attribute('href')
        ]

        for page_url in pages:
            for item in self.parse_page(page_url, driver):
                yield item

    def parse_page(self, url, driver) -> DocItem:
        if "Instruction" in url or "Notice" in url:
            type_prefix = "COMNAVRESFORCOM"
        if "RESPERSMAN" in url:
            type_prefix = "RESPERSMAN"

        driver.get(url)
        init_webpage = Selector(text=driver.page_source)
        # get a list headers from tables that need parsing
        table_headers_raw = init_webpage.css(
            'div.base-container.blue-header2 h2.title span.Head::text').getall()

        # associate a table header with a table index
        for table_index, header_text_raw in enumerate(table_headers_raw):

            doc_type = header_text_raw.strip()

            has_next_page = True
            while has_next_page:
                # have to reselect page source each loop b/c it refreshes on page change
                webpage = Selector(text=driver.page_source)

                # grab associated tables again each page
                data_table = webpage.css(self.tables_selector)[table_index]
                paging_table = driver.find_elements_by_css_selector('table.PagingTable')[
                    table_index]

                # check has next page link
                try:
                    el = paging_table.find_element_by_xpath(
                        "//a[contains(text(), 'Next')]")
                except (StaleElementReferenceException, NoSuchElementException):
                    # expected when one page or on last page, set exit condition then parse table
                    has_next_page = False
                except Exception as e:
                    # other exceptions not expected, dont page this table and try to continue
                    print(
                        f'Unexpected Exception - attempting to continue: {e}')
                    has_next_page = False

                # parse data table
                try:
                    for tr in data_table.css('tbody tr:not(.dnnGridHeader)'):
                        doc_num_raw = tr.css('td:nth-child(1)::text').get()
                        doc_title_raw = tr.css('td:nth-child(2)::text').get()
                        href_raw = tr.css(
                            'td:nth-child(3) a::attr(href)').get()

                        doc_num = doc_num_raw.strip().replace(" ", "_").replace(u'\u200b', '')
                        if not bool(re.search(r'\d', doc_num)):
                            continue
                        if "RESPERSMAN" in driver.current_url:
                            type_suffix = ""
                        elif '.' in doc_num:
                            type_suffix = "INST"
                        else:
                            type_suffix = "NOTE"

                        doc_title = doc_title_raw.strip()

                        doc_type = type_prefix + type_suffix
                        doc_name = doc_type + " " + doc_num
                        if re.search(r'\(\d\)', doc_title):
                            doc_name_suffix = re.split('\(', doc_title)
                            doc_name_suffix = re.split(
                                '\)', doc_name_suffix[1])
                            if doc_name_suffix[0].strip() != "":
                                doc_name = doc_name + '_' + doc_name_suffix[0]
                            if len(doc_name_suffix) > 1 and doc_name_suffix[1].strip() != "":
                                doc_name = doc_name + '_' + \
                                    doc_name_suffix[1].strip().replace(
                                        " ", "_")

                        web_url = self.ensure_full_href_url(
                            href_raw, driver.current_url)

                        doc_title = self.ascii_clean(doc_title_raw)

                        fields = {
                            "doc_name": doc_name.strip(),
                            "doc_title": doc_title.strip(),
                            "doc_num": doc_num.strip(),
                            "doc_type": doc_type.strip(),
                            "source_page_url": driver.current_url,
                            "href_raw": href_raw,
                            "download_url": web_url.replace(' ', '%20')
                            }
                        yield self.populate_doc_item(fields)

                except:
                    print(
                        f'Unexpected Parsing Exception - attempting to continue: {e}')
                    pass

                if has_next_page:
                    el.click()
                    # wait until paging table is stale from page load, can be slow
                    WebDriverWait(driver, 100).until(
                        EC.staleness_of(paging_table))


    def populate_doc_item(self, fields:dict) -> DocItem:
        display_org = 'US Navy Reserve'  # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = 'U.S. Navy Reserve' # Level 2: GC app 'Source' metadata field for docs from this crawler 
        source_title = "Unlisted Source" # Level 3 filter
        file_type = 'pdf'
        cac_login_required = False
        is_revoked = False
        publication_date = "N/A"

        doc_name = fields.get('doc_name')
        doc_title = fields.get('doc_title')
        doc_num = fields.get('doc_num')
        doc_type = fields.get('doc_type')
        display_doc_type = self.get_display_doc_type(doc_type)
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + ": " + doc_title

        source_page_url = fields.get('source_page_url')
        download_url = fields.get('download_url')

        version_hash_fields = {
            "download_url": fields.get('href_raw'),
            "doc_name": doc_title,
            "doc_num": doc_num,
            "doc_title": doc_title,
            "publication_date": publication_date,
            "display_title": display_title
        }
        downloadable_items = [
            {
                "doc_type": file_type,
                "download_url": download_url,
                "compression_type": None
            }
        ]
        source_fqdn = urlparse(source_page_url).netloc
        version_hash = dict_to_sha256_hex_digest(version_hash_fields)

        return DocItem(
            doc_name = doc_name,
            doc_title = doc_title,
            doc_num = doc_num,
            doc_type = doc_type,
            display_doc_type = display_doc_type,
            publication_date = publication_date,
            cac_login_required = cac_login_required,
            crawler_used = self.name,
            downloadable_items = downloadable_items,
            source_page_url = source_page_url,
            source_fqdn = source_fqdn,
            download_url = download_url, 
            version_hash_raw_data = version_hash_fields,
            version_hash = version_hash,
            display_org = display_org,
            data_source = data_source,
            source_title = source_title,
            display_source = display_source,
            display_title = display_title,
            file_ext = file_type,
            is_revoked = is_revoked,
            )