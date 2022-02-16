# -*- coding: utf-8 -*-
from tabnanny import check
from time import sleep
from scrapy import Selector
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import Chrome
from selenium.common.exceptions import NoSuchElementException
import re
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSeleniumSpider import GCSeleniumSpider


class SecNavSpider(GCSeleniumSpider):
    name = "secnav_pubs"

    start_urls = [
        "https://www.secnav.navy.mil/doni/default.aspx"
    ]

    # start_urls = [
    #     "http://localhost:3000/"
    # ]

    randomly_delay_request = True

    selenium_spider_start_request_retries_allowed = 5
    selenium_spider_start_request_retry_wait = 90
    selenium_request_overrides = {
        "wait_time": 10,
        "wait_until": EC.presence_of_element_located(
            (By.CSS_SELECTOR, "div#DeltaTopNavigation a.dynamic"))
    }
    file_type = 'pdf'
    table_selector = 'table.ms-listviewtable'

    def parse(self, response):
        driver: Chrome = response.meta["driver"]
        hrefs = [
            link.get_attribute('href') for link in driver.find_elements_by_css_selector('a.dynamic')[0:2]
        ]

        print('all hrefs', hrefs)

        for href in hrefs:
            print('href', href)
            yield from self.parse_table_page(href, driver)
            print('after yield from parse_table_page')

    def parse_table_page(self, href: str, driver: Chrome):
        print('getting table page', href)
        driver.get(href)
        self.wait_until_css_located(driver, self.table_selector, wait=20)

        has_next_page = True
        while(has_next_page):
            try:
                el = driver.find_element_by_css_selector(
                    'td#pagingWPQ3next > a')
                print(el)

            except NoSuchElementException:
                # expected when on last page, set exit condition then parse table
                print('has_next_page = False   did not find td#pagingWPQ3next > a')
                has_next_page = False
            except Exception as e:
                print('NEXT PAGE EL unknown exception', e)
                has_next_page = False

            try:
                print('would parse table')
                for _ in range(10):
                    sleep(1)
                # for item in self.parse_table(driver):
                #     # print('yielding item')
                #     yield item

            except NoSuchElementException:
                raise NoSuchElementException(
                    f"Failed to find table to scrape from using css selector: {self.table_selector}"
                )

            if has_next_page:
                print('has next page')
                sleep(2)
                try:
                    current_url = driver.current_url
                    print('current url', current_url)

                    el.send_keys(Keys.ENTER)
                    print('clicked el, sent keys')
                    WebDriverWait(driver, 90).until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR, self.table_selector)
                        )
                    )

                    print('WebDriverWait found element', self.table_selector)

                except Exception as e:
                    print(e)
                    has_next_page = False

            print(f"has next page? {has_next_page}")

        print('after while loop... table page parsed')

    def parse_table(self, driver: Chrome):
        webpage = Selector(text=driver.page_source)
        row_selector = f'{self.table_selector} tbody tr'
        url = driver.current_url
        print('parsing table at', url)
        type_suffix = ""
        if "instruction" in url:
            type_suffix = "INST"
        if "notice" in url:
            type_suffix = "NOTE"

        for i, row in enumerate(webpage.css(row_selector)):

            doc_type_raw = row.css('td:nth-child(1)::text').get()
            href_raw = row.css('td:nth-child(2) a::attr(href)').get()
            doc_num_raw = row.css('td:nth-child(2) a::text').get()
            doc_title_raw = row.css('td:nth-child(3)::text').get()
            effective_date_raw = row.css('td:nth-child(4) span::text').get()
            status_raw = row.css('td:nth-child(5)::text').get()
            sponsor_raw = row.css('td:nth-child(6)::text').get()
            cancel_date = row.css('td:nth-child(10)::text').get()

            doc_type = self.ascii_clean(doc_type_raw)
            doc_num = self.ascii_clean(doc_num_raw)
            doc_title = self.ascii_clean(doc_title_raw)

            version_hash_fields = {
                "item_currency": href_raw,
                "effective_date": effective_date_raw,
                "status": status_raw,
                "sponsor": sponsor_raw,
                "cancel_date": cancel_date
            }

            web_url = self.ensure_full_href_url(href_raw, self.start_urls[0])

            downloadable_items = [
                {
                    "doc_type": self.file_type,
                    "web_url": web_url.replace(" ", '%20'),
                    "compression_type": None
                }
            ]

            doc_type = f"{doc_type}{type_suffix}"
            doc_name = f"{doc_type} {doc_num}"
            cac_login_required = re.match('^[A-Za-z]', doc_num) != None

            if i % 100 == 0:
                print('row', i, doc_name)

            yield DocItem(
                doc_name=doc_name,
                doc_title=doc_title,
                doc_num=doc_num,
                doc_type=doc_type,
                publication_date=effective_date_raw,
                cac_login_required=cac_login_required,
                downloadable_items=[],
                version_hash_raw_data=version_hash_fields
            )
