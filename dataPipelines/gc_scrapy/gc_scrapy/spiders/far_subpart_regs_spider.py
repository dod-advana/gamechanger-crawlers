from typing import Iterator, List

from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.GCSeleniumSpider import GCSeleniumSpider
from dataPipelines.gc_scrapy.gc_scrapy.utils import parse_timestamp, dict_to_sha256_hex_digest
from urllib.parse import urlparse

from datetime import datetime
from scrapy import Selector
from scrapy.http import TextResponse
from selenium.webdriver import Chrome
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException


class FarSubpartSpider(GCSeleniumSpider):
    name = "far_subpart_regs" # Crawler name

    start_urls = [
        'https://www.acquisition.gov/far'
    ]
    
    randomly_delay_request = True

    @staticmethod
    def get_pub_date(publication_date):
        '''
        This function convverts publication_date from DD Month YYYY format to YYYY-MM-DDTHH:MM:SS format.
        T is a delimiter between date and time.
        '''
        try:
            date = parse_timestamp(publication_date, None)
            if date:
                publication_date = datetime.strftime(date, '%Y-%m-%dT%H:%M:%S')
        except:
            publication_date = ""
        return publication_date

    def parse(self, response: TextResponse):
        pub_date = self.parse_pub_date(response)
        yield from self.parse_parts(response, pub_date)

        driver: Chrome = response.meta["driver"]

        # open each part and then parse the subparts
        num_buttons = len(driver.find_elements_by_css_selector('a.agov-open-subparts-link'))
        for i in range(num_buttons):
            # the rows seem to be recreated so find the button again
            button = driver.find_elements_by_css_selector(f'a.agov-open-subparts-link')[i]
            button_title = button.get_attribute('title')

            # sending the enter key seems to work more reliably than clicking
            button.send_keys(Keys.ENTER)
            try:
                self.wait_until_css_located(driver, 'tr.agov-browse-tr')
            except TimeoutException:
                # these don't have subparts
                if (s in button_title for s in ('Part III',)):
                    continue
                raise TimeoutException(f'timed opening subparts for {button_title}')

            yield from self.parse_subparts(Selector(text=driver.page_source), pub_date)

            # the rows seem to be recreated so find the button again
            button = driver.find_elements_by_css_selector(f'a.agov-open-subparts-link')[i]
            button.send_keys(Keys.ENTER)
            self.wait_until_css_not_located(driver, 'tr.agov-browse-tr')

    def parse_parts(self, response: TextResponse, pub_date: str) -> Iterator[DocItem]:
        docs_table = response.css('table.usa-table:nth-of-type(2)')
        rows = docs_table.css('tbody > tr')
        yield from self.parse_rows(rows, pub_date)

    def parse_subparts(self, response: Selector, pub_date: str) -> Iterator[DocItem]:
        rows = response.css('tr.agov-browse-tr')
        yield from self.parse_rows(rows, pub_date)

    def parse_rows(self, rows: List[Selector], pub_date: str) -> Iterator[DocItem]:
        for row in rows:
            doc_num_title_raw = row.css('td:nth-child(1) a::text').get()

            doc_title = self.ascii_clean(doc_num_title_raw)

            # if doc_title.lower().startswith('appendix'):  # ???
            #     break

            href_raw = row.css('td:nth-child(2) a::attr(href)').get()
            doc_num = doc_title.split()[0] + ' ' + doc_title.split()[1]
            
            web_url = self.ensure_full_href_url(href_raw, self.start_urls[0])
            fields = {
                "doc_num": doc_num,
                "doc_title": doc_title,
                "publication_date": pub_date,
                "href_raw": href_raw,
                "download_url": web_url.replace(' ', '%20')
            }

            doc_item = self.populate_doc_item(fields)
            
            yield doc_item


    def parse_pub_date(self, response: TextResponse) -> str:
        meta_table = response.css('table.usa-table:nth-of-type(1)')
        pub_date_raw = meta_table.css('tbody > tr > td:nth-child(2)::text').get()
        pub_date = self.ascii_clean(pub_date_raw)
        return pub_date


    def populate_doc_item(self, fields: dict) -> DocItem:
        display_org = "FAR" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "Federal Acquisition Regulation" # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Unlisted Source" # Level 3 filter
        doc_type = "FAR"
        file_type = "html"
        display_doc_type = "Regulation"
        cac_login_required = False
        is_revoked = False

        doc_title = fields.get('doc_title')
        publication_date = fields.get('publication_date', '')
        publication_date = self.get_pub_date(publication_date)
        source_page_url = self.start_urls[0]
        download_url = fields.get("download_url")
        doc_num = fields.get('doc_num')
        doc_name = doc_type + " " + doc_num
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + ": " + doc_title
        downloadable_items = [{
                "doc_type": file_type,
                "download_url": download_url,
                "compression_type": 'zip'
            }]
        version_hash_fields = {
                "download_url": fields.get("href_raw"),
                "doc_name": doc_name,
                "doc_num": doc_num,
                "publication_date": publication_date,
                "display_title": display_title
            }
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