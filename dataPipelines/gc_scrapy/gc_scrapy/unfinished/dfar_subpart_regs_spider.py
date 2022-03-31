from typing import Iterator, List

from scrapy import Selector
from scrapy.http import TextResponse
from selenium.webdriver import Chrome
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException

from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSeleniumSpider import GCSeleniumSpider


class DfarsSubpartSpider(GCSeleniumSpider):
    name = "dfar_subpart_regs"

    start_urls = [
        "https://www.acquisition.gov/dfars"
    ]
    cac_login_required = False
    doc_type = "DFARS"

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
                if (s in button_title for s in ('APPENDIX', 'PART 210', 'PART 220')):
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

            if doc_title.lower().startswith('appendix'):  # ???
                break

            href_raw = row.css('td:nth-child(2) a::attr(href)').get()

            doc_num = doc_title.split()[0] + ' ' + doc_title.split()[1]
            doc_name = self.doc_type + " " + doc_num

            web_url = self.ensure_full_href_url(href_raw, self.start_urls[0])

            downloadable_items = [
                {
                    "doc_type": 'html',
                    "web_url": web_url.replace(' ', '%20'),
                    "compression_type": 'zip'
                }
            ]

            version_hash_fields = {
                "doc_type": self.doc_type,
                "item_currency": href_raw,
                "pub_date": pub_date
            }

            yield DocItem(
                doc_name=doc_name,
                doc_num=doc_num,
                doc_title=doc_title,
                publication_date=pub_date,
                downloadable_items=downloadable_items,
                version_hash_raw_data=version_hash_fields,
            )

    def parse_pub_date(self, response: TextResponse) -> str:
        meta_table = response.css('table.usa-table:nth-of-type(1)')
        pub_date_raw = meta_table.css('tbody > tr > td:nth-child(2)::text').get()
        pub_date = self.ascii_clean(pub_date_raw)
        return pub_date
