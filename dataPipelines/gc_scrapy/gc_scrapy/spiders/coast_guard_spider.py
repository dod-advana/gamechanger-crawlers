# -*- coding: utf-8 -*-
from scrapy import Selector
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import Chrome
from selenium.common.exceptions import NoSuchElementException

from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSeleniumSpider import GCSeleniumSpider


class CoastGuardSpider(GCSeleniumSpider):
    """
        Parser for Coast Guard Commandant Instruction Manuals
    """

    name = 'Coast_Guard' # Crawler name
    display_org = "Coast Guard" # Level 1: GC app 'Source' filter for docs from this crawler
    data_source = "Coast Guard Deputy Commandant for Mission Support" # Level 2: GC app 'Source' metadata field for docs from this crawler
    source_title = "Unlisted Source" # Level 3 filter
    cac_login_required = False

    allowed_domains = ['dcms.uscg.mil']
    start_urls = [
        'https://www.dcms.uscg.mil/Our-Organization/Assistant-Commandant-for-C4IT-CG-6/The-Office-of-Information-Management-CG-61/About-CG-Directives-System/'
    ]
    pages = [
        'https://www.dcms.uscg.mil/Our-Organization/Assistant-Commandant-for-C4IT-CG-6/The-Office-of-Information-Management-CG-61/About-CG-Directives-System/Commandant-Instruction-Manuals/',
        'https://www.dcms.uscg.mil/Our-Organization/Assistant-Commandant-for-C4IT-CG-6/The-Office-of-Information-Management-CG-61/About-CG-Directives-System/Commandant-Instructions/',
        'https://www.dcms.uscg.mil/Our-Organization/Assistant-Commandant-for-C4IT-CG-6/The-Office-of-Information-Management-CG-61/About-CG-Directives-System/Commandant-Notice/',
        'https://www.dcms.uscg.mil/Our-Organization/Assistant-Commandant-for-C4IT-CG-6/The-Office-of-Information-Management-CG-61/About-CG-Directives-System/Commandant-Change-Notices/',
        'https://www.dcms.uscg.mil/Our-Organization/Assistant-Commandant-for-C4IT-CG-6/The-Office-of-Information-Management-CG-61/About-CG-Directives-System/DCMS-Instructions/'
    ]
    current_page_selector = 'div.numericDiv ul li.active a.Page'
    next_page_selector = 'div.numericDiv ul li.active + li a'
    rows_selector = "table.Dashboard tbody tr"

    def parse(self, response):
        driver: Chrome = response.meta["driver"]

        for page_url in self.pages:
            # navigate to page for each doc type
            driver.get(page_url)

            self.wait_until_css_clickable(
                driver, css_selector=self.current_page_selector)

            has_next_page = True
            while(has_next_page):
                try:
                    el = driver.find_element_by_css_selector(
                        self.next_page_selector)

                except NoSuchElementException:
                    # expected when on last page, set exit condition then parse table
                    has_next_page = False

                for item in self.parse_table(driver):
                    yield item

                if has_next_page:
                    el.click()
                    self.wait_until_css_clickable(
                        driver, css_selector=self.rows_selector)

    def parse_table(self, driver):
        webpage = Selector(text=driver.page_source)

        for row in webpage.css(self.rows_selector):
            doc_type_num_raw = row.css('td:nth-child(1)::text').get()

            if '_' in doc_type_num_raw:
                doc_type_raw, _, doc_num_raw = doc_type_num_raw.partition('_')
            else:
                # catch case where text doesnt use _ separators
                doc_type_raw, _, doc_num_raw = doc_type_num_raw.partition(' ')

            # catch case of COMDTINST spelled out
            if doc_type_raw == 'COMDTINST':
                doc_type_raw = 'CI'

            doc_num = doc_num_raw.replace('_', '.')

            doc_title_raw = row.css('td:nth-child(2) a::text').get()
            doc_title = self.ascii_clean(doc_title_raw)

            office_primary_resp_raw = row.css('td:nth-child(3)::text').get()
            office_primary_resp = self.ascii_clean(office_primary_resp_raw)

            href_raw = row.css('td:nth-child(2) a::attr(href)').get()

            web_url = self.ensure_full_href_url(href_raw, driver.current_url)

            publication_date = row.css('td:nth-child(5)::text').get()

            version_hash_fields = {
                "item_currency": href_raw,
                "document_title": doc_title
            }

            file_type = self.get_href_file_extension(href_raw)

            downloadable_items = [
                {
                    "doc_type": file_type,
                    "web_url": web_url.replace(' ', '%20'),
                    "compression_type": None
                }
            ]

            yield DocItem(
                doc_type=doc_type_raw,
                doc_name=f"{doc_type_raw} {doc_num}",
                doc_title=doc_title,
                doc_num=doc_num,
                publication_date=publication_date,
                downloadable_items=downloadable_items,
                version_hash_raw_data=version_hash_fields,
                source_page_url=driver.current_url,
                office_primary_resp=office_primary_resp,
            )
