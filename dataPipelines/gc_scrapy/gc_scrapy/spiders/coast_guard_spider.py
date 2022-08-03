# -*- coding: utf-8 -*-
from scrapy import Selector
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import Chrome
from selenium.common.exceptions import NoSuchElementException
from datetime import datetime
from dataPipelines.gc_scrapy.gc_scrapy.utils import parse_timestamp, dict_to_sha256_hex_digest
from urllib.parse import urlparse

from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSeleniumSpider import GCSeleniumSpider


class CoastGuardSpider(GCSeleniumSpider):
    """
        Parser for Coast Guard Commandant Instruction Manuals
    """

    name = 'Coast_Guard' # Crawler name
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


    @staticmethod
    def get_display_doc_type(display_doc_type):
        """This function returns value for display_doc_type based on doc_type -> display_doc_type mapping"""
        display_type_dict = {"cngbi": "Instruction",
            "cim": "Manual",
            "ci": "Instruction",
            "cn": "Notice",
            "ccn": "Notice",
            "dcmsi": "Instruction"}
        return display_type_dict.get(display_doc_type.lower())

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

            doc_title_raw = row.css('td:nth-child(2) a::text').get()
            office_primary_resp_raw = row.css('td:nth-child(3)::text').get()
            href_raw = row.css('td:nth-child(2) a::attr(href)').get()
            download_url = self.ensure_full_href_url(href_raw, driver.current_url)
            publication_date = row.css('td:nth-child(5)::text').get()

            fields = {
                "doc_type_raw": doc_type_raw,
                "doc_num_raw": doc_num_raw,
                "doc_title_raw": doc_title_raw,
                "href_raw": href_raw,
                "download_url": download_url,
                "publication_date": publication_date,
                "office_primary_resp_raw": office_primary_resp_raw
            }

            yield self.populate_doc_item(fields)

    def populate_doc_item(self, fields):
        '''
        This functions provides both hardcoded and computed values for the variables
        in the imported DocItem object and returns the populated metadata object
        '''
        display_org = "US Navy" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "MyNavy HR" # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Bureau of Naval Personnel Instructions" # Level 3 filter
        cac_login_required = False # No CAC required for any documents
        is_revoked = False

        doc_num = fields.get("doc_num_raw").replace('_', '.')
        doc_type = fields.get("doc_type_raw")
        doc_title = self.ascii_clean(fields.get("doc_title_raw"))
        display_doc_type = self.get_display_doc_type(doc_type)
        file_ext = self.get_href_file_extension(fields.get("href_raw")) #########
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + " " + doc_title
        access_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f") # T added as delimiter between date and time
        publication_date = self.get_pub_date(fields.get("publication_date"))
        doc_name = f"{doc_type} {doc_num}"
        download_url = fields.get("download_url").replace(' ', '%20')
        version_hash_fields = {
                "doc_name": doc_name,
                "doc_num": doc_num,
                "publication_date": publication_date,
                "download_url": download_url
            }
        downloadable_items = [{
                "doc_type": doc_type,
                "download_url": download_url,
                "compression_type": None
            }]
        office_primary_resp = self.ascii_clean(fields.get("office_primary_resp_raw"))
        source_page_url = fields.get("download_url")
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
                    file_ext = file_ext,
                    office_primary_resp = office_primary_resp,
                    is_revoked = is_revoked,
                    access_timestamp = access_timestamp
                )
