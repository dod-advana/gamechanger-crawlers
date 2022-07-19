# -*- coding: utf-8 -*-
from scrapy import Selector
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import Chrome
from selenium.common.exceptions import NoSuchElementException
from datetime import datetime
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest
from dataPipelines.gc_scrapy.gc_scrapy.utils import parse_timestamp

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
    def get_display_doc_type(doc_type):
        """This function returns value for display_doc_type based on doc_type -> display_doc_type mapping"""
        display_type_dict = {"cngbi": "Instruction",
            "cim": "Manual",
            "ci": "Instruction",
            "cn": "Notice",
            "ccn": "Notice",
            "dcmsi": "Instruction"}
        return display_type_dict.get(doc_type)

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

# ############################

    def populate_doc_item(self, hrefs, item_currency, doc_num, doc_type, doc_title, publication_date):
        '''
        This functions provides both hardcoded and computed values for the variables
        in the imported DocItem object and returns the populated metadata object
        '''
        display_org = "US Navy" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "MyNavy HR" # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Bureau of Naval Personnel Instructions" # Level 3 filter
        cac_login_required = False # No CAC required for any documents
        is_revoked = False

        display_doc_type = get_display_doc_type(doc_type)
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + " " + doc_title

        

        access_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f") # T added as delimiter between date and time
        publication_date = self.get_pub_date(publication_date)
        source_page_url = self.start_urls[0]
        source_fqdn = urlparse(source_page_url).netloc
        downloadable_items = self.get_downloadables(hrefs)
        download_url = downloadable_items[0]['download_url']
        file_ext = downloadable_items[0]['doc_type']
        doc_name = self.match_old_doc_name(f"{doc_type} {doc_num}")
        version_hash_fields = {
            "item_currency": item_currency,
            "document_title": doc_title,
            "document_number": doc_num,
            "doc_name": doc_name
        }
        version_hash = dict_to_sha256_hex_digest(version_hash_fields)

        return DocItem(
                    doc_name = doc_name,
                    doc_title = doc_title,
                    doc_num = doc_num,
                    doc_type = doc_type,
                    display_doc_type_s = display_doc_type,
                    publication_date_dt = publication_date,
                    cac_login_required_b = cac_login_required,
                    crawler_used_s = self.name,
                    downloadable_items = downloadable_items,
                    source_page_url_s = source_page_url,
                    source_fqdn_s = source_fqdn,
                    download_url_s = download_url, 
                    version_hash_raw_data = version_hash_fields,
                    version_hash_s = version_hash,
                    display_org_s = display_org,
                    data_source_s = data_source,
                    source_title_s = source_title,
                    display_source_s = display_source,
                    display_title_s = display_title,
                    file_ext_s = file_ext,
                    is_revoked_b = is_revoked,
                    access_timestamp_dt = access_timestamp
                )
