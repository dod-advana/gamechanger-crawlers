import re
import time
from scrapy.http import TextResponse
from scrapy.selector import Selector
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver import Chrome
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from dataPipelines.gc_scrapy.gc_scrapy.GCSeleniumSpider import GCSeleniumSpider
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from selenium.webdriver.common.keys import Keys

from urllib.parse import urljoin, urlparse
from datetime import datetime
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest, get_pub_date


class MARADMINSpider(GCSeleniumSpider):
    name = 'maradmin_pubs' # Crawler name

    start_urls = ['https://www.marines.mil/News/Messages/MARADMINS/']
    allowed_domains = ['marines.mil/']

    def parse(self, response: TextResponse):
        driver: Chrome = response.meta["driver"]

        while True:
            try:
                self.wait_until_css_located(driver, '#Form')
                doc_rows: list = driver.find_elements_by_class_name('items.alist-more-here > *')
            except Exception as e:
                print('error in grabbing table: ' + e)
                return

            time.sleep(2)  # wait between pages to disencourage getting banned. adds ~16 minutes to runtime
            for doc_row in doc_rows[1:]:
                try:
                    doc_type = "MARADMIN"
                    doc_title = doc_row.find_element_by_class_name('msg-title.msg-col a').get_attribute("textContent")
                    doc_num = doc_row.find_element_by_class_name('msg-num.msg-col a').get_attribute("textContent")
                    publication_date = doc_row.find_element_by_class_name('msg-pub-date.msg-col').get_attribute("textContent")
                    web_url = doc_row.find_element_by_class_name('msg-title.msg-col a').get_attribute('href')
                    doc_status = doc_row.find_element_by_class_name('msg-status.msg-col').get_attribute('textContent').strip()
                    doc_name = doc_type + " " + doc_num.replace("/", "-") + " " + doc_title

                    is_revoked = doc_status != 'Active'

                    fields = {
                        'doc_name': " ".join(self.ascii_clean(doc_name).split(" ")[:8]).replace("/", "-"),
                        'doc_num': self.ascii_clean(doc_num),
                        'doc_title': self.ascii_clean(doc_title),
                        'doc_type': doc_type,
                        'cac_login_required': False,
                        'source_page_url':response.url,
                        'download_url': web_url,
                        'publication_date': publication_date
                    }
                    ## Instantiate DocItem class and assign document's metadata values
                    doc_item = self.populate_doc_item(fields)
                
                    yield from doc_item
                except Exception as e:
                    print('error in processing row: ' + str(e))

            try:
                table: WebElement = driver.find_element_by_css_selector('#Form')
                next_btn: WebElement = driver.find_element_by_css_selector('a.fas.fa.fa-angle-right.da_next_pager')
                try:
                    next_btn.send_keys(Keys.ENTER)
                    WebDriverWait(driver, 20).until(EC.staleness_of(table))
                except Exception as e:
                    print("Error with loading next page: " + str(e))
                    break
            except NoSuchElementException:
                print("Last button encountered. Ending crawler")
                break
            
            
        


    def populate_doc_item(self, fields):
        '''
        This functions provides both hardcoded and computed values for the variables
        in the imported DocItem object and returns the populated metadata object
        '''
        display_org = 'US Marine Corps' # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = 'Marine Corps Publications Electronic Library' # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = 'Marine Administrative Message' # Level 3 filter

        doc_name = fields['doc_name']
        doc_num = fields['doc_num']
        doc_title = fields['doc_title']
        doc_type = fields['doc_type']
        cac_login_required = fields['cac_login_required']
        download_url = fields['download_url']
        publication_date = get_pub_date(fields['publication_date'])

        display_doc_type = "Document" # Doc type for display on app
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + ": " + doc_title
        is_revoked = False
        source_page_url = fields['source_page_url']
        source_fqdn = urlparse(source_page_url).netloc

        downloadable_items = [{
                "doc_type": "html",
                "download_url": download_url,
                "compression_type": None,
            }]

        ## Assign fields that will be used for versioning
        version_hash_fields = {
            "doc_name": doc_name,
            "doc_num": doc_num,
            "publication_date": publication_date,
            "download_url": download_url,
            "display_title": display_title
        }

        version_hash = dict_to_sha256_hex_digest(version_hash_fields)

        yield DocItem(
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
                    file_ext = doc_type,
                    is_revoked = is_revoked,
                )