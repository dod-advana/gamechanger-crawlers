from datetime import datetime
import re
from urllib.parse import urljoin

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


class MARADMINSpider(GCSeleniumSpider):
    name = 'maradmin_pubs'

    display_org = 'US Marine Corps'
    data_source = 'Marine Corps Publications Electronic Library'
    source_title = 'Marine Administrative Message'

    start_urls = ['https://www.marines.mil/News/Messages/MARADMINS/Customstatus/4000/']
    allowed_domains = ['marines.mil/']

    cac_login_required = False

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
                    doc_name = doc_type + " " + doc_num.replace("/", "-") + " " + doc_title

                    version_hash_fields = {
                        "document_number": doc_num,
                        "publication_date": publication_date,
                        "web_url": web_url
                    }

                    downloadable_items = [
                        {
                            "doc_type": "html",
                            "web_url": web_url,
                            "compression_type": None
                        }
                    ]

                    doc_item = DocItem(
                        doc_name=" ".join(self.ascii_clean(doc_name).split(" ")[:8]).replace("/", "-"),                       
                        doc_num=self.ascii_clean(doc_num),
                        doc_title=self.ascii_clean(doc_title),
                        doc_type=doc_type,
                        publication_date=publication_date,
                        source_page_url=response.url,
                        display_org=self.display_org,
                        data_source=self.data_source,
                        source_title=self.source_title,
                        downloadable_items=downloadable_items,
                        version_hash_raw_data=version_hash_fields,
                    )
                    yield doc_item
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
