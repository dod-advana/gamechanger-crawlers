from datetime import datetime
import re
from urllib.parse import urljoin

from scrapy.http import TextResponse
from scrapy.selector import Selector
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver import Chrome
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait


from dataPipelines.gc_scrapy.gc_scrapy.GCSeleniumSpider import GCSeleniumSpider
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem

class AssistQuicksearchSpider(GCSeleniumSpider):
    name = 'assist_quicksearch'

    display_org = 'Defense Logistics Agency'
    data_source = 'ASSIST'
    source_title = 'Acquisition Streamlining and Standardization Information System'

    start_urls = ['https://quicksearch.dla.mil/qsSearch.aspx']
    allowed_domains = ['quicksearch.dla.mil']

    cac_login_required = False
    doc_type = ''  # XXX: ???

    def parse(self, response: TextResponse):
        driver: Chrome = response.meta["driver"]
        Select(
            driver.find_element_by_css_selector('#DropDownListStatus')
        ).select_by_value("1")  # Active
        self.wait_until_css_located(driver, '#DocumentSearchFilters tr:nth-child(2)')

        # doc_id_text_box: WebElement = driver.find_element_by_css_selector('#DocumentIDTextBox')  # XXX: delete
        # doc_id_text_box.send_keys('MIL-DTL-17/92')  # XXX: delete
        # doc_id_text_box.send_keys(Keys.ENTER)  # XXX: delete
        # self.wait_until_css_located(driver, '#DocumentSearchFilters tr:nth-child(3)')  # XXX: delete
        
        search_btn = driver.find_element_by_css_selector('#GetFilteredButton')
        search_btn.click()

        while True:
            self.wait_until_css_located(driver, '#GV')
            selector = Selector(text=driver.page_source)
            
            doc_rows = selector.css('#GV tr.grid_item,#GV tr.grid_alternate')
            doc_row: Selector
            for doc_row in doc_rows:
                has_img = doc_row.css('td:nth-child(1) > a::text').get()
                if has_img != 'Y':  # no available images
                    continue
                doc_num = ''.join(doc_row.css('td:nth-child(2) *::text').getall())
                doc_link = doc_row.css('td:nth-child(2) > a[title="Go to the Document Details."]::attr(href)').get()
                meta = {'doc_num': doc_num}
                yield response.follow(doc_link, callback=self.parse_doc_details, meta=meta)

            try:
                next_btn: WebElement = driver.find_element_by_css_selector('#btnNextEx')
                if not next_btn.is_enabled():
                    break
            except NoSuchElementException:
                break

            table: WebElement = driver.find_element_by_css_selector('#GV')
            next_btn.click()
            WebDriverWait(driver, 10).until(EC.staleness_of(table))

    def parse_doc_details(self, response: TextResponse):
        doc_num = response.meta['doc_num']

        doc_title = response.css('#general_titleLabel::text').get()

        doc_name = f'{doc_num} - {doc_title}'
        
        publication_date = response.css('#general_doc_dateLabel::text').get()
        publication_date = datetime.strptime(publication_date, '%d-%b-%Y').strftime('%m/%d/%Y')

        doc_url = response.css('#GVRevisionHistory tr:nth-child(2) > td > '
                               'a[title="Click here to view the Document Image"]::attr(href)').get()
        if not doc_url:  # no publicly available download
            return
        doc_token = re.match(r"javascript:spawnPDFWindow\('\.\/ImageRedirector\.aspx\?token=.+,(?P<token>\d+)\);", doc_url)['token']
        web_url = urljoin(response.url, f'../../WMX/Default.aspx?token={doc_token}')

        version_hash_fields = {
            "item_currency": web_url.split('/')[-1],
            "document_title": doc_title,
            "document_number": doc_num,
            "publication_date": publication_date,
        }

        downloadable_items = [
            {
                "doc_type": "pdf",
                "web_url": web_url,
                "compression_type": None
            }
        ]

        pgi_doc_item = DocItem(
            doc_name=self.clean_name(doc_name),
            doc_num=self.ascii_clean(doc_num),
            doc_title=self.ascii_clean(doc_title),
            publication_date=publication_date,
            source_page_url=response.url,
            downloadable_items=downloadable_items,
            version_hash_raw_data=version_hash_fields,
        )
        yield pgi_doc_item

    def clean_name(self, name):
        return ' '.join(re.sub(r'[^a-zA-Z0-9. ()-_]', '', self.ascii_clean(name).replace('/', '_')).split())
