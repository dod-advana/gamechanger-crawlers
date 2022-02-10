from scrapy import Selector

from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSeleniumSpider import GCSeleniumSpider
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import Chrome
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException
import time


class NavyMedSpider(GCSeleniumSpider):
    name = "navy_med_pubs"

    start_urls = [
        "https://www.med.navy.mil/Directives/"
    ]
    tabs_ul_selector = 'ul.z-tabs-nav.z-tabs-desktop'

#    selenium_request_overrides = {
#        "wait_until": EC.element_to_be_clickable(
#            (By.CSS_SELECTOR, tabs_ul_selector))
#    }

    tabs_parsed = set({})
    tabs_doc_type_dict = {
        "BUMED Instructions": "BUMEDINST",
        "BUMED Notices (Notes)": "BUMEDNOTE",
        "All Pubs and Manuals": "NAVMED"
    }

    def get_tab_button_els(self, driver: Chrome):
        tab_button_els = driver.find_elements_by_css_selector(
            f"{self.tabs_ul_selector} li a")

        tabs_to_click = [
            el for el in tab_button_els if el.get_attribute("textContent") in self.tabs_doc_type_dict.keys()
        ]

        return tabs_to_click

    def parse(self, response):
        driver: Chrome = response.meta["driver"]

        for i, doc_type in enumerate(self.tabs_doc_type_dict.values()):
            # must re-grab button ref if page has changed (table paged etc)
            driver.get(self.start_urls[0])    # navigating to the homepage again to reset the page (because refresh doesn't work)
            time.sleep(5)   # waiting to be sure that it loaded
            try:
                button = self.get_tab_button_els(driver)[i]
            except Exception as e:
                print("Error when getting tab button: " + e)
            try:
                ActionChains(driver).move_to_element(button).click(button).perform()
            except Exception as e:
                print("Could not click tab: " + e)

            # self.parse_tab(driver, doc_type)
            try:
                for item in self.parse_tab(driver, doc_type, i):
                    yield item
            except Exception as e:
                print("error when getting items: " + e)

    def get_next_page_anchor(self, driver):
        els = driver.find_elements_by_css_selector(
            'table.PagingTable tr td:nth-child(2) a')

        try:
            next_button_el = next(
                iter([el for el in els if el.text == 'Next']))

            return next_button_el
        except Exception as e:
            raise NoSuchElementException

    def parse_tab(self, driver: Chrome, doc_type, index):
        has_next_page = True
        page_num = 1

        while(has_next_page):
            try:
                next_page_el = self.get_next_page_anchor(driver)

            except NoSuchElementException as e:
                # expected when on last page, set exit condition then parse table
                has_next_page = False

            try:
                for item in self.parse_table(driver, doc_type, index):
                    yield item

            except Exception:
                raise NoSuchElementException(
                    f"Failed to find table to scrape from using css selector: {self.table_selector}"
                )
            try:
                if has_next_page:
                    next_page_el.click()
                    page_num += 1
            except Exception as e:
                print("Could not go to next page: " + e)

    def parse_table(self, driver: Chrome, doc_type, index):
        response = Selector(text=driver.page_source)
        rows_selector = f'table#dnn_ctr48257_ViewTabs_rptTabBody_Default_{index}_List_{index}_grdData_{index} tr'
        rows = response.css(rows_selector)

        bumednote_seen = set({})
        dup_change_seen = False
        for row in rows:
            doc_num_raw: str = row.css('td:nth-child(1)::text').get(default='')
            if not doc_num_raw:
                continue

            doc_title_raw = row.css(
                'td:nth-child(2)::text').get(default='')
            publication_date = row.css('td:nth-child(3)::text').get(default='')
            href_raw = row.css('td:nth-child(4) a::attr(href)').get()

            doc_name = None
            doc_num = None
            doc_title = None

            # Changes for each tab
            # BUMEDINST
            if index == 0:
                doc_num_raw = doc_num_raw.split()[0]
            # BUMEDNOTE
            elif index == 1:
                doc_num_raw = doc_num_raw.replace('NOTE ', '')
                # BUMEDNOTE has a lot of duplicate nums with completely different docs
                if doc_num_raw in bumednote_seen:
                    doc_num_raw = f"{doc_num_raw} {doc_title_raw}"

                bumednote_seen.add(doc_num_raw)

            # NAVMED
            elif index == 2:
                doc_num_raw = doc_num_raw.replace('.pdf', '')
                publication_date, doc_title_raw = doc_title_raw, publication_date

                if doc_num_raw[0].isdigit():
                    doc_num_raw = "P-" + doc_num_raw
                    doc_name = "NAVMED " + doc_num_raw
                else:
                    ref_name = "NAVMED P-117"

                    doc_title = self.ascii_clean(doc_title_raw)
                    doc_name = f"{ref_name} {doc_num_raw}"

                    # special case to match old crawler
                    if doc_name == "NAVMED P-117 MANMED CHANGE 126" and not dup_change_seen:
                        dup_change_seen = True
                    elif doc_name == "NAVMED P-117 MANMED CHANGE 126" and dup_change_seen:
                        doc_name = "NAVMED P-117 MANMED CHANGE 126-1"

            if not doc_num:
                doc_num = self.ascii_clean(doc_num_raw)
            if not doc_title:
                doc_title = self.ascii_clean(doc_title_raw)

            version_hash_fields = {
                "item_currency": href_raw,
                "publication_date": publication_date
            }

            if not href_raw:
                continue

            web_url = self.ensure_full_href_url(
                href_raw, self.start_urls[0])

            downloadable_items = [{
                "doc_type": "pdf",
                "web_url": web_url,
                "compression_type": None,
            }]

            if not doc_name:
                doc_name = f"{doc_type} {doc_num}"

            cac_login_required = False
            if doc_title.endswith('*'):
                cac_login_required = True
                doc_title = doc_title[:-1]
                doc_name = doc_name[:-1]

            yield DocItem(
                doc_name=doc_name,
                doc_num=doc_num,
                doc_type=doc_type,
                doc_title=doc_title,
                publication_date=publication_date,
                cac_login_required=cac_login_required,
                downloadable_items=downloadable_items,
                version_hash_raw_data=version_hash_fields
            )
