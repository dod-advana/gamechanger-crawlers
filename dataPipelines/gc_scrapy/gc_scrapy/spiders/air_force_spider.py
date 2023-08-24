# -*- coding: utf-8 -*-
import scrapy
from scrapy import Selector
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver import Chrome
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import re
import time

from dataPipelines.gc_scrapy.gc_scrapy.middleware_utils.selenium_request import SeleniumRequest
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSeleniumSpider import GCSeleniumSpider

from urllib.parse import urljoin, urlparse
from datetime import datetime
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest, get_pub_date

## Universal variables - Regular expression matching
squash_spaces = re.compile(r'\s*[\n\t\r\s+]\s*') # Find redundant spaces in a string
type_pattern_start = re.compile('^[A-Z]+') # Find the first letter in a capital letter string
type_pattern_mid = re.compile('[A-Z]+') # Find capital letter substring


class AirForcePubsSpider(GCSeleniumSpider):
    '''
    Class defines the behavior for crawling and extracting text-based documents from the "Department of the Air Force E-publishing" site. 
    This class inherits the 'GCSeleniumSpider' class from GCSeleniumSpider.py. The GCSeleniumSpider class applies Selenium settings to the standard
    parse method used in Scrapy crawlers in order to return a Selenium response instead of a standard Scrapy response.

    This class and its methods = the air_force_pubs "spider".
    '''

    name = 'air_force_pubs' # Crawler name

    allowed_domains = ['e-publishing.af.mil'] # Domains the spider is allowed to crawl
    start_urls = [
        'https://www.e-publishing.af.mil/Product-Index/#/?view=cat&catID=1', # AIR FORCE
        'https://www.e-publishing.af.mil/Product-Index/#/?view=cat&catID=16', # AIR NATIONAL GUARD
        'https://www.e-publishing.af.mil/Product-Index/#/?view=cat&catID=20', # UNITED STATES SPACE FORCE
        'https://www.e-publishing.af.mil/Product-Index/#/?view=cat&catID=2', # MAJOR COMMANDS
        'https://www.e-publishing.af.mil/Product-Index/#/?view=cat&catID=18', # LEAD COMMANDS
        'https://www.e-publishing.af.mil/Product-Index/#/?view=cat&catID=3', # DRUs
        'https://www.e-publishing.af.mil/Product-Index/#/?view=cat&catID=4', # FOAs
        'https://www.e-publishing.af.mil/Product-Index/#/?view=cat&catID=5', # NUMBERED AIR FORCES
        'https://www.e-publishing.af.mil/Product-Index/#/?view=cat&catID=7' # UNITS

    ] # URL where the spider begins crawling

    file_type = "pdf" # Define filetype for the spider to identify.

    cac_required_options = ['physical.pdf', 'PKI certificate required', 'placeholder', 'FOUO', 
                            'for_official_use_only'] # Possible values in raw URLs or titles for documents that would indicate 
                                                     #  that a CAC is required to view a document

    item_count_dropdown_selector = 'label select[name="data_length"]' # Count of a given dropdown's selection options
    table_selector = "table.epubs-table.dataTable.no-footer.dtr-inline" # Define CSS selector for tables
    
    def select_dropdown(self, driver):
        dropdown = WebDriverWait(driver, 5).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, self.item_count_dropdown_selector)))
        
        Select(dropdown).select_by_value("100")

    def parse(self, response):
        '''
        This function finds the the "Product Index" table at the end of each of the "dropdown" (or element tree) pathways.
        The parse_table function is called to get documents.
        '''
        driver: Chrome = response.meta["driver"] # Assign Chrome as the WebDriver instance to perform "user" actions  ##(**What is .meta['driver']?)
        
        for page_url in self.start_urls:
            driver.get(page_url)
            time.sleep(5)
            
            init_webpage = Selector(text=driver.page_source)
            
            cat_id_raw = re.search('(catID=\d*)', page_url, re.IGNORECASE) # Find Category ID from URL
            cat_id = str(cat_id_raw.group(0)).replace("ID=", "-").lower()
            
            organizations = init_webpage.css(f'#{cat_id} > div > ul > li a::text').getall() # List of organizations in specified category
            
            # if page_url.endswith('catID=2'):  # Optional condition to pull AF Reserve Command docs from Major Commands section
            #     organizations = ['Air Force Reserve Command'] 
            
            for org in organizations:                
                driver.execute_script("arguments[0].click();", WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.LINK_TEXT, org))))
                
                try:
                    all_pubs = WebDriverWait(driver, 5).until(
                        EC.visibility_of_element_located((By.LINK_TEXT, '00   ALL PUBLICATIONS')))
    
                except:
                    driver.back()
                    print(f"Failed to find publications link for: {org} at {page_url}")
                    continue
                
                all_pubs.click()
 
                anchor_after_current_selector = "div.dataTables_paginate.paging_simple_numbers a.paginate_button.current + a" # Next page button element
                
                self.select_dropdown(driver)
                
                for item in self.parse_table(driver):
                    yield item
                            
                last_page_raw = driver.find_element(By.CSS_SELECTOR, '#data_paginate > span > a:last-child')
                last_page = int(last_page_raw.text)
                
                while last_page > 1:
                    driver.execute_script("arguments[0].click();", WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, anchor_after_current_selector))))
                    for item in self.parse_table(driver):
                        yield item
                    last_page -= 1
                
                driver.get(page_url)
                time.sleep(5)
                

    def parse_table(self, driver):
        '''
        This function generates a link and metadata for each document in the "Product Index" table on the Air Force E-Publishing 
        site for download.
        '''
        webpage = Selector(text=driver.page_source) # Raw HTML of webpage
        row_selector = f'{self.table_selector} tbody tr ' # Define list of table rows

        ## Iterate through each row in table get column values as metadata for each downloadable document
        for row in webpage.css(row_selector):
            product_number_raw = row.xpath('td//text()')[0].extract()
            ## If the table contains no entries then skip
            if product_number_raw == "No data available in table":
                continue
            web_url = row.css(
                f'td:nth-child(1) a::attr(href)').get(default='')
            title_raw = row.css(
                f'td:nth-child(2) a::text').get(default='')
            publish_date_raw = row.css(
                f'td:nth-child(3) span::text').get(default='')
            certification_date_raw = row.css(
                f'td:nth-child(4) span::text').get(default='')
            last_action_raw = row.css(
                f'td:nth-child(5)::text').get(default='')

            prod_num = squash_spaces.sub(" ", product_number_raw).strip() # Clean superfluous spaces in raw value

            ## Assign document metadata by utilizing file naming convention, acconting for inconsistencies
            if prod_num.find('CFETP') != -1:
                doc_type = 'CFETP'
                doc_num = re.sub(doc_type, '', prod_num)
                doc_name = ' '.join((doc_type, doc_num))
            elif prod_num == '2T0X1_F-35_AFJQS':
                doc_type = 'AFJQS'
                doc_num = '2T0X1_F-35'
                doc_name = ' '.join((doc_type, doc_num))
            elif prod_num == 'AFHandbook1':
                doc_type = 'AFH'
                doc_num = '1'
                doc_name = ' '.join((doc_type, doc_num))
            elif prod_num == 'BOWFUSF':
                doc_type = 'AF MISC'
                doc_name = 'BOWFUSF'
            elif prod_num == 'MCMUS':
                doc_type = 'AF MISC'
                doc_name = 'MCMUS'
            elif prod_num.endswith('SMALL'):
                prod_num_new = re.sub('SMALL', '', prod_num)
                doc_type = type_pattern_start.findall(prod_num_new)[0]
                doc_num = re.sub(doc_type, '', prod_num_new)
                doc_name = ' '.join((doc_type, doc_num))
            elif 'DOD' in prod_num.upper() or 'DESR' in prod_num.upper():
                prod_num_new = prod_num.split('.')[-1]
                prod_num_new = prod_num_new.split('_')[-1]
                type_extract = type_pattern_mid.findall(prod_num_new)
                doc_type = type_extract[0] if type_extract else type_pattern_start.findall(prod_num)[
                    0]
                doc_num = re.sub(doc_type, '', prod_num_new) if type_extract else re.sub(
                    doc_type, '', prod_num)
                doc_name = ' '.join((doc_type, doc_num))
            elif not type_pattern_start.findall(prod_num):
                doc_type = "DAFGM"
                doc_num = re.sub(doc_type, '', prod_num)
                doc_name = ' '.join((doc_type, doc_num))
            else:
                doc_type = type_pattern_start.findall(prod_num)[0]
                # doc_type = 'AF '+doc_type if doc_type in ['HOI', 'QTP'] else doc_type     ##(**Why hashed out?)
                doc_num = re.sub(doc_type, '', prod_num)
                doc_name = ' '.join((doc_type, doc_num))

            if doc_name == 'MCMUS': # Skip over MCMUS doc with broken download link
                continue
            
            ## Clean raw metadata values
            doc_title = squash_spaces.sub(' ', title_raw).strip() # Clean any superfluous spaces in raw value

            publication_date = squash_spaces.sub(' ', publish_date_raw).strip() # Clean any superfluous spaces in raw value
            publication_date = publication_date.split(' ')[0] # Get only year-month-day from published_date_raw value
            publication_date = datetime.strptime(
                publication_date, '%Y%m%d').strftime('%Y-%m-%d')

            # CERTIFICATION DATE
            cert_date = squash_spaces.sub(' ', certification_date_raw).strip() # Clean any superfluous spaces in raw value
            cert_date = cert_date.split(' ')[0] # Get only year-month-day from certfication_date_raw value
            cert_date = datetime.strptime(cert_date, '%Y%m%d').strftime('%Y-%m-%d')

            # LAST DOCUMENT ACTION
            last_action = squash_spaces.sub(' ', last_action_raw).strip() # Clean any superfluous spaces in raw value

            ## Set boolean if CAC is required to view document
            cac_login_required = True if any(x in web_url for x in self.cac_required_options) \
                or any(x in doc_title for x in self.cac_required_options) \
                or '-S' in prod_num else False

            source_page_url = driver.current_url

            fields = {
                'doc_name': doc_name,
                'doc_num': doc_num,
                'doc_title': re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_title),
                'doc_type': doc_type,
                'cac_login_required': cac_login_required,
                'download_url': web_url,
                'source_page_url': source_page_url,
                'publication_date': publication_date
            }

            ## Instantiate DocItem class and assign document's metadata values
            doc_item = self.populate_doc_item(fields)
            yield doc_item


    def populate_doc_item(self, fields):
        '''
        This functions provides both hardcoded and computed values for the variables
        in the imported DocItem object and returns the populated metadata object
        '''
        display_org = "Dept. of the Air Force" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "Dept. of the Air Force E-Publishing" # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Unlisted Source" # Level 3 filter

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
                "doc_type": self.file_type,
                "download_url": download_url,
                "compression_type": None
            }]

        ## Assign fields that will be used for versioning
        version_hash_fields = {
            "doc_name":doc_name,
            "doc_num": doc_num,
            "publication_date": publication_date,
            "download_url": download_url.split('/')[-1],
            "display_title": display_title
        }
        file_ext = downloadable_items[0]["doc_type"]
        version_hash = dict_to_sha256_hex_digest(version_hash_fields)

        return DocItem(
                    doc_name = doc_name,
                    doc_title = doc_title,
                    doc_num = doc_num,
                    doc_type = doc_type,
                    display_doc_type = display_doc_type, #
                    publication_date = publication_date,
                    cac_login_required = cac_login_required,
                    crawler_used = self.name,
                    downloadable_items = downloadable_items,
                    source_page_url = source_page_url, #
                    source_fqdn = source_fqdn, #
                    download_url = download_url, #
                    version_hash_raw_data = version_hash_fields, #
                    version_hash = version_hash,
                    display_org = display_org, #
                    data_source = data_source, #
                    source_title = source_title, #
                    display_source = display_source, #
                    display_title = display_title, #
                    file_ext = file_ext, #
                    is_revoked = is_revoked, #

                )