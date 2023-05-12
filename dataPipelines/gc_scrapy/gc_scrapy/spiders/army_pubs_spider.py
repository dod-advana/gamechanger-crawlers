import scrapy
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
import time
from dataPipelines.gc_scrapy.gc_scrapy.utils import abs_url
from selenium import webdriver

from urllib.parse import urljoin, urlparse
from datetime import datetime
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest, get_pub_date

class ArmySpider(GCSpider):
    '''
    Class defines the behavior for crawling and extracting text-based documents from the "Army Publishing Directorate" site.
    This class inherits the 'GCSpider' class from GCSpider.py. The GCSpider class is Gamechanger's implementation of the standard
    parse method used in Scrapy crawlers in order to return a response.
    
    The "class" and its methods = the army_pubs "spider".
    '''

    name = "army_pubs" # Crawler name
    

    allowed_domains = ['armypubs.army.mil'] # Domains the spider is allowed to crawl
    start_urls = [
        'https://armypubs.army.mil/'
    ] # URL where the spider begins crawling

    base_url = 'https://armypubs.army.mil' # Landing page/ base URL
    pub_url = base_url + '/ProductMaps/PubForm/' # Add extension to landing page base URL to get base URL for document links
    rotate_user_agent = True

    file_type = "pdf" # Define filetype for the spider to download

    def parse(self, response):
        '''
        This function compiles relevant document links.
        '''
        do_not_process = ["/ProductMaps/PubForm/PB.aspx",
                          "/Publications/Administrative/POG/AllPogs.aspx"] # URL stop list

        all_hrefs = response.css(
            'li.usa-nav__primary-item')[2].css('a::attr(href)').getall() # Get all hyperlinks on page

        links = [link for link in all_hrefs if link not in do_not_process] # Remove items in URL stop list from hyperlinks list

        yield from response.follow_all(links, self.parse_source_page) # Follow each link and call parse_source_page function for each

    def parse_source_page(self, response):
        '''
        This function grabs links from the raw html for the table on page, calling the parse_detail_page function for the 
        list of table links.
        '''
        table_links = response.css('table td a::attr(href)').extract() # Extract all links in the html table
        yield from response.follow_all([self.pub_url+link for link in table_links], self.parse_detail_page) # Call parse_detail_page function for each link

    def get_cells_from_row(self, row_selector):
        '''
        Takes a tr selector and gets all cell values as strings
        '''
        cells = row_selector.css('td, th')
        return [ ''.join(cell.css('*::text').getall()) for cell in cells]
    
    def parse_detail_page(self, response):
        '''
        This function generates a link and metadata for each document for use by bash download script.
        '''
        import pdb; pdb.set_trace()
        
        driver = webdriver.Chrome()
        button = driver.find_element_by_xpath("//*[contains(text(), ' View More Details  +  ')]")
        button.click()

        rows = response.css('tr') # Get table row for document
        x = list(map(self.get_cells_from_row, rows))
        
        #rows[0].xpath('td//text()').getall()
        doc_name_raw = rows.css('span#MainContent_PubForm_Number::text').get() # Get 'Number' from table as document name
        doc_title = rows.css('span#MainContent_PubForm_Title::text').get() # Get document 'Title' from table
        doc_num_raw = doc_name_raw.split()[-1] # Get numeric portion of document name as doc_num   #### TODO: Sometimes this is Nonetype and causes an error
        doc_type_raw = doc_name_raw.split()[0] # Get alphabetic portion of document name as doc_type
        publication_date = rows.css(
            "span#MainContent_PubForm_Date::text").get() # Get document publication date
        dist_stm = rows.css("span#MainContent_PubForm_Dist_Rest::text").get() # Get document distribution statment (re: doc accessibility)
        proponent = self.ascii_clean(rows.css(
            "span#MainContent_PubForm_Proponent::text").get(default="")) # Get document "Proponent"
        if dist_stm and (dist_stm.startswith("A") or dist_stm.startswith("N")):
            cac_login_required = False # The distribution statement is either "A" or "Not Applicable", i.e. anyone can access
        else:
            cac_login_required = True # The distribution statement has more restrictions

        linked_items = rows.css("div#MainContent_uoicontainer a") # Get document link in row
        downloadable_items = []

        if not linked_items: # Apply generic metadata if no document link
            filetype = rows.css("div#MainContent_uoicontainer::text").get() ##(**does this assign 'html' as value?)
            if filetype:
                di = {
                    "doc_type": filetype.strip().lower(),
                    "download_url": self.base_url, # 'Army Publishing Directorate' base URL as web_url for item
                    "compression_type": None
                }
                downloadable_items.append(di)
            else:
                return
        else:
            for item in linked_items: # Get document-specific metadata
                di = {
                    "doc_type": item.css("::text").get().strip().lower(),
                    "download_url": abs_url(self.base_url, item.css("::attr(href)").get()).replace(' ', '%20'),
                    "compression_type": None
                }
                downloadable_items.append(di)
        source_page_url=response.url
        fields = {
                'doc_name': self.ascii_clean(doc_name_raw),
                'doc_num': self.ascii_clean(doc_num_raw),
                'doc_title': self.ascii_clean(doc_title),
                'doc_type': self.ascii_clean(doc_type_raw),
                'cac_login_required': cac_login_required,
                'download_url': downloadable_items[0]['download_url'],
                'publication_date': self.ascii_clean(publication_date),
                'downloadable_items': downloadable_items,
                'source_page_url': source_page_url
            }
        ## Instantiate DocItem class and assign document's metadata values
        doc_item = self.populate_doc_item(fields)
        yield doc_item


    def populate_doc_item(self, fields):
        '''
        This functions provides both hardcoded and computed values for the variables
        in the imported DocItem object and returns the populated metadata object
        '''
        display_org = "Dept. of the Army" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "Army Publishing Directorate" # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Unlisted Source" # Level 3 filter
        
        doc_name = fields['doc_name']
        doc_num = fields['doc_num']
        doc_title = fields['doc_title']
        doc_type = fields['doc_type']
        cac_login_required = fields['cac_login_required']
        download_url = fields['download_url']
        publication_date = get_pub_date(fields['publication_date'])
        downloadable_items = fields['downloadable_items']
        file_ext = downloadable_items[0]['doc_type']

        display_doc_type = "Document" # Doc type for display on app
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + ": " + doc_title
        is_revoked = False
        source_page_url = fields['source_page_url']
        source_fqdn = urlparse(source_page_url).netloc

        ## Assign fields that will be used for versioning
        version_hash_fields = {
            "doc_name":doc_name,
            "doc_num": doc_num,
            "publication_date": publication_date,
            "download_url": download_url,
            "display_title": display_title
        }

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