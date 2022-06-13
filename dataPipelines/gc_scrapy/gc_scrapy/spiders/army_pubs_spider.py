import scrapy
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
import time
from dataPipelines.gc_scrapy.gc_scrapy.utils import abs_url


class ArmySpider(GCSpider):
    '''
    Class defines the behavior for crawling and extracting text-based documents from the "Army Publishing Directorate" site.
    This class inherits the 'GCSpider' class from GCSpider.py. The GCSpider class is Gamechanger's implementation of the standard
    parse method used in Scrapy crawlers in order to return a response.
    
    The "class" and its methods = the army_pubs "spider".
    '''

    name = "army_pubs" # Spider name (required variable for Scrapy to locate and instantiate the spider)
    allowed_domains = ["armypubs.army.mil"] # Domains the spider is allowed to crawl
    start_urls = [
        "https://armypubs.army.mil/"
    ] # URL where the spider begins crawling

    file_type = "pdf" # Define filetype for the spider to download

    base_url = "https://armypubs.army.mil" # Landing page/ base URL
    pub_url = base_url + '/ProductMaps/PubForm/' # Add extension to landing page base URL to get base URL for document links

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

    def parse_detail_page(self, response):
        '''
        This function generates a link and metadata for each document for use by bash download script.
        '''
        rows = response.css('tr') # Get table row for document
        doc_name_raw = rows.css('span#MainContent_PubForm_Number::text').get() # Get 'Number' from table as document name
        doc_title = rows.css('span#MainContent_PubForm_Title::text').get() # Get document 'Title' from table
        doc_num_raw = doc_name_raw.split()[-1] # Get numeric portion of document name as doc_num
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
                    "web_url": self.base_url, # 'Army Publishing Directorate' base URL as web_url for item
                    "compression_type": None
                }
                downloadable_items.append(di) 
            else:
                return
        else:
            for item in linked_items: # Get document-specific metadata
                di = {
                    "doc_type": item.css("::text").get().strip().lower(),
                    "web_url": abs_url(self.base_url, item.css("::attr(href)").get()).replace(' ', '%20'),
                    "compression_type": None
                }
                downloadable_items.append(di)
        version_hash_fields = {
            "pub_date": publication_date,
            "pub_pin": rows.css("span#MainContent_PubForm_PIN::text").get(),
            "pub_status": rows.css("span#MainContent_PubForm_Status::text").get(),
            "product_status": rows.css("span#MainContent_Product_Status::text").get(),
            "replaced_info": rows.css("span#MainContent_PubForm_Superseded::text").get()
        } # Add version hash metadata

        ## Instantiate DocItem class and assign document's metadata values
        yield DocItem(
            doc_name=self.ascii_clean(doc_name_raw),
            doc_title=self.ascii_clean(doc_title),
            doc_num=self.ascii_clean(doc_num_raw),
            doc_type=self.ascii_clean(doc_type_raw),
            source_page_url=response.url,
            publication_date=self.ascii_clean(publication_date),
            cac_login_required=cac_login_required,
            downloadable_items=downloadable_items,
            version_hash_raw_data=version_hash_fields,
            office_primary_resp=proponent
        )