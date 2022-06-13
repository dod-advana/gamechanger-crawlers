import scrapy
import re
from urllib.parse import urljoin, urlencode, parse_qs
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider


type_and_num_regex = re.compile(r"([a-zA-Z].*) (\d.*)") # Get 'type' (alphabetic) value and 'num' (numeric) value from 'doc_name' string


class ArmyReserveSpider(GCSpider):
    '''
    Class defines the behavior for crawling and extracting text-based documents from the U.S. Army Reserves "Publications" site.
    This class inherits the 'GCSpider' class from GCSpider.py. The GCSpider class is Gamechanger's implementation of the standard
    parse method used in Scrapy crawlers in order to return a response.

    The "class" and its methods = the army_reserve "spider".
    '''

    name = "Army_Reserve" # Spider name (required variable for Scrapy to locate and instantiate the spider)
    allowed_domains = ['usar.army.mil'] # Domains the spider is allowed to crawl
    start_urls = [
        'https://www.usar.army.mil/Publications/'
    ] # URL where the spider begins crawling

    file_type = "pdf" # Define filetype for the spider to download
    cac_login_required = False # Assume document is accessible without CAC

    @staticmethod
    def clean(text):
        '''
        This function forces text into the ASCII characters set, ignoring errors
        '''
        return text.encode('ascii', 'ignore').decode('ascii').strip()

    def parse(self, response):
        '''
        This function generates a link and metadata for each document found on the Army Reserves Publishing
        site for use by bash download script.
        '''
        selected_items = response.css(
            "div.DnnModule.DnnModule-ICGModulesExpandableTextHtml div.Normal > div p") # Get expandable section (each exposes doc links on webpage when selected)
        for item in selected_items: # Iterate over each link in the section
            pdf_url = item.css('a::attr(href)').get() # Get link url
            if pdf_url is None: # Fail-safe
                continue
            # join relative urls to base
            web_url = urljoin(self.start_urls[0], pdf_url) if pdf_url.startswith(
                '/') else pdf_url
            # encode spaces from pdf names
            web_url = web_url.replace(" ", "%20") # Add document to base url, encoding spaces (with %20)

            cac_login_required = True if "usar.dod.afpims.mil" in web_url else False # Determine if CAC is required from url

            downloadable_items = [
                {
                    "doc_type": self.file_type,
                    "web_url": web_url,
                    "compression_type": None
                }
            ] # Get document metadata
            doc_name_raw = "".join(item.css('strong::text').getall()) # Bolded portion of displayed document name as doc_name_raw
            doc_title_raw = item.css('a::text').get() # Unbolded portion of displayed document name as doc_title_raw
            # some are nested in span
            if doc_title_raw is None:
                doc_title_raw = item.css('a span::text').get() # If no unbolded text, some have title nested in span
                if doc_title_raw is None:
                    doc_title_raw = doc_name_raw # Some only have the bolded name, e.g. FY20 USAR IDT TRP Policy Update

            doc_name = self.clean(doc_name_raw) # ASCII fail-safe
            doc_title = self.clean(doc_title_raw) # ASCII fail-safe

            type_and_num_groups = re.search(type_and_num_regex, doc_name) # Get doc_type and doc_num (using naming convention) from 'doc_name'
            if type_and_num_groups is not None: 
                doc_type = type_and_num_groups[1] # Assign value to 'doc_type'
                doc_num = type_and_num_groups[2] # Assign value to 'doc_num'
            else: # Apply default values if doc type and num are not decipherable from 'doc_name'
                doc_type = "USAR Doc"
                doc_num = ""

            version_hash_fields = {
                "item_currency": web_url.split('/')[-1], # Item currency found on pdf link
                "document_title": doc_title,
                "document_number": doc_num
            } # Add version hash metadata

            ## Instantiate DocItem class and assign document's metadata values
            yield DocItem(
                doc_name=doc_name,
                doc_title=doc_title,
                doc_num=doc_num,
                doc_type=doc_type,
                cac_login_required=cac_login_required,
                downloadable_items=downloadable_items,
                version_hash_raw_data=version_hash_fields,
            )