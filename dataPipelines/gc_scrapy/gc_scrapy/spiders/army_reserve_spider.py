import scrapy
import re
from urllib.parse import urljoin, urlparse, urlencode, parse_qs
from datetime import datetime
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest
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

    name = "Army_Reserve" # Crawler name
    display_org = "Dept. of the Army" # Level 1: GC app 'Source' filter for docs from this crawler
    data_source = "Army Publishing Directorate" # Level 2: GC app 'Source' metadata field for docs from this crawler
    source_title = "Unlisted Source" # Level 3 filter

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
            # Join relative urls to base
            web_url = urljoin(self.start_urls[0], pdf_url) if pdf_url.startswith(
                '/') else pdf_url
            # Encode spaces from pdf names
            web_url = web_url.replace(" ", "%20") # Add document to base url, encoding spaces (with %20)

            cac_login_required = True if "usar.dod.afpims.mil" in web_url else False # Determine if CAC is required from url

            doc_name_raw = "".join(item.css('strong::text').getall()) # Bolded portion of displayed document name as doc_name_raw
            doc_title_raw = item.css('a::text').get() # Unbolded portion of displayed document name as doc_title_raw
            
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


            ## Instantiate DocItem class and assign document's metadata values
            doc_item = self.populate_doc_item(doc_name, doc_type, doc_num, doc_title, web_url, cac_login_required)
       
            yield doc_item
        


    def populate_doc_item(self, doc_name, doc_type, doc_num, doc_title, web_url, cac_login_required):
        '''
        This functions provides both hardcoded and computed values for the variables
        in the imported DocItem object and returns the populated metadata object
        '''
        display_org = "Dept. of the Army" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "Army Publishing Directorate" # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Unlisted Source" # Level 3 filter

        display_doc_type = "Document" # Doc type for display on app
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + " " + doc_title
        is_revoked = False
        access_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f") # T added as delimiter between date and time
        source_page_url = self.start_urls[0]
        source_fqdn = urlparse(source_page_url).netloc

        downloadable_items = [
                {
                    "doc_type": self.file_type,
                    "download_url": web_url,
                    "compression_type": None
                }
            ] # Get document metadata

        ## Assign fields that will be used for versioning
        version_hash_fields = {
            "doc_name":doc_name,
            "doc_num": doc_num,
            #"publication_date": publication_date,
            "download_url": web_url.split('/')[-1]
        }

        version_hash = dict_to_sha256_hex_digest(version_hash_fields)

        return DocItem(
                    doc_name = doc_name,
                    doc_title = doc_title,
                    doc_num = doc_num,
                    doc_type = doc_type,
                    display_doc_type_s = display_doc_type, #
                    #publication_date_dt = publication_date,
                    cac_login_required_b = cac_login_required,
                    crawler_used_s = self.name,
                    downloadable_items = downloadable_items,
                    source_page_url_s = source_page_url, #
                    source_fqdn_s = source_fqdn, #
                    download_url_s = web_url, #
                    version_hash_raw_data = version_hash_fields, #
                    version_hash_s = version_hash,
                    display_org_s = display_org, #
                    data_source_s = data_source, #
                    source_title_s = source_title, #
                    display_source_s = display_source, #
                    display_title_s = display_title, #
                    file_ext_s = doc_type, #
                    is_revoked_b = is_revoked, #
                    access_timestamp_dt = access_timestamp #
                )
