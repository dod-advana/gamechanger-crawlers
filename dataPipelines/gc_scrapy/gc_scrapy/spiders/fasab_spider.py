import scrapy
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
import datetime
import time
from dataPipelines.gc_scrapy.gc_scrapy.utils import abs_url
import re
from urllib.parse import urljoin, urlparse
from datetime import datetime
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest


class BrickSetSpider(GCSpider):
    name = 'FASAB Crawler' # Crawler name
    display_org = "Uncategorized" # Level 1: value TBD for this crawler
    data_source = "Unlisted Source" # Level 2: value TBD for this crawler
    source_title = "Unlisted Source" # Level 3 filter
    
    allowed_domains = ['fasab.gov']
    file_type = "pdf"
    start_urls = ['https://fasab.gov/accounting-standards/document-by-chapter/']
    start_url = 'https://fasab.gov/accounting-standards/document-by-chapter/'
    rotate_user_agent = True

    def parse(self, response):
        SET_SELECTOR = 'li'
        for brickset in response.css(SET_SELECTOR):

            NAME_SELECTOR = 'a ::text'
            URL_SELECTOR = 'a::attr(href)'
            TITLE_SELECTOR = 'ul li::text'
            doc_name = brickset.css(NAME_SELECTOR).extract_first()
            url = brickset.css(URL_SELECTOR).extract_first()
            doc_title = brickset.css(TITLE_SELECTOR).extract_first()
            if doc_title is None:
                continue
            if doc_name is None:
                continue
            if url is None:
                continue
            if "SFFAS" not in str(doc_name) and "SFFAC" not in str(doc_name):
                doc_name = "FASAB " + str(doc_name)
            doc_num = doc_name.rsplit(' ', 1)[-1]
            doc_type = doc_name.rsplit(' ', 1)[0]
            
            if not url.startswith("http"):
                url = "https:" + url

            doc_item = self.populate_doc_item(re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_name), re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_type), 
                                                re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_num), re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_title), 
                                                url, "")
    
            yield doc_item
        


    def populate_doc_item(self, doc_name, doc_type, doc_num, doc_title, web_url, publication_date):
        '''
        This functions provides both hardcoded and computed values for the variables
        in the imported DocItem object and returns the populated metadata object
        '''
        display_org = "FASAB"  # Level 1: value TBD for this crawler
        data_source = "Federal Accounting Standards Advisory Board"  # Level 2: value TBD for this crawler
        source_title = "Handbook of Accounting Standards"  # Level 3 filter

        cac_login_required = False

        display_doc_type = "Document"  # Doc type for display on app
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + ": " + doc_title
        is_revoked = False
        source_page_url = self.start_urls[0]
        source_fqdn = urlparse(source_page_url).netloc

        downloadable_items = [
            {
                "doc_type": "pdf",
                "download_url": web_url,
                "compression_type": None,
            }
        ]

        ## Assign fields that will be used for versioning
        version_hash_fields = {
            "display_org": display_org,
            "doc_name": doc_name,
            "doc_num": doc_num,
            "publication_date": publication_date,
            "download_url": web_url.split('/')[-1]
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
                    download_url = web_url, #
                    version_hash_raw_data = version_hash_fields, #
                    version_hash = version_hash,
                    display_org = display_org, #
                    data_source = data_source, #
                    source_title = source_title, #
                    display_source = display_source, #
                    display_title = display_title, #
                    file_ext = "pdf", #
                    is_revoked = is_revoked, #
                )


