from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
import re

from urllib.parse import urljoin, urlparse
from datetime import datetime
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest, get_pub_date

class StigSpider(GCSpider):
    name = "stig_pubs" # Crawler name
    

    start_urls = [
        "https://public.cyber.mil/stigs/downloads/"
    ]

    download_base_url = 'https://public.cyber.mil/'
    rotate_user_agent = True

    doc_type = "STIG"

    @staticmethod
    def extract_doc_number(doc_title):
        if doc_title.find(" Ver ") != -1:
            ver_num = (re.findall(r' Ver (\w+)', doc_title))[0]
        else:
            if " Version " in doc_title:
                ver_num = (re.findall(r' Version (\w+)', doc_title))[0]
            else:
                ver_num = 0

        if doc_title.find(" Rel ") != -1:
            ref_num = (re.findall(r' Rel (\w+)', doc_title))[0]
        else:
            if "Release Memo" in doc_title:
                ref_num = 1
            else:
                ref_num = 0

        doc_num = "V{}R{}".format(ver_num, ref_num)
        return doc_title, doc_num

    def parse(self, response):
        rows = response.css('table tbody tr')
        rows = [a for a in rows if a.css('a::attr(href)').get()]
        rows = [a for a in rows if a.css('a::attr(href)').get().endswith("pdf")]

        for row in rows:
            href_raw = row.css('a::attr(href)').get()
            doc_title_text, publication_date_raw = row.css('span[style="display:none;"] ::text').getall()
            doc_title = self.ascii_clean(doc_title_text).replace("/ ", " ").replace("/", " ")
            publication_date = self.ascii_clean(publication_date_raw)
            doc_title, doc_num = StigSpider.extract_doc_number(doc_title)
            doc_name = f"{self.doc_type} {doc_num} {doc_title}"

            if "Memo" in doc_title:
                display_doc_type = "Memo"
            else:
                display_doc_type = "STIG"

            file_type = self.get_href_file_extension(href_raw)
            web_url = self.ensure_full_href_url(
                href_raw, self.download_base_url)

            fields = {
                'doc_name': doc_name,
                'doc_num': doc_num,
                'doc_title': doc_title,
                'doc_type': self.doc_type,
                'display_doc_type':display_doc_type,
                'file_type':file_type,
                'cac_login_required': False,
                'download_url': web_url,
                'source_page_url':response.url,
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
        display_org = "Security Technical Implementation Guides" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "Security Technical Implementation Guides" # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Unlisted Source" # Level 3 filter

        doc_name = fields['doc_name']
        doc_num = fields['doc_num']
        doc_title = fields['doc_title']
        doc_type = fields['doc_type']
        cac_login_required = fields['cac_login_required']
        download_url = fields['download_url']
        publication_date = get_pub_date(fields['publication_date'])

        display_doc_type = fields['display_doc_type'] # Doc type for display on app
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + ": " + doc_title
        is_revoked = False
        source_page_url = fields['source_page_url']
        source_fqdn = urlparse(source_page_url).netloc

        downloadable_items = [{
                "doc_type": fields['file_type'],
                "download_url": download_url.replace(' ', '%20'),
                "compression_type": None,
            }]

        ## Assign fields that will be used for versioning
        version_hash_fields = {
            "doc_name":doc_name,
            "doc_num": doc_num,
            "publication_date": publication_date,
            "download_url": download_url
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
                    file_ext = fields['file_type'], #
                    is_revoked = is_revoked, #
                )