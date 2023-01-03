from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
import json
import scrapy

from urllib.parse import urljoin, urlparse
from datetime import datetime
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest, get_pub_date

class SornSpider(GCSpider):
    name = "SORN" # Crawler name
    
    
    start_urls = [
        "https://www.federalregister.gov/api/v1/agencies/defense-department"
    ]

    rotate_user_agent = True
    doc_type = "SORN"
    display_source = "Federal Registry"

    def parse(self, response):
        data = json.loads(response.body)
        agency_list_pull = data['child_slugs']
        conditions = ""
        for item in agency_list_pull:
            conditions = conditions + "&conditions[agencies][]=" + item
        notices = "&conditions[type][]=NOTICE"
        page_size = "1000"
        base_url = "https://www.federalregister.gov/api/v1/documents.json?per_page=" + page_size + \
            "&order=newest&conditions[term]=%22Privacy%20Act%20of%201974%22%20%7C%20%22System%20of%20Records%22"
        next_url = base_url+conditions+notices
        yield scrapy.Request(url=next_url, callback=self.parse_data)

    def parse_data(self, response):
        response_json = json.loads(response.body)
        sorns_list = response_json['results']

        for sorn in sorns_list:

            fields = {
                'doc_name': "SORN " + sorn["document_number"],
                'doc_num': sorn["document_number"],
                'doc_title': sorn["title"],
                'doc_type': "SORN",
                'display_doc_type':"Notice",
                'cac_login_required': False,
                'download_url': sorn["pdf_url"],
                'source_page_url':sorn["html_url"],
                'publication_date': sorn["publication_date"]
            }
            ## Instantiate DocItem class and assign document's metadata values
            doc_item = self.populate_doc_item(fields)
        
            yield doc_item

        if 'next_page_url' in response_json:
            yield scrapy.Request(url=response_json['next_page_url'], callback=self.parse_data)



        


    def populate_doc_item(self, fields):
        '''
        This functions provides both hardcoded and computed values for the variables
        in the imported DocItem object and returns the populated metadata object
        '''
        display_org = "Dept. of Defense" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "Federal Register" # Level 2: value TBD for this crawler
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
        file_ext = "pdf"
        downloadable_items = [{
                "doc_type": file_ext,
                "download_url": download_url,
                "compression_type": None,
            }]
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
                    display_doc_type = display_doc_type,
                    publication_date = publication_date,
                    cac_login_required = cac_login_required,
                    crawler_used = self.name,
                    downloadable_items = downloadable_items,
                    source_page_url = source_page_url,
                    source_fqdn = source_fqdn,
                    download_url = download_url,
                    version_hash_raw_data = version_hash_fields,
                    version_hash = version_hash,
                    display_org = display_org,
                    data_source = data_source,
                    source_title = source_title,
                    display_source = display_source,
                    display_title = display_title,
                    file_ext = file_ext,
                    is_revoked = is_revoked,
                )
