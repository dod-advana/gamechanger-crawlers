from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
import json
import re
import scrapy

from urllib.parse import urljoin, urlparse
from datetime import datetime
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest, get_pub_date

bill_version_re = re.compile(r'\((.*)\)')


class CFRSpider(GCSpider):
    name = "code_of_federal_regulations" # Crawler name
    
    rotate_user_agent = True
    visible_start = "https://www.govinfo.gov/app/collection/cfr"
    start_urls = [
        "https://www.govinfo.gov/wssearch/rb/cfr?fetchChildrenOnly=0"
    ]

    headers = {
        "accept": "application/json",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "no-cache",
        "content-type": "application/json",
        "pragma": "no-cache",
        "sec-ch-ua": "\" Not;A Brand\";v=\"99\", \"Google Chrome\";v=\"91\", \"Chromium\";v=\"91\"",
        "sec-ch-ua-mobile": "?0",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "x-requested-with": "XMLHttpRequest"
    }

    def start_requests(self):
        yield scrapy.Request(url=self.start_urls[0], method='GET', headers=self.headers)

    @staticmethod
    def get_visible_detail_url(package_id: str) -> str:
        return f"https://www.govinfo.gov/app/details/{package_id}"

    @staticmethod
    def get_pdf_file_download_url_from_id(package_id: str) -> str:
        return f"https://www.govinfo.gov/content/pkg/{package_id}/pdf/{package_id}.pdf"

    def parse(self, start_url_response):
        data = json.loads(start_url_response.body)
        years = [
            node.get('nodeValue').get('browsePath')for node in data.get('childNodes', [])
        ]

        for year in years:
            year_url = f"https://www.govinfo.gov/wssearch/rb//cfr/{year}/?fetchChildrenOnly=1"
            yield start_url_response.follow(url=year_url, callback=self.handle_title_nums, headers=self.headers)

    def handle_title_nums(self, year_response):
        data = json.loads(year_response.body)
        cnodes = data.get('childNodes', [])
        title_num_nodes = [cnode['nodeValue'] for cnode in cnodes]

        for title_num_dict in title_num_nodes:

            if title_num_dict.get('volumes'):
                for vol in title_num_dict.get('volumes'):
                    package_id = vol.get('packageid')
                    vol_num = vol.get('volume')

                    vol_data = title_num_dict.copy()
                    vol_data.update({
                        "packageid": package_id,
                        "volume": vol_num
                    })

                    yield self.make_doc_item_from_dict(vol_data)
            else:
                yield self.make_doc_item_from_dict(title_num_dict)

    def make_doc_item_from_dict(self, data):
        publication_date = data.get('publishdate')
        title = data.get('title')
        title_num = data.get('cfrtitlenumber', "")
        package_id = data.get('packageid')
        vol_num = data.get('volume')

        source_page_url = self.get_visible_detail_url(package_id)

        is_index_type = "GPO-CFR-INDEX" in package_id

        doc_type = "CFR Index" if is_index_type else 'CFR Title'
        doc_title = title if is_index_type else title.title()
        doc_num = f"{title_num} Vol. {vol_num}" if vol_num else title_num

        web_url = self.get_pdf_file_download_url_from_id(package_id)

        fields = {
                'doc_name': package_id,
                'doc_num': doc_num,
                'doc_title': doc_title,
                'doc_type': doc_type,
                'cac_login_required': False,
                'download_url': web_url,
                'source_page_url': source_page_url,
                'publication_date': publication_date
            }

        ## Instantiate DocItem class and assign document's metadata values
        doc_item = self.populate_doc_item(fields)
    
        return doc_item
        


    def populate_doc_item(self, fields):
        '''
        This functions provides both hardcoded and computed values for the variables
        in the imported DocItem object and returns the populated metadata object
        '''
        display_org = "Congress" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "U.S. Government Publishing Office" # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Unlisted Source" # Level 3 filter

        doc_name = fields['doc_name']
        doc_num = fields['doc_num']
        doc_title = fields['doc_title']
        doc_type = fields['doc_type']
        cac_login_required = fields['cac_login_required']
        download_url = fields['download_url']
        publication_date = get_pub_date(fields['publication_date'])

        display_doc_type = doc_type
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + " " + doc_title
        is_revoked = False
        access_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f") # T added as delimiter between date and time
        source_page_url = fields['source_page_url']
        source_fqdn = urlparse(source_page_url).netloc

        downloadable_items = [
            {
                "doc_type": "pdf",
                "download_url": download_url,
                "compression_type": None,
            }
        ]
        file_ext = downloadable_items[0]["doc_type"]
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
                    file_ext = file_ext, #
                    is_revoked = is_revoked, #
                    access_timestamp = access_timestamp #
                )

