from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.utils import parse_timestamp
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest, get_pub_date
from datetime import datetime
from urllib.parse import urlparse
import scrapy


display_types = ["Instruction", "Manual", "Memo", "Regulation"]


class DHASpider(GCSpider):
    name = "dha_pubs" # Crawler name
    start_urls = [
        'https://www.health.mil/About-MHS/OASDHA/Defense-Health-Agency/Administration-and-Management/DHA-Publications'
    ]

    file_type = "pdf"
    randomly_delay_request = True
    rotate_user_agent = True

    @staticmethod
    def get_display(doc_type):
        for dt in display_types:
            if dt in doc_type:
                return dt

        return "Document"

    def parse(self, response):
        sections = response.css('div[data-ga-cat="File Downloads List"]')
        for section in sections:
            header = self.ascii_clean(section.css('h2::text').get(default=''))
            doc_type = header.replace('DHA-', 'DHA ').strip()
            display_doc_type = self.get_display(doc_type)

            rows = section.css('table.dataTable tbody tr')
            for row in rows:
                doc_num = self.ascii_clean(
                    row.css('td:nth-child(1) a::text').get(default=''))
                href = row.css('td:nth-child(1) a::attr(href)').get(default='')
                publication_date_raw = self.ascii_clean(
                    row.css('td:nth-child(3)::text').get(default=''))
                publication_date = get_pub_date(publication_date_raw)
                doc_title = self.ascii_clean(
                    row.css('td:nth-child(2)::text').get(default='')).replace('\r', '').replace('\n', '')

                doc_name = f"{doc_type} {doc_num}"
                web_url = f"https://www.health.mil{href}"

                fields = {
                'doc_name': doc_name,
                'doc_num': doc_num,
                'doc_title': doc_title,
                'doc_type': doc_type,
                'cac_login_required': False,
                'download_url': web_url,
                'publication_date': publication_date,
                'display_doc_type': display_doc_type
            }

                doc_item = self.populate_doc_item(fields)
                
                yield from doc_item

    def populate_doc_item(self, fields):
        display_org = "Defense Health Agency" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "Military Health System" # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Defense Health Agency Publications" # Level 3 filter

        doc_name = fields['doc_name']
        doc_num = fields['doc_num']
        doc_title = fields['doc_title']
        doc_type = fields['doc_type']
        cac_login_required = fields['cac_login_required']
        download_url = fields['download_url']
        publication_date = get_pub_date(fields['publication_date'])
        display_doc_type = fields['display_doc_type']
        
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + " " + doc_title
        is_revoked = False
        access_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f") # T added as delimiter between date and time
        source_page_url = self.start_urls[0]
        source_fqdn = urlparse(source_page_url).netloc

        downloadable_items = [
            {
                "doc_type": self.file_type,
                "download_url": download_url,
                "compression_type": None
            }
        ]
        
        version_hash_fields = {
            "doc_name":doc_name,
            "doc_num": doc_num,
            "publication_date": publication_date,
            "download_url": download_url
        }
        
        version_hash = dict_to_sha256_hex_digest(version_hash_fields)
        
        yield DocItem(
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
                    file_ext = doc_type, #
                    is_revoked = is_revoked, #
                    access_timestamp = access_timestamp #
                )    
        