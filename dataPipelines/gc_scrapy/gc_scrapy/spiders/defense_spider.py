from urllib.parse import urlparse
import re
import scrapy
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.utils import parse_timestamp, dict_to_sha256_hex_digest

# This crawler is NOT ment to be added to crawler scheduler
# Request was to scrape 1 DOC from the page
# specifically https://media.defense.gov/2022/Jan/04/2002917147/-1/-1/0/JTR.PDF

# If additional pdfs are to be targetted, this will require a soft rewrite
# Values like pub date are HARD CODED for this specific doc

class DefenseTravelSpider(GCSpider):
    name = 'defense_travel'
    allowed_domains = ['www.travel.dod.mil']
    start_urls = ['https://www.travel.dod.mil/Policy-Regulations/Joint-Travel-Regulations/']

    rotate_user_agent = True
    randomly_delay_request = True

    headers = {
        "accept-language": "en-US,en;q=0.9",
    }

    def start_requests(self):
        for start_url in self.start_urls:
            yield scrapy.Request(url=start_url, method='GET', headers=self.headers)

    def parse(self, response):
        # Only targetting ONE pdf
        div_selector = '//div[contains(@class, "grid-item")]/button[contains(@onclick, "JTR.PDF")]' 
        button = response.xpath(div_selector).get()
        if button:
            # Extract URL
            pdf_link = re.search(r"'(.*?)'", button).group(1)
            absolute_pdf_link = response.urljoin(pdf_link)

            # Extract document metadata
            doc_name = self.extract_doc_name_from_url(absolute_pdf_link)
            doc_num = self.extract_doc_number(doc_name)
            publication_date = '2024-01-01',  # Hard coded

            fields = {
                'doc_name': doc_name,
                'doc_num': doc_num,
                'doc_title': "Joint Travel Regulations",
                'doc_type': "PDF",
                'display_doc_type': "Document",
                'file_type': 'pdf',
                'download_url': absolute_pdf_link,
                'source_page_url': response.url,
                'publication_date': '2024-01-01',  # Hard coded
                'cac_login_required': False,
                'is_revoked': False
            }

            doc_item = self.populate_doc_item(fields)
            yield doc_item

    def extract_doc_number(self, doc_name):
        # Update logic to extract document number if necessary
        return doc_name.split('/')[-1].split('.')[0]

    def extract_doc_name_from_url(self, url):
        # Update logic to extract the document name if necessary
        return url.split('/')[-1].split('.')[0]

    def populate_doc_item(self, fields):
        display_org = "Defense Travel"
        data_source = "Defense Travel"
        source_title = "Defense Travel Management Office"

        version_hash_fields = {
            "doc_name": fields['doc_name'],
            "doc_num": fields['doc_num'],
            "publication_date": '2024-01-01',  # Hard coded
            "download_url": fields['download_url'],
            "display_title": fields['doc_title']
        }

        version_hash = dict_to_sha256_hex_digest(version_hash_fields)

        return DocItem(
            doc_name=fields['doc_name'],
            doc_title=fields['doc_title'],
            doc_num=fields['doc_num'],
            doc_type=fields['doc_type'],
            display_doc_type=fields['display_doc_type'],
            publication_date='2024-01-01',  # Hard coded
            cac_login_required=fields['cac_login_required'],
            crawler_used=self.name,
            downloadable_items=[{
                "doc_type": fields['file_type'],
                "download_url": fields['download_url'],
                "compression_type": None
            }],
            source_page_url=fields['source_page_url'],
            source_fqdn=urlparse(fields['source_page_url']).netloc,
            download_url=fields['download_url'],
            version_hash_raw_data=version_hash_fields,
            version_hash=version_hash,
            display_org=display_org,
            data_source=data_source,
            source_title=source_title,
            display_source=data_source + " - " + "Management Office",
            display_title=fields['doc_num'] + ": " + fields['doc_title'],
            file_ext='pdf',
            is_revoked=fields['is_revoked']
        )