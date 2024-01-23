import scrapy
import re
import time
from urllib.parse import urljoin, urlparse
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest, get_pub_date
from datetime import datetime


# 878 unique PDFs as of 22 Jan 2023

class DlaDedsoSpider(GCSpider):
    name = 'dla_dedso_pubs'
    allowed_domains = ['dla.mil']
    start_urls = ['https://www.dla.mil/Defense-Data-Standards/Resources/ADC/']
    rotate_user_agent = True
    randomly_delay_request = True

    @staticmethod
    def extract_doc_number(doc_name):
        pattern = re.compile(r'[A-Za-z_]*(\d{1,5}[A-Za-z]?)')
        match = pattern.search(doc_name)
        return match.group(1) if match else "1" #match.group(0) if match else "1"
    
    def extract_doc_title(self, row):
        extracted = row.xpath('normalize-space(.//td[2])').get()
        title = extracted.split(':', 1)[-1].strip()
        return title

    def parse(self, response):
        # Iterate over each row in the table that contains either 'dnnGridItem' or 'dnnGridAltItem' class
        for row in response.xpath('//tr[contains(@class, "dnnGridItem") or contains(@class, "dnnGridAltItem")]'):
            pdf_link = row.xpath('.//a[contains(@href, ".pdf")]/@href').get()
            if pdf_link:
                absolute_pdf_link = response.urljoin(pdf_link)

            doc_name = self.extract_doc_name_from_url(absolute_pdf_link)
            doc_num = self.extract_doc_number(doc_name)
            doc_title = self.extract_doc_title(row)

            doc_type = "PDF"
            display_doc_type = "PDF Document"
            
            publication_date_raw = row.xpath('.//td[position()=3]/text()').get().strip()
            publication_date = datetime.strptime(publication_date_raw, '%m/%d/%Y').strftime('%Y-%m-%d')
            
            fields = {
                'doc_name': doc_name,
                'doc_num': doc_num,
                'doc_title': doc_title,
                'doc_type': doc_type,
                'display_doc_type': display_doc_type,
                'file_type': 'pdf',
                'download_url': absolute_pdf_link,
                'source_page_url': response.url,
                'publication_date': publication_date,
                'cac_login_required': False,
                'is_revoked': False
            }

            doc_item = self.populate_doc_item(fields)
            yield doc_item

    def populate_doc_item(self, fields):
        display_org = "Defense Logistics Agency"
        data_source = "DLA DEDSO"
        source_title = "DLA DEDSO"

        version_hash_fields = {
            "doc_name": fields['doc_name'],
            "doc_num": fields['doc_num'],
            "publication_date": get_pub_date(fields['publication_date']),
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
            publication_date=get_pub_date(fields['publication_date']),
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
            display_source=data_source + " - " + source_title,
            display_title=fields['doc_type'] + " " + fields['doc_num'] + ": " + fields['doc_title'],
            file_ext='pdf',
            is_revoked=fields['is_revoked']
        )

    def extract_doc_name_from_url(self, url):
        doc_name =  url.split('/')[-1].split('.')[0]
        doc_name = doc_name.replace('_', ' ')
        return doc_name