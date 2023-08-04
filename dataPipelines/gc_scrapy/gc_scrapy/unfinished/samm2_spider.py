import scrapy
import time
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest, get_pub_date
from urllib.parse import urljoin, urlparse
import re

class SammPolicyMemorandaSpider(GCSpider):
    name = "samm_policy_memoranda" 
    allowed_domains = ["samm.dsca.mil"]
    start_urls = ["https://samm.dsca.mil/policy-memoranda/PolicyMemoList-All"]
    rotate_user_agent = True
    doc_type = "SAMM Policy Memoranda"

    @staticmethod
    def extract_doc_number(doc_title):
        ver_num = 0
        ref_num = 0

        doc_num = f"V{ver_num}R{ref_num}"
        return doc_title, doc_num

    def parse(self, response):
        base_url = "https://samm.dsca.mil"

        for row in response.xpath('//div[@class="view-content"]//table/tbody/tr'):
            pm_status_text = row.xpath('td[6]/text()').get()
            pm_status = pm_status_text.strip() if pm_status_text is not None else ""
            if pm_status != "Incorporated": 
                relative_urls = row.xpath('td[2]/a/@href').get()

                absolute_url = urljoin(base_url, relative_urls)
                yield scrapy.Request(absolute_url, callback=self.parse_document_page)

    def parse_document_page(self, response):
            pdf_link = response.xpath('//div[contains(@class, "PM_PDF_ink")]//a/@href').get()
            if pdf_link is not None:
                doc_name = self.ascii_clean(pdf_link.split("/")[-1])
                doc_title_text = "test"
                doc_title, doc_num = self.extract_doc_number(doc_title_text)

                doc_name = self.ascii_clean(doc_title_text.replace(" ", "_"))
                display_doc_type = "SAMM"

                doc_name, doc_title, unique_doc_name = self.generate_unique_doc_name({'doc_name': doc_name, 'doc_title': "test"})
                file_type = self.get_href_file_extension(pdf_link)
                web_url = self.ensure_full_href_url(pdf_link, response.url)
                
                fields = {
                    'doc_name': doc_name,
                    'unique_doc_name': unique_doc_name,
                    'doc_num': doc_num,
                    'doc_title': doc_title,
                    'doc_type': self.doc_type,
                    'display_doc_type': display_doc_type,
                    'file_type': file_type,
                    'cac_login_required': False,
                    'download_url': web_url,
                    'source_page_url': response.url,
                    'publication_date': None
                }

                doc_item = self.populate_doc_item(fields)

                yield doc_item

    def generate_unique_doc_name(self, data):
        # Clean doc_name
        doc_name = re.sub(r'[\(\),]', '', data['doc_name'])  # Remove parentheses and commas
        doc_name = re.sub(r'[\W_\.]+$', '', doc_name)  # Remove any special characters at the end

        # Clean doc_title
        doc_title = re.sub(r'[\(\),]', '', data['doc_title'])  # Remove parentheses and commas
        doc_title = re.sub(r'[\W_\.]+$', '', doc_title)  # Remove any special characters at the end

        # Generate unique doc name
        unique_doc_name = doc_name + "_" + doc_title

        # Remove any trailing underscores
        unique_doc_name = re.sub(r'_+$', '', unique_doc_name)

        # Remove any trailing special characters, including periods and underscores
        unique_doc_name = re.sub(r'[\W_\.]+$', '', unique_doc_name)

        return doc_name, doc_title, unique_doc_name

    def populate_doc_item(self, fields):
        #
        # This functions provides both hardcoded and computed values for the variables
        # in the imported DocItem object and returns the populated metadata object
        #
        display_org = "SAMM"    # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "SAMM"    # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Unlisted Source" 

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
            "download_url": download_url,
            "display_title": display_title,
            "scrape_time" : time.time() #current time
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
                    file_ext = fields['file_type'],
                    is_revoked = is_revoked,
                )
