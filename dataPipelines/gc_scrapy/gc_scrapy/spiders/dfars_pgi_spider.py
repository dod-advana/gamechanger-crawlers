import re
from urllib.parse import urljoin, urlparse

from scrapy.http import TextResponse
from scrapy.selector import Selector

from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem

from datetime import datetime
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest

class DoDSpider(GCSpider):
    name = 'dfars_pgi' # Crawler name

    start_urls = ['https://www.acq.osd.mil/dpap/dars/dfarspgi/current']
    allowed_domains = ['www.acq.osd.mil']

    rotate_user_agent = True

    def parse(self, response: TextResponse):
        options = response.css('select.tocselect option::text')
        publication_date = next(match['date'] for match in
                                map(lambda o: re.match(
                                    r'Current Version \((?P<date>\d{2}/\d{2}/\d{4})\)', o.get()), options)
                                if match)
        meta = {'publication_date': publication_date}

        table_iframe = response.css('iframe[title="DFARS Table"]')
        table_iframe_src = table_iframe.attrib['src']
        yield response.follow(table_iframe_src, callback=self.parse_table_iframe, meta=meta)

    def parse_table_iframe(self, response: TextResponse):
        publication_date = response.meta['publication_date']

        table = response.css('#toctable2')
        rows = table.css('tr')
        prev_num = 'CHAPTER 2'  # ?
        prev_title = 'DEFENSE FEDERAL ACQUISITION REGULATION SUPPLEMENT'
        row: Selector
        for row in rows:
            if row.attrib['class'] == 'rule':
                part_and_title_raw: str = row.css('td:nth-child(1)::text').get()
                if part_and_title_raw is None:
                    part_and_title_raw = row.css('td:nth-child(1) p::text').get()
                part_and_title = self.clean_name(part_and_title_raw)
                part_and_title_split = part_and_title.split(' - ', 1)
                part_num = part_and_title_split[0]
                part_title = part_and_title_split[1] if len(part_and_title_split) > 1 else part_num
                prev_num = part_num
                prev_title = part_title
            else:
                doc_subpart_raw: str = row.css('td:nth-child(1) span::text').get()
                doc_title_raw: str = row.css('td:nth-child(1)::text').get()
                dfars_pdf_href_raw: str = row.css('td:nth-child(3) a::attr(href)').get()
                pgi_pdf_href_raw: str = row.css('td:nth-child(6) a::attr(href)').get()
                
                doc_subpart = self.clean_name(doc_subpart_raw)
                if (doc_subpart in ('TABLE OF CONTENTS', 'COVER PAGE')
                        or (doc_subpart.startswith('PART') and prev_num.startswith('APPENDIX'))):
                    doc_num = f'{prev_num} {doc_subpart}'
                else:
                    prev_num = doc_num = doc_subpart

                doc_title = self.clean_name(doc_title_raw)
                if doc_title.startswith('-'):
                    doc_title = doc_title[2:]
                if doc_title:
                    prev_title = doc_title
                else:
                    doc_title = prev_title

                if 'NO DFARS TEXT' in doc_title_raw or 'NO DFARS TEXT' in prev_title:
                    continue

                # DFARS
                if dfars_pdf_href_raw:
                    dfars_pdf_href = urljoin(self.start_urls[0], dfars_pdf_href_raw)

                    doc_name = f'DFARS {doc_num} - {doc_title}'

                    dfars_doc_item = self.populate_doc_item(doc_name, 'DFARS', doc_num, doc_title, dfars_pdf_href, publication_date)
    
                    yield dfars_doc_item

                # PGI
                if pgi_pdf_href_raw:
                    pgi_pdf_href = urljoin(self.start_urls[0], pgi_pdf_href_raw)

                    doc_num = self.derive_pgi_num(doc_num)

                    doc_name = f'{doc_num} - {doc_title}'
                    
                    pgi_doc_item = self.populate_doc_item(doc_name, 'PGI', doc_num, doc_title, pgi_pdf_href, publication_date)
    
                    yield pgi_doc_item
                                
    
    def clean_name(self, name):
        return ' '.join(re.sub(r'[^a-zA-Z0-9. ()\\-]', '', self.ascii_clean(name).replace('/', '-')).split())

    def derive_pgi_num(self, dfars_num):
        num_match = re.match(r'[A-Z]+ (?P<num>\d+(?:\.\d+)?)', dfars_num)
        if num_match:
            doc_num = f'PGI {num_match["num"]}'
            return doc_num
        num_match = re.match(r'APPENDIX (?P<num>[A-Z]+(?: PART \d+)?)', dfars_num)
        if num_match:
            doc_num = f'PGI {num_match["num"]}'
            return doc_num
        # ?
        return f'PGI {dfars_num}'

    def populate_doc_item(self, doc_name, doc_type, doc_num, doc_title, web_url, publication_date):
        '''
        This functions provides both hardcoded and computed values for the variables
        in the imported DocItem object and returns the populated metadata object
        '''
        display_org = "DFARS" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "Defense Federal Acquisition Regulation" # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Unlisted Source" # Level 3 filter

        cac_login_required = False

        display_doc_type = "Document" # Doc type for display on app
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + " " + doc_title
        is_revoked = False
        access_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f") # T added as delimiter between date and time
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
            "doc_name":doc_name,
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
                    display_doc_type_s = display_doc_type, #
                    publication_date_dt = publication_date,
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

