# -*- coding: utf-8 -*-
import re
import bs4
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest, get_pub_date
from datetime import datetime
from urllib.parse import urlparse

class OmbSpider(GCSpider):
    name = 'omb_pubs' # Crawler name
    rotate_user_agent = True

    start_urls = [
        'https://www.whitehouse.gov/omb/information-for-agencies/memoranda/'
    ]

    def parse(self, response):
        page_url = response.url
        base_url = 'https://www.whitehouse.gov'

        soup = bs4.BeautifulSoup(response.body, features="html.parser")

        parsed_nums = []

        # get target column of list items
        parsed_docs = []
        li_list = soup.find_all('li')
        for li in li_list:
            doc_type = 'OMBM'
            doc_num = ''
            doc_name = ''
            doc_title = ''
            chapter_date = ''
            publication_date = ''
            pdf_url = ''
            exp_date = ''
            issuance_num = ''
            pdf_di = None
            if 'supersede' not in li.text.lower():
                a_list = li.findChildren('a')
                for a in a_list:
                    href = 'href'
                    if a.get('href') is None:
                        href = 'data-copy-href'
                    if a[href].lower().endswith('.pdf'):
                        if a[href].startswith('http'):
                            pdf_url = a[href]
                        else:
                            pdf_url = base_url + a[href].strip()
                    commaTokens = a.text.strip().split(',', 1)
                    spaceTokens = a.text.strip().split(' ', 1)
                    if len(commaTokens) > 1 and len(commaTokens[0]) < len(spaceTokens[0]):
                        doc_num = commaTokens[0]
                        doc_title = re.sub(r'^.*?,', '', a.text.strip())
                        doc_name = "OMBM " + doc_num
                    elif len(spaceTokens) > 1 and len(spaceTokens[0]) < len(commaTokens[0]):
                        doc_num = spaceTokens[0].rstrip(',.*')
                        doc_title = spaceTokens[1]
                        doc_name = "OMBM " + doc_num
                    possible_date = re.search(pattern=r"\(.* \d+, \d{4}\)", string=li.text)
                    if possible_date:
                        publication_date_raw = possible_date[0]
                        publication_date = get_pub_date(publication_date_raw[1:-1])
                if pdf_url != '' and doc_num.count('-') == 2:
                    pdf_di = [{
                        'doc_type': 'pdf',
                        'download_url': pdf_url,
                        'compression_type': None
                    }]

                    version_hash_fields = {
                        "doc_name":doc_name,
                        "doc_num": doc_num,
                        "publication_date": publication_date,
                        "download_url": pdf_url
                    }
                    
                    version_hash = dict_to_sha256_hex_digest(version_hash_fields)
                    parsed_title = self.ascii_clean(re.sub('\\"', '', doc_title))
                    parsed_num = doc_num.strip()
                    if parsed_num not in parsed_nums:
                        fields = {
                            'doc_name': doc_name.strip(),
                            'doc_title': parsed_title,
                            'doc_num': parsed_num,
                            'doc_type': doc_type.strip(),
                            'display_doc_type': doc_type.strip(),
                            'publication_date': publication_date,
                            'cac_login_required': False,
                            'source_page_url': page_url.strip(),
                            'version_hash_raw_data': version_hash_fields,
                            'downloadable_items': pdf_di,
                            'download_url': pdf_url,
                            'version_hash': version_hash,
                        }
                        doc_item = self.populate_doc_item(fields)                 
                        yield from doc_item
                        
                        parsed_nums.append(parsed_num)  
                        
                    
    def populate_doc_item(self, fields):
        display_org = "OMB" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "Executive Office of the President" # Level 2: GC app 'Source' metadata field for docs from this crawler 
        source_title = "Office of Management and Budget Memoranda" # Level 3 filter

        doc_name = fields['doc_name']
        doc_num = fields['doc_num']
        doc_title = fields['doc_title']
        doc_type = fields['doc_type']
        publication_date = fields['publication_date']
        cac_login_required = fields['cac_login_required']
        download_url = fields['download_url']
        display_doc_type = fields['display_doc_type']
        downloadable_items = fields['downloadable_items']
        version_hash = fields['version_hash']
        version_hash_fields = fields['version_hash_raw_data']
        
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + " " + doc_title
        is_revoked = False
        access_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f") # T added as delimiter between date and time
        source_page_url = self.start_urls[0]
        source_fqdn = urlparse(source_page_url).netloc

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
        