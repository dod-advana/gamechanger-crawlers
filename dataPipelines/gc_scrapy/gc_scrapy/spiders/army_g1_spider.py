import scrapy
import re
import time
from urllib.parse import urljoin, urlparse
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest, get_pub_date
from datetime import datetime
from bs4 import BeautifulSoup
import json
import html

class ArmyG1Spider(GCSpider):
    name = 'army_g1_pubs'
    start_urls = ['https://www.army.mil/g-1#org-g-1-publications']
    rotate_user_agent = True
    randomly_delay_request = True
    custom_settings = {
        **GCSpider.custom_settings,
        "DOWNLOAD_DELAY": 5,
        "AUTOTHROTTLE_ENABLED": True, 
        "AUTOTHROTTLE_START_DELAY": 1,
        "AUTOTHROTTLE_MAX_DELAY": 10,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
    }

    def encoding(self, text):
        try:
            text.encode('ascii')
            return False
        except UnicodeEncodeError:
            return True

    def extract_doc_name_from_url(self, url):
        doc_name =  url.split('/')[-1].split('.')[0]
        return doc_name

    def extract_doc_number(self, doc_number):
        pattern = r'(\d{2,4}-\d{1,4})'
        match = re.search(pattern, doc_number)    
        if match:
            return match.group(1)
        else:
            return 'N/A'

    def title_edge_cases(self, text, label):
        # renames documents if incorrect on website
        if "Board Brief; NCO Evaluation Board Supplement" in text:
            return (label + " Board Brief")
        elif "NCO Evaluation Board Supplement" in text:
            return label
        elif text.endswith('.pdf') or text.endswith('docx'):
            return label
        else:
            pattern = r'(?:DA\s+)?PAM\s+\d{2,4}-\d{2,4}'
            cleaned_text = re.sub(pattern, '', text)
            stripped_text = cleaned_text.strip()
            # remove HTML encoding
            if "\\xc2\\xa0" in stripped_text:
                stripped_text = stripped_text.replace("\\xc2\\xa0", " ")
            decoded_text = html.unescape(stripped_text)
            return decoded_text
    
    def extract_date_from_url(self, url):
        pattern = r'(\d{4}/\d{2}/\d{2})'
        match = re.search(pattern, url)    
        if match:
            date = match.group(1)
            datetime_ = datetime.strptime(date, "%Y/%m/%d")
            return datetime_.strftime("%m-%d-%Y")
        else:
            return "Unknown"


    def parse(self, response):
        for container in response.css('.inner-container'):
            # title of each section
            container_label = container.css('h4::text').extract_first()

            for accordion in container.css('.accordion-container'):

                for item in accordion.css('.accordion li'):

                    # get title text *within* each accordion tab           
                    label_text = item.css('label[for]::text').get().strip()
                    
                    # convert html to string
                    soup = BeautifulSoup(item.get(), 'html.parser')
                    div_tag = soup.find('div', class_='rich-text-element bodytext')

                    if div_tag:
                        delta_data = div_tag.get('data-delta')
                        if delta_data:
                            # parse delta_data as JSON
                            data = json.loads(delta_data)
                            for op in data["ops"]:
                                if 'attributes' in op and 'link' in op['attributes']:
                                    # URL link
                                    link = op['attributes']['link']
                                    
                                    # only consider links that lead to documents
                                    if link.endswith('.pdf') or link.endswith('.docx'):
                                        # extract title
                                        text = op['insert']

                                        # check if title needs to be encoded before conversion to string
                                        if self.encoding(text):
                                            text = str(text.encode('utf-8'))[2:-1]

                                        # clean data for `fields` dictionary
                                        doc_title = self.title_edge_cases(text, label_text)
                                        doc_number = self.extract_doc_number(container_label)
                                        doc_name = self.extract_doc_name_from_url(link)
                                        publication_date = self.extract_date_from_url(link)
                                        #file_type = 'pdf' if link.endswith('.pdf') else ('docx' if link.endswith('.docx') else None)
                                        file_type = self.get_href_file_extension(link)

                                        fields = {
                                            'doc_name': doc_name,
                                            'doc_num': doc_number,
                                            'doc_title': doc_title,
                                            'doc_type': "DA PAM",
                                            'display_doc_type': "DA PAM",
                                            'file_type': file_type,
                                            'download_url': link,
                                            'source_page_url': response.url,
                                            'publication_date': publication_date,
                                            'cac_login_required': False,
                                            'is_revoked': False
                                        }

                                        doc_item = self.populate_doc_item(fields)
                                        yield doc_item

    def populate_doc_item(self, fields):
        display_org = "Dept. of the Army"
        data_source = "Army Publishing Directorate"
        source_title = "G-1 Publications"

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
            file_ext=fields['file_type'], # 'pdf'
            is_revoked=fields['is_revoked']
        )