import re
import typing as t
from datetime import datetime
from urllib.parse import urljoin

from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from scrapy.http.response.text import TextResponse
from scrapy.selector import Selector

from urllib.parse import urljoin, urlparse
from datetime import datetime
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest, get_pub_date

class TRADOCSpider(GCSpider):
    name = 'navy_personnel_messages' # Crawler name
    allowed_domains = ['mynavyhr.navy.mil']
    start_urls = [
        'https://www.mynavyhr.navy.mil/References/Messages/'
    ]

    rotate_user_agent = True

    def parse(self, response: TextResponse):
        links = response.css('div.afMenuLinkHeader > a::attr(href)').getall()
        for link in links:
            yield response.follow(link, callback=self.parse_page)

    def parse_page(self, response: TextResponse):
        page_title = response.css('div.ContainerPane strong *::text').get()
        page_title_match = re.match(r'(?P<type>\S+) (?P<year>\d+)', page_title)
        doc_type = page_title_match['type']
        doc_year = page_title_match['year']

        table_rows = response.css('#dnn_CenterPane_Top div > table > tbody > tr')
        table_rows = table_rows[1:]  # skip header row
        table_row: Selector
        for table_row in table_rows:
            doc_num = self.join_text(table_row.css('td:nth-child(1) *::text').getall())
            # several docs seem to have a typo for the year portion of the doc number so fill it in ourselves
            doc_num = f'{doc_num.split("/")[0]}/{doc_year[-2:]}'

            doc_title = self.join_text(table_row.css('td:nth-child(2) *::text').getall())

            is_revoked = 'cancelled' in doc_title.lower()

            # there seem to be some dead hidden links to the BUPERS site so ignore
            doc_url = table_row.css('td:nth-child(2) a:not([href*="/bupers-npc/"])::attr(href)').get()
            doc_date = self.join_text(table_row.css('td:nth-child(3) *::text').getall())
            publication_date = self.parse_date(doc_date)
            doc_name = f'{doc_type} {doc_num}'
            web_url = urljoin(response.url, doc_url)

            fields = {
                'doc_name': self.clean_name(doc_name),
                'doc_num': self.ascii_clean(doc_num),
                'doc_title': self.ascii_clean(doc_title),
                'doc_type': self.ascii_clean(doc_type),
                "file_ext": self.get_href_file_extension(doc_url),
                'cac_login_required': False,
                'download_url': web_url,
                'source_page_url': response.url,
                'is_revoked': is_revoked,
                'publication_date': publication_date
            }
            ## Instantiate DocItem class and assign document's metadata values
            doc_item = self.populate_doc_item(fields)
        
            yield doc_item

    def join_text(self, texts: t.List[str]) -> str:
        # the text seems to be stored in a variety of different ways within table cells so it
        # is easiest to just extract all the text and then join it back together cleanly
        return ' '.join(filter(None, map(lambda s: self.ascii_clean(s), texts)))

    def clean_name(self, name: str) -> str:
        return ' '.join(re.sub(r'[^a-zA-Z0-9. ()-_]', '', self.ascii_clean(name).replace('/', '_')).split())

    def parse_date(self, date_str: str) -> str:
        try:
            return datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y-%m-%d')
        except ValueError:
            pass

        try:
            return datetime.strptime(date_str, '%m/%d %Y').strftime('%Y-%m-%d')
        except ValueError:
            pass

        try:
            return datetime.strptime(date_str, '%m/%d%Y').strftime('%Y-%m-%d')
        except ValueError:
            pass

        # this date seems to be a typo, just hardcoding an exception :/
        if date_str == '8/16/201':
            return '2021-08-16'

        raise ValueError(f'unknown date format {date_str}')

    def populate_doc_item(self, fields):
        '''
        This functions provides both hardcoded and computed values for the variables
        in the imported DocItem object and returns the populated metadata object
        '''
        display_org = 'US Navy'  # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = 'MyNavy HR' # Level 2: GC app 'Source' metadata field for docs from this crawler 
        source_title = 'Bureau of Naval Personnel Messages' # Level 3 filter

        doc_name = fields['doc_name']
        doc_num = fields['doc_num']
        doc_title = fields['doc_title']
        doc_type = fields['doc_type']
        cac_login_required = fields['cac_login_required']
        download_url = fields['download_url']
        publication_date = get_pub_date(fields['publication_date'])

        display_doc_type = "Document" # Doc type for display on app
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + " " + doc_title
        is_revoked = fields['is_revoked']
        source_page_url = fields['source_page_url']
        source_fqdn = urlparse(source_page_url).netloc
        file_ext = fields['file_ext']

        downloadable_items = [{
                "doc_type": "txt",
                "download_url": download_url,
                "compression_type": None,
            }]

        ## Assign fields that will be used for versioning
        version_hash_fields = {
            "doc_name":doc_name,
            "doc_num": doc_num,
            "publication_date": publication_date,
            "download_url": download_url.split('/')[-1]
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
                )