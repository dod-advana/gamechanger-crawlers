from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest #, get_pub_date, parse_timestamp
from datetime import datetime
from urllib.parse import urlparse
import scrapy
import re
import typing as t
import pandas

display_types = ["Instruction", "Manual", "Memo", "Regulation"]

class DHASpider(GCSpider):
    name = "dha_pubs" # Crawler name
    display_org = "Defense Health Agency" # Level 1: GC app 'Source' filter for docs from this crawler
    data_source = "Military Health System" # Level 2: GC app 'Source' metadata field for docs from this crawler
    source_title = "Defense Health Agency Publications" # Level 3 filter

    start_urls = [
        'https://www.health.mil/Reference-Center/DHA-Publications'
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

#####################

    def parse_timestamp(ts: t.Union[str, datetime], raise_parse_error: bool = False) -> t.Optional[datetime]:
        """Parse date/timestamp with no particular format
        :param ts: date/timestamp string
        :return: datetime.datetime if parsing was successful, else None
        """
        def _parse(ts):
            if isinstance(ts, datetime):
                return ts

            try:
                ts = pandas.to_datetime(ts).to_pydatetime()
                if str(ts) == 'NaT':
                    return None
                else:
                    return ts
            except:
                return None

        parsed_ts = _parse(ts)
        if parsed_ts is None and raise_parse_error:
            raise ValueError(f"Invalid timestamp: '{ts!r}'")
        else:
            return parsed_ts


    def get_pub_date(self, publication_date):
        '''
        This function convverts publication_date from DD Month YYYY format to YYYY-MM-DDTHH:MM:SS format.
        T is a delimiter between date and time.
        '''
        try:
            date = parse_timestamp(publication_date, None)
            if date:
                publication_date = date.strftime("%Y-%m-%dT%H:%M:%S")
        except:
            publication_date = None
        return publication_date

#################

    def parse(self, response):
        sections = response.css('table[class="dataTable tabpanel"]')
        for section in sections:
            headers = section.css('button , th.p:nth-child(1) , th.p:nth-child(2) , th.p:nth-child(5) , th.p:nth-child(4)::text').extract()
            headers = [re.sub(r'<.+?>', '', header).strip() for header in headers]
            rows = section.css('table.dataTable tbody tr')

            for row in rows:
                doc_type = self.ascii_clean(
                    row.css('td:nth-child(1)::text').get(default='')) \
                    .replace('DHA-', 'DHA ').strip()
                display_doc_type = self.get_display(doc_type)
                doc_num = self.ascii_clean(
                    row.css('td:nth-child(2) a::text').get(default=''))
                href = row.css('td:nth-child(2) a::attr(href)').get(default='')
                publication_date_raw = self.ascii_clean(
                    row.css('td:nth-child(5)::text').get(default=''))
                publication_date = publication_date_raw
                doc_title = self.ascii_clean(
                    row.css('td:nth-child(3)::text').get(default='')).replace('\r', '').replace('\n', '')
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
        publication_date = self.get_pub_date(fields['publication_date']) #######
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
                "web_url": download_url, # download_url: download_url
                "compression_type": None
            }
        ]
        
        version_hash_fields = {
            #"doc_name":doc_name,
            #"doc_num": doc_num,
            "item_currency": download_url,
            "publication_date": publication_date
            #"download_url": download_url
        }

        version_hash = dict_to_sha256_hex_digest(version_hash_fields)
        
        yield DocItem(
            doc_name = doc_name,
            doc_title = doc_title,
            doc_num = doc_num,
            doc_type = doc_type,
            display_doc_type = display_doc_type,
            publication_date = publication_date,
            cac_login_required = cac_login_required,
            crawler_used = self.name,
            downloadable_items = downloadable_items,
            version_hash_raw_data = version_hash_fields
            )