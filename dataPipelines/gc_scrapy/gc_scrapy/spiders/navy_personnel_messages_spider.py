import re
import typing as t
from datetime import datetime
from urllib.parse import urljoin

from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from scrapy.http.response.text import TextResponse
from scrapy.selector import Selector


class TRADOCSpider(GCSpider):
    name = 'navy_personnel_messages'

    display_org = 'US Navy'
    data_source = 'MyNavy HR'
    source_title = 'Bureau of Naval Personnel Messages'

    allowed_domains = ['mynavyhr.navy.mil']
    start_urls = [
        'https://www.mynavyhr.navy.mil/References/Messages/'
    ]

    cac_login_required = False

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
            downloadable_items = [
                {
                    "doc_type": "txt",
                    "web_url": web_url,
                    "compression_type": None
                }
            ]

            version_hash_fields = {
                "item_currency": downloadable_items[0]["web_url"].split('/')[-1],
                "document_title": doc_title,
                "publication_date": publication_date,
            }

            doc_item = DocItem(
                doc_name=self.clean_name(doc_name),
                doc_num=self.ascii_clean(doc_num),
                doc_title=self.ascii_clean(doc_title),
                doc_type=self.ascii_clean(doc_type),
                publication_date=publication_date,
                source_page_url=response.url,
                display_org=self.display_org,
                data_source=self.data_source,
                source_title=self.source_title,
                downloadable_items=downloadable_items,
                version_hash_raw_data=version_hash_fields,
                is_revoked=is_revoked,
            )
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
