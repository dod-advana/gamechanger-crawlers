import re
from datetime import datetime
from urllib.parse import urljoin

from scrapy.http.response.text import TextResponse
from scrapy.selector import Selector

from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem


class TRADOCSpider(GCSpider):
    name = 'tradoc'

    display_org = 'United States Army Training and Doctrine Command'
    data_source = 'TRADOC'
    source_title = 'TRADOC Administrative Publications'

    allowed_domains = ['adminpubs.tradoc.army.mil']
    start_urls = [
        'https://adminpubs.tradoc.army.mil/index.html'
    ]

    cac_login_required = False

    _doc_num_rgx = re.compile(r'^(?P<num>[-0-9a-zA-Z]+)?(?: with )?(?:Change (?P<change>\d+))?$',
                              re.IGNORECASE)

    def parse(self, response: TextResponse):
        links = response.css('#content > p > a::attr(href)').getall()
        for link in links:
            yield response.follow(link, callback=self.parse_page)

    def parse_page(self, response: TextResponse):
        doc_category = response.css('#content > h2::text').get()
        doc_category_code = re.match(r'TRADOC .+ \((?P<code>.+)s\)', doc_category)['code']

        col_headers = response.css('table.pubsTable > thead > tr > td::text').getall()
        num_col_i = next(i+1 for i, header in enumerate(col_headers) if 'Number' in header)
        date_col_i = next(i+1 for i, header in enumerate(col_headers) if 'Published' in header)
        title_col_i = next(i+1 for i, header in enumerate(col_headers) if 'Title' in header)

        table_rows = response.css('table.pubsTable > tbody > tr')
        table_row: Selector
        for table_row in table_rows:
            col1 = table_row.css('td:nth-child(1)')
            if col1.attrib.get('colspan'):
                continue  # header or padding row
            if table_row.css(f'td:nth-child({title_col_i}) > span.fileLink > span.CACrequired'):
                continue  # no download available here

            doc_nums = list(text.strip() for text in table_row.css(
                f'td:nth-child({num_col_i})::text').getall() if text.strip())
            doc_dates = list(text.strip() for text in table_row.css(
                f'td:nth-child({date_col_i})::text').getall() if text.strip())
            row_title = table_row.css(
                f'td:nth-child({title_col_i})::text').get()
            doc_url_lists = []
            file_span: Selector
            for file_span in table_row.css(f'td:nth-child({title_col_i}) > span.fileLink'):
                doc_url_list = file_span.css('a::attr(href)').getall()
                doc_url_lists.append(doc_url_list)

            assert len(doc_nums) == len(doc_dates) == len(doc_url_lists)
            for doc_num, doc_date, doc_url_list in zip(doc_nums, doc_dates, doc_url_lists):
                doc_num, doc_change = self.parse_doc_num(doc_category_code, doc_num, doc_nums[0])

                if doc_change:
                    doc_title = f'{row_title} with Change {doc_change}'
                else:
                    doc_title = row_title

                publication_date = self.parse_date(doc_date)

                web_urls = list(urljoin(response.url, url) for url in doc_url_list)
                downloadable_items = []
                for web_url in web_urls:
                    ext = self.get_href_file_extension(web_url)
                    item = {
                        "doc_type": ext,
                        "web_url": web_url,
                        "compression_type": None
                    }
                    # ensure pdf first file
                    if ext == 'pdf':
                        downloadable_items.insert(0, item)
                    else:
                        downloadable_items.append(item)

                version_hash_fields = {
                    "item_currency": downloadable_items[0]["web_url"].split('/')[-1],
                    "document_title": doc_title,
                    "document_number": doc_num,
                    "publication_date": publication_date,
                }

                doc_name = f'TRADOC {doc_num}'

                pgi_doc_item = DocItem(
                    doc_name=self.clean_name(doc_name),
                    doc_num=self.ascii_clean(doc_num),
                    doc_title=self.ascii_clean(doc_title),
                    doc_type=self.ascii_clean(doc_category),
                    publication_date=publication_date,
                    source_page_url=response.url,
                    display_org=self.display_org,
                    data_source=self.data_source,
                    source_title=self.source_title,
                    downloadable_items=downloadable_items,
                    version_hash_raw_data=version_hash_fields,
                )
                yield pgi_doc_item

    def clean_name(self, name):
        return ' '.join(re.sub(r'[^a-zA-Z0-9. ()-_]', '', self.ascii_clean(name).replace('/', '_')).split())

    def parse_doc_num(self, doc_category_code, doc_num, base_doc_num):
        match = self._doc_num_rgx.match(doc_num)
        if not match:
            raise ValueError(f'unknown doc num format {str(doc_num)}')
        matchdict = match.groupdict()
        if not matchdict.get('num'):
            base_match = self._doc_num_rgx.match(base_doc_num)
            if not base_match or not base_match.groupdict().get('num'):
                raise ValueError(f'unknown doc num format {str(base_doc_num)}')
            matchdict['num'] = base_match['num']
        doc_num = matchdict['num']
        doc_change = matchdict.get('change')
        if doc_change:
            doc_num = f'{doc_category_code}{doc_num}C{doc_change}'
        else:
            doc_num = f'{doc_category_code}{doc_num}'
        return doc_num, doc_change

    def parse_date(self, date_str):
        try:
            return datetime.strptime(date_str, '%d %b %Y').strftime('%Y-%m-%d')
        except ValueError:
            pass

        try:
            return datetime.strptime(date_str, '%b %d, %Y').strftime('%Y-%m-%d')
        except ValueError:
            pass

        try:
            return datetime.strptime(date_str, '%b %Y').strftime('%Y-%m-%d')
        except ValueError:
            pass

        try:
            return datetime.strptime(date_str, '%B %Y').strftime('%Y-%m-%d')
        except ValueError:
            pass

        raise ValueError(f'unknown date format {str(date_str)}')
