import re
from datetime import datetime
from urllib.parse import urljoin, urlparse

from scrapy.http.response.text import TextResponse
from scrapy.selector import Selector

from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.utils import parse_timestamp, dict_to_sha256_hex_digest


class TRADOCSpider(GCSpider):
    name = 'tradoc' # Crawler name
    allowed_domains = ['adminpubs.tradoc.army.mil']
    start_urls = [
        'https://adminpubs.tradoc.army.mil/index.html'
    ]
    rotate_user_agent = True

    _doc_num_rgx = re.compile(r'^(?P<num>[-0-9a-zA-Z]+)?(?: with )?(?:Change (?P<change>\d+))?$',
                              re.IGNORECASE)

    @staticmethod
    def get_pub_date(publication_date):
        '''
        This function convverts publication_date from DD Month YYYY format to YYYY-MM-DDTHH:MM:SS format.
        T is a delimiter between date and time.
        '''
        try:
            date = parse_timestamp(publication_date, None)
            if date:
                publication_date = datetime.strftime(date, '%Y-%m-%dT%H:%M:%S')
        except:
            publication_date = ""
        return publication_date

    def parse(self, response: TextResponse):
        links = response.css('#content > p > a::attr(href)').getall()
        for link in links:
            yield response.follow(link, callback=self.parse_page)

    def parse_page(self, response: TextResponse):
        doc_category = response.css('#content > h2::text').get()
        doc_category_code = re.match(
            r'TRADOC .+ \((?P<code>.+)s\)', doc_category)['code']

        col_headers = response.css(
            'table.pubsTable > thead > tr > td::text').getall()
        num_col_i = next(
            i+1 for i, header in enumerate(col_headers) if 'Number' in header)
        date_col_i = next(
            i+1 for i, header in enumerate(col_headers) if 'Published' in header)
        title_col_i = next(
            i+1 for i, header in enumerate(col_headers) if 'Title' in header)

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
                doc_num, doc_change = self.parse_doc_num(
                    doc_category_code, doc_num, doc_nums[0])

                if doc_change:
                    doc_title = f'{row_title} with Change {doc_change}'
                else:
                    doc_title = row_title

                publication_date = self.parse_date(doc_date)

                web_urls = list(urljoin(response.url, url)
                                for url in doc_url_list)
                downloadable_items = []
                for web_url in web_urls:
                    ext = self.get_href_file_extension(web_url)
                    item = {
                        "doc_type": ext,
                        "download_url": web_url,
                        "compression_type": None
                    }
                    # ensure pdf first file
                    if ext == 'pdf':
                        downloadable_items.insert(0, item)
                    else:
                        downloadable_items.append(item)

                doc_name = f'TRADOC {doc_num}'

                fields = {
                    "doc_name": self.clean_name(doc_name),
                    "doc_num": self.ascii_clean(doc_num),
                    "doc_title": self.ascii_clean(doc_title),
                    "doc_type": self.ascii_clean(doc_category),
                    "publication_date": publication_date,
                    "source_page_url": response.url,
                    "downloadable_items": downloadable_items
                }
                yield self.populate_doc_item(fields)

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

    def populate_doc_item(self, fields:dict) -> DocItem:
        display_org = 'United States Army Training and Doctrine Command' # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = 'TRADOC' # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = 'TRADOC Administrative Publications' # Level 3
        office_primary_resp = "Training and Doctrine Command"
        display_doc_type = "Document"
        cac_login_required = False
        is_revoked = False

        doc_name = fields.get('doc_name')
        doc_num = fields.get('doc_num')
        doc_title = fields.get('doc_title')
        doc_type = fields.get('doc_type')
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + " " + doc_title
        source_page_url = fields.get('source_page_url')
        publication_date = fields.get("publication_date")
        downloadable_items = fields.get("downloadable_items")
        download_url = downloadable_items[0]['download_url']
        download_url = download_url.replace(' ', '%20')
        version_hash_fields = {
            "download_url": download_url,
            "doc_name": doc_name,
            "doc_num": doc_num,
            "publication_date": publication_date,
        }
        file_ext = downloadable_items[0]["doc_type"]
        source_fqdn = urlparse(source_page_url).netloc
        version_hash = dict_to_sha256_hex_digest(version_hash_fields)
        access_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f") # T added as delimiter between date and time
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
            file_ext = file_ext,
            is_revoked = is_revoked,
            office_primary_resp = office_primary_resp,
            access_timestamp = access_timestamp
            )