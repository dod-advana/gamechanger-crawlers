from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
import scrapy
import typing as t

from urllib.parse import urljoin, urlparse
from datetime import datetime
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest, get_pub_date


class SASCSpider(GCSpider):
    name = "SASC" # Crawler name
    
    cac_login_required = False

    base_url = "https://www.armed-services.senate.gov"

    start_urls = [base_url]

    randomly_delay_request = True
    rotate_user_agent = True

    def parse(self, _):
        pages_parser_map = [
            # TODO: after running this in prod remove ?c=all and it automatically only gets the current congress' hearings
            (f"{self.base_url}/hearings?c=all", self.parse_hearings),
            # (f"{self.base_url}/legislation?c=all",)
        ]

        for page_url, parser_func in pages_parser_map:
            yield scrapy.Request(page_url, callback=parser_func)

    @staticmethod
    def get_last_page_number(response) -> str:
        last_page_str = response.css(
            "#main_column > div.pagination-right.pull-left > div > select > option:last-child::text").get()
        return int(last_page_str)

    @staticmethod
    def add_page_num_query_string(url: str, num: int) -> str:
        junction = '&' if '?' in url else '?'
        return f"{url}{junction}PageNum_rs={num}"

    def parse_hearings(self, response):

        last_page_num = self.get_last_page_number(response)

        for num in range(1, last_page_num+1):
            page_url = self.add_page_num_query_string(response.url, num)
            yield scrapy.Request(url=page_url, callback=self.parse_hearings_table_page)

    def parse_hearings_table_page(self, response):
        urls = [
            a.strip() for a in
            response.css(
                "table.table-striped tr.vevent a::attr(href)"
            ).getall()
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse_hearing_detail_page)

    def join_multitext(self, texts: t.List[str]) -> str:
        return ' '.join([self.ascii_clean(t) for t in texts]).strip()

    def follow_pdf_redirect(self, response):
        redirect_doc_href = response.css(
            '#content div.row p a::attr(href)').get()

        downloadable_items = [
            {
                "doc_type": 'pdf',
                "download_url": redirect_doc_href,
                "compression_type": None
            }
        ]

        doc = response.meta['doc']
        doc['downloadable_items'] = downloadable_items
        doc['download_url'] = redirect_doc_href
        doc['version_hash_raw_data']['download_url'] = redirect_doc_href

        yield doc

    def parse_hearing_detail_page(self, response):
        try:
            #########################################################
            # Get the hearing detail page as a document

            main = response.css("div#main_column")
            title = self.join_multitext(
                main.css("h1:nth-child(1) *::text").getall())
            meta = main.css("p.hearing-meta.row")
            # # multiple texts from single elements here, have to join all
            date = self.join_multitext(meta.css("span.date::text").getall())

            hearing_type, _, part_was_found = title.partition(' - ')
            spaced_title = f" - {title}" if title else ""
            base_doc_name = f"{self.name} Hearing{spaced_title}"

            if not part_was_found:
                hearing_type = "Hearing"

            downloadable_items = [
                {
                    "doc_type": 'html',
                    "download_url": response.url,
                    "compression_type": None
                }
            ]

            fields = {
                'doc_name': base_doc_name,
                'doc_title': title,
                'doc_type': hearing_type,
                'display_doc_type':'Hearing',
                'cac_login_required': False,
                'download_url': response.url,
                'source_page_url':response.url,
                'downloadable_items':downloadable_items,
                'publication_date': date
            }
            ## Instantiate DocItem class and assign document's metadata values
            doc_item = self.populate_doc_item(fields)
        
            yield doc_item

            #########################################################
            # Get transcript files from hearing
            asides = response.css("div#asides li.acrobat")
            for aside in asides:
                aside_text = aside.css('a::text').get()
                aside_href = aside.css('a::attr(href)').get()
                aside_doc_name = f"{base_doc_name} - {aside_text}"

                fields = {
                    'doc_name': aside_doc_name,
                    'doc_title': title,
                    'doc_type': hearing_type,
                    'display_doc_type':'Transcript',
                    'cac_login_required': False,
                    'download_url': '',
                    'source_page_url':response.url,
                    'downloadable_items':[],
                    'publication_date': date
                }
                ## Instantiate DocItem class and assign document's metadata values
                aside_doc = self.populate_doc_item(fields)

                yield scrapy.Request(aside_href, callback=self.follow_pdf_redirect, meta={'doc': aside_doc})

            #########################################################
            # Get pdfs of each witness APQ and testimony
            witness_blocks = main.css('section li.vcard')
            for witblock in witness_blocks:
                honorific = self.ascii_clean(
                    witblock.css('span.honorific-prefix::text').get())
                name = ' '.join(witblock.css('span.fn::text').get().split())
                hnr = honorific + ' ' if honorific else ''
                full_name = f"{hnr}{name}"

                witness_docs = witblock.css('li.acrobat')
                for witdoc in witness_docs:
                    witness_text = witdoc.css('a::text').get()
                    witness_href = witdoc.css('a::attr(href)').get()

                    wit_doc_type = 'Advance Policy Questions' if witness_text.endswith(
                        'APQ') else 'Testimony'
                    witness_doc_name = f"{base_doc_name} - {full_name} {wit_doc_type}"

                    full_witness_doc_type = f"{self.name} {hearing_type} {wit_doc_type}"

                    fields = {
                        'doc_name': witness_doc_name,
                        'doc_title': witness_doc_name,
                        'doc_type': full_witness_doc_type,
                        'display_doc_type':wit_doc_type,
                        'cac_login_required': False,
                        'download_url': '',
                        'source_page_url':response.url,
                        'downloadable_items':[],
                        'publication_date': date
                    }

                    witness_doc = self.populate_doc_item(fields)

                    yield scrapy.Request(witness_href, callback=self.follow_pdf_redirect, meta={'doc': witness_doc})

        except Exception as e:
            print(e)


    def populate_doc_item(self, fields):
        '''
        This functions provides both hardcoded and computed values for the variables
        in the imported DocItem object and returns the populated metadata object
        '''
        display_org = "Congress" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "Senate Armed Services Committee Publications" # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Senate Armed Services Committee" # Level 3 filter

        doc_name = fields['doc_name']
        #doc_num = fields['doc_num']
        doc_title = fields['doc_title']
        doc_type = fields['doc_type']
        cac_login_required = fields['cac_login_required']
        download_url = fields['download_url']
        publication_date = get_pub_date(fields['publication_date'])

        display_doc_type = fields['display_doc_type'] # Doc type for display on app
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_title #doc_type + " " + doc_num + " " + doc_title
        is_revoked = False
        access_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f") # T added as delimiter between date and time
        source_page_url = fields['source_page_url']
        source_fqdn = urlparse(source_page_url).netloc

        downloadable_items = fields['downloadable_items']

        ## Assign fields that will be used for versioning
        version_hash_fields = {
            "doc_name":doc_name,
            #"doc_num": doc_num,
            "publication_date": publication_date,
            "download_url": download_url
        }

        version_hash = dict_to_sha256_hex_digest(version_hash_fields)

        return DocItem(
                    doc_name = doc_name,
                    doc_title = doc_title,
                    #doc_num = doc_num,
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