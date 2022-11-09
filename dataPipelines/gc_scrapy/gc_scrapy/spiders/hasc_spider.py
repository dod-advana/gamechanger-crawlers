from calendar import day_abbr
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
import scrapy
import typing as t

from urllib.parse import urljoin, urlparse
from datetime import datetime
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest, get_pub_date


class HASCSpider(GCSpider):
    name = "HASC" # Crawler name

    base_url = "https://armedservices.house.gov"

    start_urls = [base_url]

    randomly_delay_request = True
    rotate_user_agent = True

    def parse(self, _):
        pages_parser_map = [
            (f"{self.base_url}/hearings", self.recursive_parse_hearings),
            # (f"{self.base_url}/legislation",) # setup for future crawlers if needed
        ]

        for page_url, parser_func in pages_parser_map:
            yield scrapy.Request(page_url, callback=parser_func)

    @staticmethod
    def get_next_relative_url(response):
        return response.css('div#copy-content div.navbar-inner a:nth-child(3):not(.disabled)::attr(href)').get()

    def recursive_parse_hearings(self, response):

        yield from self.parse_hearings_table_page(response)

        next_relative_url = self.get_next_relative_url(response)
        if next_relative_url:
            next_url = f"{self.base_url}{next_relative_url}"
            yield scrapy.Request(url=next_url, callback=self.recursive_parse_hearings)

    def parse_hearings_table_page(self, response):

        rows = response.css(
            "div#copy-content div.recordsContainer table tbody tr")

        for row in rows:
            try:
                link = row.css("td:nth-child(2) a::attr(href)").get()

                if not link:
                    continue

                follow_link = f"{self.base_url}{link}"
                yield scrapy.Request(url=follow_link, callback=self.parse_hearing_detail_page)
            except Exception as e:
                print(f'Error following url: {follow_link}', e)

    def parse_hearing_detail_page(self, response):
        try:

            header = response.css(
                "div#copy-content div.header")
            title = self.ascii_clean(header.css("h1 a::text").get())

            date_el = header.css("span.date")
            month = date_el.css('span.month::text').get()
            day = date_el.css('span.day::text').get()
            year = date_el.css('span.year::text').get()

            date = f"{month} {day}, {year}"

            permalink = response.css(
                'div#copy-content div.foot p.permalink a::attr(href)').get()

            doc_type = "HASC Hearing"
            doc_name = f"{doc_type} - {date} - {title}"

            version_hash_raw_data = {
                'item_currency': permalink,
            }

            fields = {
                'doc_name': doc_name,
                #'doc_num': doc_num, # No doc num for this crawler
                'doc_title': title,
                'doc_type': doc_type,
                'cac_login_required': False,
                'source_page_url': permalink,
                'download_url': permalink,
                'publication_date': date
            }
            ## Instantiate DocItem class and assign document's metadata values
            doc_item = self.populate_doc_item(fields)
        
            yield doc_item

        except Exception as e:
            print(e)



    def populate_doc_item(self, fields):
        '''
        This functions provides both hardcoded and computed values for the variables
        in the imported DocItem object and returns the populated metadata object
        '''
        display_org = "Congress" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "House Armed Services Committee Publications" # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "House Armed Services Committee" # Level 3 filter

        doc_name = fields['doc_name']
        #doc_num = fields['doc_num']
        doc_title = fields['doc_title']
        doc_type = fields['doc_type']
        cac_login_required = fields['cac_login_required']
        download_url = fields['download_url']
        publication_date = get_pub_date(fields['publication_date'])

        display_doc_type = "Hearing" # Doc type for display on app
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_title
        is_revoked = False
        source_page_url = fields['source_page_url']
        source_fqdn = urlparse(source_page_url).netloc

        downloadable_items = [{
                "doc_type": "html",
                "download_url": download_url,
                "compression_type": None,
            }]
        file_ext = downloadable_items[0]["doc_type"]
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
                    file_ext = file_ext, #
                    is_revoked = is_revoked, #
                )