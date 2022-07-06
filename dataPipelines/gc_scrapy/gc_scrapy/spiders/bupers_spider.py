# -*- coding: utf-8 -*-
import scrapy
import re
from urllib.parse import urljoin
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider


class BupersSpider(GCSpider):
    '''
    Class defines the behavior for crawling and extracting text-based documents from the "MyNavy HR" site, "Bureau of Naval Personnel (BUPERS) Instructions" page. 
    This class inherits the 'GCSpider' class from GCSpider.py. The GCSpider class is Gamechanger's implementation of the standard
    parse method used in Scrapy crawlers in order to return a response.

    This class and its methods = the bupers "spider".
    '''

    name = "Bupers_Crawler" # Crawler name
    rotate_user_agent = True # ?
    allowed_domains = ['mynavyhr.navy.mil']
    start_urls = [
        "https://www.mynavyhr.navy.mil/References/Instructions/BUPERS-Instructions/"
    ]

    @staticmethod
    def clean(text):
        '''
        This function cleans unicode characters and encodes to ascii
        '''
        return text.replace('\xa0', ' ').encode('ascii', 'ignore').decode('ascii').strip()

    @staticmethod
    def filter_empty(text_list):
        '''
        This function filters out empty strings from string lists
        '''
        return list(filter(lambda a: a, text_list))

    @staticmethod
    def match_old_doc_name(text):
        '''
        This function normalizes document names, accounting for subtle naming differences in newer 
        versions from older ones.
        '''
        if text == "BUPERSINST BUPERSNOTE 1401":
            return "BUPERSINST BUPERSNOTE1401"

        if text == "BUPERSINST 1510-100":
            return "BUPERSINST 1510100"

        if text == "BUPERSINST 5800.1A CH-1":
            return "BUPERSINST 5800.1A"

        if text == "BUPERSINST 12600.4 CH-1":
            return "BUPERSINST 12600.4CH1"

        if not "1640.20B" in text:
            # turn CH-1 into CH1 to match old doc_names
            doc_name = re.sub(r'CH-(\d)', r'CH\1', text)
            return doc_name
        else:
            return text

    def populate_doc_item(self, hrefs, item_currency, doc_num, doc_title, publication_date):
        '''
        This functions provides both hardcoded and computed values for the variables
        in the imported DocItem object and returns the populated metadata object
        '''
        display_org = "US Navy" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "MyNavy HR" # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Unlisted Source" # Level 3 filter
        doc_type = "BUPERSINST" # The doc_type value is constant for this crawler
        cac_login_required = False # No CAC required for any documents
        downloadable_items = []
        for href in hrefs: 
            file_type = self.get_href_file_extension(href)
            web_url = urljoin(self.start_urls[0], href).replace(' ', '%20')
            downloadable_items.append(
                {
                    "doc_type": file_type,
                    "web_url": web_url,
                    "compression_type": None
                }
            )
        version_hash_fields = {
            "item_currency": item_currency,
            "document_title": doc_title,
            "document_number": doc_num
        }
        doc_name = self.match_old_doc_name(f"{doc_type} {doc_num}")
        return DocItem(
                    doc_name = doc_name,
                    doc_title = doc_title,
                    doc_num = doc_num,
                    doc_type =doc_type,
                    publication_date = publication_date,
                    cac_login_required = cac_login_required,
                    crawler_used = self.name,
                    downloadable_items = downloadable_items,
                    version_hash_raw_data = version_hash_fields,
                    display_org = display_org,
                    data_source = data_source,
                    source_title = source_title,
                )

    def parse(self, response):
        '''
        This function generates a link and metadata for each document found on "MyNavy HR" site's "Bureau of 
        Naval Personnel Instructions" page for use by bash download script.
        '''
        # first 3 rows are what should be header content but are just regular rows, so nth-child used
        rows = response.css("div.livehtml > table > tbody tr:nth-child(n + 4)")

        for row in rows:
            links_raw = row.css('td:nth-child(1) a::attr(href)').getall()
            if not len(links_raw):
                # skip rows without anchor or any downloadable docs
                continue

            # data is nested in a variety of ways, therefore lots of checks
            doc_nums_raw = []
            for selector in ['a strong::text', 'a::text', 'span::text', 'font::text']:
                nums = row.css(f'td:nth-child(1) {selector}').getall()
                if nums is not None:
                    doc_nums_raw += nums

            doc_titles_raw = []
            for selector in ['strong::text', 'span::text', 'font::text']:
                titles = row.css(f'td:nth-child(2) {selector}').getall()
                if titles is not None:
                    doc_titles_raw += titles

            dates_raw = []
            for selector in ['strong::text', 'span::text', 'font::text']:
                dates = row.css(f'td:nth-child(3) {selector}').getall()
                if titles is not None:
                    dates_raw += dates

            # clean unicode and filter out empty strings from lists
            doc_nums_cleaned = self.filter_empty(
                [self.clean(text) for text in doc_nums_raw])
            doc_title = " ".join(self.filter_empty(
                [self.clean(text) for text in doc_titles_raw]))
            dates_cleaned = self.filter_empty(
                [self.clean(text) for text in dates_raw])
            links_cleaned = self.filter_empty(links_raw)

            # happy path, equal num of docs, links, dates (??)
            # some doc nums arent downloadable but have dates
                # special case for equal num but should be a combined doc num with CH-1
            if ((len(doc_nums_cleaned) == len(links_cleaned) == len(dates_cleaned))
                    or (len(dates_cleaned) > len(doc_nums_cleaned))) \
                    and (not 'CH-1' in doc_nums_cleaned):
                for i in range(len(doc_nums_cleaned)):
                    doc_num = doc_nums_cleaned[i]
                    hrefs = [links_cleaned[i]]
                    item_currency = links_cleaned[i].replace(' ', '%20')
                    publication_date = dates_cleaned[i]
                    doc_item = self.populate_doc_item(hrefs, item_currency, doc_num, doc_title, publication_date)
                    yield doc_item

            # doc num was split, combine them into one string
            elif (len(doc_nums_cleaned) > len(dates_cleaned) and len(links_cleaned) == len(dates_cleaned)) \
                    or (any(item in ['Vol 1', 'Vol 2', 'CH-1', 'w/CH-1'] for item in doc_nums_cleaned)):
                # special cases for split names of same doc
                doc_num = " ".join(doc_nums_cleaned)
                hrefs = [links_cleaned[0]]
                item_currency = hrefs
                publication_date = next(iter(dates_cleaned or []), None)
                doc_item = self.populate_doc_item(hrefs, item_currency, doc_num, doc_title, publication_date)
                yield doc_item

            # there are supplemental downloadable items
            elif len(links_cleaned) > len(dates_cleaned):
                doc_num = next(iter(doc_nums_cleaned or []), None)
                hrefs = [href for href in links_cleaned]
                item_currency = next(iter(links_cleaned or []), None)
                publication_date = next(iter(dates_cleaned or []), None)
                doc_item = self.populate_doc_item(hrefs, item_currency, doc_num, doc_title, publication_date)
                yield doc_item
            else:
                raise Exception(
                    'Row data not captured, doesnt match known cases', row)
