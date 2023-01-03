# -*- coding: utf-8 -*-
import scrapy
import re
from urllib.parse import urljoin, urlparse
from datetime import datetime
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest
from dataPipelines.gc_scrapy.gc_scrapy.utils import parse_timestamp
import pandas as pd #############


class BupersSpider(GCSpider):
    '''
    Class defines the behavior for crawling and extracting text-based documents from the "MyNavy HR" site, "Bureau of Naval Personnel (BUPERS) Instructions" page. 
    This class inherits the 'GCSpider' class from GCSpider.py. The GCSpider class is Gamechanger's implementation of the standard
    parse method used in Scrapy crawlers in order to return a response.

    This class and its methods = the bupers "spider".
    '''
    name = "Bupers_Crawler" # Crawler name
    rotate_user_agent = True #
    allowed_domains = ['mynavyhr.navy.mil']
    start_urls = [
        "https://www.mynavyhr.navy.mil/References/Instructions/BUPERS-Instructions/"
    ]
    v_list = iter(['Vol 1', 'Vol 2'])

    @staticmethod
    def clean(text):
        """This function cleans unicode characters and encodes to ascii"""
        return text.replace('\xa0', ' ').encode('ascii', 'ignore').decode('ascii').strip()

    @staticmethod
    def filter_empty(text_list):
        """This function filters out empty strings from string lists"""
        return list(filter(lambda a: a, text_list))

    @staticmethod
    def merge_suffix(doc_nums):
        """This function merges suffixes to doc_nums that erroneously parse as separate doc_num"""
        if len(doc_nums) > 1 and doc_nums[-2] in ['A']:
            doc_nums = [''.join(doc_nums[:2])] + doc_nums[-1:]
        if doc_nums[-1] in ['w/CH-1']:
            return doc_nums[:-2] + [' '.join(doc_nums[-2:])]
        elif doc_nums[-1] in ['A','B','C']:
            return doc_nums[:-2] + [''.join(doc_nums[-2:])]
        elif "- Cancellation" in doc_nums[-1]:
            return [' '.join(i for i in doc_nums)]
        else:
            return doc_nums

    def dedup_nums(self, doc_nums):
        """This function removes duplicate doc_nums from doc_nums_raw list generated in the parse function
        and processed through the merge_suffix function.
        """
        if len(doc_nums) > 1:
            return [i for i in doc_nums if "CH-1" in i]
        elif doc_nums[0] == '1750.10D':
            doc_nums[0] = '1750.10D ' + next(self.v_list, None)
            return doc_nums
        else:
            return doc_nums

    @staticmethod
    def latest_date(dates):
        """This function filters out irrelevant date values; takes the latest date as publication_date"""
        dates = [date for date in dates if date != '0'] # Filter out hidden '0' date value
        return dates[-1]
    
    @staticmethod
    def dedup_link(links):
        """This function eliminates superfluous links grabed by the parse function"""
        links = [link for link in links if 'Supplement to BUPERSINST 1730.11' not in link] # Special case: exclude one superfluous link
        return links[-1].replace(' ', '%20') # Take last link in list; replace spaces in url with '%20'

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

    def get_downloadables(self, href):
        """This function creates a list of downloadable_items dictionaries from a document link"""
        file_type = self.get_href_file_extension(href)
        web_url = urljoin(self.start_urls[0], href)
        downloadable_items = {
            "doc_type": file_type,
            "download_url": web_url,
            "compression_type": None
            }
        return [downloadable_items]

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
            doc_nums_cleaned = self.merge_suffix(doc_nums_cleaned)
            doc_nums_cleaned = self.dedup_nums(doc_nums_cleaned)
            doc_title = " ".join(self.filter_empty(
                [self.clean(text) for text in doc_titles_raw]))
            dates_cleaned = self.filter_empty(
                [self.clean(text) for text in dates_raw])
            publication_date = self.latest_date(dates_cleaned)
            links_cleaned = self.filter_empty(links_raw)
            href = self.dedup_link(links_cleaned)

            fields = {
                "doc_num": self.ascii_clean(doc_nums_cleaned[0]),
                "doc_title": self.ascii_clean(doc_title),
                "publication_date": publication_date,
                "source_page_url": response.url,
                "href": href
                }
            yield self.populate_doc_item(fields)


    def populate_doc_item(self, fields):
        '''
        This functions provides both hardcoded and computed values for the variables
        in the imported DocItem object and returns the populated metadata object
        '''
        doc_type = "BUPERSINST" # The doc_type value is constant for this crawler
        display_org = "US Navy" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "MyNavy HR" # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Bureau of Naval Personnel Instructions" # Level 3 filter
        display_doc_type = "Document" # Doc type for display on app
        cac_login_required = False
        is_revoked = False

        display_source = data_source + " - " + source_title
        source_page_url = fields.get('source_page_url')
        publication_date = fields.get("publication_date")
        doc_num = fields.get("doc_num")
        doc_title = fields.get("doc_title")
        display_title = doc_type + " " + doc_num + ": " + doc_title
        doc_name = self.match_old_doc_name(f"{doc_type} {doc_num}")
        download_url = "https://www." + self.allowed_domains[0] + fields.get("href")
        downloadable_items = self.get_downloadables(fields.get("href"))
        version_hash_fields = {
            "download_url": download_url,
            "doc_name": doc_name,
            "doc_num": doc_num,
            "publication_date": publication_date,
            "display_title": display_title
        }
        file_ext = downloadable_items[0]["doc_type"]
        source_fqdn = urlparse(source_page_url).netloc
        version_hash = dict_to_sha256_hex_digest(version_hash_fields)
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
            )