# -*- coding: utf-8 -*-
import scrapy
import re
from urllib.parse import urljoin
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider


class BupersSpider(GCSpider):
    name = "Bupers_Crawler"
    allowed_domains = ['mynavyhr.navy.mil']
    start_urls = [
        "https://www.mynavyhr.navy.mil/References/Instructions/BUPERS-Instructions/"
    ]

    doc_type = "BUPERSINST"
    cac_login_required = False

    @staticmethod
    def clean(text):
        return text.replace('\xa0', ' ').encode('ascii', 'ignore').decode('ascii').strip()

    @staticmethod
    def filter_empty(text_list):
        return list(filter(lambda a: a, text_list))

    @staticmethod
    def match_old_doc_name(text):
        # match all these cases where the name came out different
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

    def parse(self, response):
        # first 3 rows are what should be header content but are just regular rows, so nth-child used
        rows = response.css("div.livehtml > table > tbody tr:nth-child(n + 4)")

        for row in rows:
            links_raw = row.css('td:nth-child(1) a::attr(href)').getall()
            if not len(links_raw):
                # skip rows without anchor, no downloadable docs
                continue

            # data is nested in a variety of ways, lots of checks :/
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

            # clean unicode and filter empty strings after
            doc_nums_cleaned = self.filter_empty(
                [self.clean(text) for text in doc_nums_raw])
            doc_title = " ".join(self.filter_empty(
                [self.clean(text) for text in doc_titles_raw]))
            dates_cleaned = self.filter_empty(
                [self.clean(text) for text in dates_raw])
            links_cleaned = self.filter_empty(links_raw)

            # happy path, equal num of docs, links, dates
            if ((len(doc_nums_cleaned) == len(links_cleaned) == len(dates_cleaned))
                    or (len(dates_cleaned) > len(doc_nums_cleaned))) \
                    and (not 'CH-1' in doc_nums_cleaned):
                # some doc nums arent downloadable but have dates
                # special case for equal num but should be a combined doc num with CH-1

                for i in range(len(doc_nums_cleaned)):
                    doc_num = doc_nums_cleaned[i]
                    href = links_cleaned[i]
                    file_type = self.get_href_file_extension(href)

                    web_url = urljoin(
                        self.start_urls[0], href).replace(' ', '%20')
                    downloadable_items = [
                        {
                            "doc_type": file_type,
                            "web_url": web_url,
                            "compression_type": None
                        }
                    ]

                    version_hash_fields = {
                        "item_currency": href.replace(' ', '%20'),
                        "document_title": doc_title,
                        "document_number": doc_num
                    }

                    doc_name = self.match_old_doc_name(
                        f"{self.doc_type} {doc_num}")

                    yield DocItem(
                        doc_name=doc_name,
                        doc_title=doc_title,
                        doc_num=doc_num,
                        publication_date=dates_cleaned[i],
                        downloadable_items=downloadable_items,
                        version_hash_raw_data=version_hash_fields,
                    )

            # doc num was split, combine them into one string
            elif (len(doc_nums_cleaned) > len(dates_cleaned) and len(links_cleaned) == len(dates_cleaned)) \
                    or (any(item in ['Vol 1', 'Vol 2', 'CH-1', 'w/CH-1'] for item in doc_nums_cleaned)):
                # special cases for spit names of same doc

                doc_num = " ".join(doc_nums_cleaned)

                href = links_cleaned[0]
                file_type = self.get_href_file_extension(href)

                web_url = urljoin(self.start_urls[0], href).replace(' ', '%20')
                downloadable_items = [
                    {
                        "doc_type": file_type,
                        "web_url": web_url,
                        "compression_type": None
                    }
                ]

                version_hash_fields = {
                    "item_currency": href,
                    "document_title": doc_title,
                    "document_number": doc_num
                }
                doc_name = self.match_old_doc_name(
                    f"{self.doc_type} {doc_num}")

                publication_date = next(iter(dates_cleaned or []), None)

                yield DocItem(
                    doc_name=doc_name,
                    doc_title=doc_title,
                    doc_num=doc_num,
                    publication_date=publication_date,
                    downloadable_items=downloadable_items,
                    version_hash_raw_data=version_hash_fields,
                )

            # there are supplemental downloadable items
            elif len(links_cleaned) > len(dates_cleaned):
                doc_num = next(iter(doc_nums_cleaned or []), None)
                downloadable_items = []

                for href in links_cleaned:
                    file_type = self.get_href_file_extension(href)
                    web_url = urljoin(
                        self.start_urls[0], href).replace(' ', '%20')
                    downloadable_items.append(
                        {
                            "doc_type": file_type,
                            "web_url": web_url,
                            "compression_type": None
                        }
                    )

                item_currency = next(iter(links_cleaned or []), None)

                version_hash_fields = {
                    "item_currency": item_currency,
                    "document_title": doc_title,
                    "document_number": doc_num
                }

                doc_name = self.match_old_doc_name(
                    f"{self.doc_type} {doc_num}")

                publication_date = next(iter(dates_cleaned or []), None)

                yield DocItem(
                    doc_name=doc_name,
                    doc_title=doc_title,
                    doc_num=doc_num,
                    publication_date=publication_date,
                    downloadable_items=downloadable_items,
                    version_hash_raw_data=version_hash_fields,
                )
            else:
                raise Exception(
                    'Row data not captured, doesnt match known cases', row)
