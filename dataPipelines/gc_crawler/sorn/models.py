import bs4
import re
import os
import requests
import json
import datetime
from typing import Iterable

from dataPipelines.gc_crawler.requestors import MapBasedPseudoRequestor
from dataPipelines.gc_crawler.exec_model import Crawler, Parser, Pager
from dataPipelines.gc_crawler.data_model import Document, DownloadableItem
from dataPipelines.gc_crawler.utils import abs_url

from . import SOURCE_SAMPLE_DIR, BASE_SOURCE_URL


class SORNOrderPager(Pager):
    """Pager for Executive Order crawler"""

    def iter_page_links(self) -> Iterable[str]:
        """Iterator for page links"""
        yield self.starting_url


class SORNOrderParser(Parser):
    """Parser for SORN crawler"""

    def parse_docs_from_page(self, page_url: str, page_text: str) -> Iterable[Document]:
        """Parse document objects from page of text"""

        sorns_list = []

        req = requests.get("https://www.federalregister.gov/api/v1/agencies/defense-department")
        agency_list_pull = req.json()['child_slugs']

        conditions = ""
        for item in agency_list_pull:
            conditions = conditions + "&conditions[agencies][]=" + item

        notices = "&conditions[type][]=NOTICE"

        page_size = "400"

        base_url = "https://www.federalregister.gov/api/v1/documents.json?per_page=" + page_size + "&order=newest&conditions[term]=%22Privacy%20Act%20of%201974%22%20%7C%20%22System%20of%20Records%22"
        another_req = requests.get(base_url+conditions+notices)
        response_json = another_req.json()

        sorns_list = response_json['results']

        while 'next_page_url' in response_json:
            next_page = requests.get(response_json['next_page_url']).json()
            sorns_list.extend(next_page['results'])
            response_json = next_page

        parsed_docs = []
        for sorn in sorns_list:

            # DOWNLOAD INFO
            if sorn["pdf_url"]:
                pdf_di = DownloadableItem(doc_type='pdf', web_url=sorn["pdf_url"])
            else:
                pass

            # generate final document object
            doc = Document(
                doc_name="SORN " + sorn["document_number"] ,
                doc_title=sorn["title"] ,
                doc_num=sorn["document_number"] ,
                doc_type="SORN",
                display_doc_type= "SORN",
                display_org= "Dept. of Defense",
                display_source= "Federal Registry",
                publication_date=sorn["publication_date"],
                cac_login_required=False,
                crawler_used="sorn",
                source_page_url=sorn["html_url"],
                version_hash_raw_data={
                    "item_currency": sorn["publication_date"],
                    "version_hash": sorn["document_number"],
                    "public_inspection": sorn["public_inspection_pdf_url"],
                    "title": sorn["title"],
                },
                access_timestamp="{:%Y-%m-%d %H:%M:%S.%f}".format(
                    datetime.datetime.now()
                ),
                source_fqdn="https://www.federalregister.gov",
                downloadable_items=[pdf_di],
            )

            parsed_docs.append(doc)

        return parsed_docs


class SORNOrderCrawler(Crawler):
    """Crawler for the example web scraper"""

    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            **kwargs,
            pager=SORNOrderPager(starting_url=BASE_SOURCE_URL),
            parser=SORNOrderParser(),
        )


class FakeSORNOrderCrawler(Crawler):
    """Executive Order crawler that just uses stubs and local source files"""

    def __init__(self, *args, **kwargs):
        with open(os.path.join(SOURCE_SAMPLE_DIR, 'sorn_orders.html'), encoding='utf-8') as f:
            default_text = f.read()

        super().__init__(
            *args,
            **kwargs,
            pager=SORNOrderPager(
                requestor=MapBasedPseudoRequestor(default_text=default_text),
                starting_url=BASE_SOURCE_URL,
            ),
            parser=SORNOrderParser(),
        )
