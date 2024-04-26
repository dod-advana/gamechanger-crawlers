# -*- coding: utf-8 -*-
from typing import Any, Generator
import bs4
import re
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.utils import (
    dict_to_sha256_hex_digest,
    get_pub_date,
)
from urllib.parse import urljoin
from datetime import datetime

from urllib.parse import urlparse
import scrapy


class DisaPubsSpider(GCSpider):
    """
    As of 04/26/2024
    crawls https://disa.mil/About/DISA-Issuances/Instructions for 42 pdfs (doc_type = Instruction)
    and https://disa.mil/About/DISA-Issuances/Circulars for 6 pdfs (doc_type = Circulars)
    """

    name = "DISA_pubs"  # Crawler name
    rotate_user_agent = True
    date_format = "%m/%d/%y"

    domain = "disa.mil"
    base_url = f"https://{domain}"
    allowed_domains = [domain]
    start_urls = [
        f"{base_url}/About/DISA-Issuances/Instructions",
        f"{base_url}/About/DISA-Issuances/Circulars",
    ]

    def parse(self, response: scrapy.http.Response) -> Generator[DocItem, Any, None]:
        page_url = response.url
        soup = bs4.BeautifulSoup(response.body, features="html.parser")
        main_content = soup.find(id="main-content")

        for row in main_content.find_all("tr"):
            row_items = row.find_all("td")

            if len(row_items) != 3:  # Ensure elements are present and skip header row
                continue

            link_cell, title_cell, publication_cell = row_items

            url = self.base_url + link_cell.find("a").get("href")
            doc_name = self.ascii_clean(link_cell.find("a").get_text().strip())
            doc_num = doc_name.split(" ")[-1]
            doc_type = self.get_doc_type(doc_name)

            doc_title = self.ascii_clean(title_cell.get_text().strip())

            published_date = self.format_publication_date(publication_cell.get_text())

            pdf_di = [
                {"doc_type": "pdf", "download_url": url, "compression_type": None}
            ]

            fields = {
                "doc_name": doc_name,
                "doc_title": doc_title,
                "doc_num": doc_num,
                "doc_type": doc_type,
                "display_doc_type": doc_type,
                "publication_date": published_date,
                "cac_login_required": False,
                "source_page_url": page_url,
                "downloadable_items": pdf_di,
                "download_url": url,
                "file_ext": "pdf",
            }

            yield self.populate_doc_item(fields)

    def format_publication_date(self, input_date: str) -> str:
        # dates formatted as 03/17/17 and one has an accidental space (04/15/ 13)
        published = input_date.strip().replace(" ", "")
        published_timestamp = datetime.strptime(published, self.date_format)
        published_date = published_timestamp.strftime("%Y-%m-%dT%H:%M:%S")
        return published_date

    def get_doc_type(self, doc_name: str) -> str:
        if "DISAC" in doc_name:
            return "Circular"
        elif "DISAI" in doc_name:
            return "Instruction"
        else:
            raise ValueError(f"Unexpected value for doc_name {doc_name}")

    def populate_doc_item(self, fields: dict) -> DocItem:
        display_org = "Defense Information Systems Agency"  # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "Defense Information Systems Agency"  # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "DISA Policy/Issuances"  # Level 3 filter

        doc_name = fields["doc_name"]
        doc_num = fields["doc_num"]
        doc_title = fields["doc_title"]
        doc_type = fields["doc_type"]
        publication_date = fields["publication_date"]
        cac_login_required = fields["cac_login_required"]
        download_url = fields["download_url"]
        display_doc_type = fields["display_doc_type"]
        downloadable_items = fields["downloadable_items"]
        file_ext = fields["file_ext"]
        source_page_url = fields["source_page_url"]

        display_source = data_source + " - " + source_title
        is_revoked = False
        source_fqdn = urlparse(source_page_url).netloc
        version_hash_fields = {
            "doc_name": doc_name,
            "doc_num": doc_num,
            "publication_date": publication_date,
            "download_url": download_url,
            "display_title": doc_title,
        }
        version_hash = dict_to_sha256_hex_digest(version_hash_fields)

        return DocItem(
            doc_name=doc_name,
            doc_title=doc_title,
            doc_num=doc_num,
            doc_type=doc_type,
            display_doc_type=display_doc_type,
            publication_date=publication_date,
            cac_login_required=cac_login_required,
            crawler_used=self.name,
            downloadable_items=downloadable_items,
            source_page_url=source_page_url,
            source_fqdn=source_fqdn,
            download_url=download_url,
            version_hash_raw_data=version_hash_fields,
            version_hash=version_hash,
            display_org=display_org,
            data_source=data_source,
            source_title=source_title,
            display_source=display_source,
            display_title=doc_title,
            file_ext=file_ext,
            is_revoked=is_revoked,
        )
