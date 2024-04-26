# -*- coding: utf-8 -*-
from typing import Any, Generator
import bs4
import scrapy
from urllib.parse import urljoin
from datetime import datetime

from dataPipelines.gc_scrapy.gc_scrapy.doc_item_fields import DocItemFields
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider


class DisaPubsSpider(GCSpider):
    """
    As of 04/26/2024
    crawls https://disa.mil/About/DISA-Issuances/Instructions for 42 pdfs (doc_type = Instruction)
    and https://disa.mil/About/DISA-Issuances/Circulars for 6 pdfs (doc_type = Circulars)
    """

    name = "DISA_pubs"  # Crawler name
    display_org = "Defense Information Systems Agency"  # Level 1: GC app 'Source' filter for docs from this crawler
    data_source = "Defense Information Systems Agency"  # Level 2: GC app 'Source' metadata field for docs from this crawler
    source_title = "DISA Policy/Issuances"  # Level 3 filter

    domain = "disa.mil"
    base_url = f"https://{domain}"
    allowed_domains = [domain]
    start_urls = [
        urljoin(base_url, "/About/DISA-Issuances/Instructions"),
        urljoin(base_url, "/About/DISA-Issuances/Circulars"),
    ]

    rotate_user_agent = True
    date_format = "%m/%d/%y"

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

            fields = DocItemFields(
                doc_name=doc_name,
                doc_title=doc_title,
                doc_num=doc_num,
                doc_type=doc_type,
                publication_date=published_date,
                cac_login_required=False,
                source_page_url=page_url,
                downloadable_items=pdf_di,
                download_url=url,
                file_ext="pdf",
            )

            yield fields.populate_doc_item(
                display_org=self.display_org,
                data_source=self.data_source,
                source_title=self.source_title,
                crawler_used=self.name,
            )

    def format_publication_date(self, input_date: str) -> datetime:
        # dates formatted as 03/17/17 and one has an accidental space (04/15/ 13)
        published = input_date.strip().replace(" ", "")
        published_timestamp = datetime.strptime(published, self.date_format)
        return published_timestamp

    def get_doc_type(self, doc_name: str) -> str:
        if "DISAC" in doc_name:
            return "Circular"
        elif "DISAI" in doc_name:
            return "Instruction"
        else:
            raise ValueError(f"Unexpected value for doc_name {doc_name}")
