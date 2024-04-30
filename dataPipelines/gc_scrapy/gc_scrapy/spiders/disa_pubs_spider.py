from typing import Any, Generator
from urllib.parse import urljoin
from datetime import datetime
import bs4
import scrapy

from dataPipelines.gc_scrapy.gc_scrapy.doc_item_fields import DocItemFields
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider


class DisaPubsSpider(GCSpider):
    """
    As of 04/26/2024
    crawls https://disa.mil/About/DISA-Issuances/Instructions for 42 pdfs (doc_type = Instruction)
    and https://disa.mil/About/DISA-Issuances/Circulars for 6 pdfs (doc_type = Circulars)
    """

    # Crawler name
    name = "DISA_pubs"
    # Level 1: GC app 'Source' filter for docs from this crawler
    display_org = "Defense Information Systems Agency"
    # Level 2: GC app 'Source' metadata field for docs from this crawler
    data_source = "Defense Information Systems Agency"
    # Level 3 filter
    source_title = "DISA Policy/Issuances"

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
        """Parses doc items out of DISA Policy/Issuances site"""
        page_url = response.url
        soup = bs4.BeautifulSoup(response.body, features="html.parser")

        for row in soup.find(id="main-content").find_all("tr"):
            row_items = row.find_all("td")

            # Ensure elements are present and skip the header row
            if len(row_items) != 3:
                continue

            link_cell, title_cell, publication_cell = row_items

            url = urljoin(self.base_url, link_cell.find("a").get("href"))
            doc_name = self.ascii_clean(link_cell.find("a").get_text().strip())

            pdf_di = [
                {"doc_type": "pdf", "download_url": url, "compression_type": None}
            ]

            fields = DocItemFields(
                doc_name=doc_name,
                doc_title=self.ascii_clean(title_cell.get_text().strip()),
                doc_num=doc_name.split(" ")[-1],
                doc_type=self.get_doc_type(doc_name),
                publication_date=self.extract_date(publication_cell.get_text()),
                cac_login_required=False,
                source_page_url=page_url,
                downloadable_items=pdf_di,
                download_url=url,
                file_ext="pdf",
            )
            fields.set_display_name(f"{fields.doc_name}: {fields.doc_title}")

            yield fields.populate_doc_item(
                display_org=self.display_org,
                data_source=self.data_source,
                source_title=self.source_title,
                crawler_used=self.name,
            )

    def extract_date(self, input_date: str) -> datetime:
        """Takes in dates formatted as 03/17/17 or 04/15/ 13 and returns datetime object"""
        published = input_date.strip().replace(" ", "")
        published_timestamp = datetime.strptime(published, self.date_format)
        return published_timestamp

    def get_doc_type(self, doc_name: str) -> str:
        """Takes in the doc name and returns the type, only handles DISAC and DISAI docs"""
        if "DISAC" in doc_name:
            return "Circular"
        if "DISAI" in doc_name:
            return "Instruction"
        raise ValueError(f"Unexpected value for doc_name {doc_name}")
