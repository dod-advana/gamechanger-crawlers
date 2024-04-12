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

from urllib.parse import urlparse
import scrapy


class NDAASpider(GCSpider):
    name = "ndaa_fy24"  # Crawler name
    rotate_user_agent = True
    base_url = "https://armedservices.house.gov"
    start_urls = [base_url + "/fy24-ndaa-resources"]

    def parse(self, response: scrapy.http.Response) -> Generator[DocItem, Any, None]:
        page_url = response.url

        soup = bs4.BeautifulSoup(response.body, features="html.parser")
        for link in soup.find_all("a"):
            if link is None:
                continue
            url = link.get("href")
            if url is None:
                continue
            if "fy24-ndaa-subcommittee" in url.lower():
                yield scrapy.Request(
                    url=self.base_url + url,
                    method="GET",
                    callback=self.parse_marks,
                )
            elif "fy24-ndaa-floor-amendment-tracker" in url.lower():
                yield scrapy.Request(
                    url=self.base_url + url,
                    method="GET",
                    callback=self.parse_amendment_tracker,
                )
            elif (
                "news/press-releases/rogers-applauds-committee-passage-fy24-ndaa"
                in url.lower()
            ):
                yield scrapy.Request(
                    url=self.base_url + url,
                    method="GET",
                    callback=self.parse_press_release,
                )
            elif "calendar/byevent" in url.lower():
                yield scrapy.Request(
                    url=url, method="GET", callback=self.parse_amendments_considered
                )
            elif url.lower().endswith("pdf"):
                yield from self.get_doc_from_url(url, page_url)

    def parse_amendment_tracker(
        self, response: scrapy.http.Response
    ) -> Generator[DocItem, Any, None]:
        page_url = response.url
        soup = bs4.BeautifulSoup(response.body, features="html.parser")

        title = self.ascii_clean(soup.find(id="page-title").text)
        date_el = response.css("p:nth-child(2) ::text").get()
        date = self.parse_date(date_el)

        doc_type = self.name
        doc_name = f"{doc_type} - {date} - {title}"

        html_di = [
            {"doc_type": "html", "download_url": page_url, "compression_type": None}
        ]

        fields = {
            "doc_name": doc_name,
            "doc_num": " ",  # No doc num for this crawler
            "doc_title": title,
            "doc_type": doc_type,
            "cac_login_required": False,
            "source_page_url": page_url,
            "download_url": page_url,
            "publication_date": date,
            "display_doc_type": doc_type,
            "downloadable_items": html_di,
        }
        ## Instantiate DocItem class and assign document's metadata values
        doc_item = self.populate_doc_item(fields)

        yield from doc_item

    def parse_press_release(
        self, response: scrapy.http.Response
    ) -> Generator[DocItem, Any, None]:
        page_url = response.url
        soup = bs4.BeautifulSoup(response.body, features="html.parser")

        title = self.ascii_clean(soup.find(id="page-title").text)
        date_el = response.css(".pane-node-created .pane-content ::text").get()
        date = self.parse_date(date_el)

        doc_type = self.name
        doc_name = f"{doc_type} - {date} - {title}"

        html_di = [
            {"doc_type": "html", "download_url": page_url, "compression_type": None}
        ]

        fields = {
            "doc_name": doc_name,
            "doc_num": " ",  # No doc num for this crawler
            "doc_title": title,
            "doc_type": doc_type,
            "cac_login_required": False,
            "source_page_url": page_url,
            "download_url": page_url,
            "publication_date": date,
            "display_doc_type": doc_type,
            "downloadable_items": html_di,
        }
        ## Instantiate DocItem class and assign document's metadata values
        doc_item = self.populate_doc_item(fields)

        yield from doc_item

    def parse_marks(
        self, response: scrapy.http.Response
    ) -> Generator[DocItem, Any, None]:
        page_url = response.url
        soup = bs4.BeautifulSoup(response.body, features="html.parser")

        if "chairman" in page_url.lower():
            date_el = response.css(".pane-node-created .pane-content ::text").get()
            date = self.parse_date(date_el)
        else:
            date_el = response.css(".date-display-single ::text").get()
            date = self.parse_date(date_el)

        yield from self.get_all_pdf(soup, page_url, date)

    def parse_amendments_considered(
        self, response: scrapy.http.Response
    ) -> Generator[DocItem, Any, None]:
        page_url = response.url
        soup = bs4.BeautifulSoup(response.body, features="html.parser")
        yield from self.get_all_pdf(soup, page_url)

    def get_all_pdf(
        self, soup: bs4.BeautifulSoup, page_url: str, date: str = ""
    ) -> Generator[DocItem, Any, None]:
        for link_el in soup.find_all("a"):
            if link_el is None:
                continue
            if date == "":  # need format 2023-06-14T00:00:00
                next_sibling = link_el.find_next_sibling("strong")
                if next_sibling is not None:
                    print(next_sibling.get_text())
            url = link_el.get("href")
            if url is None:
                continue
            if url.lower().endswith("pdf"):
                yield from self.get_doc_from_url(url, page_url, date)

    def find_date(self, text: str) -> str:
        # Example regex pattern for "month day year" format
        # Define a regex pattern to match various date formats
        date_pattern = (
            r"\b(?:\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|"
            r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2},? \d{2,4})\b"
        )
        dates_found = re.findall(date_pattern, text, flags=re.IGNORECASE)
        return dates_found[0]

    def parse_date(self, date_el: str) -> str:
        date = self.find_date(date_el)
        month, day, year = date.strip().split(" ")
        month, day, year = month.strip(), day.strip(), year.strip()
        date = f"{day} {month} {year}"
        date = get_pub_date(date)
        return date

    def get_doc_from_url(
        self, url: str, source_url: str, publication_date: str = ""
    ) -> Generator[DocItem, Any, None]:
        url = self.ascii_clean(url)
        source_url = self.ascii_clean(source_url)
        doc_type = self.name
        doc_num = "0"
        doc_name = (
            url.split("/")[-1].split(".")[-2].replace(" ", "_").replace("%20", "_")
        )
        doc_title = self.name + doc_name

        if url.lower().startswith("http"):
            pdf_url = url
        else:
            pdf_url = self.base_url + url.strip()
        pdf_di = [
            {"doc_type": "pdf", "download_url": pdf_url, "compression_type": None}
        ]

        fields = {
            "doc_name": doc_name.strip(),
            "doc_title": doc_title,
            "doc_num": doc_num,
            "doc_type": doc_type.strip(),
            "display_doc_type": doc_type.strip(),
            "publication_date": publication_date,
            "cac_login_required": False,
            "source_page_url": source_url.strip(),
            "downloadable_items": pdf_di,
            "download_url": pdf_url,
        }
        return self.populate_doc_item(fields)

    def populate_doc_item(self, fields: dict) -> Generator[DocItem, Any, None]:
        display_org = (
            "ndaa_fy24"  # Level 1: GC app 'Source' filter for docs from this crawler
        )
        data_source = "House Armed Services Committee"  # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "House Armed Services Committee"  # Level 3 filter

        doc_name = fields["doc_name"]
        doc_num = fields["doc_num"]
        doc_title = fields["doc_title"]
        doc_type = fields["doc_type"]
        publication_date = fields["publication_date"]
        cac_login_required = fields["cac_login_required"]
        download_url = fields["download_url"]
        display_doc_type = fields["display_doc_type"]
        downloadable_items = fields["downloadable_items"]

        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + ": " + doc_title
        is_revoked = False
        source_page_url = self.start_urls[0]
        source_fqdn = urlparse(source_page_url).netloc
        version_hash_fields = {
            "doc_name": doc_name,
            "doc_num": doc_num,
            "publication_date": publication_date,
            "download_url": download_url,
            "display_title": display_title,
        }
        version_hash = dict_to_sha256_hex_digest(version_hash_fields)

        yield DocItem(
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
            display_title=display_title,
            file_ext=doc_type,
            is_revoked=is_revoked,
        )
