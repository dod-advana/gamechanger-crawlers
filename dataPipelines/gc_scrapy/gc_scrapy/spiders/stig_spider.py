from typing import Any, Generator, Tuple
import scrapy
import re

from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.utils import parse_timestamp

from dataPipelines.gc_scrapy.gc_scrapy.doc_item_fields import DocItemFields


class StigSpider(GCSpider):
    """
    As of 04/30/2024
    crawls https://public.cyber.mil/stigs/downloads/ for 47 pdfs (doc_type = stig)
    """

    name = "stig_pubs"  # Crawler name

    start_urls = ["https://public.cyber.mil/stigs/downloads/"]

    download_base_url = "https://public.cyber.mil/"
    rotate_user_agent = True

    doc_type = "STIG"

    custom_settings = {
        **GCSpider.custom_settings,
        "DOWNLOAD_TIMEOUT": 6.0,
    }

    @staticmethod
    def extract_doc_number(doc_title: str) -> Tuple[str, str]:
        """Accepts doc title and returns a tuple of the doc_title and doc_num"""
        if doc_title.find(" Ver ") != -1:
            ver_num = (re.findall(r" Ver (\w+)", doc_title))[0]
        else:
            if " Version " in doc_title:
                ver_num = (re.findall(r" Version (\w+)", doc_title))[0]
            else:
                ver_num = 0

        if doc_title.find(" Rel ") != -1:
            ref_num = (re.findall(r" Rel (\w+)", doc_title))[0]
        else:
            if "Release Memo" in doc_title:
                ref_num = 1
            else:
                ref_num = 0

        doc_num = f"V{ver_num}R{ref_num}"
        return doc_title, doc_num

    def parse(self, response: scrapy.http.Response) -> Generator[DocItem, Any, None]:
        """Parses doc items out of STIG downloads site"""
        rows = response.css("table tbody tr")
        rows = [a for a in rows if a.css("a::attr(href)").get()]
        rows = [a for a in rows if a.css("a::attr(href)").get().endswith("pdf")]

        for row in rows:
            href_raw = row.css("a::attr(href)").get()
            doc_title_text, publication_date_raw = row.css(
                'span[style="display:none;"] ::text'
            ).getall()
            doc_title = (
                self.ascii_clean(doc_title_text).replace("/ ", " ").replace("/", " ")
            )
            publication_date = self.ascii_clean(publication_date_raw)
            doc_title, doc_num = StigSpider.extract_doc_number(doc_title)
            doc_name = f"{self.doc_type} {doc_num} {doc_title}"

            if "Memo" in doc_title:
                display_doc_type = "Memo"
            else:
                display_doc_type = "STIG"

            file_type = self.get_href_file_extension(href_raw)
            web_url = self.ensure_full_href_url(href_raw, self.download_base_url)

            downloadable_items = [
                {
                    "doc_type": file_type,
                    "download_url": web_url.replace(" ", "%20"),
                    "compression_type": None,
                }
            ]

            doc_item_fields = DocItemFields(
                doc_name=doc_name,
                doc_num=doc_num,
                doc_title=doc_title,
                doc_type=self.doc_type,
                display_doc_type=display_doc_type,
                cac_login_required=False,
                source_page_url=response.url,
                downloadable_items=downloadable_items,
                download_url=web_url,
                publication_date=parse_timestamp(publication_date),
                file_ext=file_type,
            )
            yield doc_item_fields.populate_doc_item(
                display_org="Security Technical Implementation Guides",
                data_source="Security Technical Implementation Guides",
                source_title="Unlisted Source",
                crawler_used=self.name,
            )
