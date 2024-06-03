from typing import Any, Generator
from urllib.parse import urljoin
import bs4
import scrapy

from dataPipelines.gc_scrapy.gc_scrapy.utils import parse_timestamp
from dataPipelines.gc_scrapy.gc_scrapy.doc_item_fields import DocItemFields
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider


class UFCSpider(GCSpider):
    """
    As of 05/29/2024
    crawls https://www.wbdg.org/ffc/dod/unified-facilities-criteria-ufc for 168 pdfs (doc_type = Document)
    """

    # Crawler name
    name = "UFC"
    # Level 1: GC app 'Source' filter for docs from this crawler
    display_org = "Department of Defense"
    # Level 2: GC app 'Source' metadata field for docs from this crawler
    data_source = "Whole Building Design Guide"
    # Level 3 filter
    source_title = "Unified Facilities Criteria"

    domain = "wbdg.org"
    base_url = f"https://{domain}"
    allowed_domains = [domain]
    start_urls = [urljoin(base_url, "/ffc/dod/unified-facilities-criteria-ufc")]

    def parse(self, response: scrapy.http.Response) -> Generator[DocItem, Any, None]:
        """Parses UFC doc items out of Whole Building Design Guide site"""
        page_url = response.url
        yield scrapy.Request(
            url=page_url, callback=self.parse_table, meta={"page_id": 0}
        )

    def parse_table(
        self, response: scrapy.http.Response
    ) -> Generator[DocItem, Any, None]:
        """Recursively parses each table in the main content"""
        page_id = response.meta["page_id"]
        soup = bs4.BeautifulSoup(
            response.xpath('//*[@id="block-system-main"]/div').get(),
            features="html.parser",
        )
        table_body = soup.find("tbody")

        if table_body is not None:
            for row in table_body.find_all("tr"):

                cells = row.find_all("td")

                try:
                    full_title = cells[0].get_text().strip()
                    doc_num = full_title.split(" ")[1]
                    doc_title = " ".join(full_title.split(" ")[2:])
                    publication_date = parse_timestamp(cells[1].get_text().strip())
                    url = cells[3].find("a").get("href")
                except AttributeError as e:
                    print(e)
                    continue

                fields = DocItemFields(
                    doc_name=full_title,
                    doc_title=self.ascii_clean(doc_title),
                    doc_num=doc_num,
                    doc_type="Document",
                    publication_date=publication_date,
                    cac_login_required=False,
                    source_page_url=url,
                    downloadable_items=[
                        {
                            "doc_type": "pdf",
                            "download_url": url,
                            "compression_type": None,
                        }
                    ],
                    download_url=url,
                    file_ext="pdf",
                )
                fields.set_display_name(full_title)

                yield fields.populate_doc_item(
                    display_org=self.display_org,
                    data_source=self.data_source,
                    source_title=self.source_title,
                    crawler_used=self.name,
                )

            page_id += 1
            yield scrapy.Request(
                url=urljoin(self.start_urls[0], f"?page={page_id}"),
                callback=self.parse_table,
                meta={"page_id": page_id},
            )
