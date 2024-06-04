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
    crawls https://www.wbdg.org/ffc/dod/unified-facilities-guide-specifications-ufgs for 168 pdfs (doc_type = Document)
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
    start_urls = [
        urljoin(base_url, "/ffc/dod/unified-facilities-criteria-ufc"),
        urljoin(base_url, "/ffc/dod/unified-facilities-guide-specifications-ufgs"),
    ]

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
        page_url = response.url
        page_id = response.meta["page_id"]
        soup = bs4.BeautifulSoup(
            response.xpath('//*[@id="block-system-main"]/div').get(),
            features="html.parser",
        )
        table_body = soup.find("tbody")

        if table_body is not None:
            for row in table_body.find_all("tr"):

                cells = row.find_all("td")
                doc_url = cells[0].find("a").get("href")
                if doc_url:
                    if "fc-2-000-05n" in doc_url:
                        yield scrapy.Request(
                            url=urljoin(self.base_url, doc_url),
                            callback=self.parse_fc_2,
                        )
                    if "ufgs-changes-and-revisions" in doc_url:
                        yield scrapy.Request(
                            url=urljoin(self.base_url, doc_url),
                            callback=self.parse_changes_revisions,
                            meta={"page_id": 0},
                        )
                    else:
                        yield scrapy.Request(
                            url=urljoin(self.base_url, doc_url),
                            callback=self.parse_doc_page,
                        )

            page_id += 1
            url = page_url.split("?")[0]
            yield scrapy.Request(
                url=urljoin(url, f"?page={page_id}"),
                callback=self.parse_table,
                meta={"page_id": page_id},
            )

    def parse_doc_page(
        self, response: scrapy.http.Response
    ) -> Generator[DocItem, Any, None]:
        """Get doc data from doc's page instead of table"""
        soup = bs4.BeautifulSoup(
            response.xpath('//*[@id="main"]').get(),
            features="html.parser",
        )
        full_title = soup.find("h1").get_text().strip()
        split_title = full_title.split(" ")
        acronym = split_title[0].strip()
        if acronym in ["FC", "UFC"]:
            doc_num = split_title[1]
            doc_title = " ".join(split_title[2:])
        # Added split_title[1].isdigit() to send UFGS Master to else statement
        elif acronym == "UFGS" and split_title[1].isdigit():
            title_start_idx = 4
            if split_title[title_start_idx].isdigit():
                title_start_idx = 5
            doc_num = " ".join(split_title[1:title_start_idx])
            doc_title = " ".join(split_title[title_start_idx:])
        else:
            doc_num = " "
            doc_title = full_title

        soup = bs4.BeautifulSoup(
            response.xpath(
                '//*[(@id = "block-system-main")]//*[contains(concat( " ", @class, " " ), concat( " ", "content", " " ))] '
            ).get(),
            features="html.parser",
        )

        date_div = soup.find("div", text="Date: ")
        publication_date = None
        if date_div:
            publication_date = date_div.find_next_sibling("div").get_text().strip()

        change_date_div = soup.find("div", text="Change / Revision Date: ")
        if change_date_div:
            publication_date = (
                change_date_div.find_next_sibling("div").get_text().strip()
            )

        status_div = soup.find("div", text="Status: ")
        status = "Active"
        if status_div:
            status = status_div.find_next_sibling("div").get_text().strip()
            if status == "Inactive":
                return None

        download_div = soup.find("div", text="View/Download: ")
        if not download_div:
            return None

        links = download_div.find_next_sibling("div").find_all("a")
        download_url = ""
        for link in links:
            url = link.get("href")
            if url.endswith(".pdf"):
                download_url = url
        if download_url == "":
            return None

        fields = DocItemFields(
            doc_name=full_title,
            doc_title=self.ascii_clean(doc_title),
            doc_num=doc_num,
            doc_type="Document",
            publication_date=parse_timestamp(publication_date),
            cac_login_required=False,
            source_page_url=response.url,
            downloadable_items=[
                {
                    "doc_type": "pdf",
                    "download_url": download_url,
                    "compression_type": None,
                }
            ],
            download_url=download_url,
            file_ext="pdf",
        )
        fields.set_display_name(full_title)

        yield fields.populate_doc_item(
            display_org=self.display_org,
            data_source=self.data_source,
            source_title=self.source_title,
            crawler_used=self.name,
        )

    def parse_changes_revisions(
        self, response: scrapy.http.Response
    ) -> Generator[DocItem, Any, None]:
        """
        Find all urls in the table from
        https://wbdg.org/ffc/dod/unified-facilities-guide-specifications-ufgs/ufgs-changes-revisions
        """
        page_url = response.url
        page_id = response.meta["page_id"]
        content = response.xpath('//*[@id="block-system-main"]/div/div/div[3]').get()
        if content is None:
            return None
        soup = bs4.BeautifulSoup(
            content,
            features="html.parser",
        )
        table_body = soup.find("tbody")

        if table_body is not None:
            for row in table_body.find_all("tr"):

                cells = row.find_all("td")
                doc_url = cells[2].find("a").get("href")
                yield scrapy.Request(
                    url=urljoin(self.base_url, doc_url),
                    callback=self.parse_doc_page,
                )

            page_id += 1
            url = page_url.split("?")[0]
            yield scrapy.Request(
                url=urljoin(url, f"?page={page_id}"),
                callback=self.parse_changes_revisions,
                meta={"page_id": page_id},
            )

    def parse_fc_2(
        self, response: scrapy.http.Response
    ) -> Generator[DocItem, Any, None]:
        """
        Parses docs from table in
        https://wbdg.org/ffc/dod/unified-facilities-criteria-ufc/fc-2-000-05n
        """
        content = response.xpath(
            '//*[@id="node-6064"]/div/div/div[3]/div/div/table'
        ).get()
        if content is None:
            return None
        soup = bs4.BeautifulSoup(
            content,
            features="html.parser",
        )
        table_body = soup.find("tbody")

        if table_body is not None:
            for row in table_body.find_all("tr"):

                try:
                    cells = row.find_all("td")
                    doc_title = self.ascii_clean(cells[0].get_text().strip())
                    publication_date = cells[1].get_text().strip()
                    download_url = urljoin(
                        self.base_url, cells[2].find("a").get("href")
                    )
                except IndexError as e:
                    print(e)
                    continue

            fields = DocItemFields(
                doc_name=doc_title,
                doc_title=doc_title,
                doc_num=" ",
                doc_type="Document",
                publication_date=parse_timestamp(publication_date),
                cac_login_required=False,
                source_page_url=response.url,
                downloadable_items=[
                    {
                        "doc_type": "pdf",
                        "download_url": download_url,
                        "compression_type": None,
                    }
                ],
                download_url=download_url,
                file_ext="pdf",
            )
            fields.set_display_name(doc_title)

            yield fields.populate_doc_item(
                display_org=self.display_org,
                data_source=self.data_source,
                source_title=self.source_title,
                crawler_used=self.name,
            )
