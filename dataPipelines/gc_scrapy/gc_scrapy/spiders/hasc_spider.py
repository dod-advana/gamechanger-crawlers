from typing import Any, Generator
from urllib.parse import urljoin
from datetime import datetime
import scrapy
from scrapy.http import Response

from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.doc_item_fields import DocItemFields


class HASCSpider(GCSpider):
    """
    As of 06/24/2024
    crawls https://armedservices.house.gov/committee-activity/hearings/all for 179 pdfs (doc_type = Witness Statement)
    """

    # Crawler name
    name = "HASC"
    # Level 1: GC app 'Source' filter for docs from this crawler
    display_org = "Congress"
    # Level 2: GC app 'Source' metadata field for docs from this crawler
    data_source = "House Armed Services Committee Publications"
    # Level 3 filter
    source_title = "House Armed Services Committee"

    allowed_domains = ["armedservices.house.gov"]
    base_url = "https://armedservices.house.gov"
    start_urls = [f"{base_url}/committee-activity/hearings/all?page=0"]

    rotate_user_agent = True
    randomly_delay_request = True
    custom_settings = {
        **GCSpider.custom_settings,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 10,
        "AUTOTHROTTLE_MAX_DELAY": 60,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
    }

    @staticmethod
    def extract_doc_name_from_url(url: str) -> str:
        """Returns a doc name given a full URL"""
        doc_name = url.split("/")[-1]
        doc_name = (
            doc_name.replace(".pdf", "")
            .replace("%", "_")
            .replace(".", "")
            .replace("-", "")
        )
        return doc_name

    def parse(self, response: Response) -> Generator[DocItem, Any, None]:
        """Recursively parses doc items out of House Armed Services Committee site"""
        page_id = int(response.url[-1])
        rows = response.css(".evo-views-row")

        for row in rows:
            try:
                link = row.css("div.h3.mt-0.font-weight-bold a::attr(href)").get()
                if not link:
                    continue

                follow_link = f"{self.base_url}{link}"
                yield scrapy.Request(url=follow_link, callback=self.parse_hearing_page)
            except ValueError as e:
                print(e)

        if len(rows) > 0:
            next_url = f"{response.url[0:-1]}{page_id+1}"
            yield scrapy.Request(url=next_url, callback=self.parse)

    def parse_hearing_page(self, response: Response) -> Generator[DocItem, Any, None]:
        """Parses all statements available given a hearing details page"""
        try:
            # Get the basic details like title and date from the page
            title = self.ascii_clean(response.css("h1.display-4 ::text").get())
            date = datetime.strptime(
                response.css("time ::text").get(), "%a, %m/%d/%Y - %I:%M %p"
            )
            doc_type = "Witness Statement"

            # Extract names of speakers
            names = response.css("b ::text").getall()

            # Find all links and check if they contain the word "statement" and point to a PDF
            for link in response.css("p a"):
                href = link.css("::attr(href)").get()
                if not href or not href.endswith(".pdf"):
                    continue
                # Get the text and convert it to lower case for comparison
                link_text = link.css("::text").get("").lower()
                if "statement" not in link_text:
                    continue

                # Check if any of the speaker names is in the link text
                for name in names:
                    if name.lower() not in link_text:
                        continue

                    follow_link = urljoin(self.base_url, href)
                    display_title = self.ascii_clean(f"HASC {title} - {name}")
                    doc_name = self.extract_doc_name_from_url(follow_link)

                    fields = DocItemFields(
                        doc_name=doc_name,
                        doc_title=title,
                        doc_num=" ",
                        doc_type=doc_type,
                        publication_date=date,
                        cac_login_required=False,
                        source_page_url=response.url,
                        downloadable_items=[
                            {
                                "doc_type": "pdf",
                                "download_url": follow_link,
                                "compression_type": None,
                            }
                        ],
                        download_url=follow_link,
                        file_ext="pdf",
                        display_doc_type=doc_type,
                    )
                    # Match fields to previous crawler iterations
                    fields.remove_version_hash_field("doc_num")
                    fields.set_version_hash_field("doc_title", title)
                    fields.set_display_name(display_title)

                    yield fields.populate_doc_item(
                        display_org=self.display_org,
                        data_source=self.data_source,
                        source_title=self.source_title,
                        crawler_used=self.name,
                    )

        except ValueError as e:
            print(e)
