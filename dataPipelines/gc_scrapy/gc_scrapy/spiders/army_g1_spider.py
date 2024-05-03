from typing import Any, Generator, Union
import re
from bs4 import BeautifulSoup
import html
from datetime import datetime
import scrapy

from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.doc_item_fields import DocItemFields
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem


class ArmyG1Spider(GCSpider):
    """
    As of 05/01/2024
    crawls https://www.army.mil/g-1#org-g-1-publications for 122 pdfs (doc_type = DA PAM)
    """

    # Crawler name
    name = "army_g1_pubs"
    # Level 1: GC app 'Source' filter for docs from this crawler
    display_org = "Dept. of the Army"
    # Level 2: GC app 'Source' metadata field for docs from this crawler
    data_source = "Army Publishing Directorate"
    # Level 3 filter
    source_title = "G-1 Publications"

    start_urls = ["https://www.army.mil/g-1#org-g-1-publications"]
    rotate_user_agent = True
    randomly_delay_request = True
    custom_settings = {
        **GCSpider.custom_settings,
        "DOWNLOAD_DELAY": 5,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1,
        "AUTOTHROTTLE_MAX_DELAY": 10,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
    }

    @staticmethod
    def is_ascii_encoded(text: str) -> bool:
        """Returns true if text is ascii encoded"""
        try:
            text.encode("ascii")
            return False
        except UnicodeEncodeError:
            return True

    @staticmethod
    def extract_doc_name_from_url(url: str) -> str:
        """Parses doc name out of url"""
        doc_name = url.split("/")[-1].split(".")[0]
        return doc_name

    @staticmethod
    def extract_doc_number(text: str):
        """Uses regex to pull doc number from container label"""
        pattern = r"(\d{2,4}-\d{1,4})"
        match = re.search(pattern, text)
        if match:
            return match.group(1)
        return "N/A"

    @staticmethod
    def title_edge_cases(text: str, label: str) -> str:
        """Renames documents if incorrect on website"""
        if "Board Brief; NCO Evaluation Board Supplement" in text:
            return label + " Board Brief"
        if "NCO Evaluation Board Supplement" in text:
            return label
        if text.endswith(".pdf") or text.endswith("docx"):
            return label
        pattern = r"(?:DA\s+)?PAM\s+\d{2,4}-\d{2,4}"
        cleaned_text = re.sub(pattern, "", text)
        stripped_text = cleaned_text.strip()
        if "\\xc2\\xa0" in stripped_text:
            stripped_text = stripped_text.replace("\\xc2\\xa0", " ")
        decoded_text = html.unescape(stripped_text)
        return decoded_text

    @staticmethod
    def extract_date_from_url(url: str) -> Union[datetime, str]:
        """Accepts url then parses and returns a datetime object"""
        pattern = r"(\d{4}/\d{2}/\d{2})"
        match = re.search(pattern, url)
        if match:
            date = match.group(1)
            datetime_ = datetime.strptime(date, "%Y/%m/%d")
            return datetime_
        return "Unknown"

    def parse_anchor_tag(
        self, link: str, text: str, label_text: str, container_label: str, url: str
    ) -> Generator[DocItem, Any, None]:
        """Takes in data from anchor tag element and returns the DocItem"""
        # only consider links that lead to documents
        if link.endswith(".pdf") or link.endswith(".docx"):
            # check if title needs to be encoded before conversion to string
            if self.is_ascii_encoded(text):
                text = str(text.encode("utf-8"))[2:-1]

            # clean data for `fields` dictionary
            doc_title = self.title_edge_cases(text, label_text)
            doc_number = self.extract_doc_number(container_label)
            doc_name = self.extract_doc_name_from_url(link)
            publication_date = self.extract_date_from_url(link)
            # 'pdf' if link.endswith('.pdf'), 'docx' if link.endswith('.docx'), else None
            file_type = self.get_href_file_extension(link)

            downloadable_items = [
                {
                    "doc_type": file_type,
                    "download_url": link,
                    "compression_type": None,
                }
            ]
            fields = DocItemFields(
                doc_name=doc_name,
                doc_title=doc_title,
                doc_num=doc_number,
                doc_type="DA PAM",
                display_doc_type="DA PAM",
                publication_date=publication_date,
                cac_login_required=False,
                source_page_url=url,
                downloadable_items=downloadable_items,
                download_url=link,
                file_ext=file_type,
            )
            # backwards compatability by setting display_title to doc_title in hash fields
            fields.set_version_hash_field("display_title", fields.doc_title)

            yield fields.populate_doc_item(
                display_org=self.display_org,
                data_source=self.data_source,
                source_title=self.source_title,
                crawler_used=self.name,
            )

    def parse(self, response: scrapy.http.Response) -> Generator[DocItem, Any, None]:
        """Parses doc items out of Army G1 Publications site"""
        for container in response.css(".inner-container"):
            # title of each section
            container_label = container.css("h4::text").extract_first()

            for accordion in container.css(".accordion-container"):

                for item in accordion.css(".accordion"):

                    # get title text *within* each accordion tab
                    label_text = item.css("label[for]::text").get().strip()

                    # convert html to string
                    soup = BeautifulSoup(item.get(), "html.parser")
                    div_tag = soup.find("div", class_="rich-text-element bodytext")

                    if div_tag is None:
                        continue
                    # Find all anchor tags
                    anchor_tags = soup.find_all("a")

                    # Extract URLs and text
                    for tag in anchor_tags:
                        link = tag["href"]
                        text = tag.get_text()
                        yield from self.parse_anchor_tag(
                            link, text, label_text, container_label, response.url
                        )
