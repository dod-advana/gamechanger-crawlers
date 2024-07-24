import re
import bs4
from scrapy import Request

from dataPipelines.gc_scrapy.gc_scrapy.doc_item_fields import DocItemFields
from dataPipelines.gc_scrapy.gc_scrapy.GCSeleniumSpider import GCSeleniumSpider
from dataPipelines.gc_scrapy.gc_scrapy.utils import abs_url, parse_timestamp


class IcPoliciesSpider(GCSeleniumSpider):
    """
    As of 05/15/2024
    crawls https://www.dni.gov/index.php/what-we-do/ic-related-menus/ic-related-links/intelligence-community-directives for 70 pdfs (doc_type = ICD)
    and https://www.dni.gov/index.php/what-we-do/ic-related-menus/ic-related-links/intelligence-community-policy-guidance for 29 pdfs (doc_type = ICPG)
    and https://www.dni.gov/index.php/what-we-do/ic-related-menus/ic-related-links/intelligence-community-policy-memorandums for 5 pdfs (doc_type = ICPG)
    and https://www.dni.gov/index.php/who-we-are/organizations/ogc/ogc-related-menus/ogc-related-content/ic-legal-reference-book for 1 pdf (doc_type = ICLR)
    """

    # Crawler name
    name = "ic_policies"
    # Level 1: GC app 'Source' filter for docs from this crawler
    display_org = "Intelligence Community"
    # Level 2: GC app 'Source' metadata field for docs from this crawler
    data_source = "Office of Director of National Intelligence"
    # Level 3 filter
    source_title = "Unlisted Source"

    allowed_domains = ["www.dni.gov"]
    base_url = "https://www.dni.gov"
    links_path = "index.php/what-we-do/ic-related-menus/ic-related-links"
    start_urls = [
        f"{base_url}/{links_path}/intelligence-community-directives",
        f"{base_url}/{links_path}/intelligence-community-policy-guidance",
        f"{base_url}/{links_path}/intelligence-community-policy-memorandums",
        f"{base_url}/{links_path}/intelligence-community-policy-framework-on-commercially-available-information",
        f"{base_url}/index.php/who-we-are/organizations/ogc/ogc-related-menus/ogc-related-content/ic-legal-reference-book",
    ]

    rotate_user_agent = True
    randomly_delay_request = range(2, 6)
    headers = {
        "Host": "www.dni.gov",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "DNT": "1",
        "Sec-GPC": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Priority": "u=0, i",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
        "TE": "trailers",
    }
    custom_settings = {
        **GCSeleniumSpider.custom_settings,
        "DOWNLOAD_TIMEOUT": 7.0,
        "DOWNLOAD_DELAY": 5,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1,
        "AUTOTHROTTLE_MAX_DELAY": 10,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
    }

    @staticmethod
    def get_display_doc_type(doc_type: str) -> str:
        """Returns value for display_doc_type based on doc_type -> display_doc_type mapping"""
        display_type_dict = {"icd": "Directive", "icpg": "Guide", "icpm": "Manual"}
        if doc_type.lower() in display_type_dict:
            return display_type_dict[doc_type.lower()]
        return "Document"

    @staticmethod
    def get_doc_type(url: str) -> str:
        """Set doc type from URL"""
        if url.endswith("directives"):
            return "ICD"
        if url.endswith("guidance"):
            return "ICPG"
        if url.endswith("memorandums"):
            return "ICPM"
        return "ICLR"

    @staticmethod
    def get_policy_doc_info(data: str) -> tuple:
        """Get doc info for ICD, ICPG, and ICPM docs"""
        name_pattern = re.compile(r"^[A-Z]*\s\d*.\d*.\d*.\d*\s")
        names = re.findall(name_pattern, data)
        parsed_text = names[0]
        parsed_name = parsed_text.split(" ")

        doc_name = " ".join(parsed_name[:2])
        doc_num = parsed_name[1]
        doc_title = re.sub(parsed_text, "", data)
        return doc_name, doc_num, doc_title

    @staticmethod
    def get_legal_doc_info(data: str) -> tuple:
        """Get doc info for legal reference"""
        split_data = data.split(" ")
        doc_name = " ".join(split_data[:-1])
        doc_num = split_data[-1]
        doc_title = doc_name
        return doc_name, doc_num, doc_title

    @staticmethod
    def is_cac_required(pdf_url: str, doc_title: str) -> bool:
        """Return true if cac is required for access"""
        cac_required = ["CAC", "PKI certificate required", "placeholder", "FOUO"]
        return (
            True
            if any(x in pdf_url for x in cac_required)
            or any(x in doc_title for x in cac_required)
            else False
        )

    def start_requests(self):
        for start_url in self.start_urls:
            yield Request(start_url, dont_filter=True)

    def parse(self, response):
        """Parses doc items out of IC Policies and Directives site"""

        # parse html response
        div = bs4.BeautifulSoup(response.body, features="html.parser").find(
            "div", attrs={"itemprop": "articleBody"}
        )

        # set policy type
        doc_type = self.get_doc_type(response.url)

        # iterate through each publication
        for row in div.find_all("p"):
            # skip empty rows
            if row.a is None:
                continue

            data = re.sub(r"\u00a0", " ", row.text)
            pdf_url = abs_url(self.base_url, row.a["href"])

            # patterns to match
            try:
                doc_name, doc_num, doc_title = self.get_policy_doc_info(data)
            except IndexError:
                doc_name, doc_num, doc_title = self.get_legal_doc_info(data)

            # extract publication date from the pdf url
            matches = re.findall(r"\((.+)\)", pdf_url.replace("%20", "-"))
            publication_date = matches[-1] if len(matches) > 0 else None

            fields = DocItemFields(
                doc_name=doc_name.strip(),
                doc_title=doc_title,
                doc_num=doc_num,
                doc_type=doc_type,
                publication_date=parse_timestamp(publication_date),
                cac_login_required=self.is_cac_required(pdf_url, doc_title),
                source_page_url=response.url.strip(),
                downloadable_items=[
                    {
                        "doc_type": "pdf",
                        "download_url": pdf_url,
                        "compression_type": None,
                    }
                ],
                download_url=pdf_url,
                file_ext="pdf",
                display_doc_type=self.get_display_doc_type(doc_type),
            )

            yield fields.populate_doc_item(
                display_org=self.display_org,
                data_source=self.data_source,
                source_title=self.source_title,
                crawler_used=self.name,
            )
