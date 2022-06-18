from pathlib import Path
import re
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider

SUPPORTED_URL_EXTENSIONS = ["pdf"]

STRATEGY_DOCUMENT_NAME_MAP = {
    "c3": "C3 Modernization",
    "oconuscloud": "OCONUS Cloud",
    "digitalmodernization": "Digital Modernization",
    "softwaremodstrat": "Software Modernization",
    "summaryofai": "Summary of AI",
    "data": "Summary of Data",
}


class DoDStrategySpider(GCSpider):
    name = "dod_strategy"

    start_urls = ["https://dodcio.defense.gov/Library"]
    allowed_domains = ["dodcio.defense.gov"]
    download_base_url = "https://dodcio.defense.gov"

    doc_type_prefix = "DoD"
    doc_type_suffix = "Strategy"
    doc_type = "DoDStrategy"

    cac_login_required = False
    rotate_user_agent = True

    def parse(self, response):
        # rows = [el for el in response.css("div.col-md-4").css("tr").css("a") if el.css("img")]
        rows = [el for el in response.css("div.col-md-4").css("tr") if el.css("a > img")]

        for row in rows:

            href_raw = row.css("::attr(href)").get()
            web_url = self.ensure_full_href_url(href_raw, self.download_base_url)
            file_type = self.get_href_file_extension(href_raw)

            # skip .aspx and non-strategy docs
            if not (file_type in SUPPORTED_URL_EXTENSIONS):
                print(f"SKIPPING: {web_url}")
                continue

            # get document tile
            doc_title_raw: str = row.css("::attr(title)").get()
            if not doc_title_raw:
                doc_title_raw = row.css("a::text").get()

            doc_title = self.ascii_clean(doc_title_raw)
            doc_name = doc_title
            doc_num = "N/A"

            # exclude non-strategy documents
            if not (self.doc_type_suffix.lower() in doc_title_raw.lower()):
                continue

            downloadable_items = [{"doc_type": file_type, "web_url": web_url, "compression_type": None}]
            version_hash_fields = {"item_currency": href_raw}

            yield DocItem(
                doc_name=doc_name,
                doc_num=doc_num,
                doc_title=doc_title,
                source_page_url=response.url,
                downloadable_items=downloadable_items,
                version_hash_raw_data=version_hash_fields,
                cac_login_required=self.cac_login_required,
            )
