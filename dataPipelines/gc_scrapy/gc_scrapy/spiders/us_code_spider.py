# -*- coding: utf-8 -*-
import scrapy
from pathlib import Path
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest
import copy

PART = " - "
SUPPORTED_URL_EXTENSIONS = ["PDF"]


def index_containing_substring(the_list, substring):
    for i, s in enumerate(the_list):
        if s in substring:
            return i
    return None


class GCSpiderCP(GCSpider):
    pass


class USCodeSpider(GCSpiderCP):
    name = "us_code"
    start_urls = ["https://uscode.house.gov/download/download.shtml"]
    doc_type = "Title"
    cac_login_required = False
    rotate_user_agent = True

    custom_settings = \
        {"ITEM_PIPELINES": {
            "dataPipelines.gc_scrapy.gc_scrapy.pipelines.FileNameFixerPipeline": 50,
            "dataPipelines.gc_scrapy.gc_scrapy.pipelines.DeduplicaterPipeline": 100,
            "dataPipelines.gc_scrapy.gc_scrapy.pipelines.AdditionalFieldsPipeline": 200,
            "dataPipelines.gc_scrapy.gc_scrapy.pipelines.ValidateJsonPipeline": 300,
            "dataPipelines.gc_scrapy.gc_scrapy.pipelines.USCodeFileDownloadPipeline": 400
        },
        "FEED_EXPORTERS": {
            "json": "dataPipelines.gc_scrapy.gc_scrapy.exporters.ZippedJsonLinesAsJsonItemExporter",
        },
        "DOWNLOADER_MIDDLEWARES": {
            "dataPipelines.gc_scrapy.gc_scrapy.downloader_middlewares.BanEvasionMiddleware": 100,
        },
        # 'STATS_DUMP': False,
        "ROBOTSTXT_OBEY": False,
        "LOG_LEVEL": "INFO",
        "DOWNLOAD_FAIL_ON_DATALOSS": False,
        }

    def parse(self, response):
        rows = [el for el in response.css("div.uscitemlist > div.uscitem") if el.css("::attr(id)").get() != "alltitles"]
        prev_doc_num = None

        # for each link in the current start_url
        for row in rows:
            doc_type_num_title_raw = row.css("div:nth-child(1)::text").get()
            is_appendix = row.css("div.usctitleappendix::text").get()

            doc_type_num_raw, _, doc_title_raw = doc_type_num_title_raw.partition(PART)

            # handle appendix rows
            if is_appendix and prev_doc_num:
                doc_num = prev_doc_num
                doc_title = "Appendix"
            else:
                doc_num = self.ascii_clean(doc_type_num_raw.replace("Title", ""))
                prev_doc_num = doc_num
                doc_title = self.ascii_clean(doc_title_raw)

            # e.x. - Title 53 is reserved for now
            if not doc_title:
                continue

            doc_title = doc_title.replace(",", "").replace("'", "")
            doc_name = f"{self.doc_type} {doc_num}{PART}{doc_title}"

            item_currency_raw = row.css("div.itemcurrency::text").get()
            item_currency = self.ascii_clean(item_currency_raw)
            # Setting doc name and version hash here because the downloaded file is single (a zip of zips)
            # so it should have one hash so it doesnt get re-downloaded
            version_hash_fields = {"item_currency": item_currency, "doc_name": doc_type_num_title_raw}
            version_hash = dict_to_sha256_hex_digest(version_hash_fields)

            links = row.css("div.itemdownloadlinks a")
            downloadable_items = []
            for link in links:
                link_title = link.css("::attr(title)").get()
                href_raw = link.css("::attr(href)").get()
                web_url = f"https://uscode.house.gov/download/{href_raw}"

                ext_idx = index_containing_substring(SUPPORTED_URL_EXTENSIONS, link_title)
                if ext_idx is not None:
                    doc_type = SUPPORTED_URL_EXTENSIONS[ext_idx].lower()
                    compression_type = "zip"
                    downloadable_items.append(
                        {"doc_type": doc_type, "web_url": web_url, "compression_type": compression_type}
                    )
                else:
                    continue

            item = DocItem(
                doc_name=doc_name,
                doc_num=doc_num,
                doc_title=doc_title,
                downloadable_items=downloadable_items,
                version_hash_raw_data=version_hash_fields,
                version_hash=version_hash,
            )

            yield item
