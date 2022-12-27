# -*- coding: utf-8 -*-
import scrapy
from pathlib import Path
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest
from urllib.parse import urljoin, urlparse
from datetime import datetime
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest, get_pub_date

PART = " - "
SUPPORTED_URL_EXTENSIONS = ["PDF"]


def index_containing_substring(the_list, substring):
    for i, s in enumerate(the_list):
        if s in substring:
            return i
    return None


class USCodeSpider(GCSpider):
    name = "us_code"
    start_urls = ["https://uscode.house.gov/download/download.shtml"]
    doc_type = "Title"
    rotate_user_agent = True


    custom_settings = \
        {"ITEM_PIPELINES": {
            "dataPipelines.gc_scrapy.gc_scrapy.pipelines.FileNameFixerPipeline": 50,
            "dataPipelines.gc_scrapy.gc_scrapy.pipelines.DeduplicaterPipeline": 100,
            "dataPipelines.gc_scrapy.gc_scrapy.pipelines.AdditionalFieldsPipeline": 200,
            "dataPipelines.gc_scrapy.gc_scrapy.pipelines.ValidateJsonPipeline": 300,
            "dataPipelines.gc_scrapy.gc_scrapy.pipelines.FileDownloadPipeline": 400
        },
        "FEED_EXPORTERS": {
            "jsonlines": "dataPipelines.gc_scrapy.gc_scrapy.exporters.ZippedJsonLinesAsJsonItemExporter",
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
                    file_type = SUPPORTED_URL_EXTENSIONS[ext_idx].lower()
                    compression_type = "zip"
                    downloadable_items.append(
                        {"doc_type": file_type, "web_url": web_url, "compression_type": compression_type}
                    )

                    fields = {
                        'doc_name': doc_name,
                        'doc_num': doc_num,
                        'doc_title': doc_title,
                        'file_type': file_type,
                        'doc_type': self.doc_type,
                        'cac_login_required': False,
                        'downloadable_items': downloadable_items,
                        'compression_type': compression_type,
                        'download_url': web_url  # ,
                        # 'publication_date': publication_date
                    }
                    ## Instantiate DocItem class and assign document's metadata values
                    doc_item = self.populate_doc_item(fields)

                    yield doc_item

    def populate_doc_item(self, fields):
        '''
        This functions provides both hardcoded and computed values for the variables
        in the imported DocItem object and returns the populated metadata object
        '''
        display_org = "United States Code"  # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "Office of Law Revision Counsel"  # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Unlisted Source"  # Level 3 filter

        doc_name = fields['doc_name']
        doc_num = fields['doc_num']
        doc_title = fields['doc_title']
        file_type = fields['file_type']
        doc_type = fields['doc_type']
        cac_login_required = fields['cac_login_required']
        download_url = fields['download_url']
        # publication_date = get_pub_date(fields['publication_date'])

        display_doc_type = "Title"  # Doc type for display on app
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + " " + doc_title
        is_revoked = False
        source_page_url = self.start_urls[0]
        source_fqdn = urlparse(source_page_url).netloc

        downloadable_items = [{
            "doc_type": file_type,
            "download_url": download_url,
            "compression_type": fields['compression_type'],
        }]

        ## Assign fields that will be used for versioning
        version_hash_fields = {
            "doc_name": doc_name,
            "doc_num": doc_num,
            "file_type": file_type,
            # "publication_date": publication_date,
            "download_url": download_url
        }

        version_hash = dict_to_sha256_hex_digest(version_hash_fields)

        return DocItem(
            doc_name=doc_name,
            doc_title=doc_title,
            doc_num=doc_num,
            doc_type=doc_type,
            display_doc_type=display_doc_type,  #
            publication_date="N/A",
            cac_login_required=cac_login_required,
            crawler_used=self.name,
            downloadable_items=downloadable_items,
            source_page_url=source_page_url,  #
            source_fqdn=source_fqdn,  #
            download_url=download_url,  #
            version_hash_raw_data=version_hash_fields,  #
            version_hash=version_hash,
            display_org=display_org,  #
            data_source=data_source,  #
            source_title=source_title,  #
            display_source=display_source,  #
            display_title=display_title,  #
            file_ext=doc_type,  #
            is_revoked=is_revoked  #
        )
