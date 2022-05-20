# -*- coding: utf-8 -*-
import scrapy
from pathlib import Path
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.utils import unzip_docs_as_needed

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
    cac_login_required = False

    GCSpider.custom_settings["ITEM_PIPELINES"][
        "dataPipelines.gc_scrapy.gc_scrapy.pipelines.USCodeFileDownloadPipeline"
    ] = GCSpider.custom_settings["ITEM_PIPELINES"].pop(
        "dataPipelines.gc_scrapy.gc_scrapy.pipelines.FileDownloadPipeline"
    )

    def parse_data(self, response):
        output_file_name = response.meta["output_file_name"]
        doc_type = response.meta["doc_type"]
        compression_type = response.meta["compression_type"]
        doc_num = response.meta["doc_num"]
        downloadable_items = response.meta["downloadable_items"]
        version_hash_raw_data = response.meta["version_hash_raw_data"]

        if compression_type:
            file_download_path = Path(self.download_output_dir, output_file_name).with_suffix(f".{compression_type}")
            file_unzipped_path = Path(self.download_output_dir, output_file_name)
        else:
            file_download_path = Path(self.download_output_dir, output_file_name)

        with open(file_download_path, "wb") as f:
            try:
                to_write = self.download_response_handler(response)
                f.write(to_write)
                f.close()

                if compression_type:
                    if compression_type.lower() == "zip":
                        unzipped_files = unzip_docs_as_needed(file_download_path, file_unzipped_path, doc_type)
            except Exception as e:
                print("Failed to write file to", file_download_path, "Error:", e)

        for unzipped_file in unzipped_files:
            version_hash_raw_data.update({"doc_name": unzipped_file.stem})
            doc_title = unzipped_file.stem.split("-", 1)[1].strip()
            item = DocItem(
                doc_name=unzipped_file.stem,
                doc_num=doc_num,
                doc_title=doc_title,
                downloadable_items=downloadable_items,
                version_hash_raw_data=version_hash_raw_data,
            )

            yield item

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
            version_hash_fields = {"item_currency": item_currency}

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

                    meta = {
                        "output_file_name": f"{doc_name}.{doc_type}",
                        "doc_type": doc_type,
                        "compression_type": compression_type,
                        "doc_num": doc_num,
                        "downloadable_items": downloadable_items,
                        "version_hash_raw_data": version_hash_fields,
                        "web_url": web_url,
                        "doc_title": doc_title,
                    }

                    yield scrapy.Request(url=web_url, callback=self.parse_data, meta=meta)

                else:
                    # print("NO DOWNLOADABLE ITEMS", doc_title)
                    continue
