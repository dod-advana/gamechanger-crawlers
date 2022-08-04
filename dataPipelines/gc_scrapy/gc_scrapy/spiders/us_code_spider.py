# -*- coding: utf-8 -*-
import scrapy
from pathlib import Path
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.utils import unzip_docs_as_needed

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

    GCSpider.custom_settings["ITEM_PIPELINES"][
        "dataPipelines.gc_scrapy.gc_scrapy.pipelines.USCodeFileDownloadPipeline"
    ] = GCSpider.custom_settings["ITEM_PIPELINES"].pop(
        "dataPipelines.gc_scrapy.gc_scrapy.pipelines.FileDownloadPipeline"
    )

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
                        "web_url": web_url,
                        "doc_title": doc_title,
                    }

                    yield scrapy.Request(url=web_url, callback=self.parse_data, meta=meta)

                else:
                    # print("NO DOWNLOADABLE ITEMS", doc_title)
                    continue

 
    def parse_data(self, response):
        output_file_name = response.meta["output_file_name"]
        doc_type = response.meta["doc_type"]
        compression_type = response.meta["compression_type"]
        doc_num = response.meta["doc_num"]
        downloadable_items = response.meta["downloadable_items"]
        doc_title = response.meta["doc_title"]

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
            if not ("Appendix" in doc_title):
                doc_title = unzipped_file.stem.split("-", 1)[1].strip()
            

            fields = {
                'doc_name': unzipped_file.stem,
                'doc_num': doc_num,
                'doc_title': doc_title,
                'doc_type': doc_type,
                'cac_login_required': False,
                'downloadable_items':downloadable_items,
                'compression_type': compression_type,
                'download_url': response.meta["web_url"]#,
                #'publication_date': publication_date
            }
            ## Instantiate DocItem class and assign document's metadata values
            doc_item = self.populate_doc_item(fields)
        
            yield doc_item
        


    def populate_doc_item(self, fields):
        '''
        This functions provides both hardcoded and computed values for the variables
        in the imported DocItem object and returns the populated metadata object
        '''
        display_org="United States Code" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "Office of Law Revision Counsel" # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Unlisted Source" # Level 3 filter

        doc_name = fields['doc_name']
        doc_num = fields['doc_num']
        doc_title = fields['doc_title']
        doc_type = fields['doc_type']
        cac_login_required = fields['cac_login_required']
        download_url = fields['download_url']
        #publication_date = get_pub_date(fields['publication_date'])

        display_doc_type = "Document" # Doc type for display on app
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + " " + doc_title
        is_revoked = False
        access_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f") # T added as delimiter between date and time
        source_page_url = self.start_urls[0]
        source_fqdn = urlparse(source_page_url).netloc

        downloadable_items = [{
                "doc_type": doc_type,
                "download_url": download_url,
                "compression_type": fields['compression_type'],
            }]

        ## Assign fields that will be used for versioning
        version_hash_fields = {
            "doc_name":doc_name,
            "doc_num": doc_num,
            #"publication_date": publication_date,
            "download_url": download_url
        }

        version_hash = dict_to_sha256_hex_digest(version_hash_fields)

        return DocItem(
                    doc_name = doc_name,
                    doc_title = doc_title,
                    doc_num = doc_num,
                    doc_type = doc_type,
                    display_doc_type = display_doc_type, #
                    #publication_date = publication_date,
                    cac_login_required = cac_login_required,
                    crawler_used = self.name,
                    downloadable_items = downloadable_items,
                    source_page_url = source_page_url, #
                    source_fqdn = source_fqdn, #
                    download_url = download_url, #
                    version_hash_raw_data = version_hash_fields, #
                    version_hash = version_hash,
                    display_org = display_org, #
                    data_source = data_source, #
                    source_title = source_title, #
                    display_source = display_source, #
                    display_title = display_title, #
                    file_ext = doc_type, #
                    is_revoked = is_revoked, #
                    access_timestamp = access_timestamp #
                )