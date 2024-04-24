##########################################################################################
# This script defines the pipelines for post processing of scraped items. Item Pipelines are 
# a standard component of Scrapy implementation. After an item has been scraped by a spider,
# it is sent through this Item Pipeline for processing through several components, executed
# sequentially.
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
##########################################################################################

import copy
from typing import Union
from itemadapter import ItemAdapter
from datetime import datetime
import os
from pathlib import Path
import json
from jsonschema.exceptions import ValidationError

import scrapy
from scrapy.pipelines.media import MediaPipeline
from scrapy.exceptions import DropItem

from dataPipelines.gc_scrapy.gc_scrapy.utils import unzip_docs_as_needed
from .validators import DefaultOutputSchemaValidator, SchemaValidator
from . import OUTPUT_FOLDER_NAME
from .utils import dict_to_sha256_hex_digest, get_fqdn_from_web_url


SUPPORTED_FILE_EXTENSIONS = [
    "pdf",
    "html",
    "txt",
    "zip",
] # File types the item pipeline supports


class FileDownloadPipeline(MediaPipeline):
    def __init__(self, download_func=None, settings=None):
        settings = dict(settings) if settings else {}
        settings.setdefault("MEDIA_ALLOW_REDIRECTS", True)
        super().__init__(download_func, settings)

    previous_hashes = set()
    output_dir: Path
    previous_manifest_path: Path
    job_manifest_path: Path
    dont_filter_previous_hashes: bool

    def open_spider(self, spider):
        super().open_spider(spider)
        print("++ Initiating downloader for", spider.name)

        self.output_dir = Path(spider.download_output_dir).resolve()
        self.job_manifest_path = Path(self.output_dir, "manifest.json").resolve()

        self.previous_manifest_path = Path(spider.previous_manifest_location).resolve()

        if not spider.dont_filter_previous_hashes:
            self.load_hashes_from_cumulative_manifest(self.previous_manifest_path, spider.name)

    def load_hashes_from_cumulative_manifest(self, previous_manifest_path, spider_name):
        file_location = Path(previous_manifest_path).resolve() if previous_manifest_path else None

        if not file_location or not os.path.isfile(file_location):
            print(f"\n\nPrevious manifest at {file_location} is not a file! Nothing will be filtered!\n\n")
            if self.dont_filter_previous_hashes:
                return
            else:
                exit(1)

        print("Reading in previous manifest")
        count = 0
        with file_location.open(mode="r") as f:
            for line in f.readlines():
                count += 1
                if count % 1000 == 0:
                    print(f"{count} lines read in")
                if not line.strip():
                    continue

                jdoc = json.loads(line)
                crawler_used = jdoc.get("crawler_used")
                # covers old manifest items with no crawler info
                if not crawler_used:
                    self.previous_hashes.add(jdoc["version_hash"])
                # skips adding hashes for other spiders for combined manifest files with that info
                elif crawler_used == spider_name:
                    self.previous_hashes.add(jdoc["version_hash"])

        num_hashes = len(self.previous_hashes)
        print(f"Previous manifest loaded, will filter {num_hashes} hashes")

    @staticmethod
    def create_items_from_nested_zip(zipped_item_paths, item):
        for sub_path in zipped_item_paths:
            new_item = copy.deepcopy(item)
            new_item["doc_name"] = sub_path.stem
            if item['crawler_used'] == "far_subpart_regs":
                new_item["doc_title"] = sub_path.stem
            else:
                new_item["doc_title"] = sub_path.stem.split("-", 1)[1].strip()
            new_item["version_hash_raw_data"]["doc_name"] = new_item["doc_name"]
            new_item["version_hash_raw_data"]["sub_file_version_hash"] = dict_to_sha256_hex_digest(
                new_item["version_hash_raw_data"]
            )
            yield new_item

    @staticmethod
    def get_first_supported_downloadable_item(downloadable_items: list) -> Union[dict, None]:
        """Get first supported downloadable item corresponding to doc, has correct type and is not cac blocked"""
        return next((item for item in downloadable_items if item["doc_type"] in SUPPORTED_FILE_EXTENSIONS), None)

    def get_media_requests(self, item, info):
        """Called per DocItem from spider output, yields the media requests to download, response sent to media_downloaded"""

        # info = SpiderInfo
        # class SpiderInfo:
        # self.spider = spider
        # self.downloading = set()
        # self.downloaded = {}
        # self.waiting = defaultdict(list)

        doc_name = item["doc_name"]
        if item["version_hash"] in self.previous_hashes:
            # dont download anything just send item to crawl output
            print(f"Skipping download of {item.get('doc_name')} because it was in previous_hashes")
            info.spider.increment_in_previous_hashes()
            return item

        if item["cac_login_required"]:
            print(f"Skipping download of {item.get('doc_name')} because it requires cac login")
            info.spider.increment_required_cac()
            return item

        # currently we only associate 1 file with each doc, this gets the first we know how to parse
        file_item = self.get_first_supported_downloadable_item(item["downloadable_items"])

        if file_item:
            url = file_item["download_url"]
            extension = file_item["doc_type"]
            output_file_name = f"{doc_name}.{extension}"

            meta = {
                "output_file_name": output_file_name,
                "doc_type": file_item["doc_type"],
                "compression_type": file_item["compression_type"],
            }

            try:
                if info.spider.download_request_headers:
                    yield scrapy.Request(url, headers=info.spider.download_request_headers, meta=meta)
                else:
                    yield scrapy.Request(url, meta=meta)
            except Exception as probably_url_error:
                print("~~~~ REQUEST ERR", probably_url_error)
        else:
            print(f"No supported downloadable item for {item['doc_name']}")
            return item

    def media_downloaded(self, response, request, info, *, item=None):
        """Called for each completed response from get_media_requests, returned to item_completed"""
        # I dont know why this isnt being handled automatically here
        # Just filtering by response code
        if 200 <= response.status < 300:
            return (True, response, None)
        elif not len(response.body):
            return (False, response, "Response has empty body")
        else:
            return (False, response, None)

    def media_failed(self, failure, request, info):
        # I have never seen this called
        print("**** MEDIA FAILED")
        print(failure)
        print(info.spider)
        return (False, failure, "Pipeline Media Request Failed")

    def add_to_dead_queue(self, item, reason):
        path = Path(self.output_dir, "dead_queue.json").resolve() if self.output_dir else None
        if isinstance(reason, int):
            reason_text = f"HTTP Response Code {reason}"
        elif isinstance(reason, str):
            reason_text = reason
        else:
            reason_text = "Unknown failure"

        with open(path, "a+") as f:
            dead_dict = {"document": dict(item), "failure_reason": reason_text}
            try:
                f.write(json.dumps(dead_dict))
                f.write("\n")

            except Exception as e:
                print("Failed to write to dead_queue file", path, e)

    def add_to_manifest(self, item):
        path = self.job_manifest_path
        with open(self.job_manifest_path, "a") as f:
            try:
                f.write(
                    json.dumps(
                        {
                            "version_hash": item["version_hash"],
                            "doc_name": item["doc_name"],
                            "crawler_used": item["crawler_used"],
                            "access_timestamp": item["access_timestamp"],
                        }
                    )
                )
                f.write("\n")

            except Exception as e:
                print("Failed to write to manifest file", path, e)

    def item_completed(self, results, item, info):
        """The function is called for each item after all media requests have been processed"""
        
        if not info.downloaded:
            return item # return item for crawler output if download was skipped

        ### first in results is supposed to be 'ok' status but it always returns true b/c 404 doesnt cause failure for some reason :(
        ### so added to the media_downloaded function as a sub-tuple in return
        file_downloads = []
        unzipped_items = []
        for (_, (okay, response, reason)) in results: # Loop over results of requests made during crawling
            if not okay:
                self.add_to_dead_queue(item, reason if reason else int(response.status))
            else:
                # Get values from metadata:
                output_file_name = response.meta["output_file_name"] # Assigned to metadata above in get_media_requests function
                doc_type = response.meta["doc_type"]
                compression_type = response.meta["compression_type"]
                # Build a path to each file associated with an item:
                if compression_type:
                    file_download_path = Path(self.output_dir, output_file_name).with_suffix(f".{compression_type}") # Path for downloaded zipped file
                    file_unzipped_path = Path(self.output_dir, output_file_name)
                    # Path for unzipped files
                    metadata_download_path = f"{file_unzipped_path}.metadata" # Path for the accompanying metadata file
                else:
                    # If it is a jbook crawler (and needs a different file output style)
                    if 'rdte;' in output_file_name or 'procurement;' in output_file_name:
                        jbook_output_file_path = output_file_name.replace(';', '/')
                        # self.output_dir is set when the crawler is crawled and is the high level directory information
                        # Should point to bronze/jbook/pdfs instead of bronze/gamechanger/pdf
                        # jbook_output_file_path is type/year/filename
                        file_download_path = Path(self.output_dir, jbook_output_file_path)
                    else:
                        file_download_path = Path(self.output_dir, output_file_name)  # Path for downloaded file
                    metadata_download_path = f"{file_download_path}.metadata"  # Path for the accompanying metadata file

                with open(file_download_path, "wb") as f: # Download each file to it's download path
                    try:
                        to_write = info.spider.download_response_handler(response)
                        f.write(to_write)
                        f.close()
                    except Exception as e:
                        print("Failed to write file to", file_download_path, "Error:", e)
                        return item

                if compression_type:
                    if compression_type.lower() == "zip":
                        unzipped_files = unzip_docs_as_needed(file_download_path, file_unzipped_path, doc_type) # Unzip downloaded zip documents

                        if unzipped_files: # If files have been unzipped...
                            for unzipped_item in self.create_items_from_nested_zip(unzipped_files, item): # Create new DocItem for each unzipped file
                                self.add_to_manifest(unzipped_item)

                                metadata_download_path = Path(self.output_dir, unzipped_item["doc_name"])
                                suffix_doc_type = f'{unzipped_item["downloadable_items"][0]["doc_type"]}'

                                # when making metadata_download_path, need to add the previous suffix in case there are
                                # periods in filename. will mess up metadata names otherwise
                                metadata_download_path = metadata_download_path.with_suffix(metadata_download_path.suffix + f'.{suffix_doc_type}.metadata')

                                with open(metadata_download_path, "w") as f: # Write the metadata for each unzipped file
                                    try:
                                        f.write(json.dumps(dict(unzipped_item)))

                                    except Exception as e:
                                        print("Failed to write metadata", metadata_download_path, e)

                                unzipped_items.append(unzipped_item)
                else: # If original download is not a compressed file...
                    with open(metadata_download_path, "w") as f: # Write the metadata for each file
                        try:
                            f.write(json.dumps(dict(item)))
                        except Exception as e:
                            print("Failed to write metadata", file_download_path, e)

                file_downloads.append(file_download_path)

        if file_downloads: # If file was downloaded, add to manifest
            self.add_to_manifest(item)

        if len(unzipped_items) > 1: # If there were unzipped files, return each as item in list 'unzipped_items'
            return unzipped_items

        return item # Return item to crawl output (if no unzipped files)


class DeduplicaterPipeline:
    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        if not item["doc_name"]:
            raise DropItem("No doc_name")

        elif item["doc_name"] in self.ids_seen:
            raise DropItem("Duplicate doc_name found")

        else:
            self.ids_seen.add(item["doc_name"])

        return item


class AdditionalFieldsPipeline:
    def process_item(self, item, spider):

        if getattr(spider, "display_org", None): # If DocItem.display_org= None, propogate value with value of spider class variable display_org
            item["display_org"] = spider.display_org

        if getattr(spider, "data_source", None): # If DocItem.data_source = None, propogate value with value of spider class variable data_source
            item["data_source"] = spider.data_source
        
        if getattr(spider, "source_title", None): # If DocItem.source_title = None, propogate value with value of spider class variable source_title
            item["source_title"] = spider.source_title

        if getattr(spider, "display_source", None):
            item["display_source"] = spider.display_source

        if not item.get("crawler_used"):
            item["crawler_used"] = spider.name

        source_page_url = item.get("source_page_url")
        if not source_page_url:
            if getattr(spider, "source_page_url", None):
                item["source_page_url"] = spider.source_page_url
            else:
                source_page_url = spider.start_urls[0]
                item["source_page_url"] = source_page_url

        if not item.get("source_fqdn"):
            item["source_fqdn"] = get_fqdn_from_web_url(source_page_url)

 #       if not item.get("version_hash"):
            # ensure doc_name is part of hash
            # item["version_hash_raw_data"]["doc_name"] = item["doc_name"]
            # item["version_hash"] = dict_to_sha256_hex_digest(item["version_hash_raw_data"])

        if not item.get("access_timestamp"):
            item["access_timestamp"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S") # T added as delimiter between date and time

        if not item.get("publication_date"):
            item["publication_date"] = None

        if not item.get("cac_login_required"):
            item["cac_login_required"] = getattr(spider, "cac_login_required", False)

        if not item.get("doc_type"):
            item["doc_type"] = getattr(spider, "doc_type", None)

        if not item.get("doc_num"):
            item["doc_num"] = None

        # if item.get("is_revoked") is not None:
           # ensure is_revoked is part of hash
           # item["version_hash_raw_data"]["is_revoked"] = item["is_revoked"]

        return item


class ValidateJsonPipeline:
    """Validates json as Scrapy passes each item to be validated to self.process_item
    :param validator: output validator"""

    def __init__(self, validator: SchemaValidator = DefaultOutputSchemaValidator()):

        if not isinstance(validator, SchemaValidator):
            raise TypeError("arg: validator must be of type SchemaValidator")

        self.validator = validator

    def process_item(self, item, spider):
        item_dict = ItemAdapter(item).asdict()
        name = item_dict.get("doc_name", str(item_dict))

        try:
            self.validator.validate_dict(item_dict)
            return item
        except ValidationError as ve:
            raise DropItem(f"Dropped Item: {name} failed validation: {ve}")


class JsonWriterPipeline(object):
    def open_spider(self, spider):
        if not os.path.exists(OUTPUT_FOLDER_NAME):
            os.makedirs(OUTPUT_FOLDER_NAME)
        json_name = "./" + OUTPUT_FOLDER_NAME + "/" + spider.name + ".json"

        self.file = open(json_name, "w")
        # Your scraped items will be saved in the file 'scraped_items.json'.
        # You can change the filename to whatever you want.

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        doc = item["document"]

        validator = DefaultOutputSchemaValidator()
        validator.validate(doc)
        self.file.write(doc + "\n")
        return doc


class FileNameFixerPipeline:
    def process_item(self, item, spider):
        if not item["doc_name"]:
            raise DropItem("No doc_name")

        # limit length for OS filename limitations, replace / for filename dir confusion
        item["doc_name"] = item["doc_name"].replace("/", "_")[0:235]
        return item
