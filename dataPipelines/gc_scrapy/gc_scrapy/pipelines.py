# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from datetime import datetime
import os
from scrapy.exceptions import DropItem
from jsonschema.exceptions import ValidationError

from .validators import DefaultOutputSchemaValidator, SchemaValidator
from . import OUTPUT_FOLDER_NAME

from dataPipelines.gc_crawler.utils import dict_to_sha256_hex_digest, get_fqdn_from_web_url

from urllib.parse import urlparse
import scrapy
from scrapy.pipelines.media import MediaPipeline
import random


class FileDownloadPipeline(MediaPipeline):
    # steps
    # crawl page
    # for each downloadable item in a doc, check prior downloads list and filter if needed
    # if need to download, download
    # ++> success
    #       scan downloaded
    #       pass scan >
    #            put in upload list
    #
    # --> fail
    #       put doc in dead queue
    #
    # after all downloads attempted
    # upload doc (as metadata) TODO!! should this be edited to remove docs that failed? !!
    #  and all successful downloads
    MEDIA_ALLOW_REDIRECTS = True

    def get_media_requests(self, item, info):
        """Yields the media requests to download"""
        # info.spider.prior_downloads
        # info = SpiderInfo
        # self.spider = spider
        # self.downloading = set()
        # self.downloaded = {}
        # self.waiting = defaultdict(list)
        # TODO filter these
        doc_name = item["doc_name"]
        print('item', item["doc_name"], len(item["downloadable_items"]))
        for file_item in item["downloadable_items"]:
            # rando = file_item.get('web_url', '') + 'gobbledygoop'
            # url = random.choice(
            #     [file_item.get('web_url'), rando, 'gobbledygoop'])
            url = file_item.get('web_url')
            print('url', url)
            yield scrapy.Request(url, meta={"doc_name": doc_name})

    def media_downloaded(self, response, request, info):
        """Handler for success downloads"""
        # TODO run scanner
        print('media_downloaded response', response)
        if 200 <= int(response.status) <= 299:
            name = response.meta["doc_name"]
            path = '/Users/dakotahavel/Desktop/gamechanger-crawlers/tmp/test/' + name
            print('path', path)
            with open(path, 'wb') as f:
                f.write(response.body)

        return response

    def media_failed(self, failure, request, info):
        """Handler for failed downloads"""
        # TODO add to dead queue
        print('------------------------------- media failed', failure)
        return failure

    def item_completed(self, results, item, info):
        """Called per item when all media requests has been processed"""
        # TODO upload item as metadata file

        # if self.LOG_FAILED_RESULTS:
        #     for ok, value in results:
        #         if not ok:
        #             logger.error(
        #                 '%(class)s found errors processing %(item)s',
        #                 {'class': self.__class__.__name__, 'item': item},
        #                 exc_info=failure_to_exc_info(value),
        #                 extra={'spider': info.spider}
        #             )
        return item


class DeduplicaterPipeline():
    def __init__(self):
        self.ids_seen = set()
        self.dropped_count = 0

    def process_item(self, item, spider):
        if not item['doc_name']:
            raise DropItem("No doc_name")

        elif item['doc_name'] in self.ids_seen:
            self.dropped_count += 1
            raise DropItem("Duplicate doc_name found")

        else:
            self.ids_seen.add(item['doc_name'])

        return item

    # def close_spider(self, spider):
    #     print()
    #     print(spider.name, 'closed with', self.dropped_count, 'dropped items')


class AdditionalFieldsPipeline:
    def process_item(self, item, spider):

        if item.get('crawler_used') is None:
            item['crawler_used'] = spider.name

        source_page_url = item.get('source_page_url')
        if source_page_url is None:
            if spider.source_page_url:
                item["source_page_url"] = spider.source_page_url
            else:
                source_page_url = spider.start_urls[0]
                item['source_page_url'] = source_page_url

        if item.get('source_fqdn') is None:
            item['source_fqdn'] = get_fqdn_from_web_url(source_page_url)

        if item.get('version_hash') is None:
            item['version_hash'] = dict_to_sha256_hex_digest(
                item.get('version_hash_raw_data'))

        if item.get('access_timestamp') is None:
            item['access_timestamp'] = datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S.%f')

        if item.get('publication_date') is None:
            item['publication_date'] = "N/A"

        if item.get('cac_login_required') is None:
            item['cac_login_required'] = spider.cac_login_required

        if item.get('doc_type') is None:
            item['doc_type'] = spider.doc_type

        if item.get('doc_num') is None:
            item['doc_num'] = ""

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
        name = item_dict.get('doc_name', str(item_dict))

        try:
            self.validator.validate_dict(item_dict)
            return item
        except ValidationError as ve:
            raise DropItem(f'Dropped Item: {name} failed validation: {ve}')


class JsonWriterPipeline(object):
    def open_spider(self, spider):
        if not os.path.exists(OUTPUT_FOLDER_NAME):
            os.makedirs(OUTPUT_FOLDER_NAME)
        json_name = './' + OUTPUT_FOLDER_NAME + '/' + spider.name + '.json'

        self.file = open(json_name, 'w')
        # Your scraped items will be saved in the file 'scraped_items.json'.
        # You can change the filename to whatever you want.

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        doc = item['document']

        validator = DefaultOutputSchemaValidator()
        validator.validate(doc)
        self.file.write(doc + '\n')
        return doc
