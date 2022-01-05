# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class DocItem(scrapy.Item):
    doc_name = scrapy.Field()
    doc_title = scrapy.Field()
    doc_num = scrapy.Field()
    doc_type = scrapy.Field()
    publication_date = scrapy.Field()
    cac_login_required = scrapy.Field()
    crawler_used = scrapy.Field()
    source_page_url = scrapy.Field()
    downloadable_items = scrapy.Field()
    version_hash_raw_data = scrapy.Field()
    access_timestamp = scrapy.Field()
    source_fqdn = scrapy.Field()
    version_hash = scrapy.Field()
    display_doc_type = scrapy.Field()
    display_org = scrapy.Field()
    display_source = scrapy.Field()
    data_source = scrapy.Field()
    source_title = scrapy.Field()
    is_revoked = scrapy.Field()
