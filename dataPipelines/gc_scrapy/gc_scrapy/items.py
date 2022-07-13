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
    publication_date_dt = scrapy.Field()
    cac_login_required_b = scrapy.Field()
    crawler_used_s = scrapy.Field()
    source_page_url_s = scrapy.Field()
    downloadable_items = scrapy.Field()
    version_hash_raw_data = scrapy.Field()
    access_timestamp_dt = scrapy.Field()
    source_fqdn_s = scrapy.Field()
    version_hash_s = scrapy.Field()
    display_doc_type_s = scrapy.Field()
    display_org_s = scrapy.Field()
    display_source_s = scrapy.Field()
    data_source_s = scrapy.Field()
    source_title_s = scrapy.Field()
    file_ext_s = scrapy.Field()
    display_title_s = scrapy.Field()
    is_revoked_b = scrapy.Field()
    office_primary_resp = scrapy.Field()


