# -*- coding: utf-8 -*-
import scrapy
import re
from urllib.parse import urljoin
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider

PART = " - "


class USCodeSpider(GCSpider):
    name = 'us_code'
    start_urls = ['https://uscode.house.gov/download/download.shtml']
    doc_type = "Title"
    cac_login_required = False

    def parse(self, response):
        rows = [
            el for el in response.css("div.uscitemlist > div.uscitem")
            if el.css('::attr(id)').get() != 'alltitles'
        ]
        prev_doc_num = None
        for row in rows:
            doc_type_num_title_raw = row.css('div:nth-child(1)::text').get()
            is_appendix = row.css('div.usctitleappendix::text').get()

            doc_type_num_raw, _, doc_title_raw = doc_type_num_title_raw.partition(
                PART)

            # handle appendix rows
            if is_appendix and prev_doc_num:
                doc_num = prev_doc_num
                doc_title = 'Appendix'
            else:
                doc_num = self.ascii_clean(
                    doc_type_num_raw.replace('Title', ''))
                prev_doc_num = doc_num

                doc_title = self.ascii_clean(doc_title_raw)

            # e.x. - Title 53 is reserved for now
            if not doc_title:
                continue

            doc_title = doc_title.replace(',', '').replace("'", '')
            doc_name = f"{self.doc_type} {doc_num}{PART}{doc_title}"

            item_currency_raw = row.css('div.itemcurrency::text').get()
            item_currency = self.ascii_clean(item_currency_raw)
            version_hash_fields = {
                "item_currency": item_currency
            }

            links = row.css('div.itemdownloadlinks a')
            downloadable_items = []
            for link in links:
                link_title = link.css('::attr(title)').get()
                href_raw = link.css('::attr(href)').get()
                web_url = f"https://uscode.house.gov/download/{href_raw}"

                if 'PDF' in link_title:
                    downloadable_items.append({
                        "doc_type": "pdf",
                        "web_url": web_url,
                        "compression_type": "zip",
                    })
                elif 'XML' in link_title:
                    downloadable_items.append({
                        "doc_type": "xml",
                        "web_url": web_url,
                        "compression_type": "zip",
                    })
                else:
                    continue

            if not len(downloadable_items):
                print('NO DOWNLOADABLE ITEMS', doc_title)
                continue

            yield DocItem(
                doc_name=doc_name,
                doc_num=doc_num,
                doc_title=doc_title,
                # publication_date=last_action_date,
                # source_page_url=source_page_url,
                downloadable_items=downloadable_items,
                version_hash_raw_data=version_hash_fields
            )
