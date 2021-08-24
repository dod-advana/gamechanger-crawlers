# -*- coding: utf-8 -*-
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
import json
import re

exec_order_re = re.compile(
    r'(?:(?:Executive Order)|(?:Proclamation))\s*(\d+)', flags=re.IGNORECASE)


class ExecutiveOrdersSpider(GCSpider):
    name = "ex_orders"
    start_urls = [
        "https://www.federalregister.gov/presidential-documents/executive-orders"
    ]
    cac_login_required = False
    doc_type = "EO"

    @staticmethod
    def make_doc_item_from_dict(doc: dict) -> DocItem:
        doc_num = doc.get('executive_order_number', '')
        doc_title = doc.get('title')
        publication_date = doc.get('publication_date', '')
        source_page_url = doc.get('html_url')
        disposition_notes = doc.get('disposition_notes', '')
        signing_date = doc.get('signing_date', '')

        if doc_num == "12988" and 'CHAMPUS' in doc_title:
            # this is not an executive order, its a notice from OSD
            # there may be other errors but this has a conflicting doc num
            # https://www.federalregister.gov/documents/1996/02/09/96-2755/civilian-health-and-medical-program-of-the-uniformed-services-champus
            return

        pdf_url = doc.get('pdf_url')
        xml_url = doc.get('full_text_xml_url')
        txt_url = doc.get('raw_text_url')

        downloadable_items = []
        if pdf_url:
            downloadable_items.append(
                {
                    "doc_type": "pdf",
                    "web_url": pdf_url,
                    "compression_type": None,
                }
            )

        if xml_url:
            downloadable_items.append(
                {
                    "doc_type": "xml",
                    "web_url": xml_url,
                    "compression_type": None,
                }
            )

        if txt_url:
            downloadable_items.append(
                {
                    "doc_type": "txt",
                    "web_url": txt_url,
                    "compression_type": None,
                }
            )

        version_hash_fields = {
            "publication_date": publication_date,
            "signing_date": signing_date,
            "disposition_notes": disposition_notes
        }

        # handles rare case where a num cant be found
        doc_name = f"EO {doc_num}" if doc_num else f"EO {doc_title}"

        return DocItem(
            doc_name=doc_name,
            doc_title=doc_title,
            doc_num=doc_num,
            publication_date=publication_date,
            source_page_url=source_page_url,
            downloadable_items=downloadable_items,
            version_hash_raw_data=version_hash_fields
        )

    def parse(self, response):
        all_orders_json_href = response.css(
            'div.page-summary.reader-aid ul.bulk-files li:nth-child(1) > span.links > a:nth-child(2)::attr(href)'
        ).get()

        yield response.follow(url=all_orders_json_href, callback=self.parse_data_page)

    def parse_data_page(self, response):
        data = json.loads(response.body)
        results = data.get('results')

        for doc in results:
            json_url = doc.get('json_url')
            yield response.follow(url=json_url, callback=self.get_doc_detail_data)

        next_url = data.get('next_page_url')

        if next_url:
            yield response.follow(url=next_url, callback=self.parse_data_page)

    def get_doc_detail_data(self, response):
        data = json.loads(response.body)

        doc_num = data.get("executive_order_number")
        raw_text_url = data.get("raw_text_url")
        if not doc_num:
            yield response.follow(
                url=raw_text_url,
                callback=self.get_exec_order_num_from_text,
                meta={"doc": data}
            )
        else:
            yield self.make_doc_item_from_dict(data)

    def get_exec_order_num_from_text(self, response):
        raw_text = str(response.body)
        doc = response.meta['doc']

        exec_order_num_groups = exec_order_re.search(raw_text)
        if exec_order_num_groups:
            exec_order_num = exec_order_num_groups.group(1)
            doc.update({"executive_order_number": exec_order_num})
            yield self.make_doc_item_from_dict(doc)

        else:
            # still no number found, just use title
            # 1 known example
            # "Closing of departments and agencies on April 27, 1994, in memory of President Richard Nixon"
            yield self.make_doc_item_from_dict(doc)
