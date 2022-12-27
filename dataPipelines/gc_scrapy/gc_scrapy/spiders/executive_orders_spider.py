# -*- coding: utf-8 -*-
import json
import re
from datetime import datetime
from urllib.parse import urlparse
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.utils import parse_timestamp, dict_to_sha256_hex_digest


exec_order_re = re.compile(
    r'(?:(?:Executive Order)|(?:Proclamation))\s*(\d+)', flags=re.IGNORECASE)


class ExecutiveOrdersSpider(GCSpider):
    name = "ex_orders" # Crawler name

    start_urls = [
        "https://www.federalregister.gov/presidential-documents/executive-orders"
    ]

    rotate_user_agent = True
    randomly_delay_request = True

    @staticmethod
    def get_pub_date(publication_date):
        '''
        This function convverts publication_date from DD Month YYYY format to YYYY-MM-DDTHH:MM:SS format.
        T is a delimiter between date and time.
        '''
        try:
            date = parse_timestamp(publication_date, None)
            if date:
                publication_date = datetime.strftime(date, '%Y-%m-%dT%H:%M:%S')
        except:
            publication_date = ""
        return publication_date

    def get_downloadables(self, pdf_url, xml_url, txt_url):
        """This function creates a list of downloadable_items dictionaries from a list of document links"""
        downloadable_items = []
        if pdf_url:
            downloadable_items.append(
                {
                    "doc_type": "pdf",
                    "download_url": pdf_url,
                    "compression_type": None,
                }
            )

        if xml_url:
            downloadable_items.append(
                {
                    "doc_type": "xml",
                    "download_url": xml_url,
                    "compression_type": None,
                }
            )

        if txt_url:
            downloadable_items.append(
                {
                    "doc_type": "txt",
                    "download_url": txt_url,
                    "compression_type": None,
                }
            )
        return downloadable_items

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
            yield self.populate_doc_item(data)

    def get_exec_order_num_from_text(self, response):
        raw_text = str(response.body)
        doc = response.meta['doc']

        exec_order_num_groups = exec_order_re.search(raw_text)
        if exec_order_num_groups:
            exec_order_num = exec_order_num_groups.group(1)
            doc.update({"executive_order_number": exec_order_num})
            yield self.populate_doc_item(doc)

        else:
            # still no number found, just use title
            # 1 known example
            # "Closing of departments and agencies on April 27, 1994, in memory of President Richard Nixon"
            yield self.populate_doc_item(doc)

    def populate_doc_item(self, doc: dict) -> DocItem:
        '''
        This functions provides both hardcoded and computed values for the variables
        in the imported DocItem object and returns the populated metadata object
        '''
        display_org = "Executive Branch" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "Federal Register" # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Unlisted Source" # Level 3 filter
        cac_login_required = False
        is_revoked = False
        doc_type = "EO" # All documents are excutive orders
        display_doc_type = "Order"
 
        doc_title = doc.get('title')
        publication_date = doc.get('publication_date', '')
        publication_date = self.get_pub_date(publication_date)
        source_page_url = doc.get('html_url')
        disposition_notes = doc.get('disposition_notes', '')
        signing_date = doc.get('signing_date', '')
        doc_num = doc.get('executive_order_number', '')
        if doc_num == "12988" and 'CHAMPUS' in doc_title:
            # this is not an executive order, its a notice from OSD
            # there may be other errors but this has a conflicting doc num
            # https://www.federalregister.gov/documents/1996/02/09/96-2755/civilian-health-and-medical-program-of-the-uniformed-services-champus
            return

        pdf_url = doc.get('pdf_url')
        xml_url = doc.get('full_text_xml_url')
        txt_url = doc.get('raw_text_url')
        downloadable_items = self.get_downloadables(pdf_url, xml_url, txt_url)
        doc_name = f"EO {doc_num}" if doc_num else f"EO {doc_title}"
        download_url = downloadable_items[0]["download_url"]
        file_type = self.get_href_file_extension(download_url)
        version_hash_fields = {
            "publication_date": publication_date,
            "signing_date": signing_date,
            "disposition_notes": disposition_notes,
            "doc_name": doc_name,
            "doc_num": doc_num,
            "download_url": download_url
        }
        # handles rare case where a num cant be found
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + " " + doc_title
        source_fqdn = urlparse(source_page_url).netloc
        version_hash = dict_to_sha256_hex_digest(version_hash_fields)

        return DocItem(
            doc_name = doc_name,
            doc_title = doc_title,
            doc_num = doc_num,
            doc_type = doc_type,
            display_doc_type = display_doc_type,
            publication_date = publication_date,
            cac_login_required = cac_login_required,
            crawler_used = self.name,
            downloadable_items = downloadable_items,
            source_page_url = source_page_url,
            source_fqdn = source_fqdn,
            download_url = download_url, 
            version_hash_raw_data = version_hash_fields,
            version_hash = version_hash,
            display_org = display_org,
            data_source = data_source,
            source_title = source_title,
            display_source = display_source,
            display_title = display_title,
            file_ext = file_type,
            is_revoked = is_revoked,
            )

