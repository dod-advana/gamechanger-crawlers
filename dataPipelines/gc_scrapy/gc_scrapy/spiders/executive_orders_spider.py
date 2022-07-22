# -*- coding: utf-8 -*-
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
import json
import re

exec_order_re = re.compile(
    r'(?:(?:Executive Order)|(?:Proclamation))\s*(\d+)', flags=re.IGNORECASE)


class ExecutiveOrdersSpider(GCSpider):
    name = "ex_orders" # Crawler name

    start_urls = [
        "https://www.federalregister.gov/presidential-documents/executive-orders"
    ]

    rotate_user_agent = True
    randomly_delay_request = True

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

    def populate_doc_item(self, doc: dict) -> DocItem:
        display_org = "Executive Branch" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "Federal Register" # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Unlisted Source" # Level 3 filter
        cac_login_required = False
        doc_type = "EO" # All documents are excutive orders
 
        doc_title = doc.get('title')
        publication_date = doc.get('publication_date', '')
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


########################################################

    def populate_doc_item(self, fields):
        '''
        This functions provides both hardcoded and computed values for the variables
        in the imported DocItem object and returns the populated metadata object
        '''
        display_org = "Dept. of Defense" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "WHS DoD Directives Division" # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Unlisted Source" # Level 3 filter
        is_revoked = False
        cac_login_required = fields.get("cac_login_required")
        source_page_url = fields.get("page_url")
        office_primary_resp = fields.get("office_primary_resp")

        doc_name = self.ascii_clean(fields.get("doc_name").strip())
        doc_title = self.ascii_clean(re.sub('\\"', '', fields.get("doc_title")))
        doc_num = self.ascii_clean(fields.get("doc_num").strip())
        doc_type = self.ascii_clean(fields.get("doc_type").strip())
        publication_date = self.ascii_clean(fields.get("publication_date").strip())
        publication_date = self.get_pub_date(publication_date)
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + " " + doc_title
        display_doc_type = self.get_display_doc_type(doc_type.lower())
        download_url = fields.get("pdf_url")
        file_type = self.get_href_file_extension(download_url)
        downloadable_items = [fields.get("pdf_di")]
        version_hash_fields = {
                "download_url": download_url,
                "pub_date": publication_date,
                "change_date": fields.get("chapter_date").strip(),
                "doc_num": doc_num,
                "doc_name": doc_name
            }
        source_fqdn = urlparse(source_page_url).netloc
        version_hash = dict_to_sha256_hex_digest(version_hash_fields)
        access_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f") # T added as delimiter between date and time

        return DocItem(
                doc_name = doc_name,
                doc_title = doc_title,
                doc_num = doc_num,
                doc_type = doc_type,
                display_doc_type_s = display_doc_type,
                publication_date_dt = publication_date,
                cac_login_required_b = cac_login_required,
                crawler_used_s = self.name,
                downloadable_items = downloadable_items,
                source_page_url_s = source_page_url,
                source_fqdn_s = source_fqdn,
                download_url_s = download_url, 
                version_hash_raw_data = version_hash_fields,
                version_hash_s = version_hash,
                display_org_s = display_org,
                data_source_s = data_source,
                source_title_s = source_title,
                display_source_s = display_source,
                display_title_s = display_title,
                file_ext_s = file_type,
                is_revoked_b = is_revoked,
                office_primary_resp = office_primary_resp,
                access_timestamp_dt = access_timestamp
            )

