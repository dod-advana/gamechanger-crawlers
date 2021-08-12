from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
import json
import scrapy


class SornSpider(GCSpider):
    name = "SORN"
    start_urls = [
        "https://www.federalregister.gov/api/v1/agencies/defense-department"
    ]
    cac_login_required = False
    doc_type = "SORN"
    display_org = "Dept. of Defense"
    display_source = "Federal Registry"

    def parse(self, response):
        data = json.loads(response.body)
        agency_list_pull = data['child_slugs']
        conditions = ""
        for item in agency_list_pull:
            conditions = conditions + "&conditions[agencies][]=" + item
        notices = "&conditions[type][]=NOTICE"
        page_size = "1000"
        base_url = "https://www.federalregister.gov/api/v1/documents.json?per_page=" + page_size + \
            "&order=newest&conditions[term]=%22Privacy%20Act%20of%201974%22%20%7C%20%22System%20of%20Records%22"
        next_url = base_url+conditions+notices
        yield scrapy.Request(url=next_url, callback=self.parse_data)

    def parse_data(self, response):
        response_json = json.loads(response.body)
        sorns_list = response_json['results']

        for sorn in sorns_list:

            downloadable_items = [
                {
                    "doc_type": "pdf",
                    "web_url": sorn["pdf_url"],
                    "compression_type": None
                }
            ]

            version_hash_raw_data = {
                "item_currency": sorn["publication_date"],
                "public_inspection": sorn["public_inspection_pdf_url"],
                "title": sorn["title"],
                "type": sorn["type"]
            }

            yield DocItem(
                doc_type="SORN",
                doc_name="SORN " + sorn["document_number"],
                doc_title=sorn["title"],
                doc_num=sorn["document_number"],
                display_doc_type="Notice",
                publication_date=sorn["publication_date"],
                downloadable_items=downloadable_items,
                version_hash_raw_data=version_hash_raw_data,
                source_page_url=sorn["html_url"]
            )
        if 'next_page_url' in response_json:
            yield scrapy.Request(url=response_json['next_page_url'], callback=self.parse_data)
