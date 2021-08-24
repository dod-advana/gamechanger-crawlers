from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
import json
import re


bill_version_re = re.compile(r'\((.*)\)')


class CFRSpider(GCSpider):
    name = "code_of_federal_regulations"
    display_org = "Congress"
    display_source = "U.S. Publishing Office"

    cac_login_required = False
    visible_start = "https://www.govinfo.gov/app/collection/cfr"
    start_urls = [
        "https://www.govinfo.gov/wssearch/rb/cfr?fetchChildrenOnly=0"
    ]

    @staticmethod
    def get_visible_detail_url(package_id: str) -> str:
        return f"https://www.govinfo.gov/app/details/{package_id}"

    @staticmethod
    def get_pdf_file_download_url_from_id(package_id: str) -> str:
        return f"https://www.govinfo.gov/content/pkg/{package_id}/pdf/{package_id}.pdf"

    def parse(self, start_url_response):
        data = json.loads(start_url_response.body)
        years = [
            node.get('nodeValue').get('browsePath')for node in data.get('childNodes', [])
        ]

        for year in years:
            year_url = f"https://www.govinfo.gov/wssearch/rb//cfr/{year}/?fetchChildrenOnly=1"
            yield start_url_response.follow(url=year_url, callback=self.handle_title_nums)

    def handle_title_nums(self, year_response):
        data = json.loads(year_response.body)
        cnodes = data.get('childNodes', [])
        title_num_nodes = [cnode['nodeValue'] for cnode in cnodes]

        for title_num_dict in title_num_nodes:

            if title_num_dict.get('volumes'):
                for vol in title_num_dict.get('volumes'):
                    package_id = vol.get('packageid')
                    vol_num = vol.get('volume')

                    vol_data = title_num_dict.copy()
                    vol_data.update({
                        "packageid": package_id,
                        "volume": vol_num
                    })

                    yield self.make_doc_item_from_dict(vol_data)
            else:
                yield self.make_doc_item_from_dict(title_num_dict)

    def make_doc_item_from_dict(self, data):
        publication_date = data.get('publishdate')
        title = data.get('title')
        title_num = data.get('cfrtitlenumber', "")
        package_id = data.get('packageid')
        vol_num = data.get('volume')

        source_page_url = self.get_visible_detail_url(package_id)

        is_index_type = "GPO-CFR-INDEX" in package_id

        doc_type = "CFR Index" if is_index_type else 'CFR Title'
        display_doc_type = doc_type
        doc_title = title if is_index_type else title.title()
        doc_num = f"{title_num} Vol. {vol_num}" if vol_num else title_num

        downloadable_items = []
        web_url = self.get_pdf_file_download_url_from_id(package_id)

        downloadable_items.append(
            {
                "doc_type": "pdf",
                "web_url": web_url,
                "compression_type": None,
            }
        )

        version_hash_fields = {
            "publication_date": publication_date,
            "item_currency": web_url
        }

        return DocItem(
            doc_name=package_id,
            doc_num=doc_num,
            doc_title=doc_title,
            doc_type=doc_type,
            display_doc_type=display_doc_type,
            publication_date=publication_date,
            source_page_url=source_page_url,
            downloadable_items=downloadable_items,
            version_hash_raw_data=version_hash_fields
        )
