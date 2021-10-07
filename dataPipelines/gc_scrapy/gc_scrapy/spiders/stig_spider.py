from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
import re

class StigSpider(GCSpider):
    name = "stig_pubs"

    start_urls = [
        "https://public.cyber.mil/stigs/downloads/"
    ]

    download_base_url = 'https://public.cyber.mil/'
    doc_type = "STIG"
    cac_login_required = False
    source_title = "Security Technical Implementation Guides"
    display_org = "Defense Information Systems Agency"
    data_source = "Federal Registry"

    @staticmethod
    def extract_doc_number(doc_title):
        if doc_title.find(" Ver ") != -1:
            ver_num = (re.findall(r' Ver (\w+)', doc_title))[0]
        else:
            if " Version " in doc_title:
                ver_num = (re.findall(r' Version (\w+)', doc_title))[0]
            else:
                ver_num = 0

        if doc_title.find(" Rel ") != -1:
            ref_num = (re.findall(r' Rel (\w+)', doc_title))[0]
        else:
            if "Release Memo" in doc_title:
                ref_num = 1
            else:
                ref_num = 0

        doc_num = "V{}R{}".format(ver_num, ref_num)
        return doc_title, doc_num

    def parse(self, response):
        source_page_url = response.url
        rows = response.css('table tbody tr')
        rows = [a for a in rows if a.css('a::attr(href)').get()]
        rows = [a for a in rows if a.css('a::attr(href)').get().endswith("pdf")]

        for row in rows:
            href_raw = row.css('a::attr(href)').get()
            doc_title_text, publication_date_raw = row.css('span[style="display:none;"] ::text').getall()
            doc_title = self.ascii_clean(doc_title_text).replace("/ ", " ").replace("/", " ")
            publication_date = self.ascii_clean(publication_date_raw)
            doc_title, doc_num = StigSpider.extract_doc_number(doc_title)
            doc_name = f"{self.doc_type} {doc_num} {doc_title}"

            if "Memo" in doc_title:
                display_doc_type = "Memo"
            else:
                display_doc_type = "STIG"

            file_type = self.get_href_file_extension(href_raw)
            web_url = self.ensure_full_href_url(
                href_raw, self.download_base_url)

            downloadable_items = [
                {
                    "doc_type": file_type,
                    "web_url": web_url.replace(' ', '%20'),
                    "compression_type": None
                }
            ]

            version_hash_fields = {
                "publication_date": publication_date,
                "item_currency": href_raw
            }

            yield DocItem(
                doc_name=doc_name,
                doc_num=doc_num,
                doc_title=doc_title,
                data_source=self.data_source,
                source_title=self.source_title,
                display_org=self.display_org,
                display_doc_type=display_doc_type,
                publication_date=publication_date,
                source_page_url=source_page_url,
                downloadable_items=downloadable_items,
                version_hash_raw_data=version_hash_fields
            )
