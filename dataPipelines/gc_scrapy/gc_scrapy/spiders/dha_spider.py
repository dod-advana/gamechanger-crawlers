from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider

display_types = ["Instruction", "Manual", "Memo", "Regulation"]


class DHASpider(GCSpider):
    name = "dha_pubs"
    display_org = "Defense Health Agency"
    data_source = "Military Health System"
    source_title = "Defense Health Agency Publications"

    start_urls = [
        'https://www.health.mil/About-MHS/OASDHA/Defense-Health-Agency/Administration-and-Management/DHA-Publications'
    ]

    file_type = "pdf"
    cac_login_required = False
    randomly_delay_request = True

    @staticmethod
    def get_display(doc_type):
        for dt in display_types:
            if dt in doc_type:
                return dt

        return "Document"

    def parse(self, response):
        sections = response.css('div[data-ga-cat="File Downloads List"]')
        for section in sections:
            header = self.ascii_clean(section.css('h2::text').get(default=''))
            doc_type = header.replace('DHA-', 'DHA ').strip()
            display_doc_type = self.get_display(doc_type)

            rows = section.css('table.dataTable tbody tr')
            for row in rows:
                doc_num = self.ascii_clean(
                    row.css('td:nth-child(1) a::text').get(default=''))
                href = row.css('td:nth-child(1) a::attr(href)').get(default='')
                publication_date = self.ascii_clean(
                    row.css('td:nth-child(2)::text').get(default=''))
                doc_title = self.ascii_clean(
                    row.css('td:nth-child(3)::text').get(default='')).replace('\r', '').replace('\n', '')

                version_hash_fields = {
                    "item_currency": href,
                    "publication_date": publication_date
                }

                downloadable_items = [
                    {
                        "doc_type": self.file_type,
                        "web_url": f"https://www.health.mil{href}",
                        "compression_type": None
                    }
                ]

                doc_name = f"{doc_type} {doc_num}"

                yield DocItem(
                    doc_name=doc_name,
                    doc_title=doc_title,
                    doc_num=doc_num,
                    doc_type=doc_type,
                    display_doc_type=display_doc_type,
                    publication_date=publication_date,
                    cac_login_required=False,
                    downloadable_items=downloadable_items,
                    version_hash_raw_data=version_hash_fields,
                )
