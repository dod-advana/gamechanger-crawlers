from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider


class DfarsSubpartSpider(GCSpider):
    name = "dfar_subpart_regs"

    start_urls = [
        'https://www.acquisition.gov/far'
    ]
    cac_login_required = False
    doc_type = "FAR"

    def parse(self, response):
        pub_date_raw = response.css(
            "table#browse-table-full tr:nth-child(2) td:nth-child(2)::text").get()
        pub_date = self.ascii_clean(pub_date_raw)

        rows = response.css('table#browse-table tr')

        # skip header row
        for row in rows[1:]:
            doc_num_title_raw = row.css('td:nth-child(1) a::text').get()

            doc_title = self.ascii_clean(doc_num_title_raw)

            # if doc_title.lower().startswith('appendix'):
            #     break

            href_raw = row.css('td:nth-child(4) a::attr(src)').get()

            doc_num = doc_title.split()[0] + ' ' + doc_title.split()[1]
            doc_name = self.doc_type + " " + doc_num

            web_url = self.ensure_full_href_url(href_raw, self.start_urls[0])

            downloadable_items = [
                {
                    "doc_type": 'html',
                    "web_url": web_url.replace(' ', '%20'),
                    "compression_type": None
                }
            ]

            version_hash_fields = {
                "item_currency": href_raw,
                "pub_date": pub_date
            }

            yield DocItem(
                doc_name=doc_name,
                doc_num=doc_num,
                doc_title=doc_title,
                publication_date=pub_date,
                downloadable_items=downloadable_items,
                version_hash_raw_data=version_hash_fields,
            )
