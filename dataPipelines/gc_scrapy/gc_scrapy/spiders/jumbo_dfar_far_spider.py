import scrapy
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider


class JumboDfarFarSpider(GCSpider):
    name = "jumbo_dfar_far"
    cac_login_required = False

    start_url_data = (
        (
            "https://www.acquisition.gov/dfars",
            "Federal Acquisition Regulation",
            "FAR"
        ),
        (
            "https://www.acquisition.gov/far",
            "Defense Federal Acquisition Regulation Supplement",
            "DFARS"
        )
    )

    def start_requests(self):
        for (url, doc_title, doc_type) in self.start_url_data:
            yield scrapy.Request(url=url, callback=self.parse, meta={"doc_title": doc_title, "doc_type": doc_type}, dont_filter=True)

    def parse(self, response):
        source_page_url = response.url
        doc_title = response.meta['doc_title']
        doc_type = response.meta['doc_type']
        doc_name = doc_title
        crawler_used = f"jumbo_{doc_type}"

        data_row = response.css("table#browse-table-full tr:nth-child(2)")
        pub_date_raw = data_row.css("td:nth-child(2)::text").get()
        pub_date = self.ascii_clean(pub_date_raw)

        try:
            href_raw = next(
                a.css("::attr(href)").get() for a in data_row.css('a')
                if '.pdf' in a.css("::attr(href)").get()
            )
        except:
            raise Exception(f'{doc_title}: href for pdf not found')

        web_url = self.ensure_full_href_url(href_raw, source_page_url)
        downloadable_items = [
            {
                "doc_type": 'pdf',
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
            doc_title=doc_title,
            doc_type=doc_type,
            source_page_url=source_page_url,
            crawler_used=crawler_used,
            publication_date=pub_date,
            downloadable_items=downloadable_items,
            version_hash_raw_data=version_hash_fields,
        )
