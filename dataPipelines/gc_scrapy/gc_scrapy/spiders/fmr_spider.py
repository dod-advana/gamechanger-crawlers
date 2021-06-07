from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider


class FmrSpider(GCSpider):
    name = "fmr_pubs"

    start_urls = [
        "https://comptroller.defense.gov/FMR/vol1_chapters.aspx"
    ]

    download_base_url = 'https://comptroller.defense.gov/'
    doc_type = "DoDFMR"
    cac_login_required = False

    seen = set({})

    def parse(self, response):
        volume_links = response.css('div[id="sitetitle"] a')[1:-1]
        for link in volume_links:
            vol_num = link.css('::text').get()
            yield response.follow(url=link, callback=self.parse_volume, meta={"vol_num": vol_num})

    def parse_volume(self, response):
        vol_num = response.meta["vol_num"]
        rows = response.css('tbody tr')
        source_page_url = response.url

        for row in rows:
            href_raw = row.css('td:nth-child(1) a::attr(href)').get()
            if not href_raw:
                continue

            section_num_raw = row.css('td:nth-child(1) a::text').get()
            section_type, _, ch_num = section_num_raw.rpartition(' ')

            if section_type not in ('Chapter', 'Appendix'):
                ch_num = ch_num[0:3]

            doc_title_raw = "".join(
                row.css('td:nth-child(2) *::text').getall())

            if '(' in doc_title_raw:
                doc_title_text, *_ = doc_title_raw.rpartition('(')
            else:
                doc_title_text = doc_title_raw

            doc_title = self.ascii_clean(doc_title_text)
            publication_date_raw = row.css('td:nth-child(3)::text').get()
            publication_date = self.ascii_clean(publication_date_raw)
            doc_num = f"V{vol_num}CH{ch_num}"
            doc_name = f"{self.doc_type} {doc_num}"

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

            if doc_name in self.seen:
                extra, *_ = doc_title.partition(':')
                doc_name += f" {extra}"

            self.seen.add(doc_name)

            yield DocItem(
                doc_name=doc_name,
                doc_num=doc_num,
                doc_title=doc_title,
                publication_date=publication_date,
                source_page_url=source_page_url,
                downloadable_items=downloadable_items,
                version_hash_raw_data=version_hash_fields,
            )
