from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest, get_pub_date
from urllib.parse import urlparse

class CNGBISpider(GCSpider):
    """
        Parser for Chief National Guard Bureau Instructions
    """

    name = "National_Guard"  # Crawler name
    display_org = "National Guard"  # Level 1: GC app 'Source' filter for docs from this crawler
    data_source = "National Guard Bureau Publications & Forms Library"  # Level 2: GC app 'Source' metadata field for docs from this crawler
    source_title = "Unlisted Source"  # Level 3 filter
    display_source = data_source + " - " + source_title

    allowed_domains = ['ngbpmc.ng.mil']
    start_urls = [
        'https://www.ngbpmc.ng.mil/publications1/cngbi/'
    ]

    file_type = "pdf"
    doc_type = "CNGBI"
    rotate_user_agent = True

    def parse(self, response):
        rows = response.css('div.WordSection1 table tbody tr')
        for row in rows:
            href_raw = row.css('td:nth-child(1) a::attr(href)').get()

            if not href_raw.startswith('/'):
                cac_login_required = True
            else:
                cac_login_required = False

            web_url = self.ensure_full_href_url(href_raw, self.start_urls[0])

            file_type = self.get_href_file_extension(href_raw)
            web_url = web_url.replace(' ', '%20')
            downloadable_items = [
                {
                    "doc_type": file_type,
                    "download_url": web_url,
                    "compression_type": None
                }
            ]

            # a lot of the docs have the space unicode \xa0 in them. replacing it before getting doc_num
            doc_name_raw = row.css('td:nth-child(1) a::text')
            if doc_name_raw:
                doc_name_raw = doc_name_raw.get().replace(u'\xa0', ' ')
            else:
                continue

            doc_num_raw = doc_name_raw.replace('CNGBI ', '')

            publication_date = row.css('td:nth-child(2) span::text').get()

            doc_title_raw = row.css('td:nth-child(3) a::text').get()
            if doc_title_raw is None:
                doc_title_raw = row.css('td:nth-child(3) span::text').get()
                if doc_title_raw is None:
                    doc_title_raw = row.css('td:nth-child(3) font::text').get()
                    if doc_title_raw is None:
                        print("uh oh")

            doc_title = self.ascii_clean(doc_title_raw)
            display_title = self.doc_type + " " + doc_num_raw + ": " + doc_title
            source_page_url = self.start_urls[0]
            source_fqdn = urlparse(source_page_url).netloc

            version_hash_fields = {
                "item_currency": href_raw,
                "document_title": doc_title,
                "document_number": doc_num_raw
            }
            version_hash = dict_to_sha256_hex_digest(version_hash_fields)

            yield DocItem(
                doc_name=doc_name_raw,
                doc_title=doc_title,
                doc_num=doc_num_raw,
                doc_type=self.doc_type,
                display_doc_type=self.doc_type,
                publication_date=publication_date,
                cac_login_required=cac_login_required,
                crawler_used=self.name,
                downloadable_items=downloadable_items,
                source_page_url=source_page_url,
                source_fqdn=source_fqdn,
                download_url=web_url,
                version_hash_raw_data=version_hash_fields,
                version_hash=version_hash,
                display_org=self.display_org,
                data_source=self.data_source,
                source_title=self.source_title,
                display_source=self.display_source,
                display_title=display_title,
                file_ext=self.file_type,
                is_revoked=False,
            )

