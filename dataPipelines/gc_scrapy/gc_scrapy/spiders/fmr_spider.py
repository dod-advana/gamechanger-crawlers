from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider

from urllib.parse import urljoin, urlparse
from datetime import datetime
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest, get_pub_date


class FmrSpider(GCSpider):
    name = "fmr_pubs" # Crawler name

    start_urls = [
        "https://comptroller.defense.gov/FMR/vol1_chapters.aspx"
    ]

    download_base_url = 'https://comptroller.defense.gov/'
    doc_type = "DoDFMR"
    rotate_user_agent = True

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

            

            if doc_name in self.seen:
                extra, *_ = doc_title.partition(':')
                doc_name += f" {extra}"

            self.seen.add(doc_name)

            fields = {
                'doc_name': doc_name,
                'doc_num': doc_num,
                'doc_title': doc_title,
                'doc_type': self.doc_type,
                'file_type': file_type,
                'cac_login_required': False,
                'download_url': web_url,
                'source_page_url':response.url,
                'publication_date': publication_date
            }
            ## Instantiate DocItem class and assign document's metadata values
            doc_item = self.populate_doc_item(fields)
        
            yield doc_item
        


    def populate_doc_item(self, fields):
        '''
        This functions provides both hardcoded and computed values for the variables
        in the imported DocItem object and returns the populated metadata object
        '''
        display_org = "FMR" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "Under Secretary of Defense (Comptroller)" # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Unlisted Source" # Level 3 filter

        doc_name = fields['doc_name']
        doc_num = fields['doc_num']
        doc_title = fields['doc_title']
        doc_type = fields['doc_type']
        cac_login_required = fields['cac_login_required']
        download_url = fields['download_url']
        publication_date = get_pub_date(fields['publication_date'])

        display_doc_type = "Document" # Doc type for display on app
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + ": " + doc_title
        is_revoked = False
        source_page_url = fields['source_page_url']
        source_fqdn = urlparse(source_page_url).netloc

        downloadable_items = [
            {
                "doc_type": fields['file_type'],
                "download_url": download_url.replace(' ', '%20'),
                "compression_type": None
            }
        ]

        version_hash_fields = {
            "doc_name":doc_name,
            "doc_num": doc_num,
            "publication_date": publication_date,
            "download_url": download_url
        }

        version_hash = dict_to_sha256_hex_digest(version_hash_fields)

        return DocItem(
                    doc_name = doc_name,
                    doc_title = doc_title,
                    doc_num = doc_num,
                    doc_type = doc_type,
                    display_doc_type = display_doc_type, #
                    publication_date = publication_date,
                    cac_login_required = cac_login_required,
                    crawler_used = self.name,
                    downloadable_items = downloadable_items,
                    source_page_url = source_page_url, #
                    source_fqdn = source_fqdn, #
                    download_url = download_url, #
                    version_hash_raw_data = version_hash_fields, #
                    version_hash = version_hash,
                    display_org = display_org, #
                    data_source = data_source, #
                    source_title = source_title, #
                    display_source = display_source, #
                    display_title = display_title, #
                    file_ext = doc_type, #
                    is_revoked = is_revoked, #
                )
