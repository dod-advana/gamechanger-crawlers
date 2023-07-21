from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from datetime import datetime
from dataPipelines.gc_scrapy.gc_scrapy.utils import parse_timestamp, dict_to_sha256_hex_digest
from urllib.parse import urlparse


class DCMASpider(GCSpider):
    name = "DCMA" # Crawler name
    start_urls = [
        "https://www.dcma.mil/Policy/"
    ]
    rotate_user_agent = True

    @staticmethod
    def get_pub_date(publication_date):
        '''
        This function convverts publication_date from DD Month YYYY format to YYYY-MM-DDTHH:MM:SS format.
        T is a delimiter between date and time.
        '''
        try:
            date = parse_timestamp(publication_date, None)
            if date:
                publication_date = datetime.strftime(date, '%Y-%m-%dT%H:%M:%S')
        except:
            publication_date = ""
        return publication_date

    def parse(self, response):
        sections = response.css('div#accGen div table tbody')

        for section in sections:
            rows = section.css('tr')

            # skip headers
            for row in rows[1:]:
                title_raw = row.css('td:nth-child(1)::text').get(default="")
                doc_type_raw = row.css('td:nth-child(2)::text').get(default="")
                policy_no_raw = row.css(
                    'td:nth-child(3)::text').get(default="")
                published_raw = row.css(
                    'td:nth-child(4)::text').get(default="")
                href = row.css(
                    'td:nth-child(5) a::attr(href)').get(default="")

                if not href:
                    continue

                doc_title = self.ascii_clean(title_raw)
                doc_type = self.ascii_clean(doc_type_raw)
                doc_num = self.ascii_clean(policy_no_raw)
                publication_date = self.ascii_clean(published_raw)

                if doc_type == "DPS" or doc_type == "PTM":
                    display_doc_type = "Memo"
                else:
                    display_doc_type = doc_type

                doc_type = f"DCMA {doc_type}"
                source_page_url = response.url
                doc_item = self.populate_doc_item(doc_num, doc_type, display_doc_type, doc_title, publication_date, href, source_page_url)
                yield doc_item

    def populate_doc_item(self, doc_num, doc_type, display_doc_type, doc_title, publication_date, href, source_page_url):
        '''
        This functions provides both hardcoded and computed values for the variables
        in the imported DocItem object and returns the populated metadata object
        '''
        display_org = "Dept. of Defense" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "Defense Contract Management Agency Policy Publications" # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "DCMA Policy" # Level 3 filter
        cac_login_required = False # No CAC required for any documents
        is_revoked = False

        doc_name=f"{doc_type} {doc_num}"
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + ": " + doc_title
        file_type = self.get_href_file_extension(href)
        version_hash_fields = {
                    "item_currency": href,
                    "document_title": doc_title,
                    "display_title": display_title
                }
        download_url = f'https://www.dcma.mil{href}'
        downloadable_items = [{
                        "doc_type": file_type,
                        "download_url": download_url,
                        "compression_type": None
                    }]
        publication_date = self.get_pub_date(publication_date)
        source_fqdn = urlparse(source_page_url).netloc
        version_hash = dict_to_sha256_hex_digest(version_hash_fields)


        return DocItem(
                    doc_name = doc_name,
                    doc_title = doc_title,
                    doc_num = doc_num,
                    doc_type = doc_type,
                    display_doc_type = display_doc_type,
                    publication_date = publication_date,
                    cac_login_required = cac_login_required,
                    crawler_used = self.name,
                    downloadable_items = downloadable_items,
                    source_page_url = source_page_url,
                    source_fqdn = source_fqdn,
                    download_url = download_url, 
                    version_hash_raw_data = version_hash_fields,
                    version_hash = version_hash,
                    display_org = display_org,
                    data_source = data_source,
                    source_title = source_title,
                    display_source = display_source,
                    display_title = display_title,
                    file_ext = file_type,
                    is_revoked = is_revoked,

                )
