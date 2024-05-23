from calendar import day_abbr
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
import scrapy
import typing as t

from urllib.parse import urljoin, urlparse
from datetime import datetime
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest, get_pub_date

# only scrape witness statements
# 193 documents

class HASCSpider(GCSpider):
    name = "HASC" # Crawler name

    base_url = "https://armedservices.house.gov"

    start_urls = [base_url]

    randomly_delay_request = True
    rotate_user_agent = True
    
    custom_settings = {
        **GCSpider.custom_settings,
        "AUTOTHROTTLE_ENABLED": True, 
        "AUTOTHROTTLE_START_DELAY": 10,
        "AUTOTHROTTLE_MAX_DELAY": 60,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
    }
    
    def parse(self, _):
        pages_parser_map = [
            (f"{self.base_url}/hearings", self.recursive_parse_hearings),
            # (f"{self.base_url}/legislation",) # setup for future crawlers if needed
        ]

        for page_url, parser_func in pages_parser_map:
            yield scrapy.Request(page_url, callback=parser_func)

    @staticmethod
    def get_next_relative_url(response):
        return response.css("li.pager-next > a::attr(href)").get()

    def recursive_parse_hearings(self, response):

        yield from self.parse_hearings_table_page(response)

        next_relative_url = self.get_next_relative_url(response)
        if next_relative_url:
            next_url = f"{self.base_url}{next_relative_url}"
            yield scrapy.Request(url=next_url, callback=self.recursive_parse_hearings)

    def parse_hearings_table_page(self, response):

        rows = response.css(
            "div.view-content div")

        for row in rows:
            try:
                link = row.css("h3.field-content a::attr(href)").get()

                if not link:
                    continue

                follow_link = f"{self.base_url}{link}"
                yield scrapy.Request(url=follow_link, callback=self.parse_hearing_detail_page)
            except Exception as e:
                print(e)

    def extract_doc_name_from_url(self, url):
        doc_name =  url.split('/')[-1]
        doc_name = doc_name.replace('.pdf', '').replace('%', '_').replace('.', '').replace('-', '')
        return doc_name

    def parse_hearing_detail_page(self, response):
        try:
            # Get the basic details like title and date from the page
            title = self.ascii_clean(response.css("#page-title ::text").get())
            date_el = response.css("span.date-display-single ::text").get()
            date_split = date_el.split()
            month, day, year = date_split[1], date_split[2], date_split[3]
            date = f"{month} {day} {year}"
            doc_type = "Witness Statement"

            # Extract names of speakers
            names = response.css('b ::text').getall()

            # Find all <a> tags within <p> tags and check if they contain the word "statement" and point to a PDF
            links = response.css("p a")
            for link in links:
                href = link.css("::attr(href)").get()
                link_text = link.css("::text").get("").lower() # Get the text and convert it to lower case for comparison
                
                # Check if "statement" is in the link text and the href ends with ".pdf"
                if "statement" in link_text and href and href.endswith(".pdf"):
                    # Check if any of the speaker names is in the link text
                    for name in names:
                        if name.lower() in link_text:
                            follow_link = urljoin(self.base_url, href)
                            display_title = self.ascii_clean(f"HASC {title} - {name}")
                            doc_name = self.extract_doc_name_from_url(follow_link)

                            # Set up the fields with the new PDF URL
                            fields = {
                                'doc_name': doc_name,
                                'doc_num': ' ',  # No doc num for this crawler
                                'doc_title': title,
                                'doc_type': doc_type,
                                'cac_login_required': False,
                                'source_page_url': response.url,
                                'download_url': follow_link,
                                'publication_date': date,
                                'file_ext': 'pdf', # Set to return pdf NOT html
                                'display_title': display_title
                            }
                            # Instantiate DocItem class and assign document's metadata values
                            doc_item = self.populate_doc_item(fields)

                            yield doc_item

        except Exception as e:
            print(e)


    def populate_doc_item(self, fields):
        # '''
        # This functions provides both hardcoded and computed values for the variables
        # in the imported DocItem object and returns the populated metadata object
        # '''
        display_org = "Congress" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "House Armed Services Committee Publications" # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "House Armed Services Committee" # Level 3 filter

        doc_name = fields['doc_name']
        doc_num = fields['doc_num']
        doc_title = fields['doc_title']
        doc_type = fields['doc_type']
        cac_login_required = fields['cac_login_required']
        download_url = fields['download_url']
        publication_date = get_pub_date(fields['publication_date'])

        display_doc_type = fields['doc_type'] # Doc type for display on app
        display_source = data_source + " - " + source_title
        display_title = fields['display_title']
        is_revoked = False
        source_page_url = fields['source_page_url']
        source_fqdn = urlparse(source_page_url).netloc

        downloadable_items = [{
                "doc_type": fields['file_ext'],
                "download_url": download_url,
                "compression_type": None,
            }]
        file_ext = fields['file_ext'] # Set to return pdf NOT html

        ## Assign fields that will be used for versioning
        version_hash_fields = {
            "doc_name":doc_name,
            "doc_title": fields['doc_title'],
            "publication_date": publication_date,
            "download_url": download_url,
            "display_title": display_title
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
                    file_ext = file_ext, # Set to return pdf NOT html
                    is_revoked = is_revoked, #
        )