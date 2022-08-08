import re
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem

from urllib.parse import urljoin, urlparse
from datetime import datetime
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest, get_pub_date

doc_type_num_re = re.compile(r'(.*)\s(\d+.*)')


class JcsPubsSpider(GCSpider):
    name = 'jcs_pubs' # Crawler name

    start_urls = ['https://www.jcs.mil/Library/']
    base_url = 'https://www.jcs.mil'

    cac_required_options = [
        'CAC', 'PKI certificate required', 'placeholder', 'FOUO']
    rotate_user_agent = True

    @staticmethod
    def get_display_doc_type(doc_type):
        """This function returns value for display_doc_type based on doc_type -> display_doc_type mapping"""
        display_type_dict = {
            "cjcs": 'Notice',
            "cjcsi": 'Instruction',
            "cjcsm": 'Manual',
            "cjcsg": 'Guide'
        }
        if doc_type.lower() in display_type_dict.keys():
            return display_type_dict[doc_type.lower()]
        else:
            return "Document"

    def parse(self, response):
        doc_links = [
            a for a in response.css('div.librarylinkscontainer a') if 'CJCS' in a.css('::attr(href)').get()
        ]

        for link in doc_links:
            yield response.follow(url=link, callback=self.parse_doc_table_page)

    def parse_doc_table_page(self, response):
        rows = response.css('table#JCSDocsTable tbody tr')
        if not len(rows):
            return

        for row in rows:
            doc_type_num_raw = row.css('td.DocNoCol a::text').get()

            doc_type_num_groups = doc_type_num_re.search(doc_type_num_raw)
            if doc_type_num_groups:
                doc_type_raw = doc_type_num_groups.group(1)
                doc_num_raw = doc_type_num_groups.group(2)
            else:
                print("FAILED TO FIND GROUPS", doc_type_num_raw)
                continue

            doc_type = self.ascii_clean(doc_type_raw)
            doc_num = self.ascii_clean(doc_num_raw)
            href_raw = row.css('td.DocNoCol a::attr(href)').get()
            doc_title = row.css('td.DocTitle::text').get()
            publication_date = row.css('td.DocDateCol::text').get()
            current_of_date = row.css('td.CurrentCol::text').get(default='')

            web_url = self.ensure_full_href_url(href_raw, self.base_url)

            doc_name = f"{doc_type} {doc_num}"

            # set boolean if CAC is required to view document
            cac_login_required = True if any(x in href_raw for x in self.cac_required_options) \
                or any(x in doc_title for x in self.cac_required_options) else False

            fields = {
                'doc_name': doc_name,
                'doc_num': doc_num,
                'doc_title': doc_title,
                'doc_type': doc_type,
                'cac_login_required': cac_login_required,
                'download_url': web_url,
                'source_page_url':response.url,
                'publication_date': publication_date
            }
            ## Instantiate DocItem class and assign document's metadata values
            doc_item = self.populate_doc_item(fields)
        
            yield doc_item

        try:
            nav_table = response.css('table.dnnFormItem')[1]
            next_page_link = next(
                a for a in nav_table.css('a.CommandButton')
                if a.css('::text').get() == 'Next'
            )
            yield response.follow(url=next_page_link,
                                  callback=self.parse_doc_table_page)
        except:
            pass

    def populate_doc_item(self, fields):
        '''
        This functions provides both hardcoded and computed values for the variables
        in the imported DocItem object and returns the populated metadata object
        '''
        display_org = "Joint Chiefs of Staff" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "CJCS Directives Library" # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Unlisted Source" # Level 3 filter

        doc_name = fields['doc_name']
        doc_num = fields['doc_num']
        doc_title = fields['doc_title']
        doc_type = fields['doc_type']
        cac_login_required = fields['cac_login_required']
        download_url = fields['download_url']
        publication_date = get_pub_date(fields['publication_date'])

        display_doc_type = self.get_display_doc_type(doc_type)
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + " " + doc_title
        is_revoked = False
        access_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f") # T added as delimiter between date and time
        source_page_url = fields['source_page_url']
        source_fqdn = urlparse(source_page_url).netloc

        downloadable_items = [{
                "doc_type": "pdf",
                "download_url": download_url.replace(' ', '%20'),
                "compression_type": None,
            }]
        file_ext = downloadable_items[0]["doc_type"]
        ## Assign fields that will be used for versioning
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
                    file_ext = file_ext, #
                    is_revoked = is_revoked, #
                    access_timestamp = access_timestamp #
                )
