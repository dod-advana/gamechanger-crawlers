import re

from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider

from urllib.parse import urljoin, urlparse
from datetime import datetime
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest, get_pub_date

digit_re = re.compile('\d')


def has_digit(text):
    return digit_re.match(text)


class MilpersmanSpider(GCSpider):
    name = 'milpersman_crawler' # Crawler name

    start_urls = ['https://www.mynavyhr.navy.mil/References/MILPERSMAN/']
    doc_type = "MILPERSMAN"
    rotate_user_agent = True

    def parse(self, response):

        anchors = [
            a for a in
            response.css('li[title="MILPERSMAN"] ul li a') if has_digit(a.css("::text").get())
        ]

        for res in response.follow_all(anchors, self.parse_doc_type):
            yield res

    def parse_doc_type(self, response):
        sub_anchors = response.css("ul.afAccordionMenuSubMenu a")
        # e.g. MILPERSMAN 1000 page has a dropdown for each subsection
        # pass each of those subpages to parse_page
        if len(sub_anchors):
            for res in response.follow_all(sub_anchors, self.parse_page):
                yield res
        else:
            for res in self.parse_page(response):
                yield res

    def parse_page(self, response):
        # get all rows that have an anchor tag in the first td
        rows = [
            tr for tr in
            response.css('div.livehtml table tbody tr')
            # have to check a special case
            if (len(tr.css('td:nth-child(1) a')) or len(tr.css('td:nth-child(2) a')))
        ]

        current_url = response.url

        for i, row in enumerate(rows):
            doc_num_raw = "".join(row.css('td:nth-child(1) *::text').getall())
            doc_num = self.ascii_clean(doc_num_raw)

            if not self.ascii_clean(doc_num):
                continue

            doc_title = " ".join(
                self.ascii_clean(text) for text in row.css('td:nth-child(2) *::text').getall()
            )

            href_raw = row.css("td:nth-child(1) a::attr(href)").get()
            web_url = self.ensure_full_href_url(href_raw, current_url)
            download_url = self.url_encode_spaces(web_url)

            file_type = self.get_href_file_extension(href_raw)

            downloadable_items = [
                {
                    "doc_type": file_type,
                    "download_url": download_url,
                    "compression_type": None
                }
            ]

            if doc_num == '1070-290':
                # if this changes, dont break
                try:
                    next_row = rows[i+1]
                    supp_href = next_row.css(
                        'td:nth-child(2) a::attr(href)').get()

                    supp_file_type = self.get_href_file_extension(supp_href)
                    supp_web_url = self.ensure_full_href_url(
                        supp_href, current_url)

                    downloadable_items.append({
                        "doc_type": supp_file_type,
                        "download_url": self.url_encode_spaces(supp_web_url),
                        "compression_type": None
                    })
                except:
                    pass

            doc_name = f"MILPERSMAN {doc_num}"

            fields = {
                'doc_name': doc_name,
                'doc_num': doc_num,
                'doc_title': doc_title,
                'doc_type': file_type,
                'cac_login_required': False,
                'source_page_url':current_url,
                'download_url': download_url,
                'downloadable_items': downloadable_items,
                #'publication_date': publication_date
            }
            ## Instantiate DocItem class and assign document's metadata values
            doc_item = self.populate_doc_item(fields)
        
            yield doc_item
        


    def populate_doc_item(self, fields):
        '''
        This functions provides both hardcoded and computed values for the variables
        in the imported DocItem object and returns the populated metadata object
        '''
        display_org="US Navy" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "MyNavy HR" # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Unlisted Source" # Level 3 filter

        doc_name = fields['doc_name']
        doc_num = fields['doc_num']
        doc_title = fields['doc_title']
        doc_type = fields['doc_type']
        cac_login_required = fields['cac_login_required']
        download_url = fields['download_url']
        #publication_date = get_pub_date(fields['publication_date'])

        display_doc_type = "Document" # Doc type for display on app
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + ": " + doc_title
        is_revoked = False
        source_page_url = fields['source_page_url']
        source_fqdn = urlparse(source_page_url).netloc

        downloadable_items = fields['downloadable_items']

        ## Assign fields that will be used for versioning
        version_hash_fields = {
            "doc_name":doc_name,
            "doc_num": doc_num,
            #"publication_date": publication_date,
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
                    #publication_date = publication_date,
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