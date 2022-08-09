# -*- coding: utf-8 -*-

from time import sleep
import re
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
import scrapy
import json

from urllib.parse import urljoin, urlparse
from datetime import datetime
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest, get_pub_date

json_re = re.compile("var WPQ3ListData = (?P<json>{.*?});", flags=re.M | re.S)


class SecNavSpider(GCSpider):
    name = "secnav_pubs" # Crawler name
    
    start_urls = [
        "https://www.secnav.navy.mil/doni/default.aspx",
    ]
    urls_type_map = [
        ("https://www.secnav.navy.mil/doni/allinstructions.aspx", "INST"),
        ("https://www.secnav.navy.mil/doni/notices.aspx", "NOTE")
    ]
    download_base_url = "https://www.secnav.navy.mil"

    rotate_user_agent = False
    randomly_delay_request = False

    had_error = False
    q = []
    ready_to_process = False
    done = []

    @staticmethod
    def get_display_doc_type(doc_type):
        if doc_type.strip().lower().endswith("inst"):
            return "Instruction"
        elif doc_type.strip().lower().endswith("note"):
            return "Notice"
        else:
            return "Document"

    def enqueue(self, doc):
        self.q.append(doc)

    def start_rate_limited_yield(self):
        if self.had_error:
            print("SecNavSpider Error:", self.had_error)
            return

        elif self.ready_to_process:
            print(
                f"ready to process: len of q: {len(self.q)}, done: {self.done}")
            while self.q:
                doc = self.q.pop(0)
                try:
                    sleep(0.75)
                except KeyboardInterrupt:
                    exit(0)

                yield(doc)

        else:
            print('SecNavSpider queue still accumulating... no errors but not ready to process\n',
                  f"len of q: {len(self.q)}, done: {self.done}, had_error: {self.had_error}")

    def start_requests(self):
        for url, type_suffix in self.urls_type_map:
            sleep(5)
            meta = {
                "referrer_policy": "same-origin",
                "base_url": url,
                "type_suffix": type_suffix
            }
            yield scrapy.Request(url=url, meta=meta)

    def parse(self, response):
        try:
            type_suffix = response.meta["type_suffix"]
            base_url = response.meta["base_url"]

            raw_scripts = [script for script in response.css(
                'script').getall() if 'WPQ3ListData' in script]

            raw_script = raw_scripts[0]

            matched = json_re.search(raw_script)
            json_str = matched.group('json')
            try:
                data = json.loads(json_str)
            except Exception as e:
                print('Failed to load json data from variable', e)
                return

            for r in data['Row']:
                echelon = self.ascii_clean(r.get("Echelon"))
                doc_num_file = self.ascii_clean(r.get('FileLeafRef'))
                doc_num = doc_num_file.replace('.pdf', '')
                web_url_suffix = r.get("FileRef")
                doc_type = f"{echelon}{type_suffix}"

                #office_primary_resp=sponsor
                fields = {
                    'doc_name': f"{doc_type} {doc_num}",
                    'doc_num': doc_num,
                    'doc_title': self.ascii_clean(r.get('Subject')),
                    'doc_type': doc_type,
                    'file_type': r.get("File_x0020_Type"),
                    'sponsor': r.get("Sponsor", "").replace("&amp;", "&"),
                    'cancel_date': r.get("Cancelled_x0020_Date"),
                    'cac_login_required': re.match('^[A-Za-z]', doc_num) != None,
                    'is_revoked': r.get("Status") != 'Active',
                    'download_url': f"{self.download_base_url}{web_url_suffix}",
                    'publication_date': r.get("Effective_x0020_Date")
                }
                ## Instantiate DocItem class and assign document's metadata values
                doc_item = self.populate_doc_item(fields)
                self.enqueue(doc_item)

            next_href = data.get("NextHref")
            if next_href:
                next_url = f"{response.meta['base_url']}{next_href}"
                meta = {
                    "referrer_policy": "same-origin",
                    "base_url": base_url,
                    "type_suffix": type_suffix
                }
                sleep(5)
                yield scrapy.Request(url=next_url, callback=self.parse, meta=meta)
            else:
                self.done.append(base_url)
                if len(self.done) == len(self.urls_type_map):
                    self.ready_to_process = True

        except Exception as e:
            print("Unexpected exception in SecNavSpider\n", e)
            self.had_error = e
            self.ready_to_process = True
        finally:
            yield from self.start_rate_limited_yield()


    def populate_doc_item(self, fields):
        '''
        This functions provides both hardcoded and computed values for the variables
        in the imported DocItem object and returns the populated metadata object
        '''
        display_org = "US Navy" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "Dept. of the Navy Issuances" # Level 2: GC app 'Source' metadata field for docs from this crawler
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
        is_revoked = fields['is_revoked']
        
        access_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f") # T added as delimiter between date and time
        source_page_url = "https://www.secnav.navy.mil/doni/default.aspx"
        source_fqdn = urlparse(source_page_url).netloc
        file_ext = fields['file_type']

        downloadable_items = [{
                "doc_type": file_ext,
                "download_url": download_url,
                "compression_type": None,
            }]
        ## Assign fields that will be used for versioning
        version_hash_fields = {
            "doc_name":doc_name,
            "doc_num": doc_num,
            "publication_date": publication_date,
            "download_url": download_url,
            "status": fields['status'],
            "sponsor": fields['sponsor'],
            "cancel_date": fields['cancel_date']
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
