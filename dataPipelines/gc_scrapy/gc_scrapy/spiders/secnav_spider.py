# -*- coding: utf-8 -*-

from time import sleep
import re
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
import scrapy
import json

json_re = re.compile("var WPQ3ListData = (?P<json>{.*?});", flags=re.M | re.S)


class SecNavSpider(GCSpider):
    name = "secnav_pubs"

    start_urls = [
        "https://www.secnav.navy.mil/doni/default.aspx",
    ]

    urls_type_map = [
        ("https://www.secnav.navy.mil/doni/allinstructions.aspx", "INST"),
        ("https://www.secnav.navy.mil/doni/notices.aspx", "NOTE")
    ]

    source_page_url = "https://www.secnav.navy.mil/doni/default.aspx"
    download_base_url = "https://www.secnav.navy.mil"

    rotate_user_agent = False
    randomly_delay_request = False

    had_error = False
    q = []
    ready_to_process = False
    done = []

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
                doc_title = self.ascii_clean(r.get('Subject'))
                publication_date = r.get("Effective_x0020_Date")
                web_url_suffix = r.get("FileRef")
                file_type = r.get("File_x0020_Type")
                status = r.get("Status")
                sponsor = r.get("Sponsor")
                cancel_date = r.get("Cancelled_x0020_Date")

                version_hash_fields = {
                    "item_currency": web_url_suffix,
                    "effective_date": publication_date,
                    "status": status,
                    "sponsor": sponsor,
                    "cancel_date": cancel_date
                }

                doc_type = f"{echelon}{type_suffix}"
                doc_name = f"{doc_type} {doc_num}"

                cac_login_required = re.match('^[A-Za-z]', doc_num) != None

                web_url = f"{self.download_base_url}{web_url_suffix}"

                downloadable_items = [
                    {
                        "doc_type": file_type,
                        "web_url": web_url,
                        "compression_type": None
                    }
                ]

                doc = DocItem(
                    doc_name=doc_name,
                    doc_title=doc_title,
                    doc_num=doc_num,
                    doc_type=doc_type,
                    publication_date=publication_date,
                    cac_login_required=cac_login_required,
                    downloadable_items=downloadable_items,
                    version_hash_raw_data=version_hash_fields,
                    source_page_url=self.source_page_url
                )

                self.enqueue(doc)

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
