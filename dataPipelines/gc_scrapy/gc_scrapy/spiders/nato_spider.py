# -- coding: utf-8 --
import scrapy
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider

import json
import copy
import base64


class NatoSpider(GCSpider):
    name = "nato_stanag"

    source_page_url = "https://nso.nato.int/nso/nsdd/ListPromulg.html"
    start_urls = [
        "https://nso.nato.int/nso/nsdd/webapi/api/application"
    ]
    data_url = "https://nso.nato.int/nso/nsdd/webapi/api/current-nato-standards/list"

    # nato site seems ban happy, slow requests a lot
    # randomly_delay_request = range(10, 20)

    headers = {
        "accept": "application/json",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "no-cache",
        "content-type": "application/json",
        "pragma": "no-cache",
        "requestverificationtoken": "undefined",
        "sec-ch-ua": "\" Not;A Brand\";v=\"99\", \"Google Chrome\";v=\"91\", \"Chromium\";v=\"91\"",
        "sec-ch-ua-mobile": "?0",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "x-requested-with": "XMLHttpRequest"
    }

    @staticmethod
    def download_response_handler(response):
        data = json.loads(response.body)
        content = data['content']
        return base64.b64decode(content)

    def start_requests(self):
        yield scrapy.Request(url=self.start_urls[0], method='GET', headers=self.headers)

    def create_data_headers(self, response):
        data = json.loads(response.body)
        token = data.get('token', None)
        if not token:
            raise('No requestverificationtoken, cannot request data')

        self.headers['requestverificationtoken'] = token
        self.download_request_headers = copy.deepcopy(self.headers)

        return self.headers

    def parse(self, response):
        headers = self.create_data_headers(response)
        yield scrapy.Request(url=self.data_url, callback=self.parse_data, headers=headers, method="POST", body="")

    def parse_data(self, response):
        data = json.loads(response.body)

        for listing in data:
            # items are stacked in children, unpack them then yield
            to_yield = []
            queue = [listing]
            while queue:
                current = queue.pop(0)
                children = current.get('children', [])
                to_yield.append(current)
                for child in children:
                    queue.append(child)

            for item in to_yield:
                iden = item['id']
                is_classified = item['isClassifiedEn']

                # These use `or ''` b/c they exist as None instead of being undefined so the .get(<name>, default) returns None

                item_type = item.get('type') or ''
                doc_type = item.get('documentType') or ''
                doc_num = item.get('number') or ''
                doc_title_raw = item.get('longTitle') or ''
                doc_title = self.ascii_clean(doc_title_raw)
                short_title_raw = item.get('shortTitle') or ''
                short_title = self.ascii_clean(short_title_raw)
                promulgation_date_raw = item.get('promulgationDate') or ''

                if iden == 0 and not item_type:
                    continue

                if promulgation_date_raw:
                    publication_date, *_ = promulgation_date_raw.partition('T')
                else:
                    publication_date = None

                edition = item['edition'] or ''
                volume = item['volume'] or ''
                version = item['version'] or ''

                if is_classified:
                    continue

                cac_login_required = is_classified

                doc_name_list = [
                    doc_type, item_type, doc_num, short_title, f"Ed: {edition}" if edition else "", f"Ver. {version}" if version else "", f"Vol. {volume}" if volume else ""
                ]

                doc_name = " ".join([name for name in doc_name_list if name])

                web_url = f"https://nso.nato.int/nso/nsdd/webapi/api/download-manager/download?id={iden}&type={item_type}&language=EN&subType=None"

                downloadable_items = [
                    {
                        "compression_type": None,
                        "doc_type": "pdf",
                        "web_url": web_url
                    }
                ]

                version_hash_fields = {
                    "edition": edition,
                    "volume": volume,
                    "version": version,
                    "publication_date": publication_date,
                    "web_url": web_url
                }

                yield DocItem(
                    doc_name=doc_name,
                    doc_title=doc_title,
                    doc_num=doc_num,
                    doc_type=doc_type,
                    cac_login_required=cac_login_required,
                    publication_date=publication_date,
                    version_hash_raw_data=version_hash_fields,
                    downloadable_items=downloadable_items,
                    source_fqdn="nso.nato.int"
                )
