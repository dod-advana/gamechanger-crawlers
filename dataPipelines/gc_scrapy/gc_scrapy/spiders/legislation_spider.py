from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.utils import parse_timestamp, dict_to_sha256_hex_digest
from urllib.parse import urlparse
from datetime import datetime
import json
import re
import scrapy

bill_version_re = re.compile(r'\((.*)\)')


class LegislationSpider(GCSpider):
    name = "legislation_pubs"  # Crawler name
    rotate_user_agent = True

    start_urls = [
        "https://www.govinfo.gov/wssearch/rb/plaw?fetchChildrenOnly=0"
    ]

    headers = {
        "accept": "application/json",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "no-cache",
        "content-type": "application/json",
        "pragma": "no-cache",
        "sec-ch-ua": "\" Not;A Brand\";v=\"99\", \"Google Chrome\";v=\"91\", \"Chromium\";v=\"91\"",
        "sec-ch-ua-mobile": "?0",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "x-requested-with": "XMLHttpRequest"
    }

    # ex for code base specific_congress
    # specific_congress = '117'
    # can be added in command line with arg `-a specific_congress=117`

    def start_requests(self):
        yield scrapy.Request(url=self.start_urls[0], method='GET', headers=self.headers)

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

    @staticmethod
    def get_visible_detail_url(package_id: str) -> str:
        return f"https://www.govinfo.gov/app/details/{package_id}"

    @staticmethod
    def get_api_detail_url(package_id: str) -> str:
        return f"https://www.govinfo.gov/wssearch/getContentDetail?packageId={package_id}"

    @staticmethod
    def get_browse_path_url(browse_path) -> str:
        return f"https://www.govinfo.gov/wssearch/rb//plaw/{browse_path}?fetchChildrenOnly=1&offset=0&pageSize=100"

    @staticmethod
    def get_nested_values(data, key='value') -> list:
        return [cnode.get('nodeValue').get(key) for cnode in data.get('childNodes', [])]

    def parse(self, response):
        data = json.loads(response.body)
        congress_nums_data = data.get('childNodes')

        for cong in congress_nums_data:
            if getattr(self, "specific_congress", None) is None:
                congress_num = cong.get('nodeValue').get('value')
            else:
                congress_num = self.specific_congress

            if not congress_num:
                raise RuntimeError(
                    f'Specific congress not found, specific_congress arg was {self.specific_congress}, congress num searched for was {congress_num}')
            # as of May 2021, the site only goes back to the 103rd congress, so offset iteration isnt necessary
            specific_congress_url = self.get_browse_path_url(congress_num)

            yield response.follow(url=specific_congress_url, callback=self.get_bill_type_data,
                                  meta={'congress_num': congress_num}, headers=self.headers)

    def get_bill_type_data(self, response):
        data = json.loads(response.body)

        # bill types ex. ['117/hconres', '117/hjres', '117/hr', '117/hres', '117/s', '117/sconres', '117/sjres', '117/sres']
        bill_types = self.get_nested_values(data, key='browsePath')

        for bill_type_path in bill_types:
            # there are only 8 bill types, so offset iteration isnt necessary
            # bill_type_url: 117/hconres = https://www.govinfo.gov/wssearch/rb//bills/117/hconres?fetchChildrenOnly=1&offset=0&pageSize=100
            bill_type_url = self.get_browse_path_url(bill_type_path)

            yield response.follow(url=bill_type_url, callback=self.get_bill_num_chunks, headers=self.headers)

    def get_bill_num_chunks(self, response):
        data = json.loads(response.body)

        # bill num chunks ex. ['117/sres/[0-99]', '117/sres/[100-199]', '117/sres/[200-299]']
        # bill num chunks ex. ['117/sconres/all']
        # can be all or a range of numbers, using it in the path works for the next request either way
        bill_num_chunks = self.get_nested_values(data, key='browsePathAlias')

        for bill_num_chunk_path in bill_num_chunks:
            bill_num_chunk_url = self.get_browse_path_url(bill_num_chunk_path)

            yield response.follow(url=bill_num_chunk_url, callback=self.get_package_ids, meta={"offset": 0},
                                  headers=self.headers)

    def get_package_ids(self, response):
        data = json.loads(response.body)
        current_offset = response.meta["offset"]

        packages = self.get_nested_values(data, key='packageid')
        # recursive base condition
        if not len(packages):
            return

        for package_id in packages:
            detail_url = self.get_api_detail_url(package_id)
            yield response.follow(url=detail_url, callback=self.parse_detail_data, headers=self.headers)

        # iterate offset
        next_offset = current_offset + 1
        next_offset_url = response.url.replace(
            f'offset={current_offset}', f'offset={next_offset}')

        yield response.follow(url=next_offset_url, callback=self.get_package_ids, meta={"offset": next_offset},
                              headers=self.headers)

    def parse_detail_data(self, response):
        data = json.loads(response.body)

        package_id = data['documentincontext']['packageId']
        web_url = f"https:{data['download']['pdflink']}"

        detail_data = {
            "Bill Number": "",
            "Law Number": "",
            "Full Title": "",
            "Date Approved": "",
            "Legislative History": "",
        }

        detail_data_list: list[dict] = data['metadata']['columnnamevalueset']
        for d in detail_data_list:
            if d['colname'] in detail_data:
                detail_data[d['colname']] = d['colvalue']

        raw_title = ' '.join(data['title'].split()[6:])
        doc_title = self.ascii_clean(raw_title)
        # congress_num_str = detail_data.get(
        #    'Congress Number').replace(' Congress', '')

        bill_type_raw, _, _ = detail_data.get(
            'Bill Number').rpartition(' ')

        doc_num = ''.join(detail_data['Law Number'].split()[2:])
        doc_type = "Public Law"
        bill_version_raw = detail_data.get('Bill Version')
        # bill_version = bill_version_re.search(bill_version_raw).group(1)
        doc_name = f"{detail_data['Law Number']}"

        # last_action_date = detail_data.get("Last Action Date Listed")

        # source_page_url = self.get_visible_detail_url(package_id)
        fields = {
            "doc_name": doc_name.strip(),
            "doc_title": doc_title.strip(),
            "doc_num": doc_num.strip(),
            "doc_type": doc_type,
            "source_page_url": self.get_visible_detail_url(package_id),
            "web_url": web_url,
            "publication_date": detail_data.get("Date Approved")
        }

        yield self.populate_doc_item(fields)

    def populate_doc_item(self, fields: dict) -> DocItem:
        display_org = "Congress"  # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "Congressional Legislation"  # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Unlisted Source"  # Level 3 filter
        file_type = 'pdf'
        cac_login_required = False
        is_revoked = False

        doc_name = fields.get('doc_name')
        doc_title = fields.get('doc_title')
        doc_num = fields.get('doc_num')
        doc_type = fields.get('doc_type')
        display_doc_type = "Law"
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + " " + doc_title
        web_url = fields.get("web_url")
        download_url = web_url.replace(' ', '%20')
        source_page_url = fields.get('source_page_url')
        publication_date = fields.get("publication_date")
        publication_date = self.get_pub_date(publication_date)
        downloadable_items = [{
            "doc_type": file_type,
            "download_url": web_url,
            "compression_type": None
        }]
        version_hash_fields = {
            "doc_num": doc_num,
            "doc_name": doc_name,
            "doc_title": doc_title,
            "publication_date": publication_date,
            "download_url": web_url
        }
        source_fqdn = urlparse(source_page_url).netloc
        version_hash = dict_to_sha256_hex_digest(version_hash_fields)

        return DocItem(
            doc_name=doc_name,
            doc_title=doc_title,
            doc_num=doc_num,
            doc_type=doc_type,
            display_doc_type=display_doc_type,
            publication_date=publication_date,
            cac_login_required=cac_login_required,
            crawler_used=self.name,
            downloadable_items=downloadable_items,
            source_page_url=source_page_url,
            source_fqdn=source_fqdn,
            download_url=download_url,
            version_hash_raw_data=version_hash_fields,
            version_hash=version_hash,
            display_org=display_org,
            data_source=data_source,
            source_title=source_title,
            display_source=display_source,
            display_title=display_title,
            file_ext=file_type,
            is_revoked=is_revoked,
        )
