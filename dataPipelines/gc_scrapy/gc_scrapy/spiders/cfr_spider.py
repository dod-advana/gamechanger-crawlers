from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.utils import parse_timestamp, dict_to_sha256_hex_digest, get_pub_date
from urllib.parse import urlparse
from datetime import datetime
import json
import scrapy

class CFRSpider(GCSpider):
    name = "code_of_federal_regulations"  # Crawler name
    rotate_user_agent = True
    # the years to grab documents from
    # TODO: Grab only Title 35 from 2000, don't grab all docs
    years = ["2000", "2021", "2022"]

    start_urls = [
        "https://www.govinfo.gov/wssearch/rb/cfr?fetchChildrenOnly=0"
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
        return f"https://www.govinfo.gov/wssearch/rb//cfr/{browse_path}?fetchChildrenOnly=1&offset=0&pageSize=100"

    @staticmethod
    def get_nested_values(data, key='value') -> list:
        return [cnode.get('nodeValue').get(key) for cnode in data.get('childNodes', [])]

    def parse(self, response):
        for year in self.years:
            if getattr(self, "specific_congress", None) is None:
                cfr_year = year
            else:
                cfr_year = 2022

            specific_congress_url = self.get_browse_path_url(cfr_year)

            yield response.follow(url=specific_congress_url, callback=self.get_package_ids, meta={"offset": 0, "year": year},
                                  headers=self.headers)

    def get_package_ids(self, response):
        data = json.loads(response.body)
        current_offset = response.meta["offset"]
        year = response.meta["year"]

        packages = self.get_nested_values(data, key='packageid')
        # recursive base condition
        if not len(packages):
            return

        for package_id in packages:
            detail_url = self.get_api_detail_url(package_id)
            yield response.follow(url=detail_url, callback=self.parse_detail_data, meta={"offset": 0, "year": year},
                                  headers=self.headers)

    def parse_detail_data(self, response):
        data = json.loads(response.body)
        year = response.meta["year"]

        package_id = data['documentincontext']['packageId']
        web_url = f"https:{data['download']['pdflink']}"

        detail_data = {
            "Publication Title": "",
            "Date": "",
            "Date Issued": "",
            "Collection": "",
            "Category": ""
        }

        detail_data_list = data['metadata']['columnnamevalueset']
        for d in detail_data_list:
            if d['colname'] in detail_data:
                detail_data[d['colname']] = d['colvalue']

        # TODO: doc_num and doc_title (raw_title) are kindof hardcoded, need to change this
        raw_title = ' '.join(data['title'].split()[3:])
        doc_title = self.ascii_clean(raw_title)
        doc_num = detail_data['Publication Title'].split()[1]
        doc_type = "CFR Title"
        doc_name = (f"{detail_data['Publication Title']} {year}" if year not in detail_data['Publication Title'] else f"{detail_data['Publication Title']}")
        source_page_url = self.get_visible_detail_url(package_id)
        publication_date = detail_data.get("Date")
        
        if publication_date == "":
            publication_date = detail_data.get("Date Issued")

        fields = {
            "doc_name": doc_name.strip(),
            "doc_title": doc_title.strip(),
            "doc_num": doc_num.strip(),
            "doc_type": doc_type,
            "source_page_url": source_page_url,
            "web_url": web_url,
            "publication_date": publication_date
        }

        yield self.populate_doc_item(fields)

    def populate_doc_item(self, fields: dict) -> DocItem:
        display_org = "Executive Branch" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "National Archives and Records Administration" # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Unlisted Source"  # Level 3 filter
        file_type = 'pdf'
        cac_login_required = False
        is_revoked = False

        doc_name = self.ascii_clean(fields.get('doc_name'))
        doc_title = fields.get('doc_title')
        doc_num = fields.get('doc_num')
        doc_type = fields.get('doc_type')
        display_doc_type = "CFR Title"
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + ": " + doc_title
        web_url = fields.get("web_url")
        download_url = web_url.replace(' ', '%20')
        source_page_url = fields.get('source_page_url')
        publication_date = fields.get("publication_date")
        publication_date = get_pub_date(publication_date)
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
            "download_url": web_url,
            "display_title": display_title
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
