from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
import json
import re


bill_version_re = re.compile(r'\((.*)\)')


class LegislationSpider(GCSpider):
    name = "legislation_pubs"
    cac_login_required = False

    start_urls = [
        "https://www.govinfo.gov/wssearch/rb/bills?fetchChildrenOnly=0"
    ]

    # ex for code base specific_congress
    # specific_congress = '117'
    # can be added in command line with arg `-a specific_congress=117`

    @staticmethod
    def get_visible_detail_url(package_id: str) -> str:
        return f"https://www.govinfo.gov/app/details/{package_id}"

    @staticmethod
    def get_api_detail_url(package_id: str) -> str:
        return f"https://www.govinfo.gov/wssearch/getContentDetail?packageId={package_id}"

    @staticmethod
    def get_browse_path_url(browse_path) -> str:
        return f"https://www.govinfo.gov/wssearch/rb//bills/{browse_path}?fetchChildrenOnly=1&offset=0&pageSize=100"

    @staticmethod
    def get_nested_values(data, key='value') -> list:
        return [cnode.get('nodeValue').get(key) for cnode in data.get('childNodes', [])]

    def parse(self, response):
        data = json.loads(response.body)
        congress_nums_data = data.get('childNodes')

        if getattr(self, "specific_congress", None) is None:
            congress_num = congress_nums_data[0].get('nodeValue').get('value')
        else:
            congress_num = self.specific_congress

        if not congress_num:
            raise RuntimeError(
                f'Specific congress not found, specific_congress arg was {self.specific_congress}, congress num searched for was {congress_num}')
        # as of May 2021, the site only goes back to the 103rd congress, so offset iteration isnt necessary
        specific_congress_url = self.get_browse_path_url(congress_num)

        yield response.follow(url=specific_congress_url, callback=self.get_bill_type_data, meta={'congress_num': congress_num})

    def get_bill_type_data(self, response):
        data = json.loads(response.body)

        # bill types ex. ['117/hconres', '117/hjres', '117/hr', '117/hres', '117/s', '117/sconres', '117/sjres', '117/sres']
        bill_types = self.get_nested_values(data, key='browsePath')

        for bill_type_path in bill_types:
            # there are only 8 bill types, so offset iteration isnt necessary
            # bill_type_url: 117/hconres = https://www.govinfo.gov/wssearch/rb//bills/117/hconres?fetchChildrenOnly=1&offset=0&pageSize=100
            bill_type_url = self.get_browse_path_url(bill_type_path)

            yield response.follow(url=bill_type_url,
                                  callback=self.get_bill_num_chunks)

    def get_bill_num_chunks(self, response):
        data = json.loads(response.body)

        # bill num chunks ex. ['117/sres/[0-99]', '117/sres/[100-199]', '117/sres/[200-299]']
        # bill num chunks ex. ['117/sconres/all']
        # can be all or a range of numbers, using it in the path works for the next request either way
        bill_num_chunks = self.get_nested_values(data, key='browsePathAlias')

        for bill_num_chunk_path in bill_num_chunks:
            bill_num_chunk_url = self.get_browse_path_url(bill_num_chunk_path)

            yield response.follow(url=bill_num_chunk_url, callback=self.get_package_ids, meta={"offset": 0})

    def get_package_ids(self, response):
        data = json.loads(response.body)
        current_offset = response.meta["offset"]

        packages = self.get_nested_values(data, key='packageid')
        # recursive base condition
        if not len(packages):
            return

        for package_id in packages:
            detail_url = self.get_api_detail_url(package_id)
            yield response.follow(url=detail_url, callback=self.parse_detail_data)

        # iterate offset
        next_offset = current_offset + 1
        next_offset_url = response.url.replace(
            f'offset={current_offset}', f'offset={next_offset}')

        yield response.follow(url=next_offset_url, callback=self.get_package_ids, meta={"offset": next_offset})

    def parse_detail_data(self, response):
        data = json.loads(response.body)

        package_id = data['documentincontext']['packageId']
        web_url = f"https:{data['download']['pdflink']}"

        detail_data = {
            "Congress Number": "",
            "Last Action Date Listed": "",
            "Bill Number": "",
            "Bill Version": "",
            "Full Title": "",
            "Sponsors": "",
            "Cosponsors": "",
            "Committees": "",
        }

        detail_data_list: list[dict] = data['metadata']['columnnamevalueset']
        for d in detail_data_list:
            if d['colname'] in detail_data:
                detail_data[d['colname']] = d['colvalue']

        doc_title = self.ascii_clean(detail_data.get('Full Title'))
        congress_num_str = detail_data.get(
            'Congress Number').replace(' Congress', '')

        bill_type_raw, _, doc_num = detail_data.get(
            'Bill Number').rpartition(' ')

        doc_type = bill_type_raw.replace(' ', '')
        bill_version_raw = detail_data.get('Bill Version')
        bill_version = bill_version_re.search(bill_version_raw).group(1)
        doc_name = f"{doc_type} {doc_num} {bill_version} {congress_num_str}"

        last_action_date = detail_data.get("Last Action Date Listed")

        downloadable_items = [{
            "doc_type": 'pdf',
            "web_url": web_url,
            "compression_type": None
        }]

        version_hash_fields = {
            "last_action_date": last_action_date,
            "item_currency": web_url,
            "sponsors": self.ascii_clean(detail_data.get("Sponsors", " ")),
            "cosoponsors": self.ascii_clean(detail_data.get("Cosponsors", " ")),
            "committees": self.ascii_clean(detail_data.get("Committees", " ")),
        }

        source_page_url = self.get_visible_detail_url(package_id)

        yield DocItem(
            doc_name=doc_name,
            doc_title=doc_title,
            doc_num=doc_num,
            doc_type=doc_type,
            publication_date=last_action_date,
            source_page_url=source_page_url,
            downloadable_items=downloadable_items,
            version_hash_raw_data=version_hash_fields
        )
