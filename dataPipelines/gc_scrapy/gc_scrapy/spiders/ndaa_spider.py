# -*- coding: utf-8 -*-
import bs4
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest
from urllib.parse import urlparse
import scrapy


class NDAASpider(GCSpider):
    name = "ndaa_fy24"  # Crawler name
    rotate_user_agent = True
    base_url = "https://armedservices.house.gov"
    start_urls = [base_url + "/fy24-ndaa-resources"]

    def parse(self, response):
        page_url = response.url

        soup = bs4.BeautifulSoup(response.body, features="html.parser")
        for link in soup.find_all("a"):
            if link is None:
                continue
            url = link.get("href")
            if url is None:
                continue
            if "calendar/byevent" in url.lower():
                yield scrapy.Request(
                    url=url, method="GET", callback=self.parse_amendments_considered
                )
            if url.lower().endswith("pdf"):
                yield from self.get_doc_from_url(url, page_url)

    def parse_amendments_considered(self, response):
        page_url = response.url
        soup = bs4.BeautifulSoup(response.body, features="html.parser")
        for link in soup.find_all("a"):
            if link is None:
                continue
            url = link.get("href")
            if url is None:
                continue
            if url.lower().endswith("pdf"):
                yield from self.get_doc_from_url(url, page_url)

    def get_doc_from_url(self, url, source_url):
        doc_type = self.name
        doc_num = "0"
        doc_name = url.split("/")[-1].split(".")[-2].replace(" ", "_")
        doc_title = self.name + doc_name
        chapter_date = ""
        publication_date = ""
        exp_date = ""
        issuance_num = ""

        if url.lower().startswith("http"):
            pdf_url = url
        else:
            pdf_url = self.base_url + url.strip()
        pdf_di = [
            {"doc_type": "pdf", "download_url": pdf_url, "compression_type": None}
        ]

        fields = {
            "doc_name": doc_name.strip(),
            "doc_title": doc_title,
            "doc_num": doc_num,
            "doc_type": doc_type.strip(),
            "display_doc_type": doc_type.strip(),
            "publication_date": publication_date,
            "cac_login_required": False,
            "source_page_url": source_url.strip(),
            "downloadable_items": pdf_di,
            "download_url": pdf_url,
        }
        return self.populate_doc_item(fields)

    def populate_doc_item(self, fields):
        display_org = (
            "ndaa_fy24"  # Level 1: GC app 'Source' filter for docs from this crawler
        )
        data_source = "House Armed Services Committee"  # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "House Armed Services Committee"  # Level 3 filter

        doc_name = fields["doc_name"]
        doc_num = fields["doc_num"]
        doc_title = fields["doc_title"]
        doc_type = fields["doc_type"]
        publication_date = fields["publication_date"]
        cac_login_required = fields["cac_login_required"]
        download_url = fields["download_url"]
        display_doc_type = fields["display_doc_type"]
        downloadable_items = fields["downloadable_items"]

        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + ": " + doc_title
        is_revoked = False
        source_page_url = self.start_urls[0]
        source_fqdn = urlparse(source_page_url).netloc
        version_hash_fields = {
            "doc_name": doc_name,
            "doc_num": doc_num,
            "publication_date": publication_date,
            "download_url": download_url,
            "display_title": display_title,
        }
        version_hash = dict_to_sha256_hex_digest(version_hash_fields)

        yield DocItem(
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
            file_ext=doc_type,
            is_revoked=is_revoked,
        )
