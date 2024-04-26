from urllib.parse import urlparse
from datetime import datetime

from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem


class DocItemFields:
    """Designed to store all fields necessary to generate DocItems"""

    def __init__(
        self,
        doc_name: str,
        doc_title: str,
        doc_num: str,
        doc_type: str,
        publication_date: datetime,
        cac_login_required: bool,
        source_page_url: str,
        downloadable_items: dict,
        download_url: str,
        file_ext: str,
    ):
        self.doc_name = doc_name
        self.doc_title = doc_title
        self.doc_num = doc_num
        self.doc_type = doc_type
        self.display_doc_type = doc_type
        self.publication_date = publication_date.strftime("%Y-%m-%dT%H:%M:%S")
        self.cac_login_required = cac_login_required
        self.source_page_url = source_page_url
        self.downloadable_items = downloadable_items
        self.download_url = download_url
        self.file_ext = file_ext

    def get_version_hash_fields(self) -> dict:
        """Returns a dict of the fields used for hashing"""
        return {
            "doc_name": self.doc_name,
            "doc_num": self.doc_num,
            "publication_date": self.publication_date,
            "download_url": self.download_url,
            "display_title": self.doc_title,
        }

    def populate_doc_item(
        self, display_org: str, data_source: str, source_title: str, crawler_used: str
    ) -> DocItem:
        """Takes the data stored in the current object and populates then returns a scrapy DocItem

        Args:
            display_org (str): Level 1 - GC app 'Source' filter for docs from this crawler
            data_source (str): Level 2 - GC app 'Source' metadata field for docs from this crawler
            source_title (str): Level 3 - filter
            crawler_used (str): name of crawler used

        Returns:
            DocItem: scrapy.Item sublcass for storing Documents in GC
        """

        display_source = data_source + " - " + source_title
        is_revoked = False
        source_fqdn = urlparse(self.source_page_url).netloc
        version_hash_fields = self.get_version_hash_fields()
        version_hash = dict_to_sha256_hex_digest(version_hash_fields)

        return DocItem(
            doc_name=self.doc_name,
            doc_title=self.doc_title,
            doc_num=self.doc_num,
            doc_type=self.doc_type,
            display_doc_type=self.display_doc_type,
            publication_date=self.publication_date,
            cac_login_required=self.cac_login_required,
            crawler_used=crawler_used,
            downloadable_items=self.downloadable_items,
            source_page_url=self.source_page_url,
            source_fqdn=source_fqdn,
            download_url=self.download_url,
            version_hash_raw_data=version_hash_fields,
            version_hash=version_hash,
            display_org=display_org,
            data_source=data_source,
            source_title=source_title,
            display_source=display_source,
            display_title=self.doc_title,
            file_ext=self.file_ext,
            is_revoked=is_revoked,
        )
