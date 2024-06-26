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
        display_doc_type: str = None,
    ):
        self.doc_name = doc_name
        self.doc_title = doc_title
        self.doc_num = doc_num
        self.doc_type = doc_type
        if display_doc_type is None:
            self.display_doc_type = doc_type
        else:
            self.display_doc_type = display_doc_type
        try:
            self.publication_date = publication_date.strftime("%Y-%m-%dT%H:%M:%S")
        except AttributeError:
            self.publication_date = None
        self.cac_login_required = cac_login_required
        self.source_page_url = source_page_url
        self.downloadable_items = downloadable_items
        self.download_url = download_url
        self.file_ext = file_ext
        self.display_title = doc_type + " " + doc_num + ": " + doc_title

        self.hash_fields = {
            "doc_name": self.doc_name,
            "doc_num": self.doc_num,
            "publication_date": self.publication_date,
            "download_url": self.download_url,
            "display_title": self.display_title,
        }

    def set_version_hash_field(self, key: str, value: str) -> dict:
        """Sets a new field or updates an old one in the dict used for hashing"""
        self.hash_fields[key] = value

    def remove_version_hash_field(self, key: str) -> None:
        """Removes a field from tthe dict used for hashing"""
        self.hash_fields.pop(key)

    def set_display_name(self, name: str) -> None:
        """Update display name for DocItemFields instance"""
        self.display_title = name
        self.hash_fields["display_title"] = name

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
        version_hash = dict_to_sha256_hex_digest(self.hash_fields)

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
            version_hash_raw_data=self.hash_fields,
            version_hash=version_hash,
            display_org=display_org,
            data_source=data_source,
            source_title=source_title,
            display_source=display_source,
            display_title=self.display_title,
            file_ext=self.file_ext,
            is_revoked=is_revoked,
        )
