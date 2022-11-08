from pydoc import source_synopsis
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest, get_pub_date
from dataPipelines.gc_scrapy.gc_scrapy.utils import parse_timestamp
import scrapy
import typing as t
from datetime import datetime
from urllib.parse import urlparse


def simple(doc_type_num) -> t.Tuple[str, str, str]:
    before, _, after = doc_type_num.partition(' ')
    return before.strip(), after.strip()


def policy(doc_type_num) -> t.Tuple[str, str, str]:
    before, after = simple(doc_type_num)
    return before, after, "Policy"


def memo(doc_type_num) -> t.Tuple[str, str, str]:
    before, after = simple(doc_type_num)
    return before, after, "Memo"


def tsg_std(doc_type_num) -> t.Tuple[str, str, str]:
    before, _, after = doc_type_num.partition(' STANDARD ')
    if not after:
        return before, '', "Standard"

    return f"{before.strip()} STANDARD", after.strip(), "Standard"


def tsg_info(doc_type_num) -> t.Tuple[str, str, str]:
    return 'TSG Information Series', '', "Series"


def cnss_report(doc_type_num) -> t.Tuple[str, str, str]:
    doc_type_num = doc_type_num.replace('CNSS Report:', '')
    return 'CNSS Report', doc_type_num.strip(), "Report"


def historical(doc_type_num) -> t.Tuple[str, str, str]:
    return 'CNSS Historical Index', '', "Index"


def supp(doc_type_num) -> t.Tuple[str, str, str]:
    return 'CNSS Supplement', doc_type_num, "Supplement"


def instruction(doc_type_num) -> t.Tuple[str, str, str]:
    before, after = simple(doc_type_num)
    if not after.strip():
        # some of these are actually memos
        return 'CNSSAM', before.replace('CNSS-', ''), "Memo"
    else:
        return before, after, "Instruction"


def directive(doc_type_num) -> t.Tuple[str, str, str]:
    before, after = simple(doc_type_num)
    if not after.strip():
        return 'CNSSD', f"Template {before}", "Directive"
    else:
        return before, after, "Directive"


class CNSSSpider(GCSpider):
    name = "CNSS" # Crawler name
    start_urls = [
        "https://www.cnss.gov/CNSS/index.cfm"
    ]
    root_url = "https://www.cnss.gov"
    pages = [
        ("https://www.cnss.gov/CNSS/issuances/Policies.cfm", policy),
        ("https://www.cnss.gov/CNSS/issuances/Directives.cfm", directive),
        ("https://www.cnss.gov/CNSS/issuances/Instructions.cfm", instruction),
        ("https://www.cnss.gov/CNSS/issuances/Memoranda.cfm", memo),
        ("https://www.cnss.gov/CNSS/issuances/TSG_Standards.cfm", tsg_std),
        ("https://www.cnss.gov/CNSS/issuances/TSG_Information.cfm", tsg_info),
        ("https://www.cnss.gov/CNSS/issuances/CNSS_Reports.cfm", cnss_report),
        ("https://www.cnss.gov/CNSS/issuances/Supplemental.cfm", supp),
        ("https://www.cnss.gov/CNSS/issuances/historicalIndex.cfm", historical),
    ]
    rotate_user_agent = True

    def parse(self, response):
        # do nothing on start url
        for page, split_func in self.pages:
            yield scrapy.Request(page, self.parse_page, meta={"split_func": split_func})

    def parse_page(self, response):
        split_func: t.Callable[[str], t.Tuple[str, str, str]
                               ] = response.meta["split_func"]

        rows = response.css('table.documentTable tr')
        for row in rows[1:]:
            href_raw = row.css(
                'td:nth-child(2) a::attr(href)').get() # For this website, each document's href changes with each crawl

            if not href_raw:
                continue

            doc_type_num_raw = row.css(
                'td:nth-child(2) p.documentTitle span[itemprop="name"]::text').get()
            doc_title_raw = row.css(
                'td:nth-child(2) p.documentTitle span[itemprop="description"]::text').get()

            publication_date_raw = row.css(
                'td:nth-child(2) p.documentInfo span[itemprop="dateCreated"]::text').get()

            doc_type_num = self.ascii_clean(doc_type_num_raw)
            doc_title = self.ascii_clean(doc_title_raw)
            publication_date = self.ascii_clean(publication_date_raw)
     
            doc_type, doc_num, display_doc_type = split_func(doc_type_num)
            
            doc_name = f"{doc_type} {doc_num}".strip()
            
            source_page_url = response.url

            web_url = f"{self.root_url}{href_raw}"
            
            fields = {
                    'doc_name': doc_name,
                    'doc_num': doc_num,
                    'doc_title': doc_title,
                    'doc_type': doc_type,
                    'cac_login_required': False,
                    'download_url': web_url,
                    'publication_date': publication_date,
                    'source_page_url': source_page_url,
                    'display_doc_type': display_doc_type
                }

            doc_item = self.populate_doc_item(fields)
            
            yield from doc_item
            
            
    def populate_doc_item(self, fields):
        display_org = "Dept. of Defense" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "Committee on National Security Systems Library" # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Unlisted Source" # Level 3 filter
        
        doc_name = fields['doc_name']
        doc_num = fields['doc_num']
        doc_title = fields['doc_title']
        doc_type = fields['doc_type']
        cac_login_required = fields['cac_login_required']
        download_url = fields['download_url']
        publication_date = get_pub_date(fields['publication_date'])
        source_page_url = fields['source_page_url']
        display_doc_type = fields['display_doc_type']

        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + " " + doc_title
        source_fqdn = urlparse(source_page_url).netloc
        is_revoked = False
        access_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f") # T added as delimiter between date and time

        
        downloadable_items = [
            {
                "doc_type": 'pdf',
                "download_url": download_url,
                "compression_type": None
            }
        ]
        
        version_hash_fields = {
            "doc_title": doc_title,
            "doc_num": doc_num,
            "publication_date": publication_date,
        }
        
        version_hash = dict_to_sha256_hex_digest(version_hash_fields)

        yield DocItem(
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
                    file_ext = doc_type, #
                    is_revoked = is_revoked, #
                    access_timestamp = access_timestamp #
                )
