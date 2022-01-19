from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
import scrapy
import typing as t


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
    name = "CNSS"
    cac_login_required = False
    display_org = "Dept. of Defense"
    data_source = "Committee on National Security Systems Library"

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
                'td:nth-child(2) a::attr(href)').get()

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

            doc_name = f"{doc_type} {doc_num}"

            version_hash_fields = {
                "item_currency": href_raw,
                "publication_date": publication_date
            }

            web_url = f"{self.root_url}{href_raw}"

            downloadable_items = [
                {
                    "doc_type": 'pdf',
                    "web_url": web_url,
                    "compression_type": None
                }
            ]

            yield DocItem(
                doc_name=doc_name.strip(),
                doc_title=doc_title,
                doc_num=doc_num,
                doc_type=doc_type,
                display_doc_type=display_doc_type,
                publication_date=publication_date,
                source_page_url=response.url,
                version_hash_raw_data=version_hash_fields,
                downloadable_items=downloadable_items
            )
