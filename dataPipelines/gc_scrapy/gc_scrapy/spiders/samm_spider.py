from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest, get_pub_date
import re
from urllib.parse import urljoin, urlparse
import time

class SammSpider(GCSpider):
    name = "samm_policy"
    allowed_domains = ["samm.dsca.mil"]
    start_urls = ["https://samm.dsca.mil/listing/chapters"]
    rotate_user_agent = True
    randomly_delay_request = True

    @staticmethod
    def extract_doc_number(doc_name):
        match = re.search(r'(\d+-\d+)', doc_name)

        if match:
            doc_num = match.group(0)
        else:
            doc_num = ""

        return doc_num

    def parse(self, response):
        base_url = "https://samm.dsca.mil"
        if response.url == "https://samm.dsca.mil/policy-memoranda/PolicyMemoList-All":
            time.sleep(1)
            for row in response.xpath('//div[@class="view-content"]//table/tbody/tr'):
                pm_status_text = row.xpath('td[6]/text()').get()
                pm_status = pm_status_text.strip() if pm_status_text is not None else ""
                if pm_status != "Incorporated":
                    relative_urls = row.xpath('td[2]/a/@href').get()
                    doc_name = row.xpath('td[2]/a/text()').get().strip()
                    doc_title = row.xpath('normalize-space(td[5]/text())').get()

                    pub_date_text = row.xpath('substring-before(td[1]/time/@datetime, "T")').get()
                    pub_date = get_pub_date(pub_date_text.strip()) if pub_date_text is not None else None
                    status = row.xpath('normalize-space(td[6]/text())').get()

                    absolute_url = urljoin(base_url, relative_urls)
                    yield response.follow(
                        url=absolute_url,
                        callback=self.parse_document_page_memo,
                        cb_kwargs={'doc_title': doc_title, 'doc_name': doc_name, 'publication_date': pub_date, 'status': status}
                )
        elif response.url == "https://samm.dsca.mil/listing/chapters":
            chapter_data = response.xpath(
                '//*[@id="main-menu-link-content98ac5aa1-6408-4c0c-bf10-d333b494fdbf"]//a[starts-with(@href, "/chapter/")]'
            )
            chapter_links = [chap.xpath('@href').get() for chap in chapter_data]
            chapter_names = [chap.xpath('text()').get() for chap in chapter_data]
            chapter_titles = [chap.attrib['title'] for chap in chapter_data]
            for relative_url, chapter, chapter_title in zip(chapter_links, chapter_names, chapter_titles):
                absolute_url = urljoin(base_url, relative_url)
                yield response.follow(
                    url=absolute_url,
                    callback=self.parse_document_page_listing,
                    cb_kwargs={'chapter': chapter, 'chapter_title': chapter_title}
                )

    def parse_document_page_listing(self, response, chapter, chapter_title):
        # NOTE: this function doesn't do anything on the chapter webpage currently, but we'll keep it in case of
        # future functionality

        doc_title = self.ascii_clean(chapter_title).replace("/ ", " ").replace("/", " ")
        publication_date = get_pub_date(" ")

        doc_num = chapter
        doc_type = "SAMM"
        display_doc_type = "SAMM"
        doc_name = " ".join([doc_type, chapter])
        doc_name = doc_name.replace(" ", "_")

        # hardcoded status for the chapters. should remove this for SAMM chapters
        status = 'N/A'

        file_type = "html"
        web_url = response.url

        fields = {
            'doc_name': doc_name,
            'doc_num': doc_num,
            'doc_title': doc_title,
            'doc_type': doc_type,
            'display_doc_type': display_doc_type,
            'file_type': file_type,
            'status': status,
            'cac_login_required': False,
            'is_revoked': False,
            'download_url': web_url.replace(' ', '%20'),
            'source_page_url': response.url,
            'publication_date': publication_date
        }

        ## Instantiate DocItem class and assign document's metadata values
        doc_item = self.populate_doc_item(fields)

        yield doc_item


    def parse_document_page_memo(self, response, doc_title, doc_name, publication_date, status):
            pdf_link = response.xpath('//div[contains(@class, "PM_PDF_ink")]//a/@href').get()
            if pdf_link is not None:
                doc_num = self.extract_doc_number(doc_name)
                doc_type = "SAMM Policy Memoranda"

                doc_name = self.ascii_clean(doc_name.replace(" ", "_"))
                display_doc_type = "SAMM"

                doc_name = re.sub(r'[\(\),]', '', doc_name)  # Remove parentheses and commas
                doc_name = re.sub(r'[\W_\.]+$', '', doc_name)  # Remove any special characters at the end
                file_type = self.get_href_file_extension(pdf_link)
                web_url = self.ensure_full_href_url(pdf_link, response.url)

                is_revoked = False if status == 'Active' or status == 'Incorporated' else True

                fields = {
                    'doc_name': doc_name,
                    'doc_num': doc_num,
                    'doc_title': doc_title,
                    'doc_type': doc_type,
                    'display_doc_type': display_doc_type,
                    'file_type': file_type,
                    'cac_login_required': False,
                    'download_url': web_url,
                    'source_page_url': response.url,
                    'publication_date': publication_date,
                    'status': status,
                    'is_revoked': is_revoked
                }

                doc_item = self.populate_doc_item(fields)

                yield doc_item

    def populate_doc_item(self, fields):
        #
        # This functions provides both hardcoded and computed values for the variables
        # in the imported DocItem object and returns the populated metadata object
        #
        display_org = "SAMM"    # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "SAMM"    # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Unlisted Source"

        doc_name = fields['doc_name']
        doc_num = fields['doc_num']
        doc_title = fields['doc_title']
        doc_type = fields['doc_type']
        cac_login_required = fields['cac_login_required']
        download_url = fields['download_url']
        is_revoked = fields['is_revoked']
        status = fields['status']
        publication_date = get_pub_date(fields['publication_date'])

        display_doc_type = fields['display_doc_type'] # Doc type for display on app
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + ": " + doc_title
        source_page_url = fields['source_page_url']
        source_fqdn = urlparse(source_page_url).netloc

        downloadable_items = [{
                "doc_type": fields['file_type'],
                "download_url": download_url.replace(' ', '%20'),
                "compression_type": None,
            }]

        ## Assign fields that will be used for versioning
        version_hash_fields = {
            "doc_name": doc_name,
            "doc_num": doc_num,
            "publication_date": publication_date,
            "download_url": download_url,
            "display_title": display_title,
            "is_revoked": is_revoked,
            "status": status
        }

        version_hash = dict_to_sha256_hex_digest(version_hash_fields)

        return DocItem(
                    doc_name = doc_name,
                    doc_title = doc_title,
                    doc_num = doc_num,
                    doc_type = doc_type,
                    display_doc_type = display_doc_type,
                    publication_date = publication_date,
                    cac_login_required = cac_login_required,
                    crawler_used = self.name,
                    downloadable_items = downloadable_items,
                    source_page_url = source_page_url,
                    source_fqdn = source_fqdn,
                    download_url = download_url,
                    version_hash_raw_data = version_hash_fields,
                    version_hash = version_hash,
                    display_org = display_org,
                    data_source = data_source,
                    source_title = source_title,
                    display_source = display_source,
                    display_title = display_title,
                    file_ext = fields['file_type'],
                    is_revoked = is_revoked,
                )
