from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
import scrapy
from urllib.parse import urlparse
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest, get_pub_date


class SASCSpider(GCSpider):
    name = "SASC" # Crawler name
    
    cac_login_required = False

    base_url = "https://www.armed-services.senate.gov"

    randomly_delay_request = True
    rotate_user_agent = True

    def start_requests(self):
        hearings_url = "https://www.armed-services.senate.gov/hearings"
        
        #Optional URL to replace hearings_url in request to pull only hearings from current/specific session of congress
        congress_num = '117'
        congress_url = f"{hearings_url}?c={congress_num}"
        
        yield scrapy.Request(hearings_url, callback=self.parse_hearings)

    def parse_hearings(self, response):
        last_page_num = response.css('#showing-page option:last-child::text').get()
        last_page = int(last_page_num)
        
        for num in range(1, last_page+1):
            page_url = f"{response.url}?pagenum_rs={num}"
            yield scrapy.Request(url=page_url, callback=self.parse_hearings_table_page)

    def parse_hearings_table_page(self, response):
        hearing_rows = response.css("div.LegislationList__item")

        for row in hearing_rows:
            url = row.css("a::attr(href)").get()
            htype = row.css("div.LegislationList__typeCol.col-12.col-xl-3").get()
            start = '</span>'
            end = '</div>'
            hearing_type = htype[htype.find(start)+len(start):htype.rfind(end)].strip()
            
            yield scrapy.Request(url=url, callback=self.parse_hearing_detail_page, meta={"hearing_type": hearing_type})

    def follow_pdf_redirect(self, response):
        pdf_redirect = response.css('p a::attr(href)').get()
        doc = response.meta['doc']
        
        yield scrapy.Request(pdf_redirect, callback=self.file_download_url, meta={"doc": doc})
    
    def file_download_url(self,response):
        pdf_download_url = response.url
        
        downloadable_items = [
            {
                "doc_type": 'pdf',
                "download_url": pdf_download_url,
                "compression_type": None
            }
        ]

        doc = response.meta['doc']
        doc['download_url'] = pdf_download_url
        doc['downloadable_items'] = downloadable_items
        doc['version_hash_raw_data']['download_url'] = pdf_download_url
        
        yield doc

    def parse_hearing_detail_page(self, response):
        try:
            #########################################################
            # Get the hearing detail page as a document

            main = response.css("div.SiteLayout__main")
            title_raw = self.ascii_clean(main.css("h1.Heading__title ::text").get().strip())
            title = ' '.join(title_raw.split())
            date = main.css('div.Hearing__detail time::attr(datetime)').get()
            spaced_title = f" - {title}" if title else ""
            base_doc_name = f"{self.name} Hearing{spaced_title}"
            hearing_type = response.meta["hearing_type"]
            web_url = response.url

            downloadable_items = [
                {
                    "doc_type": 'html',
                    "download_url": web_url,
                    "compression_type": None
                }
            ]

            fields = {
                'doc_name': base_doc_name,
                'doc_num': ' ',
                'doc_title': title,
                'doc_type': hearing_type,
                'display_doc_type': 'Hearing',
                'cac_login_required': False,
                'download_url': web_url,
                'source_page_url': web_url,
                'downloadable_items': downloadable_items,
                'publication_date': date
            }
            ## Instantiate DocItem class and assign document's metadata values
            doc_item = self.populate_doc_item(fields)
        
            yield doc_item

            #########################################################
            # Get transcript files from hearing
            asides = response.css("li.Hearing__linkListItem.RelatedIssuesLink.mb-2")
            for aside in asides:
                aside_href = aside.css('a::attr(href)').get()
                if aside_href is not None:
                    aside_text = aside.css('span::text').get().strip()
                    aside_doc_name = f"{base_doc_name} - {aside_text}"

                    downloadable_items = [
                        {
                            "doc_type": 'pdf',
                            "download_url": response.url,
                            "compression_type": None
                        }
                    ]

                    fields = {
                        'doc_name': aside_doc_name,
                        'doc_num': ' ',
                        'doc_title': title,
                        'doc_type': hearing_type,
                        'display_doc_type': 'Transcript',
                        'cac_login_required': False,
                        'download_url': response.url,
                        'source_page_url': response.url,
                        'downloadable_items': downloadable_items,
                        'publication_date': date
                    }
                    ## Instantiate DocItem class and assign document's metadata values
                    aside_doc = self.populate_doc_item(fields)

                    yield scrapy.Request(aside_href, callback=self.follow_pdf_redirect, meta={'doc': aside_doc})

            #########################################################
            # Get pdfs of each witness APQ and testimony
            witness_blocks = main.css('li.col-12.col-md-6.p-2')
            for witblock in witness_blocks:
                witness_docs = witblock.css('div.mt-3')
                for witdoc in witness_docs:
                    witness_href = witdoc.css('a::attr(href)').get()
                    if witness_href is not None:
                        honorific = witblock.css('h4.Heading__title span:nth-child(1)::text').get()
                        wit_name = witblock.css('h4.Heading__title span:nth-child(2)::text').get()
                        member_name = witblock.css('h4.Heading__title ::text').get()
                        
                        if honorific and wit_name is not None:
                            full_name_raw = f"{honorific} {wit_name}"
                        elif honorific is not None and wit_name is None:
                            full_name_raw = honorific
                        elif honorific is None and wit_name is not None:
                            full_name_raw = wit_name
                        else:
                            full_name_raw = member_name
                            
                        full_name = ' '.join(full_name_raw.split()).strip()
                        
                        witness_text = witdoc.css('a span::text').get().strip()
                        hearing_type = response.meta["hearing_type"]
                        wit_doc_type = 'Advance Policy Questions' if 'APQ' in witness_text else 'Testimony'
                        witness_doc_name = f"{base_doc_name} - {full_name} {wit_doc_type}"
                        full_witness_doc_type = f"{self.name} {hearing_type} {wit_doc_type}"

                        downloadable_items = [
                            {
                                "doc_type": 'pdf',
                                "download_url": witness_href,
                                "compression_type": None
                            }
                        ]

                        fields = {
                            'doc_name': witness_doc_name,
                            'doc_num': ' ',
                            'doc_title': witness_doc_name,
                            'doc_type': full_witness_doc_type,
                            'display_doc_type':wit_doc_type,
                            'cac_login_required': False,
                            'download_url': witness_href,
                            'source_page_url':response.url,
                            'downloadable_items': downloadable_items,
                            'publication_date': date
                        }

                        witness_doc = self.populate_doc_item(fields)

                        yield scrapy.Request(witness_href, callback=self.follow_pdf_redirect, meta={'doc': witness_doc})
        except Exception as e:
            print(e)


    def populate_doc_item(self, fields):
        '''
        This functions provides both hardcoded and computed values for the variables
        in the imported DocItem object and returns the populated metadata object
        '''
        display_org = "Congress" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "Senate Armed Services Committee Publications" # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Senate Armed Services Committee" # Level 3 filter

        doc_name = fields['doc_name']
        doc_num = fields['doc_num']
        doc_title = fields['doc_title']
        doc_type = fields['doc_type']
        cac_login_required = fields['cac_login_required']
        download_url = fields['download_url']
        publication_date = get_pub_date(fields['publication_date'])

        display_doc_type = fields['display_doc_type'] # Doc type for display on app
        display_source = data_source + " - " + source_title
        display_title = doc_type + " - " + doc_title # Different than other crawlers due to lack of doc_num; added a dash for clarity
        is_revoked = False
        source_page_url = fields['source_page_url']
        source_fqdn = urlparse(source_page_url).netloc

        downloadable_items = fields['downloadable_items']
        file_ext = downloadable_items[0]["doc_type"]

        ## Assign fields that will be used for versioning
        version_hash_fields = {
            "doc_name": doc_name,
            "doc_num": doc_num,
            "publication_date": publication_date,
            "download_url": download_url,
            "display_title": display_title
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
                    file_ext = file_ext,
                    is_revoked = is_revoked,
                )