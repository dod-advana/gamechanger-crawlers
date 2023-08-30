import re
import bs4
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.utils import abs_url

from urllib.parse import urljoin, urlparse
from datetime import datetime
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest, get_pub_date

class IcPoliciesSpider(GCSpider):
    name = 'ic_policies' # Crawler name

    start_urls = [
        'https://www.dni.gov/index.php/what-we-do/ic-policies-reports/'
    ]
    rotate_user_agent = True

    @staticmethod
    def get_display_doc_type(doc_type):
        """This function returns value for display_doc_type based on doc_type -> display_doc_type mapping"""
        display_type_dict = {
        "icd": 'Directive',
        "icpg": 'Guide',
        "icpm": 'Manual'
        }
        if doc_type.lower() in display_type_dict.keys():
            return display_type_dict[doc_type.lower()]
        else:
            return "Document"

    def parse(self, response):
        base_url = 'https://www.dni.gov'
        links = response.css('div[itemprop="articleBody"]').css('ul')[
            0].css('li')[:-1]
        full_links = [base_url + l.css('a::attr(href)').get() for l in links]
        yield from response.follow_all(full_links, self.parse_documents)

    def parse_documents(self, response):
        """Parse document objects from page of text"""
        page_url = response.url
        # parse html response
        base_url = 'https://www.dni.gov'
        soup = bs4.BeautifulSoup(response.body, features="html.parser")
        div = soup.find('div', attrs={'itemprop': 'articleBody'})
        pub_list = div.find_all('p')

        # set policy type
        if page_url.endswith('directives'):
            doc_type = 'ICD'
        elif page_url.endswith('guidance'):
            doc_type = 'ICPG'
        elif page_url.endswith('memorandums'):
            doc_type = 'ICPM'
        else:
            doc_type = 'ICLR'

        # iterate through each publication
        cac_required = ['CAC', 'PKI certificate required',
                        'placeholder', 'FOUO']
        for row in pub_list:

            # skip empty rows
            if row.a is None:
                continue

            data = re.sub(r'\u00a0', ' ', row.text)
            link = row.a['href']

            # patterns to match
            name_pattern = re.compile(r'^[A-Z]*\s\d*.\d*.\d*.\d*\s')

            parsed_text = re.findall(name_pattern, data)[0]
            parsed_name = parsed_text.split(' ')
            doc_name = ' '.join(parsed_name[:2])
            doc_num = parsed_name[1]
            doc_title = re.sub(parsed_text, '', data)

            pdf_url = abs_url(base_url, link)
            

            # extract publication date from the pdf url
            matches = re.findall(r'\((.+)\)', pdf_url.replace('%20', '-'))
            publication_date = matches[-1] if len(matches) > 0 else None

            # set boolean if CAC is required to view document
            cac_login_required = True if any(x in pdf_url for x in cac_required) \
                or any(x in doc_title for x in cac_required) else False


            fields = {
                'doc_name': doc_name.strip(),
                'doc_num': doc_num,
                'doc_title': doc_title,
                'doc_type': doc_type,
                'cac_login_required': cac_login_required,
                'download_url': pdf_url,
                'source_page_url':page_url.strip(),
                'publication_date': publication_date
            }
            ## Instantiate DocItem class and assign document's metadata values
            doc_item = self.populate_doc_item(fields)
        
            yield doc_item
        


    def populate_doc_item(self, fields):
        '''
        This functions provides both hardcoded and computed values for the variables
        in the imported DocItem object and returns the populated metadata object
        '''
        display_org = "Intelligence Community" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "Office of Director of National Intelligence" # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Unlisted Source" # Level 3 filter

        doc_name = fields['doc_name']
        doc_num = fields['doc_num']
        doc_title = fields['doc_title']
        doc_type = fields['doc_type']
        cac_login_required = fields['cac_login_required']
        download_url = fields['download_url']
        publication_date = get_pub_date(fields['publication_date'])

        display_doc_type = self.get_display_doc_type(doc_type)
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + ": " + doc_title
        is_revoked = False
        source_page_url = fields['source_page_url']
        source_fqdn = urlparse(source_page_url).netloc

        downloadable_items = [{
                "doc_type": "pdf",
                "download_url": download_url,
                "compression_type": None,
            }]
        file_ext = downloadable_items[0]["doc_type"]
        ## Assign fields that will be used for versioning
        version_hash_fields = {
            "doc_name":doc_name,
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
                    file_ext = file_ext, #
                    is_revoked = is_revoked, #
                )
