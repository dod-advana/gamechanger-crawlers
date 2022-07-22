import re
import bs4
from datetime import datetime
from urllib.parse import urlparse
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.utils import abs_url, parse_timestamp, dict_to_sha256_hex_digest


class DoDSpider(GCSpider):
    name = 'dod_issuances' # Crawler name

    start_urls = ['https://www.esd.whs.mil/DD/DoD-Issuances/DTM/']
    allowed_domains = ['www.esd.whs.mil']

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
    def get_display_doc_type(doc_type):
        display_dict = {
            "dod": 'Issuance',
            "dodm": 'Manual',
            "dodi": 'Instruction',
            "dodd": 'Directive',
            "ai": 'Instruction'
            }
        return display_dict.get(doc_type, "Document")

    def parse(self, response):
        links = response.css('li.col-sm-6')[0].css('a')
        yield from response.follow_all(links[4:-1], self.parse_documents)

    def fix_oprs(self, text: str) -> str:
        # remove phone numbers
        office_primary_resp = re.sub(
            '\s{0,1}\d{1,3}-\d{1,3}-\d{1,4}', '', text)
        # remove emails
        office_primary_resp = re.sub(
            '\s+[\w|.|-]+@[\w|.|-]+', '', office_primary_resp)
        # clean ends
        office_primary_resp = office_primary_resp.strip()

        return office_primary_resp

    def parse_documents(self, response):

        page_url = response.url

        # parse html response
        base_url = 'https://www.esd.whs.mil'
        soup = bs4.BeautifulSoup(response.body, features="html.parser")
        table = soup.find('table', attrs={'class': 'dnnGrid'})
        rows = table.find_all('tr')

        # set issuance type
        if page_url.endswith('dodd/'):
            doc_type = 'DoDD'
        elif page_url.endswith('dodi/'):
            doc_type = 'DoDI'
        elif page_url.endswith('dodm/'):
            doc_type = 'DoDM'
        elif page_url.endswith('inst/'):
            doc_type = 'AI'
        elif page_url.endswith('dtm/'):
            doc_type = 'DTM'
        else:
            doc_type = 'DoDI CPM'

        # iterate through each row of the table
        cac_required = ['CAC', 'PKI certificate required',
                        'placeholder', 'FOUO']

        opr_idx = None
        for oidx, header in enumerate(rows[0].find_all('th')):
            if header.text.strip() == "OPR":
                opr_idx = oidx

        for row in rows[1:]:

            # reset variables to ensure there is no carryover between rows
            doc_num = ''
            doc_name = ''
            doc_title = ''
            chapter_date = ''
            publication_date = ''
            cac_login_required = False
            pdf_url = ''
            exp_date = ''
            issuance_num = ''
            pdf_di = None
            office_primary_resp = ''

            # skip the extra rows, not included in the table
            try:
                row['class']  # all invalid rows do not have a class attribute
            except:
                continue

            # iterate through each cell of row
            for idx, cell in enumerate(row.find_all('td')):

                # remove unwanted characters
                data = re.sub(r'\s*[\n\t\r\s+]\s*', ' ', cell.text).strip()

                # set document variables based on current column
                if idx == 0:
                    pdf_url = abs_url(
                        base_url, cell.a['href']).replace(' ', '%20')
                    pdf_di = {
                        "doc_type": 'pdf',
                        "download_url": pdf_url,
                        "compression_type": None
                    }

                    # remove parenthesis from document name
                    data = re.sub(r'\(.*\)', '', data).strip()

                    # set doc_name and doc_num based on issuance
                    if page_url.endswith('dtm/'):
                        doc_name = data
                        doc_num = re.search(r'\d{2}.\d{3}', data)[0]
                    elif page_url.endswith('140025/'):
                        issuance_num = data.split()
                        doc_name = 'DoDI 1400.25 Volume ' + issuance_num[0] if issuance_num[0] != 'DoDI' \
                            else ' '.join(issuance_num).strip()

                        doc_num = issuance_num[0] if issuance_num[0] != 'DoDI' \
                            else issuance_num[-1]
                    else:
                        doc_name = data
                        doc_num = data.split(' ')[1] if data.find(
                            ' ') != -1 else data.split('-')[-1]

                elif idx == 1:
                    publication_date = data
                elif idx == 2:
                    doc_title = data
                elif idx == 3:
                    doc_name = doc_name + ' ' + data if data != '' else doc_name
                elif idx == 4:
                    chapter_date = data
                elif idx == 5:
                    exp_date = data
                elif opr_idx and idx == opr_idx:
                    office_primary_resp = self.fix_oprs(data)

                # set boolean if CAC is required to view document
                cac_login_required = True if any(x in pdf_url for x in cac_required) \
                    or any(x in doc_title for x in cac_required) else False

            fields = {
                "doc_name": doc_name,
                "doc_num": doc_num,
                "doc_title": doc_title,
                "doc_type": doc_type,
                "cac_login_required": cac_login_required,
                "page_url": page_url,
                "pdf_url": pdf_url,
                "pdf_di": pdf_di,
                "publication_date": publication_date,
                "chapter_date": chapter_date,
                "office_primary_resp": office_primary_resp
            }
            doc_item = self.populate_doc_item(fields)
            
            yield doc_item

    def populate_doc_item(self, fields):
        '''
        This functions provides both hardcoded and computed values for the variables
        in the imported DocItem object and returns the populated metadata object
        '''
        display_org = "Dept. of Defense" # Level 1: GC app 'Source' filter for docs from this crawler
        data_source = "WHS DoD Directives Division" # Level 2: GC app 'Source' metadata field for docs from this crawler
        source_title = "Unlisted Source" # Level 3 filter
        is_revoked = False
        cac_login_required = fields.get("cac_login_required")
        source_page_url = fields.get("page_url")
        office_primary_resp = fields.get("office_primary_resp")

        doc_name = self.ascii_clean(fields.get("doc_name").strip())
        doc_title = self.ascii_clean(re.sub('\\"', '', fields.get("doc_title")))
        doc_num = self.ascii_clean(fields.get("doc_num").strip())
        doc_type = self.ascii_clean(fields.get("doc_type").strip())
        publication_date = self.ascii_clean(fields.get("publication_date").strip())
        publication_date = self.get_pub_date(publication_date)
        display_source = data_source + " - " + source_title
        display_title = doc_type + " " + doc_num + " " + doc_title
        display_doc_type = self.get_display_doc_type(doc_type.lower())
        download_url = fields.get("pdf_url")
        file_type = self.get_href_file_extension(download_url)
        downloadable_items = [fields.get("pdf_di")]
        version_hash_fields = {
                "download_url": download_url,
                "pub_date": publication_date,
                "change_date": fields.get("chapter_date").strip(),
                "doc_num": doc_num,
                "doc_name": doc_name
            }
        source_fqdn = urlparse(source_page_url).netloc
        version_hash = dict_to_sha256_hex_digest(version_hash_fields)
        access_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f") # T added as delimiter between date and time

        return DocItem(
                doc_name = doc_name,
                doc_title = doc_title,
                doc_num = doc_num,
                doc_type = doc_type,
                display_doc_type_s = display_doc_type,
                publication_date_dt = publication_date,
                cac_login_required_b = cac_login_required,
                crawler_used_s = self.name,
                downloadable_items = downloadable_items,
                source_page_url_s = source_page_url,
                source_fqdn_s = source_fqdn,
                download_url_s = download_url, 
                version_hash_raw_data = version_hash_fields,
                version_hash_s = version_hash,
                display_org_s = display_org,
                data_source_s = data_source,
                source_title_s = source_title,
                display_source_s = display_source,
                display_title_s = display_title,
                file_ext_s = file_type,
                is_revoked_b = is_revoked,
                office_primary_resp = office_primary_resp,
                access_timestamp_dt = access_timestamp
            )

