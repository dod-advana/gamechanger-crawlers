# -*- coding: utf-8 -*-
import re
import bs4
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider


class OmbSpider(GCSpider):
    name = 'omb_pubs'
    source_title = "Office of Management and Budget Memoranda"
    display_org = "OMB"
    data_source = "Executive Office of the President"

    start_urls = [
        'https://www.whitehouse.gov/omb/information-for-agencies/memoranda/'
    ]

    def parse(self, response):
        page_url = response.url
        base_url = 'https://www.whitehouse.gov'

        soup = bs4.BeautifulSoup(response.body, features="html.parser")

        parsed_nums = []

        # get target column of list items
        parsed_docs = []
        li_list = soup.find_all('li')
        for li in li_list:
            doc_type = 'OMBM'
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
            if 'supersede' not in li.text.lower():
                a_list = li.findChildren('a')
                for a in a_list:
                    href = 'href'
                    if a.get('href') is None:
                        href = 'data-copy-href'
                    if a[href].lower().endswith('.pdf'):
                        if a[href].startswith('http'):
                            pdf_url = a[href]
                        else:
                            pdf_url = base_url + a[href].strip()
                    commaTokens = a.text.strip().split(',', 1)
                    spaceTokens = a.text.strip().split(' ', 1)
                    if len(commaTokens) > 1 and len(commaTokens[0]) < len(spaceTokens[0]):
                        doc_num = commaTokens[0]
                        doc_title = re.sub(r'^.*?,', '', a.text.strip())
                        doc_name = "OMBM " + doc_num
                    elif len(spaceTokens) > 1 and len(spaceTokens[0]) < len(commaTokens[0]):
                        doc_num = spaceTokens[0].rstrip(',.*')
                        doc_title = spaceTokens[1]
                        doc_name = "OMBM " + doc_num
                    possible_date = li.text[li.text.find(
                        "(") + 1:li.text.find(")")]
                    if re.match(pattern=r".*, \d{4}.*", string=possible_date):
                        publication_date = possible_date
                if pdf_url != '' and doc_num.count('-') == 2:
                    pdf_di = {
                        'doc_type': 'pdf',
                        'web_url': pdf_url,
                        'compression_type': None
                    }
                    version_hash_fields = {
                        # version metadata found on pdf links
                        "item_currency": pdf_url.split('/')[-1],
                        "pub_date": publication_date.strip(),
                        "display_org": self.display_org
                    }
                    parsed_title = self.ascii_clean(re.sub('\\"', '', doc_title))
                    parsed_num = doc_num.strip()
                    if parsed_num not in parsed_nums:
                        yield DocItem(
                            doc_name=doc_name.strip(),
                            doc_title=parsed_title,
                            doc_num=parsed_num,
                            data_source=self.data_source,
                            source_title=self.source_title,
                            display_org=self.display_org,
                            doc_type=doc_type.strip(),
                            display_doc_type=doc_type.strip(),
                            publication_date=publication_date,
                            cac_login_required=cac_login_required,
                            source_page_url=page_url.strip(),
                            version_hash_raw_data=version_hash_fields,
                            downloadable_items=[pdf_di]
                        )
                        parsed_nums.append(parsed_num)
