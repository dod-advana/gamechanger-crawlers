# JBOOK CRAWLER
# Defense Wide Budget Spider

from gc import callbacks
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
import scrapy
from scrapy import Selector
from urllib.parse import urljoin, urlparse
from datetime import datetime
import re
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem

class JBOOKDefenseWideBudgetSpider(GCSpider):
    name = 'jbook_defense_wide_budget'
    display_org = "Unknown Source"
    data_source = "Unknown Source"
    source_title = "Unknown Source"
    
    cac_login_required = False
    rotate_user_agent = True 
    root_url = 'https://comptroller.defense.gov/Budget-Materials/'   
    urls = ['https://comptroller.defense.gov/Budget-Materials/Budget{}/',
           'https://comptroller.defense.gov/Budget-Materials/FY{}BudgetJustification/']
    
    allowed_domains = ['comptroller.defense.gov/']
    
    file_type = 'pdf'
    latest_year = int(datetime.today().year) + 2
    years = range(2014, latest_year)
 
    def start_requests(self):
        for year in self.years:
            for url in self.urls:
                yield scrapy.Request(url.format(year))
        
    def parse(self, response):
        content = response.css("a[href*='.pdf']")
        for c in content:
            
            is_revoked = False

            doc_title_raw = c.css("::text").get()
            doc_title = self.ascii_clean(str(doc_title_raw))
            doc_url = c.css('::attr(href)').get()
            
            web_url = urljoin(response.url, doc_url)
            url_tags = ['02_Procurement', '_p1r', '_p1', '_r1', '03_RDT_and_E']
            
            if doc_url is None or not any(tag in doc_url for tag in url_tags):
                continue
            
            doc_type = ''
            for tag in url_tags:
                if tag in doc_url:
                    doc_type = tag
                else:
                    doc_type is None
                    
            doctype_mapping = {'02_Procurement': 'procurement', '_p1r':'procurement', '_p1':'procurement', '_r1':'rdte', '03_RDT_and_E':'rdte'}
            for key, value in doctype_mapping.items():
                doc_type = doc_type.replace(key, value)
            
            publication_year = re.search('[2][0-9]{3}', doc_url)
            publication_date = '01/01/' + publication_year.group()
            
            doc_name = doc_url.split('/')[-1][:-4]
            doc_name = doc_name.replace('%20', '_')
            if doc_title == 'None':
                doc_title = doc_name.replace('_', ' ')
                        
            amendment_search = re.search("amend\w*", doc_url, re.IGNORECASE)
            year_search = re.search('(fy|pb)[2][0-9]{1,3}|(fy|pb)_[2][0-9]{1,3}', doc_name, re.IGNORECASE) 
            rdte_tags = ['r1', 'rdte']
            procurement_tags = ['p1r', 'p1', 'procurement']
            
            if amendment_search:
                amendment_tag = doc_url.split('/')[-2]
                doc_name = doc_name  + '_' + amendment_tag    
            #elif doc_type == 'RDTE':
                #if any(tag not in doc_name.lower() for tag in rdte_tags):
                #    doc_name = doc_name + '_rdte'
                #if not year_search:
                #    doc_name = doc_name + '_fy' + publication_year.group()
            #elif (doc_type == 'Procurement'):
                #if any(tag not in doc_name.lower() for tag in procurement_tags):
                #    doc_name = doc_name + '_proc'
                #if not year_search:
                #    doc_name = doc_name + '_fy' + publication_year.group()
            #if not year_search:
            year = publication_year.group()    
            #else:
            #    year = year_search     

            doc_name = f'{doc_type};{year};{doc_name}' 
       
            downloadable_items = [
                {
                    "doc_type": "pdf",
                    "web_url": web_url,
                    "compression_type": None
                }
            ]

            version_hash_fields = {
                "item_currency": downloadable_items[0]["web_url"].split('/')[-1],
                "document_title": doc_title,
                "publication_date": publication_date,
            }
        
            doc_item = DocItem(
                doc_name=self.ascii_clean(doc_name),
                doc_title=doc_title,
                doc_type=doc_type,
                publication_date=publication_date,
                source_page_url=response.url,
                downloadable_items=downloadable_items,
                version_hash_raw_data=version_hash_fields,
                is_revoked=is_revoked
            )
            yield doc_item