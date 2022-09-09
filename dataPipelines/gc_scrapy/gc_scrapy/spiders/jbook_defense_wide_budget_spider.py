# JBOOK CRAWLER
# Defense Wide Budget Spider

from gc import callbacks
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
import scrapy
from scrapy import Selector
from urllib.parse import urljoin, urlparse
from datetime import datetime
import re

from dataPipelines.gc_scrapy.gc_scrapy.middleware_utils.selenium_request import SeleniumRequest
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSeleniumSpider import GCSeleniumSpider

class JBOOKDefenseWideBudgetSpider(GCSpider):
    name = 'jbook_defense_wide_budget'
    display_org = "Unknown Source"
    data_source = "Unknown Source"
    source_title = "Unknown Source"
    
    cac_login_required = False
    rotate_user_agent = True 
    root_url = 'https://comptroller.defense.gov/Budget-Materials/'   
    url = 'https://comptroller.defense.gov/Budget-Materials/Budget{}/'
    allowed_domains = ['comptroller.defense.gov/']
    
    file_type = 'pdf'
    latest_year = int(datetime.today().year) + 2
            
    def start_requests(self):
        for i in range(1998, self.latest_year):
            yield scrapy.Request(self.url.format(i))
        
    def parse(self, response):
        content = response.css('a')
        for c in content:
            
            is_revoked = False

            doc_title_raw = c.css("::text").get()
            doc_title = self.ascii_clean(str(doc_title_raw))
            doc_url = c.css('::attr(href)').get()

            web_url = urljoin(response.url, doc_url)
            
            if doc_url is None or not ('Procurement' in doc_title or 'Research' in doc_title) or not 'Portals' in doc_url:
                continue
            
            if 'Procurement' in doc_title:
                doc_type = 'Procurement'
            elif 'Research' in doc_title:
                doc_type = 'RDTE'
            else:
                doc_type = None
                yield doc_type
            
            publication_year = re.search("[12][0-9]{3}", doc_url)
            publication_date = '01/01/' + publication_year.group()
            
            doc_name = doc_url.split('/')[-1][:-4]
            
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
                doc_name=doc_name,
                doc_title=doc_title,
                doc_type=doc_type,
                publication_date=publication_date,
                source_page_url=response.url,
                downloadable_items=downloadable_items,
                version_hash_raw_data=version_hash_fields,
                is_revoked=is_revoked
            )
            yield doc_item