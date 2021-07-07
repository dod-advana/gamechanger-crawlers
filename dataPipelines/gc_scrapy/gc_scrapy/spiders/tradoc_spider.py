import scrapy
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
import datetime
import time
from dataPipelines.gc_scrapy.gc_scrapy.utils import abs_url
import re
import os

class BrickSetSpider(scrapy.Spider):
    name = 'TRADOC'
    allowed_domains = ['adminpubs.tradoc.army.mil']
    file_type = "pdf"
    start_urls = [
    'https://adminpubs.tradoc.army.mil/circulars.html',
    'https://adminpubs.tradoc.army.mil/memorandums.html',
    'https://adminpubs.tradoc.army.mil/pamphlets.html',
    'https://adminpubs.tradoc.army.mil/regulations.html'
    ]
    start_url = 'https://adminpubs.tradoc.army.mil/'

    def parse(self, response):
        SET_SELECTOR = 'tr'
        base_url='https://adminpubs.tradoc.army.mil/'
        for brickset in response.css(SET_SELECTOR):
            if response.url.endswith("circulars.html"):
                INFO_SELECTOR = 'td ::text'
                URL_SELECTOR = 'a::attr(href)'
                info2 = brickset.css(INFO_SELECTOR).extract()
                url = brickset.css(URL_SELECTOR).extract()
                if len(info2)==0 :
                    continue
                if len(url)==0:
                    continue
                info=[]
                [info.append(x) for x in info2 if len(re.sub(r'[^a-zA-Z0-9 ()\\-]', '', x).replace(' ',''))>1]
                if len(info)==0:
                    continue
                doc_num = info[0]
                date = info[1]
                doc_title= info[3]
                doc_type = "TRADOC Circular"
                doc_name=doc_type+' '+doc_num
                cac_login_required = False
                downloadable_items=[]
                for item in url:
                    if "doc" in item:
                        DI ={
                            "doc_type": 'doc',
                            "web_url": base_url+item,
                            "compression_type": None
                        }
                    
                        downloadable_items.append(DI)
                    elif "pdf" in item:
                        DI ={
                            "doc_type": 'pdf',
                            "web_url": base_url+item,
                            "compression_type": None
                        }
                    
                        downloadable_items.append(DI)
                version_hash_fields = {
                    # version metadata found on pdf links
                    "item_currency": url[0].split('/')[-1],
                    "doc_name": doc_name,
                }

                yield DocItem(
                    doc_name=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_name),
                    doc_title=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_title),
                    doc_num=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_num),
                    doc_type=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_type),
                    publication_date=date,
                    cac_login_required=cac_login_required,
                    downloadable_items=downloadable_items,
                    version_hash_raw_data=version_hash_fields,
                    access_timestamp=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
                    crawler_used="TRADOC Crawler",
                    source_page_url=response.url,
                    display_doc_type="TRADOC",
                    display_org="United States Army",
                    display_source="https://adminpubs.tradoc.army.mil/"                    
                )
            else:
                INFO_SELECTOR = 'td ::text'
                URL_SELECTOR = 'a::attr(href)'
                info2 = brickset.css(INFO_SELECTOR).extract()
                url = brickset.css(URL_SELECTOR).extract()
                if len(info2)==0 :
                    continue
                if len(url)==0:
                    continue
                info=[]
                [info.append(x) for x in info2 if len(re.sub(r'[^a-zA-Z0-9 ()\\-]', '', x).replace(' ',''))>1]
                if len(info)==0:
                    continue
                doc_num = info[0]
                date = info[1]
                doc_title= info[2]
                doc_type = "TRADOC"+" "+os.path.splitext(response.url)[0].split('/')[-1][:-1].capitalize()
                doc_name=doc_type+' '+doc_num
                cac_login_required = False
                downloadable_items=[]
                for item in url:
                    if "doc" in item:
                        DI ={
                            "doc_type": 'doc',
                            "web_url": base_url+item,
                            "compression_type": None
                        }
                    
                        downloadable_items.append(DI)
                    elif "pdf" in item:
                        DI ={
                            "doc_type": 'pdf',
                            "web_url": base_url+item,
                            "compression_type": None
                        }
                    
                        downloadable_items.append(DI)
                version_hash_fields = {
                    # version metadata found on pdf links
                    "item_currency": url[0].split('/')[-1],
                    "doc_name": doc_name,
                }

                yield DocItem(
                    doc_name=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_name),
                    doc_title=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_title),
                    doc_num=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_num),
                    doc_type=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_type),
                    publication_date=date,
                    cac_login_required=cac_login_required,
                    downloadable_items=downloadable_items,
                    version_hash_raw_data=version_hash_fields,
                    access_timestamp=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
                    crawler_used="TRADOC Crawler",
                    source_page_url=response.url,
                    display_doc_type="TRADOC",
                    display_org="United States Army",
                    display_source="https://adminpubs.tradoc.army.mil/" 
                )

