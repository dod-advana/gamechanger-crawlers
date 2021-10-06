import scrapy
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
import datetime
import time
from dataPipelines.gc_scrapy.gc_scrapy.utils import abs_url
import re


class TfmSpider(GCSpider):
    name = 'TFM'
    allowed_domains = ["treasury.gov"]
    start_urls = [
        "https://tfm.fiscal.treasury.gov/v1.html",
        "https://tfm.fiscal.treasury.gov/v2.html",
        "https://tfm.fiscal.treasury.gov/v3.html",
        "https://tfm.fiscal.treasury.gov/v4.html",
        "https://tfm.fiscal.treasury.gov/v1/announc.html",
        "https://tfm.fiscal.treasury.gov/v1/tl.html",
        "https://tfm.fiscal.treasury.gov/v1/bull.html",
        "https://tfm.fiscal.treasury.gov/v1/supplements/ussgl/ussgl_part_1.html",
        "https://tfm.fiscal.treasury.gov/v1/supplements/ussgl/ussgl_part_2.html",
        "https://www.fiscal.treasury.gov/reference-guidance/fast-book/",
        "https://www.fiscal.treasury.gov/dms/resources/managing-federal-receivables.html",
        "https://fiscal.treasury.gov/reference-guidance/gold-book/",
        "https://fiscal.treasury.gov/reference-guidance/green-book/downloads.html",
        "https://tfm.fiscal.treasury.gov/v1/supplements.html",
        "https://tfm.fiscal.treasury.gov/v3/tl.html",
        "https://tfm.fiscal.treasury.gov/v4/tl.html"
    ]
    display_source = "Dept of Treasury Financial Manual"
    cac_login_required = False

    def parse(self, response):
        base_url = "https://tfm.fiscal.treasury.gov"
        base_url2 = "https://www.fiscal.treasury.gov/"
        if response.url.endswith(('v1.html', 'v2.html', 'v3.html', 'v4.html')):
            SET_SELECTOR = 'p'
        elif response.url.endswith('announc.html'):
            SET_SELECTOR = 'dl.TFMDocument-Announcement'
        elif response.url.endswith('tl.html'):
            SET_SELECTOR = 'dl'
        elif response.url.endswith('bull.html'):
            SET_SELECTOR = 'dl.TFMDocument-Bulletin'
        elif response.url.endswith("ussgl_part_1.html"):
            SET_SELECTOR = "h2"
        elif response.url.endswith("ussgl_part_2.html"):
            SET_SELECTOR = "h2"
        elif response.url.endswith("fast-book/"):
            SET_SELECTOR = "li"
        elif response.url.endswith("managing-federal-receivables.html"):
            SET_SELECTOR = "main"
        elif response.url.endswith("gold-book/"):
            SET_SELECTOR = "h3"
        elif response.url.endswith("green-book/downloads.html"):
            SET_SELECTOR = "p"
        elif response.url.endswith("/v1/supplements.html"):
            SET_SELECTOR = "li"
        for brickset in response.css(SET_SELECTOR):

            if response.url.endswith(('v1.html', 'v2.html', 'v3.html', 'v4.html')):
                NAME_SELECTOR = 'a ::text'
                URL_SELECTOR = 'a::attr(href)'
                TITLE_SELECTOR = 'p::text'
                doc_name = brickset.css(NAME_SELECTOR).extract_first()
                url = brickset.css(URL_SELECTOR).extract()
                doc_title = brickset.css(TITLE_SELECTOR).extract_first()
                if doc_title is not None and doc_title.startswith('Chapter'):
                    doc_num = doc_title
                    doc_type = "TFM"
                    doc_name = doc_type+' '+doc_num
                if doc_name is None:
                    continue
                if url is None:
                    continue
                if doc_title is None:
                    continue
                url = base_url+url[-1]
                if "pdf" not in url:
                    continue

                if "pdf" in url:
                    doc_extension = "pdf"
                else:
                    doc_extension = "html"
                downloadable_items = [
                    {
                        "doc_type": doc_extension,
                        "web_url": url,
                        "compression_type": None
                    }
                ]
                version_hash_fields = {
                    # version metadata found on pdf links
                    "item_currency": url.split('/')[-1],
                    "doc_name": doc_name,
                }

                yield DocItem(
                    doc_name=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_name),
                    doc_title=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_title),
                    doc_num=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_num),
                    doc_type=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_type),
                    downloadable_items=downloadable_items,
                    version_hash_raw_data=version_hash_fields,
                    source_page_url=response.url,
                    display_doc_type="Manual",
                    display_org="Dept. of Treasury",
                    display_source=self.display_source
                )

            elif SET_SELECTOR == 'dl.TFMDocument-Announcement':
                url = brickset.css('a::attr(href)').extract()
                announce_num2 = brickset.css('strong::text').extract()
                announce_title2 = brickset.css('dd::text').extract()
                announce_title = []
                announce_num = []
                [announce_num.append(x) for x in announce_num2 if len(
                    re.sub(r'[^a-zA-Z0-9 ()\\-]', '', x)) > 5]
                [announce_title.append(x) for x in announce_title2 if len(
                    re.sub(r'[^a-zA-Z0-9 ()\\-]', '', x)) > 5]
                url2 = []
                [url2.append(x) for x in url if "pdf" in x]
                for i in range(len(announce_num)):
                    if announce_num[i].startswith('A'):
                        doc_title = announce_title[i]
                        doc_num = doc_title
                        doc_type = "TFM"
                        doc_name = doc_type+' '+doc_num
                    url = base_url+url2[i]
                    if "pdf" not in url:
                        continue

                    if "pdf" in url:
                        doc_extension = "pdf"
                    else:
                        doc_extension = "html"
                    downloadable_items = [
                        {
                            "doc_type": doc_extension,
                            "web_url": url,
                            "compression_type": None
                        }
                    ]
                    version_hash_fields = {
                        # version metadata found on pdf links
                        "item_currency": url.split('/')[-1],
                        "doc_name": doc_name,
                    }

                yield DocItem(
                    doc_name=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_name),
                    doc_title=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_title),
                    doc_num=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_num),
                    doc_type=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_type),
                    downloadable_items=downloadable_items,
                    version_hash_raw_data=version_hash_fields,
                    source_page_url=response.url,
                    display_doc_type="Manual",
                    display_org="Dept. of Treasury",
                    display_source=self.display_source
                )

            elif SET_SELECTOR == 'dl.TFMDocument-Bulletin':
                url = brickset.css('a::attr(href)').extract()
                announce_num2 = brickset.css('strong::text').extract()
                announce_title2 = brickset.css('dd::text').extract()
                announce_title = []
                announce_num = []
                url2 = []
                [announce_num.append(' '.join(x.split()))
                 for x in announce_num2 if len(' '.join(x.split())) > 5]
                [announce_title.append(' '.join(x.split())) for x in announce_title2 if len(
                    ' '.join(x.split())) > 5]
                print(announce_title2)
                print(len(announce_title))
                [url2.append(x) for x in url if "pdf" in x]
                for i in range(len(announce_num)):
                    doc_title = announce_title[i]
                    doc_num = doc_title
                    doc_type = "TFM"
                    doc_name = doc_type+' '+doc_num
                    url = base_url+url2[i]
                    if "pdf" not in url:
                        continue

                    if "pdf" in url:
                        doc_extension = "pdf"
                    else:
                        doc_extension = "html"
                    downloadable_items = [
                        {
                            "doc_type": doc_extension,
                            "web_url": url,
                            "compression_type": None
                        }
                    ]
                    version_hash_fields = {
                        # version metadata found on pdf links
                        "item_currency": url.split('/')[-1],
                        "doc_name": doc_name,
                    }

                    yield DocItem(
                        doc_name=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_name),
                        doc_title=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_title),
                        doc_num=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_num),
                        doc_type=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_type),
                        downloadable_items=downloadable_items,
                        version_hash_raw_data=version_hash_fields,
                        source_page_url=response.url,
                        display_doc_type="Manual",
                        display_org="Dept. of Treasury",
                        display_source=self.display_source
                    )
            elif SET_SELECTOR == 'dl':

                url = brickset.css('a::attr(href)').extract()
                announce_title = brickset.css('dd::text').extract()
                url2 = []
                all_values = []
                [all_values.append(x) for x in announce_title if len(x) > 5]
                doc_name_l = all_values[0:][::2]
                doc_title_l = all_values[1:][::2]
                [url2.append(x) for x in url if "pdf" in x]
                for i in range(len(doc_name_l)):
                    doc_title = doc_title_l[i]
                    doc_num = doc_title
                    doc_type = "TFM"
                    doc_name = doc_type+' '+doc_num
                    url = base_url+url2[i]
                    if "pdf" not in url:
                        continue

                    if "pdf" in url:
                        doc_extension = "pdf"
                    else:
                        doc_extension = "html"
                    downloadable_items = [
                        {
                            "doc_type": doc_extension,
                            "web_url": url,
                            "compression_type": None
                        }
                    ]
                    version_hash_fields = {
                        # version metadata found on pdf links
                        "item_currency": url.split('/')[-1],
                        "doc_name": doc_name,
                    }

                    yield DocItem(
                        doc_name=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_name),
                        doc_title=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_title),
                        doc_num=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_num),
                        doc_type=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_type),
                        downloadable_items=downloadable_items,
                        version_hash_raw_data=version_hash_fields,
                        source_page_url=response.url,
                        display_doc_type="Manual",
                        display_org="Dept. of Treasury",
                        display_source=self.display_source
                    )
            elif SET_SELECTOR == "h2":
                URL_SELECTOR = 'a::attr(href)'
                TITLE_SELECTOR = 'a ::text'
                url = brickset.css(URL_SELECTOR).extract()
                doc_title = brickset.css(TITLE_SELECTOR).extract_first()
                if "part_1" in response.url:
                    doc_num = doc_title
                    doc_type = "TFM"
                    doc_name = doc_type+' '+doc_num
                elif "part_2" in response.url:
                    doc_num = doc_title
                    doc_type = "TFM"
                    doc_name = doc_type+' '+doc_num
                if doc_name is None:
                    continue
                if url is None:
                    continue
                url = base_url+url[-1]
                if "pdf" not in url:
                    continue

                if "pdf" in url:
                    doc_extension = "pdf"
                else:
                    doc_extension = "html"
                downloadable_items = [
                    {
                        "doc_type": doc_extension,
                        "web_url": url,
                        "compression_type": None
                    }
                ]
                version_hash_fields = {
                    # version metadata found on pdf links
                    "item_currency": url.split('/')[-1],
                    "doc_name": doc_name,
                }

                yield DocItem(
                    doc_name=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_name),
                    doc_title=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_title),
                    doc_num=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_num),
                    doc_type=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_type),
                    downloadable_items=downloadable_items,
                    version_hash_raw_data=version_hash_fields,
                    source_page_url=response.url,
                    display_doc_type="Manual",
                    display_org="Dept. of Treasury",
                    display_source=self.display_source
                )
            elif response.url.endswith("fast-book/"):
                URL_SELECTOR = 'a::attr(href)'
                TITLE_SELECTOR = 'a ::text'
                url = brickset.css(URL_SELECTOR).extract()
                doc_title = brickset.css(TITLE_SELECTOR).extract_first()
                doc_num = doc_title
                doc_type = "TFM"
                doc_name = doc_type+' '+doc_num
                if doc_name is None:
                    continue
                if url is None:
                    continue
                url = base_url2+url[-1]
                if "pdf" not in url:
                    continue

                if "pdf" in url:
                    doc_extension = "pdf"
                else:
                    doc_extension = "html"
                downloadable_items = [
                    {
                        "doc_type": doc_extension,
                        "web_url": url,
                        "compression_type": None
                    }
                ]
                version_hash_fields = {
                    # version metadata found on pdf links
                    "item_currency": url.split('/')[-1],
                    "doc_name": doc_name,
                }

                yield DocItem(
                    doc_name=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_name),
                    doc_title=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_title),
                    doc_num=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_num),
                    doc_type=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_type),
                    downloadable_items=downloadable_items,
                    version_hash_raw_data=version_hash_fields,
                    source_page_url=response.url,
                    display_doc_type="Manual",
                    display_org="Dept. of Treasury",
                    display_source=self.display_source
                )

            elif response.url.endswith("managing-federal-receivables.html"):
                URL_SELECTOR = 'a::attr(href)'
                TITLE_SELECTOR = 'a ::text'
                url = brickset.css(URL_SELECTOR).extract()
                doc_title = brickset.css(TITLE_SELECTOR).extract_first()
                doc_num = doc_title
                doc_type = "TFM"
                doc_name = doc_type+' '+doc_num
                if doc_name is None:
                    continue
                if url is None:
                    continue
                url2 = []
                [url2.append(x) for x in url if "pdf" in x]
                url = base_url2+url2[0]
                if "pdf" not in url:
                    continue

                if "pdf" in url:
                    doc_extension = "pdf"
                else:
                    doc_extension = "html"
                downloadable_items = [
                    {
                        "doc_type": doc_extension,
                        "web_url": url,
                        "compression_type": None
                    }
                ]
                version_hash_fields = {
                    # version metadata found on pdf links
                    "item_currency": url.split('/')[-1],
                    "doc_name": doc_name,
                }

                yield DocItem(
                    doc_name=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_name),
                    doc_title=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_title),
                    doc_num=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_num),
                    doc_type=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_type),
                    downloadable_items=downloadable_items,
                    version_hash_raw_data=version_hash_fields,
                    source_page_url=response.url,
                    display_doc_type="Manual",
                    display_org="Dept. of Treasury",
                    display_source=self.display_source
                )

            elif response.url.endswith("gold-book/"):
                NAME_SELECTOR = 'a ::text'
                URL_SELECTOR = 'a::attr(href)'
                doc_name = brickset.css(NAME_SELECTOR).extract_first()
                url = brickset.css(URL_SELECTOR).extract()
                doc_title = doc_name
                doc_num = doc_title
                doc_type = "TFM"
                doc_name = doc_type+' '+doc_num
                if doc_name is None:
                    continue
                if url is None:
                    continue
                url = base_url2+url[-1]
                if "html" not in url:
                    continue

                if "pdf" in url:
                    doc_extension = "pdf"
                else:
                    doc_extension = "html"
                downloadable_items = [
                    {
                        "doc_type": doc_extension,
                        "web_url": url,
                        "compression_type": None
                    }
                ]
                version_hash_fields = {
                    # version metadata found on pdf links
                    "item_currency": url.split('/')[-1],
                    "doc_name": doc_name,
                }

                yield DocItem(
                    doc_name=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_name),
                    doc_title=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_title),
                    doc_num=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_num),
                    doc_type=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_type),
                    downloadable_items=downloadable_items,
                    version_hash_raw_data=version_hash_fields,
                    source_page_url=response.url,
                    display_doc_type="Manual",
                    display_org="Dept. of Treasury",
                    display_source=self.display_source
                )

            elif response.url.endswith("green-book/downloads.html"):
                NAME_SELECTOR = 'a ::text'
                URL_SELECTOR = 'a::attr(href)'
                TITLE_SELECTOR = 'p::text'
                doc_name = brickset.css(NAME_SELECTOR).extract_first()
                url = brickset.css(URL_SELECTOR).extract()
                doc_title = brickset.css(TITLE_SELECTOR).extract_first()
                if doc_name is None:
                    continue
                if url is None:
                    continue
                if doc_title is None:
                    continue
                doc_title = doc_name
                doc_num = doc_title
                doc_type = "TFM"
                doc_name = doc_type+' '+doc_num
                url = base_url2+url[-1]
                if "pdf" not in url:
                    continue

                if "pdf" in url:
                    doc_extension = "pdf"
                else:
                    doc_extension = "html"
                downloadable_items = [
                    {
                        "doc_type": doc_extension,
                        "web_url": url,
                        "compression_type": None
                    }
                ]
                version_hash_fields = {
                    # version metadata found on pdf links
                    "item_currency": url.split('/')[-1],
                    "doc_name": doc_name,
                }

                yield DocItem(
                    doc_name=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_name),
                    doc_title=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_title),
                    doc_num=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_num),
                    doc_type=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_type),
                    downloadable_items=downloadable_items,
                    version_hash_raw_data=version_hash_fields,
                    source_page_url=response.url,
                    display_doc_type="Manual",
                    display_org="Dept. of Treasury",
                    display_source=self.display_source
                )

            elif response.url.endswith("/v1/supplements.html"):
                URL_SELECTOR = 'a::attr(href)'
                TITLE_SELECTOR = 'a ::text'
                url = brickset.css(URL_SELECTOR).extract()
                doc_title = brickset.css(TITLE_SELECTOR).extract_first()
                if doc_title is None:
                    continue
                if url is None:
                    continue
                doc_num = doc_title
                doc_type = "TFM"
                doc_name = doc_type+' '+doc_num
                # print(url)
                url = url[-1]
                if "pdf" not in url:
                    continue

                if "pdf" in url:
                    doc_extension = "pdf"
                else:
                    doc_extension = "html"
                downloadable_items = [
                    {
                        "doc_type": doc_extension,
                        "web_url": url,
                        "compression_type": None
                    }
                ]
                version_hash_fields = {
                    # version metadata found on pdf links
                    "item_currency": url.split('/')[-1],
                    "doc_name": doc_name,
                }

                yield DocItem(
                    doc_name=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_name),
                    doc_title=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_title),
                    doc_num=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_num),
                    doc_type=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_type),
                    downloadable_items=downloadable_items,
                    version_hash_raw_data=version_hash_fields,
                    source_page_url=response.url,
                    display_doc_type="Manual",
                    display_org="Dept. of Treasury",
                    display_source=self.display_source
                )
