from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
import datetime
import re


class SammSpider(GCSpider):
    download_warnsize = 0
    name = 'SAMM'
    allowed_domains = ["samm.dsca.mil/"]
    start_urls = [
        "https://samm.dsca.mil/listing/chapters",
        "https://samm.dsca.mil/policy-memoranda/PolicyMemoList-All",
        "https://samm.dsca.mil/rcg/rcg-toc",
        "https://samm.dsca.mil/appendix/appendix-1",
        "https://samm.dsca.mil/appendix/appendix-2",
        "https://samm.dsca.mil/appendix/appendix-3",
        "https://samm.dsca.mil/appendix/appendix-4",
        "https://samm.dsca.mil/appendix/appendix-5",
        "https://samm.dsca.mil/appendix/appendix-6",
        "https://samm.dsca.mil/samm-archive/2003-samm-archive",
        "https://samm.dsca.mil/samm-archive/1988-samm-archive",
        "https://samm.dsca.mil/samm-archive/1984-samm-archive",
        "https://samm.dsca.mil/samm-archive/1983-masm-b-3",
        "https://samm.dsca.mil/samm-archive/1978-masm-archive",
        "https://samm.dsca.mil/samm-archive/1973-masm-archive",
        "https://samm.dsca.mil/samm-archive/1970-masm-archive"
    ]
    display_source="Defense Security Cooperation Agency Distribution Portal"
    cac_login_required = False
    randomly_delay_request = True

    def parse(self, response):
        base_url = "https://samm.dsca.mil"
        if response.url.endswith(('chapters')):
            SET_SELECTOR = 'tr'
        elif response.url.endswith('PolicyMemoList-All'):
            SET_SELECTOR = 'tr'
        elif response.url.endswith('rcg-toc'):
            SET_SELECTOR = 'tr'
        elif response.url.endswith('appendix-1'):
            SET_SELECTOR = 'div.field__item'
        elif response.url.endswith('appendix-2'):
            SET_SELECTOR = 'tr'
        elif response.url.endswith('appendix-3'):
            SET_SELECTOR = 'tr'
        elif response.url.endswith('appendix-4'):
            SET_SELECTOR = 'tr'
        elif response.url.endswith('appendix-5'):
            SET_SELECTOR = 'tr'
        elif response.url.endswith('appendix-6'):
            SET_SELECTOR = 'tbody'
        elif response.url.endswith('archive'):
            SET_SELECTOR = 'tr'
        elif response.url.endswith('1983-masm-b-3'):
            SET_SELECTOR = 'tr'

        for brickset in response.css(SET_SELECTOR):

            if response.url.endswith(('chapters')):
                TITLE_SELECTOR = 'a ::text'
                URL_SELECTOR = 'a::attr(href)'
                NUM_SELECTOR = 'p::text'
                doc_num = brickset.css(NUM_SELECTOR).extract_first()
                url = brickset.css(URL_SELECTOR).extract()
                doc_title = brickset.css(TITLE_SELECTOR).extract_first()
                if doc_num is None:
                    continue
                if url is None:
                    continue
                if doc_title is None:
                    continue
                doc_num = doc_title
                doc_type = "SAMM"
                doc_name = doc_type + ' ' + doc_num
                if url[-1].startswith("http"):
                    url = url[-1]
                else:
                    url = base_url + url[-1]
                url = url.replace(" ", "%20")

                if "pdf" in url:
                    continue
                else:
                    doc_extension = "html"

                doc_name = self.ascii_clean(doc_name)
                doc_title = self.ascii_clean(doc_title)
                doc_num = self.ascii_clean(doc_num)

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
                    display_org="Defense Security Cooperation Agency",
                    display_source=self.display_source
                )
            elif response.url.endswith('PolicyMemoList-All'):
                NAME_SELECTOR = 'a ::text'
                URL_SELECTOR = 'a::attr(href)'
                TITLE_SELECTOR = 'td.views-field-field-memo-title::text'
                DATE_SELECTOR = 'time::text'
                doc_name = brickset.css(NAME_SELECTOR).extract_first()
                url = brickset.css(URL_SELECTOR).extract()
                doc_title = brickset.css(TITLE_SELECTOR).extract_first()
                date = brickset.css(DATE_SELECTOR).extract_first()
                if doc_name is None:
                    continue
                if url is None:
                    continue
                if doc_title is None:
                    continue
                if ' ' in doc_name:
                    doc_num = doc_name.split(" ")[1]
                    doc_type = doc_name.split(" ")[0]
                    doc_name = doc_name
                else:
                    doc_num = doc_title
                    doc_type = "SAMM"
                    doc_name = doc_type + ' ' + doc_num
                if url[-1].startswith("http"):
                    url = url[-1]
                else:
                    url = base_url + url[-1]
                url = url.replace(" ", "%20")

                if "pdf" in url:
                    continue
                else:
                    doc_extension = "html"

                doc_name = self.ascii_clean(doc_name)
                doc_title = self.ascii_clean(doc_title)
                doc_num = self.ascii_clean(doc_num)

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
                    publication_date=date,
                    downloadable_items=downloadable_items,
                    version_hash_raw_data=version_hash_fields,
                    source_page_url=response.url,
                    display_doc_type="Memo",
                    display_org="Defense Security Cooperation Agency",
                    display_source=self.display_source
                )
            elif response.url.endswith('rcg-toc'):
                TITLE_SELECTOR = 'a ::text'
                URL_SELECTOR = 'a::attr(href)'
                NUM_SELECTOR = 'td.AlignCenter::text'
                doc_num = brickset.css(NUM_SELECTOR).extract_first()
                url = brickset.css(URL_SELECTOR).extract()
                doc_title = brickset.css(TITLE_SELECTOR).extract_first()
                if doc_num is None:
                    continue
                if url is None:
                    continue
                if doc_title is None:
                    continue

                doc_num = doc_title
                doc_type = "SAMM"
                doc_name = doc_type + ' ' + doc_num
                if url[-1].startswith("http"):
                    url = url[-1]
                else:
                    url = base_url + url[-1]
                url = url.replace(" ", "%20")

                if "pdf" in url:
                    continue
                else:
                    doc_extension = "html"

                doc_name = self.ascii_clean(doc_name)
                doc_title = self.ascii_clean(doc_title)
                doc_num = self.ascii_clean(doc_num)

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
                    display_org="Defense Security Cooperation Agency",
                    display_source=self.display_source
                )
            elif response.url.endswith('appendix-1'):
                TITLE_SELECTOR = 'a.ChapterTitle ::text'
                URL_SELECTOR = 'a::attr(href)'
                NUM_SELECTOR = 'td.AlignCenter::text'
                # doc_num=brickset.css(NUM_SELECTOR).extract_first()
                url = brickset.css(URL_SELECTOR).extract_first()
                doc_title = brickset.css(TITLE_SELECTOR).extract_first()
                if url is None:
                    continue
                if doc_title is None:
                    continue

                doc_num = doc_title
                doc_type = "SAMM"
                doc_name = doc_type + ' ' + doc_num
                if url.startswith("http"):
                    url = url
                else:
                    url = base_url + url
                url = url.replace(" ", "%20")

                if "pdf" in url:
                    continue
                else:
                    doc_extension = "html"

                doc_name = self.ascii_clean(doc_name)
                doc_title = self.ascii_clean(doc_title)
                doc_num = self.ascii_clean(doc_num)

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
                    display_org="Defense Security Cooperation Agency",
                    display_source=self.display_source
                )
            elif response.url.endswith('appendix-2'):
                NUM_SELECTOR = 'td.AlignCenter::text'
                URL_SELECTOR = 'a::attr(href)'
                TITLE_SELECTOR = 'a::text'
                doc_name = brickset.css(NUM_SELECTOR).extract_first()
                url = brickset.css(URL_SELECTOR).extract()
                doc_title = brickset.css(TITLE_SELECTOR).extract()
                if doc_name is None:
                    continue
                if len(url) == 0:
                    continue
                if len(doc_title) == 0:
                    continue
                if doc_name == "A2.1.":
                    doc_title = doc_title[0]
                    doc_num = doc_title
                    doc_type = "SAMM"
                    doc_name = doc_type + ' ' + doc_num
                    if url[0].startswith("http"):
                        url = url[0]
                    else:
                        url = base_url + url[0]
                    url = url.replace(" ", "%20")

                    if "pdf" in url:
                        continue
                    else:
                        doc_extension = "html"

                    doc_name = self.ascii_clean(doc_name)
                    doc_title = self.ascii_clean(doc_title)
                    doc_num = self.ascii_clean(doc_num)

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
                        doc_title=re.sub(
                            r'[^a-zA-Z0-9 ()\\-]', '', str(doc_title)),
                        doc_num=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_num),
                        doc_type=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_type),
                        downloadable_items=downloadable_items,
                        version_hash_raw_data=version_hash_fields,
                        source_page_url=response.url,
                        display_doc_type="Manual",
                        display_org="Defense Security Cooperation Agency",
                        display_source=self.display_source
                    )
                else:
                    for i in range(len(doc_title)):
                        doc_title2 = doc_title[i]
                        doc_num = doc_title2
                        doc_type = "SAMM"
                        doc_name2 = doc_type + ' ' + doc_num
                        if url[i].startswith("http"):
                            url2 = url[i]
                        else:
                            url2 = base_url + url[i]
                        url2 = url2.replace(" ", "%20")

                        if "pdf" in url2:
                            continue
                        else:
                            doc_extension = "html"

                        doc_name2 = self.ascii_clean(doc_name2)
                        doc_title2 = self.ascii_clean(doc_title2)
                        doc_num = self.ascii_clean(doc_num)

                        downloadable_items = [
                            {
                                "doc_type": doc_extension,
                                "web_url": url2,
                                "compression_type": None
                            }
                        ]
                        version_hash_fields = {
                            # version metadata found on pdf links
                            "item_currency": url2.split('/')[-1],
                            "doc_name": doc_name2,
                        }

                        yield DocItem(
                            doc_name=re.sub(
                                r'[^a-zA-Z0-9 ()\\-]', '', doc_name2),
                            doc_title=re.sub(
                                r'[^a-zA-Z0-9 ()\\-]', '', doc_title2),
                            doc_num=re.sub(r'[^a-zA-Z0-9 ()\\-]', '', doc_num),
                            doc_type=re.sub(
                                r'[^a-zA-Z0-9 ()\\-]', '', doc_type),
                            downloadable_items=downloadable_items,
                            version_hash_raw_data=version_hash_fields,
                            source_page_url=response.url,
                            display_doc_type="Manual",
                            display_org="Defense Security Cooperation Agency",
                            display_source=self.display_source
                        )
            elif response.url.endswith('appendix-3'):
                TITLE_SELECTOR = 'a ::text'
                URL_SELECTOR = 'a::attr(href)'
                NUM_SELECTOR = 'td.AlignCenter::text'
                doc_num = brickset.css(NUM_SELECTOR).extract_first()
                url = brickset.css(URL_SELECTOR).extract()
                doc_title = brickset.css(TITLE_SELECTOR).extract_first()
                if doc_num is None:
                    continue
                if url is None:
                    continue
                if doc_title is None:
                    continue
                doc_num = doc_title
                doc_type = "SAMM"
                doc_name = doc_type + ' ' + doc_num
                if url[-1].startswith("http"):
                    url = url[-1]
                else:
                    url = base_url + url[-1]
                url = url.replace(" ", "%20")

                if "pdf" in url:
                    continue
                else:
                    doc_extension = "html"

                doc_name = self.ascii_clean(doc_name)
                doc_title = self.ascii_clean(doc_title)
                doc_num = self.ascii_clean(doc_num)

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
                    display_org="Defense Security Cooperation Agency",
                    display_source=self.display_source
                )
            elif response.url.endswith('appendix-4'):
                TITLE_SELECTOR = 'a ::text'
                URL_SELECTOR = 'a::attr(href)'
                NUM_SELECTOR = 'td.AlignCenter::text'
                doc_num = brickset.css(NUM_SELECTOR).extract_first()
                url = brickset.css(URL_SELECTOR).extract()
                doc_title = brickset.css(TITLE_SELECTOR).extract_first()
                if doc_num is None:
                    continue
                if url is None:
                    continue
                if doc_title is None:
                    continue

                doc_num = doc_title
                doc_type = "SAMM"
                doc_name = doc_type + ' ' + doc_num
                if url[-1].startswith("http"):
                    url = url[-1]
                else:
                    url = base_url + url[-1]
                url = url.replace(" ", "%20")

                if "pdf" in url:
                    continue
                else:
                    doc_extension = "html"

                doc_name = self.ascii_clean(doc_name)
                doc_title = self.ascii_clean(doc_title)
                doc_num = self.ascii_clean(doc_num)

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
                    display_org="Defense Security Cooperation Agency",
                    display_source=self.display_source
                )
            elif response.url.endswith('appendix-5'):
                TITLE_SELECTOR = 'a ::text'
                URL_SELECTOR = 'a::attr(href)'
                NUM_SELECTOR = 'td.AlignCenter::text'
                doc_num = brickset.css(NUM_SELECTOR).extract_first()
                url = brickset.css(URL_SELECTOR).extract()
                doc_title = brickset.css(TITLE_SELECTOR).extract_first()
                if doc_num is None:
                    continue
                if url is None:
                    continue
                if doc_title is None:
                    continue

                doc_num = doc_title
                doc_type = "SAMM"
                doc_name = doc_type + ' ' + doc_num
                if url[-1].startswith("http"):
                    url = url[-1]
                else:
                    url = base_url + url[-1]
                url = url.replace(" ", "%20")

                if "pdf" in url:
                    continue
                else:
                    doc_extension = "html"

                doc_name = self.ascii_clean(doc_name)
                doc_title = self.ascii_clean(doc_title)
                doc_num = self.ascii_clean(doc_num)

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
                    display_org="Defense Security Cooperation Agency",
                    display_source=self.display_source
                )
            elif response.url.endswith('appendix-6'):
                TITLE_SELECTOR = 'a ::text'
                URL_SELECTOR = 'a::attr(href)'
                url2 = brickset.css(URL_SELECTOR).extract()
                doc_title2 = brickset.css(TITLE_SELECTOR).extract()
                if len(url2) == 0:
                    continue
                if len(doc_title2) == 0:
                    continue
                for i in range(len(url2)):
                    doc_title = doc_title2[i]
                    doc_num = doc_title
                    doc_type = "SAMM"
                    doc_name = doc_type + ' ' + doc_num
                    if url2[i].startswith("http"):
                        url = url2[i]
                    else:
                        url = base_url + url2[i]
                    url = url.replace(" ", "%20")

                    if "pdf" in url:
                        continue
                    else:
                        doc_extension = "html"

                    doc_name = self.ascii_clean(doc_name)
                    doc_title = self.ascii_clean(doc_title)
                    doc_num = self.ascii_clean(doc_num)

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
                    display_org="Defense Security Cooperation Agency",
                    display_source=self.display_source
                )
            elif response.url.endswith('archive') or response.url.endswith('1983-masm-b-3'):
                TITLE_SELECTOR = 'a ::text'
                URL_SELECTOR = 'a::attr(href)'
                url = brickset.css(URL_SELECTOR).extract()
                doc_title = brickset.css(TITLE_SELECTOR).extract_first()
                if url is None:
                    continue
                if doc_title is None:
                    continue

                doc_num = doc_title.replace("SAMM", "")
                doc_type = "SAMM"
                doc_name = doc_type + ' ' + doc_num
                if url[-1].startswith("http"):
                    url = url[-1]
                else:
                    url = base_url + url[-1]
                url = url.replace(" ", "%20")
                if "pdf" not in url:
                    continue

                if "pdf" in url:
                    doc_extension = "pdf"
                else:
                    doc_extension = "html"

                doc_name = self.ascii_clean(doc_name)
                doc_title = self.ascii_clean(doc_title)
                doc_num = self.ascii_clean(doc_num)

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
                    display_org="Defense Security Cooperation Agency",
                    display_source=self.display_source
                )
