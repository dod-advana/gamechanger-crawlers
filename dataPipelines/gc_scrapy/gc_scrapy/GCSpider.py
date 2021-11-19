# -*- coding: utf-8 -*-
import scrapy
import re
import typing
from urllib.parse import urljoin, urlparse
from os.path import splitext
from time import perf_counter
import urllib
from dataPipelines.gc_scrapy.gc_scrapy.runspider_settings import general_settings
import copy

url_re = re.compile("((http|https)://)(www.)?" +
                    "[a-zA-Z0-9@:%._\\+~#?&//=]" +
                    "{2,256}\\.[a-z]" +
                    "{2,6}\\b([-a-zA-Z0-9@:%" +
                    "._\\+~#?&//=]*)"
                    )

mailto_re = re.compile(r'mailto\:', re.IGNORECASE)

# placeholder so we can capture that there should be a downloadable item there but it doesnt have a file extension
# if the link is updated, the hash will change and it will be downloadable later
UNKNOWN_FILE_EXTENSION_PLACEHOLDER = "UNKNOWN"


# creates names for incementable methods on each spider
# eg In Previous Hashes creates spider.increment_in_previous_hashes()
STATS_BASE = {
    "Required CAC": 0,
    "In Previous Hashes": 0,
}


class GCSpider(scrapy.Spider):
    """
        Base Spider with settings automatically applied and some utility methods
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.setup_stats()
        if self.time_lifespan:
            self.start_time = perf_counter()

    def __del__(self):
        if self.time_lifespan:
            alive = perf_counter() - self.start_time
            print(f"{self.name} lived for {alive}")

    @staticmethod
    def close(spider, reason):
        # intercepting the default stats collector and adding to the composite spider one
        from_default_stats = {
            "elapsed_time_seconds": "Elapsed Time (sec)",
            "item_scraped_count": "Item Scraped Count",
        }

        for k, v in spider.crawler.stats._stats.items():
            if k in from_default_stats.keys():
                readable_key = from_default_stats[k]
                # spider.stats must be set here b/c there is a pointer to it tracking stats for all spiders in cli
                spider.stats[spider.name][readable_key] = v

        spider.stats[spider.name]['Close Reason'] = reason
        super().close(spider, reason)

    # this class init/del timer
    time_lifespan: bool = False
    # runspider_settings.py
    custom_settings: dict = general_settings
    # for downloader_middlewares.py#BanEvasionMiddleware
    rotate_user_agent: bool = True
    randomly_delay_request: typing.Union[bool, range, typing.List[int]] = False

    source_page_url = None
    dont_filter_previous_hashes = False
    download_request_headers = {}

    stats: dict = {}

    def create_stat_func(self, readable_name, method_name) -> typing.Callable:
        def func():
            self.stats[self.name][readable_name] += 1
        setattr(self, f"increment_{method_name}", func)

    def setup_stats(self):
        try:
            self.stats[self.name] = copy.deepcopy(STATS_BASE)
            for readable_name in STATS_BASE:
                methodized_name = readable_name.lower().replace(' ', '_')
                if methodized_name.isidentifier():
                    self.create_stat_func(readable_name, methodized_name)
                else:
                    print(f'{self.name}: Could not auto generate helper function for {readable_name}, generated {methodized_name} which is not usable as an identifier. Try changing the readable name in GCSpider STATS_BASE.')

        except Exception as e:
            print(e)

    @staticmethod
    def download_response_handler(response):
        return response.body

    @staticmethod
    def get_href_file_extension(url: str) -> str:
        """
            returns file extension if exists in passed url path, else UNKNOWN
            UNKNOWN is used so that if the website fixes their link it will trigger an update from the doc type changing
        """
        path = urlparse(url).path
        ext: str = splitext(path)[1].replace('.', '').lower()

        if not ext:
            return UNKNOWN_FILE_EXTENSION_PLACEHOLDER

        return ext.strip()

    @staticmethod
    def get_href_file_extension_does_exist(url: str) -> typing.Tuple[str, bool]:
        """
            useful if links are a mix of other pages that need parsing and links to downloadable content
            returns (file extension, True) if exists in passed url path, else ("UNKNOWN", False)
            UNKNOWN is used so that if the website fixes their link it will trigger an update from the doc type changing
        """
        path = urlparse(url).path
        ext: str = splitext(path)[1].replace('.', '').lower()

        if not ext:
            return (UNKNOWN_FILE_EXTENSION_PLACEHOLDER, False)

        return (ext.strip(), True)

    @staticmethod
    def ascii_clean(text: str) -> str:
        """
            encodes to ascii, retaining non-breaking spaces and strips spaces from ends
            applys text.replace('\u00a0', ' ').encode('ascii', 'ignore').decode('ascii').strip()
        """

        return text.replace('\u00a0', ' ').replace('\u2019', "'").encode('ascii', 'ignore').decode('ascii').strip()

    @staticmethod
    def ensure_full_href_url(href_raw: str, url_base: str) -> str:
        """
            checks if href is relative and adds to base if needed
        """
        if href_raw.startswith('/'):
            web_url = urljoin(url_base, href_raw)
        else:
            web_url = href_raw

        return web_url.strip()

    @staticmethod
    def url_encode_spaces(href_raw: str) -> str:
        """
            encodes spaces as %20
        """
        return href_raw.replace(' ', '%20')

    @staticmethod
    def is_valid_url(url: str) -> bool:
        """
            checks if url is valid
        """
        return url_re.match(url)

    @staticmethod
    def filter_mailto_hrefs(href_list: typing.List[str]) -> typing.List[str]:
        """
            Takes list of href strings and filters out those that are mailto:
        """
        return [href for href in href_list if not mailto_re.search(href)]

    @staticmethod
    def encode_url_params(params: dict) -> str:
        print(params)
        return urllib.parse.urlencode(params)
