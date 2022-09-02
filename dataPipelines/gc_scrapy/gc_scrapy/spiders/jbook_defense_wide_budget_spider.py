# JBOOK CRAWLER
# Defense Wide Budget Spider

import scrapy
from scrapy import Selector
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver import Chrome
from selenium.common.exceptions import NoSuchElementException
import re
from urllib.parse import urljoin, urlparse
from datetime import datetime

from dataPipelines.gc_scrapy.gc_scrapy.middleware_utils.selenium_request import SeleniumRequest
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSeleniumSpider import GCSeleniumSpider