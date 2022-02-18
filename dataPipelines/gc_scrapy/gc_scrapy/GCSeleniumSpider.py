# -*- coding: utf-8 -*-
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
import typing

from dataPipelines.gc_scrapy.gc_scrapy.runspider_settings import general_settings, selenium_settings
from dataPipelines.gc_scrapy.gc_scrapy.middleware_utils.selenium_request import SeleniumRequest
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider


class GCSeleniumSpider(GCSpider):
    """
        Selenium Spider with settings applied and selenium request returned for the standard parse method used in crawlers
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    custom_settings: dict = {**general_settings, **selenium_settings}
    selenium_request_overrides: dict = {}

    selenium_spider_start_request_retries_allowed: int = 5
    selenium_spider_start_request_retry_wait: int = 30

    def start_requests(self):
        """
            Applies selenium_request_overrides dict and returns a selenium response instead of standard scrapy response
        """

        opts = {
            "url": self.start_urls[0],
            "callback": self.parse,
            "wait_time": 5,
            **self.selenium_request_overrides
        }

        yield SeleniumRequest(**opts)

    @staticmethod
    def wait_until_css_clickable(driver, css_selector: str, wait: typing.Union[int, float] = 5):
        WebDriverWait(driver, wait).until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, css_selector)
            ))

    @staticmethod
    def wait_until_css_located(driver, css_selector: str, wait: typing.Union[int, float] = 5):
        WebDriverWait(driver, wait).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, css_selector)
            ))

    @staticmethod
    def wait_until_css_not_located(driver, css_selector: str, wait: typing.Union[int, float] = 5):
        WebDriverWait(driver, wait).until_not(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, css_selector)
            ))
