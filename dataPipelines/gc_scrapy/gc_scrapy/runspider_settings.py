general_settings = {
    'ITEM_PIPELINES': {
        'dataPipelines.gc_scrapy.gc_scrapy.pipelines.DeduplicaterPipeline': 1,
        'dataPipelines.gc_scrapy.gc_scrapy.pipelines.AdditionalFieldsPipeline': 2,
        'dataPipelines.gc_scrapy.gc_scrapy.pipelines.ValidateJsonPipeline': 1000,
    },
    'FEED_EXPORTERS': {
        'json': 'dataPipelines.gc_scrapy.gc_scrapy.exporters.JsonLinesAsJsonItemExporter',
    },
    'LOG_LEVEL': 'WARN',
    'DOWNLOADER_MIDDLEWARES': {
        'dataPipelines.gc_scrapy.gc_scrapy.downloader_middlewares.BanEvasionMiddleware': 1,
    },
    "ROBOTSTXT_OBEY": False
}

selenium_settings = {
    'SELENIUM_DRIVER_NAME': 'chrome',
    'SELENIUM_DRIVER_EXECUTABLE_PATH': "/usr/local/bin/chromedriver",
    'SELENIUM_DRIVER_ARGUMENTS': [
        "--headless",
        "--no-sandbox",
        "--disable-gpu",
        "--start-maximized",
        "--disable-dev-shm-usage",
        "--disable-setuid-sandbox",
        "--enable-javascript"
    ],
    'DOWNLOADER_MIDDLEWARES': {
        **general_settings["DOWNLOADER_MIDDLEWARES"],
        #                                                                              make sure the values are not clashing
        'dataPipelines.gc_scrapy.gc_scrapy.downloader_middlewares.SeleniumMiddleware': max(general_settings["DOWNLOADER_MIDDLEWARES"].values()) + 1
    }
}
