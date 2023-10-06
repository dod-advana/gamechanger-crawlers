general_settings = {
    "ITEM_PIPELINES": {
        "dataPipelines.gc_scrapy.gc_scrapy.pipelines.FileNameFixerPipeline": 50,
        "dataPipelines.gc_scrapy.gc_scrapy.pipelines.DeduplicaterPipeline": 100,
        "dataPipelines.gc_scrapy.gc_scrapy.pipelines.AdditionalFieldsPipeline": 200,
        "dataPipelines.gc_scrapy.gc_scrapy.pipelines.ValidateJsonPipeline": 300,
        "dataPipelines.gc_scrapy.gc_scrapy.pipelines.FileDownloadPipeline": 400,
    },
    "FEED_EXPORTERS": {
        "jsonlines": "dataPipelines.gc_scrapy.gc_scrapy.exporters.JsonLinesAsJsonItemExporter",
    },
    "DOWNLOADER_MIDDLEWARES": {
        "dataPipelines.gc_scrapy.gc_scrapy.downloader_middlewares.BanEvasionMiddleware": 100,
    },
    # 'STATS_DUMP': False,
    "ROBOTSTXT_OBEY": False,
    "LOG_LEVEL": "INFO",
    "DOWNLOAD_FAIL_ON_DATALOSS": False,
    
    # Throttle crawler
    "DOWNLOAD_DELAY": 0.025,  # Delay between requests in seconds
    "DOWNLOAD_TIMEOUT": 1,  # seconds
    "RETRY_ENABLE": True,
    "RETRY_TIMES": 2,
    "CONCURRENT_REQUESTS": 10,
    "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537"
}
selenium_settings = {
    "SELENIUM_DRIVER_NAME": "chrome",
    "SELENIUM_DRIVER_EXECUTABLE_PATH": "/usr/local/bin/chromedriver",
    "SELENIUM_DRIVER_ARGUMENTS": [
        "--headless",
        "--no-sandbox",
        "--disable-gpu",
        "--start-maximized",
        "--disable-dev-shm-usage",
        "--disable-setuid-sandbox",
        "--enable-javascript",
    ],
    "DOWNLOADER_MIDDLEWARES": {
        **general_settings["DOWNLOADER_MIDDLEWARES"],
        #                                                                              make sure the values are not clashing
        "dataPipelines.gc_scrapy.gc_scrapy.downloader_middlewares.SeleniumMiddleware": max(
            general_settings["DOWNLOADER_MIDDLEWARES"].values()
        )
        + 1,
    },
}
