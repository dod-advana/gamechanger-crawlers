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
    # Testing SAAM 403 kick
    "DOWNLOAD_DELAY": 3,  # Delay between requests
    "DOWNLOAD_TIMEOUT": 120,  # 2 min
    "RETRY_ENABLE": True,
    "RETRY_TIMES": 3,
    "CONCURRENT_REQUESTS": 5,
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
