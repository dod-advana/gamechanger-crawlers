This is documentation for my process for troubleshooting air_force_spider.py

1) Set up environment and run crawler
    ```shell
     $env:PYTHONPATH = "$(Get-Location)"
     $CRAWLER_DATA_ROOT= "./test-env"
    New-Item -ItemType Directory -Force -Path $CRAWLER_DATA_ROOT| Out-Null
    $prevManifestFile = Join-Path $CRAWLER_DATA_ROOT "pre-manifest.json"
    New-Item -ItemType File -Force -Path $prevManifestFile | Out-Null
    $afSpider = "dataPipelines/gc_scrapy/gc_scrapy/spiders/air_force_spider.py" 
    $outputFile = Join-Path $CRAWLER_DATA_ROOT "output.json"
    scrapy runspider $afSpider `
    -a download_output_dir=$CRAWLER_DATA_ROOT `
    -a previous_manifest_location=$prevManifestFile `
    -o $outputFile
    ```
2) store log
./test-env/local-log

3) Troubleshoot local spider prior to original problem



