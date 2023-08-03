
1) Set up environment and run crawler
    ```shell
     $env:PYTHONPATH = "$(Get-Location)"
     $CRAWLER_DATA_ROOT = "./test-samm"
    New-Item -ItemType Directory -Force -Path $CRAWLER_DATA_ROOT| Out-Null
    $prevManifestFile = Join-Path $CRAWLER_DATA_ROOT "pre-manifest.json"
    New-Item -ItemType File -Force -Path $prevManifestFile | Out-Null
    $sammSpider = "dataPipelines/gc_scrapy/gc_scrapy/unfinished/samm_spider.py"    $outputFile = Join-Path $CRAWLER_DATA_ROOT "output.json"
    scrapy runspider $sammSpider `
    -a download_output_dir=$CRAWLER_DATA_ROOT `
    -a previous_manifest_location=$prevManifestFile `
    -o $outputFile
    ```




