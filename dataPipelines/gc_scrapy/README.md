# Gamechanger Scrapy

Data source crawlers to routinely update data.

## Things to Know
Both cli's take a previous_manifest path (jsonlines format) that can be used to filter out previously downloaded documents based on the version_hash   
This step can be skipped by using `dont_filter_previous_hashes=true` or an empty file for the previous manifest

## Run using the scrapy cli (single spider)
```
	- Named Args (-a) -
	download_output_dir: directory
	[previous_manifest_location: file  OR  dont_filter_previous_hashes=true]

	- Command -
	scrapy runspider <path/to/spider.py> \
	-a download_output_dir=<path/to/output/downloads_dir> \
	[-a previous_manifest_location=<path/to/previous-manifest.json> | -a dont_filter_previous_hashes=true] \
	-o <path/to/output_file.json>
```

## Run using spiders list file (1+ sequentially run spiders)
```
	- Named Args -
	--download-output-dir: directory
	--crawler-output-location: json file
	--previous-manifest-location: json file OR dont_filter_previous_hashes=true
	--spiders-file-location: txt file
	--dont-filter-previous-hashes: bool (truthy string works)

	- Command -
	python -m dataPipelines.gc_scrapy crawl \
	--download-output-dir=<path/to/output/downloads_dir> \
	--crawler-output-location=<path/to/output_file.json> \
	--previous-manifest-location=<path/to/previous-manifest.json> \
	--spiders-file-location=<path/to/spiders_to_run.txt> \
	(optional) --dont-filter-previous-hashes=true
```
