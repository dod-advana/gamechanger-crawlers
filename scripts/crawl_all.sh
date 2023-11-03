#!/usr/bin/env bash

CRAWLER_DATA_ROOT=${CRAWLER_DATA_ROOT:=/mnt/extra/gamechanger-download}
PYTHONPATH=$(pwd)

LOG_FILE=${LOG_FILE:=crawling.log}

for c in `ls dataPipelines/gc_scrapy/gc_scrapy/spiders/*_spider.py`; do
	date | tee -a ${LOG_FILE}
	echo "Running ${c}" | tee -a ${LOG_FILE}
	scrapy runspider ${c} \
		-a download_output_dir="${CRAWLER_DATA_ROOT}" \
		-a previous_manifest_location="${CRAWLER_DATA_ROOT}/prev-manifest.json" \
		-o "${CRAWLER_DATA_ROOT}/output.json"
	date | tee -a ${LOG_FILE}
	echo "Done." | tee -a ${LOG_FILE}
done
