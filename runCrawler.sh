#!/bin/bash

export PYTHONPATH="$(pwd)"
CRAWLER_DATA_ROOT=./tmp
mkdir -p "$CRAWLER_DATA_ROOT"
touch "$CRAWLER_DATA_ROOT/prev-manifest.json"
CRAWLER_DATA_ROOT=./tmp
mkdir -p "$CRAWLER_DATA_ROOT"
touch "$CRAWLER_DATA_ROOT/prev-manifest.json"
scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/hasc_spider.py \
-a download_output_dir="$CRAWLER_DATA_ROOT" \
-a previous_manifest_location="$CRAWLER_DATA_ROOT/prev-manifest.json" \
-o "$CRAWLER_DATA_ROOT/output.json"