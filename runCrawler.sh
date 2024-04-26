#!/bin/bash

RESET=false
for i in "$@"; do
    case $i in
        -c=*|--crawler=*)
            CRAWLER="${i#*=}"
            shift # past argument=value
        ;;
        --reset)
            RESET=true
            shift # past argument with no value
        ;;
        -*|--*)
            echo "Unknown option $i"
            exit 1
        ;;
        *)
        ;;
    esac
done


export PYTHONPATH="$(pwd)"
CRAWLER_DATA_ROOT=./tmp/$CRAWLER
mkdir -p "$CRAWLER_DATA_ROOT"

echo "CRAWLER   = ${CRAWLER}"
echo "RESET     = ${RESET}"
echo "DATA_ROOT = ${CRAWLER_DATA_ROOT}"

if $RESET; then
    rm $CRAWLER_DATA_ROOT/*
fi

touch "$CRAWLER_DATA_ROOT/prev-manifest.json"
scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/$CRAWLER.py \
    -a download_output_dir="$CRAWLER_DATA_ROOT" \
    -a previous_manifest_location="$CRAWLER_DATA_ROOT/prev-manifest.json" \
    -o "$CRAWLER_DATA_ROOT/output.json"