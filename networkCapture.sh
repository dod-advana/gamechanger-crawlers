#!/bin/bash




# If validation to our TLS connection ever gets questioned, run this script

# install network tool to generate .pcap file
# yum install -y tcpdump

# Wait for a couple of files to be returned in the /tmp folder
# and can kill script / let run fully
# Then use the external app 'Wireshark' to open the generated .pcap
# Link to wireshark download --> https://www.wireshark.org/download.html
# Wireshark will generate rows of info based off of network interaction
# Key points are shown in ../image.png


# Set up your environment and crawler data root
export PYTHONPATH="$(pwd)"
CRAWLER_DATA_ROOT=./tmp
mkdir -p "$CRAWLER_DATA_ROOT"
touch "$CRAWLER_DATA_ROOT/prev-manifest.json"

# Start tcpdump in the background
tcpdump -i any -w "$CRAWLER_DATA_ROOT/capture.pcap" &
TCPDUMP_PID=$!

echo "tcpdump started with PID: $TCPDUMP_PID"

# Run your Scrapy crawler
scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/legislation_spider.py \
-a download_output_dir="$CRAWLER_DATA_ROOT" \
-a previous_manifest_location="$CRAWLER_DATA_ROOT/prev-manifest.json" \
-o "$CRAWLER_DATA_ROOT/output.json" &

CRAWLER_PID=$!
echo "Crawler started with PID: $CRAWLER_PID"

# Wait for the crawler to finish or be force-closed
wait $CRAWLER_PID

# Stop tcpdump after the crawler has finished or been terminated
kill $TCPDUMP_PID
echo "tcpdump (PID: $TCPDUMP_PID) stopped"

# Wait for tcpdump to gracefully exit and finalize the pcap file
wait $TCPDUMP_PID
