#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail
set -o noclobber

readonly SCRIPT_PARENT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
readonly SETTINGS_CONF_PATH="$SCRIPT_PARENT_DIR/settings.conf"

#####
## ## LOAD SETTINGS
#####

source "$SETTINGS_CONF_PATH"

#####
## ## SETUP FUNCTIONS
#####

function setup_local_vars_and_dirs() {

  LOCAL_CRAWLER_OUTPUT_FILE_PATH="$LOCAL_DOWNLOAD_DIRECTORY_PATH/crawler_output.json"
  LOCAL_JOB_LOG_PATH="$LOCAL_DOWNLOAD_DIRECTORY_PATH/job.log"
  LOCAL_PREVIOUS_MANIFEST_LOCATION="${LOCAL_PREVIOUS_MANIFEST_LOCATION:-$SCRIPT_PARENT_DIR/previous-manifest.json}"
  LOCAL_NEW_MANIFEST_PATH="$LOCAL_DOWNLOAD_DIRECTORY_PATH/manifest.json"

  if [[ ! -d "$LOCAL_DOWNLOAD_DIRECTORY_PATH" ]]; then
    mkdir -p "$LOCAL_DOWNLOAD_DIRECTORY_PATH"
  fi

  touch "$LOCAL_JOB_LOG_PATH"

  echo LOCAL_DOWNLOAD_DIRECTORY_PATH is "$LOCAL_DOWNLOAD_DIRECTORY_PATH"

}

#####
## ## MAIN FUNCTIONS
#####

function run_crawler() {

  if [[ "${TEST_RUN:-no}" == "yes" ]]; then
    echo -e "\n RUNNING SCRAPY SPIDER: us_code_spider.py \n"
  ( scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/us_code_spider.py -a download_output_dir="$LOCAL_DOWNLOAD_DIRECTORY_PATH/" -a previous_manifest_location="$LOCAL_PREVIOUS_MANIFEST_LOCATION"  -o $LOCAL_CRAWLER_OUTPUT_FILE_PATH ) \
   || echo "^^^ CRAWLER ERROR ^^^"
    return 0
  fi

  set +o pipefail

    echo -e "\n RUNNING SCRAPY SPIDER: far_subpart_regs_spider.py \n"
  ( scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/far_subpart_regs_spider.py -a download_output_dir="$LOCAL_DOWNLOAD_DIRECTORY_PATH/" -a previous_manifest_location="$LOCAL_PREVIOUS_MANIFEST_LOCATION" -o $LOCAL_CRAWLER_OUTPUT_FILE_PATH ) \
   || echo "^^^ CRAWLER ERROR ^^^"


    echo -e "\n RUNNING SCRAPY SPIDER: army_pubs_spider.py \n"
  ( scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/army_pubs_spider.py -a download_output_dir="$LOCAL_DOWNLOAD_DIRECTORY_PATH/" -a previous_manifest_location="$LOCAL_PREVIOUS_MANIFEST_LOCATION" -o $LOCAL_CRAWLER_OUTPUT_FILE_PATH ) \
   || echo "^^^ CRAWLER ERROR ^^^"


    echo -e "\n RUNNING SCRAPY SPIDER: marine_corp_spider.py \n"
  ( scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/marine_corp_spider.py -a download_output_dir="$LOCAL_DOWNLOAD_DIRECTORY_PATH/" -a previous_manifest_location="$LOCAL_PREVIOUS_MANIFEST_LOCATION" -o $LOCAL_CRAWLER_OUTPUT_FILE_PATH ) \
   || echo "^^^ CRAWLER ERROR ^^^"


    echo -e "\n RUNNING SCRAPY SPIDER: jcs_pubs_spider.py \n"
  ( scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/jcs_pubs_spider.py -a download_output_dir="$LOCAL_DOWNLOAD_DIRECTORY_PATH/" -a previous_manifest_location="$LOCAL_PREVIOUS_MANIFEST_LOCATION" -o $LOCAL_CRAWLER_OUTPUT_FILE_PATH ) \
   || echo "^^^ CRAWLER ERROR ^^^"


    echo -e "\n RUNNING SCRAPY SPIDER: army_reserve_spider.py \n"
  ( scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/army_reserve_spider.py -a download_output_dir="$LOCAL_DOWNLOAD_DIRECTORY_PATH/" -a previous_manifest_location="$LOCAL_PREVIOUS_MANIFEST_LOCATION" -o $LOCAL_CRAWLER_OUTPUT_FILE_PATH ) \
   || echo "^^^ CRAWLER ERROR ^^^"


    echo -e "\n RUNNING SCRAPY SPIDER: sorn_spider.py \n"
  ( scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/sorn_spider.py -a download_output_dir="$LOCAL_DOWNLOAD_DIRECTORY_PATH/" -a previous_manifest_location="$LOCAL_PREVIOUS_MANIFEST_LOCATION" -o $LOCAL_CRAWLER_OUTPUT_FILE_PATH ) \
   || echo "^^^ CRAWLER ERROR ^^^"


    echo -e "\n RUNNING SCRAPY SPIDER: fasab_spider.py \n"
  ( scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/fasab_spider.py -a download_output_dir="$LOCAL_DOWNLOAD_DIRECTORY_PATH/" -a previous_manifest_location="$LOCAL_PREVIOUS_MANIFEST_LOCATION" -o $LOCAL_CRAWLER_OUTPUT_FILE_PATH ) \
   || echo "^^^ CRAWLER ERROR ^^^"


    echo -e "\n RUNNING SCRAPY SPIDER: dfar_subpart_regs_spider.py \n"
  ( scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/dfar_subpart_regs_spider.py -a download_output_dir="$LOCAL_DOWNLOAD_DIRECTORY_PATH/" -a previous_manifest_location="$LOCAL_PREVIOUS_MANIFEST_LOCATION" -o $LOCAL_CRAWLER_OUTPUT_FILE_PATH ) \
   || echo "^^^ CRAWLER ERROR ^^^"


    echo -e "\n RUNNING SCRAPY SPIDER: milpersman_spider.py \n"
  ( scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/milpersman_spider.py -a download_output_dir="$LOCAL_DOWNLOAD_DIRECTORY_PATH/" -a previous_manifest_location="$LOCAL_PREVIOUS_MANIFEST_LOCATION" -o $LOCAL_CRAWLER_OUTPUT_FILE_PATH ) \
   || echo "^^^ CRAWLER ERROR ^^^"


    echo -e "\n RUNNING SCRAPY SPIDER: cfr_spider.py \n"
  ( scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/cfr_spider.py -a download_output_dir="$LOCAL_DOWNLOAD_DIRECTORY_PATH/" -a previous_manifest_location="$LOCAL_PREVIOUS_MANIFEST_LOCATION" -o $LOCAL_CRAWLER_OUTPUT_FILE_PATH ) \
   || echo "^^^ CRAWLER ERROR ^^^"


    echo -e "\n RUNNING SCRAPY SPIDER: us_code_spider.py \n"
  ( scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/us_code_spider.py -a download_output_dir="$LOCAL_DOWNLOAD_DIRECTORY_PATH/" -a previous_manifest_location="$LOCAL_PREVIOUS_MANIFEST_LOCATION" -o $LOCAL_CRAWLER_OUTPUT_FILE_PATH ) \
   || echo "^^^ CRAWLER ERROR ^^^"


    echo -e "\n RUNNING SCRAPY SPIDER: legislation_spider.py \n"
  ( scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/legislation_spider.py -a download_output_dir="$LOCAL_DOWNLOAD_DIRECTORY_PATH/" -a previous_manifest_location="$LOCAL_PREVIOUS_MANIFEST_LOCATION" -o $LOCAL_CRAWLER_OUTPUT_FILE_PATH ) \
   || echo "^^^ CRAWLER ERROR ^^^"


    echo -e "\n RUNNING SCRAPY SPIDER: air_force_spider.py \n"
  ( scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/air_force_spider.py -a download_output_dir="$LOCAL_DOWNLOAD_DIRECTORY_PATH/" -a previous_manifest_location="$LOCAL_PREVIOUS_MANIFEST_LOCATION" -o $LOCAL_CRAWLER_OUTPUT_FILE_PATH ) \
   || echo "^^^ CRAWLER ERROR ^^^"


    echo -e "\n RUNNING SCRAPY SPIDER: dod_issuances_spider.py \n"
  ( scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/dod_issuances_spider.py -a download_output_dir="$LOCAL_DOWNLOAD_DIRECTORY_PATH/" -a previous_manifest_location="$LOCAL_PREVIOUS_MANIFEST_LOCATION" -o $LOCAL_CRAWLER_OUTPUT_FILE_PATH ) \
   || echo "^^^ CRAWLER ERROR ^^^"


    echo -e "\n RUNNING SCRAPY SPIDER: fmr_spider.py \n"
  ( scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/fmr_spider.py -a download_output_dir="$LOCAL_DOWNLOAD_DIRECTORY_PATH/" -a previous_manifest_location="$LOCAL_PREVIOUS_MANIFEST_LOCATION" -o $LOCAL_CRAWLER_OUTPUT_FILE_PATH ) \
   || echo "^^^ CRAWLER ERROR ^^^"


    echo -e "\n RUNNING SCRAPY SPIDER: opm_pubs_spider.py \n"
  ( scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/opm_pubs_spider.py -a download_output_dir="$LOCAL_DOWNLOAD_DIRECTORY_PATH/" -a previous_manifest_location="$LOCAL_PREVIOUS_MANIFEST_LOCATION" -o $LOCAL_CRAWLER_OUTPUT_FILE_PATH ) \
   || echo "^^^ CRAWLER ERROR ^^^"


    echo -e "\n RUNNING SCRAPY SPIDER: nato_spider.py \n"
  ( scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/nato_spider.py -a download_output_dir="$LOCAL_DOWNLOAD_DIRECTORY_PATH/" -a previous_manifest_location="$LOCAL_PREVIOUS_MANIFEST_LOCATION" -o $LOCAL_CRAWLER_OUTPUT_FILE_PATH ) \
   || echo "^^^ CRAWLER ERROR ^^^"


    echo -e "\n RUNNING SCRAPY SPIDER: navy_reserve_spider.py \n"
  ( scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/navy_reserve_spider.py -a download_output_dir="$LOCAL_DOWNLOAD_DIRECTORY_PATH/" -a previous_manifest_location="$LOCAL_PREVIOUS_MANIFEST_LOCATION" -o $LOCAL_CRAWLER_OUTPUT_FILE_PATH ) \
   || echo "^^^ CRAWLER ERROR ^^^"


    echo -e "\n RUNNING SCRAPY SPIDER: chief_national_guard_bureau_spider.py \n"
  ( scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/chief_national_guard_bureau_spider.py -a download_output_dir="$LOCAL_DOWNLOAD_DIRECTORY_PATH/" -a previous_manifest_location="$LOCAL_PREVIOUS_MANIFEST_LOCATION" -o $LOCAL_CRAWLER_OUTPUT_FILE_PATH ) \
   || echo "^^^ CRAWLER ERROR ^^^"


    echo -e "\n RUNNING SCRAPY SPIDER: coast_guard_spider.py \n"
  ( scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/coast_guard_spider.py -a download_output_dir="$LOCAL_DOWNLOAD_DIRECTORY_PATH/" -a previous_manifest_location="$LOCAL_PREVIOUS_MANIFEST_LOCATION" -o $LOCAL_CRAWLER_OUTPUT_FILE_PATH ) \
   || echo "^^^ CRAWLER ERROR ^^^"


    echo -e "\n RUNNING SCRAPY SPIDER: bupers_spider.py \n"
  ( scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/bupers_spider.py -a download_output_dir="$LOCAL_DOWNLOAD_DIRECTORY_PATH/" -a previous_manifest_location="$LOCAL_PREVIOUS_MANIFEST_LOCATION" -o $LOCAL_CRAWLER_OUTPUT_FILE_PATH ) \
   || echo "^^^ CRAWLER ERROR ^^^"


    echo -e "\n RUNNING SCRAPY SPIDER: secnav_spider.py \n"
  ( scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/secnav_spider.py -a download_output_dir="$LOCAL_DOWNLOAD_DIRECTORY_PATH/" -a previous_manifest_location="$LOCAL_PREVIOUS_MANIFEST_LOCATION" -o $LOCAL_CRAWLER_OUTPUT_FILE_PATH ) \
   || echo "^^^ CRAWLER ERROR ^^^"


    echo -e "\n RUNNING SCRAPY SPIDER: dod_coronavirus_spider.py \n"
  ( scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/dod_coronavirus_spider.py -a download_output_dir="$LOCAL_DOWNLOAD_DIRECTORY_PATH/" -a previous_manifest_location="$LOCAL_PREVIOUS_MANIFEST_LOCATION" -o $LOCAL_CRAWLER_OUTPUT_FILE_PATH ) \
   || echo "^^^ CRAWLER ERROR ^^^"


    echo -e "\n RUNNING SCRAPY SPIDER: ic_policies_spider.py \n"
  ( scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/ic_policies_spider.py -a download_output_dir="$LOCAL_DOWNLOAD_DIRECTORY_PATH/" -a previous_manifest_location="$LOCAL_PREVIOUS_MANIFEST_LOCATION" -o $LOCAL_CRAWLER_OUTPUT_FILE_PATH ) \
   || echo "^^^ CRAWLER ERROR ^^^"


    echo -e "\n RUNNING SCRAPY SPIDER: executive_orders_spider.py \n"
  ( scrapy runspider dataPipelines/gc_scrapy/gc_scrapy/spiders/executive_orders_spider.py -a download_output_dir="$LOCAL_DOWNLOAD_DIRECTORY_PATH/" -a previous_manifest_location="$LOCAL_PREVIOUS_MANIFEST_LOCATION" -o $LOCAL_CRAWLER_OUTPUT_FILE_PATH ) \
   || echo "^^^ CRAWLER ERROR ^^^"

  set -o pipefail
}

# function run_downloader() {
#   echo -e "\nRUNNING DOWNLOADER\n"

#   if [[ "${TEST_RUN:-no}" == "yes" ]]; then

#     "$PYTHON_CMD" -m dataPipelines.gc_downloader download \
#       --input-json "$LOCAL_CRAWLER_OUTPUT_FILE_PATH" \
#       --output-dir "$LOCAL_DOWNLOAD_DIRECTORY_PATH" \
#       --new-manifest "$LOCAL_NEW_MANIFEST_PATH"

#   else

#     "$PYTHON_CMD" -m dataPipelines.gc_downloader download \
#       --input-json "$LOCAL_CRAWLER_OUTPUT_FILE_PATH" \
#       --output-dir "$LOCAL_DOWNLOAD_DIRECTORY_PATH" \
#       --new-manifest "$LOCAL_NEW_MANIFEST_PATH" \
#       --previous-manifest "$LOCAL_PREVIOUS_MANIFEST_LOCATION"
#   fi

#   echo -e "\nDOWNLOADED FILES LOCATED AT: $LOCAL_DOWNLOAD_DIRECTORY_PATH \n"
# }

function create_cumulative_manifest() {
  local cumulative_manifest="$LOCAL_DOWNLOAD_DIRECTORY_PATH/cumulative-manifest.json"
  if [[ -f "$LOCAL_PREVIOUS_MANIFEST_LOCATION" ]]; then
    cat "$LOCAL_PREVIOUS_MANIFEST_LOCATION" > "$cumulative_manifest"
    echo >> "$cumulative_manifest"
  fi
  cat "$LOCAL_NEW_MANIFEST_PATH" >> "$cumulative_manifest"
}

function register_log_in_manifest() {
  "$PYTHON_CMD" -m dataPipelines.gc_downloader add-to-manifest --file "$LOCAL_JOB_LOG_PATH" --manifest "$LOCAL_NEW_MANIFEST_PATH"
}

function register_crawl_log_in_manifest() {
  "$PYTHON_CMD" -m dataPipelines.gc_downloader add-to-manifest --file "$LOCAL_CRAWLER_OUTPUT_FILE_PATH" --manifest "$LOCAL_NEW_MANIFEST_PATH"
}

##### ##### #####
## ## ## ## ## ## ACTUAL EXEC FLOW
##### ##### #####

# setup
setup_local_vars_and_dirs

SECONDS=0
cat <<EOF 2>&1 | tee -a "$LOCAL_JOB_LOG_PATH"

  STARTING PIPELINE RUN
  $(date "+DATE: %Y-%m-%d TIME: %H:%M:%S")

EOF

# run
run_crawler 2>&1 | tee -a "$LOCAL_JOB_LOG_PATH"
# run_downloader 2>&1 | tee -a "$LOCAL_JOB_LOG_PATH"

cat <<EOF 2>&1 | tee -a "$LOCAL_JOB_LOG_PATH"

  SUCCESSFULLY FINISHED PIPELINE RUN
  $(date "+DATE: %Y-%m-%d TIME: %H:%M:%S")

EOF

# how long?
duration=$SECONDS
echo -e "\n $(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed." 2>&1 | tee -a "$LOCAL_JOB_LOG_PATH"

# register additional files in manifest
register_log_in_manifest
register_crawl_log_in_manifest
# create combined manifest for future runs
create_cumulative_manifest