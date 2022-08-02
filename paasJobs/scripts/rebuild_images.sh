#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail
set -o noclobber

readonly SCRIPT_PARENT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
readonly REPO_DIR="$( cd "$SCRIPT_PARENT_DIR/../../"  >/dev/null 2>&1 && pwd )"

# Load defaults
source "${SCRIPT_PARENT_DIR}/constants.conf"

function image_exists() {
  if docker image inspect "$1" &>/dev/null ; then
    return 0
  else
    return 1
  fi
}

if [[ "${SKIP_BASE_BUILD:-no}" != "yes" ]]; then
  # Set env vars
  case "${DEPLOYMENT_ENV:-dev}" in
  prod)
    if ! image_exists "$BASE_OS_IMAGE" ; then
      echo >&2 "ERROR: Could not find base image needed for build: $BASE_OS_IMAGE"
    fi
    ;;
  dev)
    docker pull "centos:7" && docker tag "centos:7" "$BASE_OS_IMAGE"
    if ! image_exists "$BASE_OS_IMAGE" ; then
      echo >&2 "ERROR: Could not find base image needed for build: $BASE_OS_IMAGE"
    fi
    ;;
  *)
    echo >&2 "ERROR: Set valid DEPLOYMENT_ENV var (prod|dev)."
    exit 2
    ;;
  esac

  # GC_CRAWLER is the image with all of the base packages / python3 installed
  echo "Rebuilding GC_CRAWLER core image"
  docker build -f "$REPO_DIR/docker/core/Dockerfile" \
    -t "$BASE_CRAWLER_IMAGE" \
    "$REPO_DIR"
fi

# the crawl_download_upload image builds off of GC_CRAWLER, adding the code to run everything
echo "Rebuilding CRAWL_AND_DOWNLOAD image"
docker build -f "$REPO_DIR/docker/crawl_download_upload/Dockerfile" \
  -t "$CRAWL_DOWNLOAD_UPLOAD_IMAGE" \
  "$REPO_DIR"