#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail
set -o noclobber

readonly SCRIPT_PARENT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
readonly REPO_DIR="$( cd "$SCRIPT_PARENT_DIR/../"  >/dev/null 2>&1 && pwd )"

# Load defaults
source "${REPO_DIR}/config/constants.conf"
echo "Building base image"
docker build -f "$REPO_DIR/Dockerfile" \
  --target=base-image \
  --build-arg BASE_IMAGE=${BASE_IMAGE} \
  --rm=false \
  -t "$BASE_CRAWLER_IMAGE" \
  "$REPO_DIR"

echo "Building crawler prod image"
docker build -f "$REPO_DIR/Dockerfile" \
  --target=crawler-prod \
  --build-arg BASE_IMAGE=${BASE_IMAGE} \
  --rm=false \
  -t "$CORE_DOWNLOADER_IMAGE" \
  "$REPO_DIR"

echo "Building crawler dev image"
docker build -f "$REPO_DIR/Dockerfile" \
  --target=crawler-dev \
  --build-arg BASE_IMAGE=${BASE_IMAGE} \
  --rm=false \
  -t "$CORE_DOWNLOADER_DEV_IMAGE" \
  "$REPO_DIR"
