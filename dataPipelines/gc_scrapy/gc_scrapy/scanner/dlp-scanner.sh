#!/usr/bin/env bash

# set -o errexit
set -o nounset
set -o pipefail
set -o noclobber


###
##### Program Args/Params
###

# dir or file, passed either as env var or 1st arg to script
SCAN_DIR_OR_FILE="${1:-${SCAN_DIR_OR_FILE:?"[ARG ERROR] Missing script target (1st-arg) or SCAN_DIR_OR_FILE env var set to the same"}}"

#
## some arg cleaning/validation
## todo: add more early validations
#


# make sure target makes sense
[[ -d "$SCAN_DIR_OR_FILE" || -f "$SCAN_DIR_OR_FILE" ]] || {
  >&2 echo "[ABORT] Script target is neither dir nor a file: $SCAN_DIR_OR_FILE"
  exit 1
}

###
##### Core Functions
###

checksum() {
	CMD=$1 ; shift
	$CMD "$@" | sed -e 's: .*::'
}

hex2ascii() {
	while read -r HEXSTR; do
		while case "${#HEXSTR}" in 0)	break;; esac; do
			printf "\x${HEXSTR:0:2}"
			HEXSTR=${HEXSTR#??}
		done
	done
}

etag() {
	for F in "$@" ; do
		FSTAT=$(stat -c '%s' "$F")
		CHUNKS=$((FSTAT/8/1024/1024))
		case "$CHUNKS" in
			0)
				md5sum "$F"
				continue
				;;
			*)
				CHUNKS=$((CHUNKS+1))
				;;
		esac

		start=$CHUNKS
		count=$start

		while case $count in 0) break;; esac; do
			dd bs=8M count=1 skip=$((start-count)) if="$F" 2> /dev/null | md5sum - | cut -f1 -d ' '
			count=$((count-1))
		done | hex2ascii | md5sum - | sed -e "s/\([[:space:]]\)/-${CHUNKS}\1/ ; s/-\$/${F//\//\\/}/"
	done
}

iscompressed() {
	case "${MIMETYPE}" in
		application/x-rpm) echo "true";;
		application/*zip) echo "true";;
	esac
}

isarchive() {
	case "${MIMETYPE}" in
		application/zip) echo true;;
		application/x-rpm) echo true;;
		application/x-tar) echo true;;
		application/x-gzip) case "${FILE}" in
				*.tgz|*.tar.gz) echo true;;
			esac;;
#			 case "$(zcat \"${FILE}\" | file --mime-type -)" in
#				applicationx-tar) echo true;;
#				*) echo false;;
#			esac;;
	esac
}

json() { python -c "import json; print(json.load(open('$1'))$2)" ; }

origin() {
	case "${FILE##*.}" in
		metadata) echo "metadata://${FILE%.metadata}";;
		*) json "${FILE}.metadata" "['source_page_url']";;
	esac
}

classifier() {
	# EDIPI 10-digit / optional 6-digit FASC-N
	# SSN 9-digit XXX-XX-XXXX, optional delimiters supported are [-_/. ]
	sed -e '
		s/.*\(TS\|S\|C\|U\).*/\1/i ;
		s/.*\(TOP[[:blank:]]SECRET\|SECRET\|CONFIDENTIAL\|UNCLASSIFIED\|UNCLASS\).*/\1/i ;
		s/.*\(SCI\|NO[[:blank:]]?FORN\).*/\1/i ;
		s/.*\(CUI\|FOUO\|CII\|SBU\|SSI\|LES\|PARD\|ORCON\).*/\1/i ;
		s/.*\(CONTROLLED UNCLASSIFIED INFORMATION\|FOR OFFICIAL USE ONLY\|CONTROLLED\|LIMITED\|PROPRIETARY\|RESTRICTED\|SENSITIVE\).*/\1/i ;
		s/.*\(PII\|PHI\).*/\1/i ;
		s/.*[[:digit:]]\{10\}\([[:digit:]]\{6\}\)\?.*/EDIPI FOUND/i ;
		s/.*[[:digit:]]\{3\}\([-_\/. ]\?\)[[:digit:]]\{2\}\1[[:digit:]]\{4\}.*/SSN FOUND/i ;
		s/.*\(solicitation\|private\|personal\|sol\|poc\|contact\|contractor\).*/DIRTY WORD FOUND/i ;
		s/.*\(labor\|cost\|rate\|wage\|salary\|diem\|week\|day\|hour\|hr\).*/DIRTY WORD FOUND/i ;
		'
}

dlpscan() {

  set -o xtrace
  # set +o errexit
  # scan & output only report flagged findings for the file
	clamscan \
	  --no-summary \
	  --detect-structured=yes \
	  --structured-ssn-format=2 \
	  --structured-ssn-count=3 \
	  --structured-cc-count=3 \
	  --alert-broken \
	  --alert-encrypted \
	  --alert-macros \
	  --alert-exceeds-max \
	  "${SCANTARGET}" \
	    | grep "^${SCANTARGET##/*/}" | grep -v -e '^[^:]*:[[:blank:]]*OK' | tr -d '\n'
	set +o xtrace


  strings "${FILE}" \
    | grep -iEf "${0%/*}/dirty-words.regex" \
      | classifier | sort | tr 'a-z' 'A-Z' \
        | uniq -c | sort -V | sed -e 's:  *: :g ; s:^ :: ; s: $:: ;' |  tr '\n' ' '

	# set -o errexit
}

avscan() {

  set -o xtrace
  # set +o errexit
  # scan & output only report flagged findings for the file
	clamscan \
	  --no-summary \
	  --alert-broken \
	  --alert-encrypted \
	  --alert-macros \
	  --alert-exceeds-max \
	  "${SCANTARGET}" \
	    | grep "^${SCANTARGET}"
	set +o xtrace
	# set -o errexit

}

scan_and_upload() {

  local FILE="$1"
  >&2 printf "\n\nProcessing file: %s\n" "${FILE}"

  KEY="${S3_UPLOAD_BASE_PATH#/}/$(basename "$FILE")"
  SOURCE="$FILE"

  MIMETYPE=$(file --mime-type "$FILE")
  MIMETYPE="${MIMETYPE#*: }"
  case "${MIMETYPE}" in
    application/x-rpm)
      SCANTARGET="${FILE%.rpm}.cpio"
      rpm2cpio "$FILE" > "${SCANTARGET}"
      ;;
    *)
      SCANTARGET="$FILE"
      ;;
  esac
  ETAG=$(etag "$FILE")
  MD5SUM=$(md5sum "$FILE")
  MD5SUM="${MD5SUM%% *}"
  SHA1SUM=$(sha1sum "$FILE")
  SHA1SUM="${SHA1SUM%% *}"
  SHA256SUM=$(sha256sum "$FILE")
  SHA256SUM="${SHA256SUM%% *}"
  SHA512SUM=$(sha512sum "$FILE")
  SHA512SUM="${SHA512SUM%% *}"
  COMPRESSED=$(iscompressed)
  ARCHIVE=$(isarchive)
  # ORIGIN=$(origin "$FILE")
  CLASSIFICATION=$(dlpscan)
  case "$CLASSIFICATION" in
    '') DLPSTATUS="OK" ;;
    *) DLPSTATUS="HIT(S) IDENTIFIED" ;;
  esac
  AVSTATUS=$(avscan)
  AVSTATUS="${AVSTATUS#*: }"

  printf "=======\nFILE: %s\nArchive: %s\nCompress: %s\nMIMETYPE: %s\nClassification: %s\nAVDBDATE: %s\nAVSTATUS: %s\nETAG: %s\nmd5: %s\nsha1: %s\nsha256: %s\nsha512: %s\n" \
    "$FILE" \
    "${ARCHIVE:-false}" \
    "${COMPRESSED:-false}" \
    "${MIMETYPE}" \
    "${CLASSIFICATION}" \
    "${AVDBDATE}" \
    "${AVSTATUS}" \
    "${ETAG}" \
    "${MD5SUM}" \
    "${SHA1SUM}" \
    "${SHA256SUM}" \
    "${SHA512SUM}"
}

###
##### Actual Run
###

# used in scan function
AVDBDATE=$(stat -c '%y' /var/lib/clamav/daily.cvd)
AVDBDATE=${AVDBDATE%% *}

find "$SCAN_DIR_OR_FILE" -type f | grep -vE '/manifest.json$' | while IFS=$'\n' read -r FILEPATH; do
  scan_and_upload "$FILEPATH"
done

>&2 printf "\n\n[INFO] Finished processing main corpus of files\n"
