#!/usr/bin/env bash
set -euo pipefail

# Fetches real NYC TLC data into this repo's gitignored data/ folder, laid
# out the same way as sample_data/ (a trips/ subfolder of parquet files, plus
# a top-level taxi_zone_lookup.csv) so iteration scripts can read either one
# via --data-dir.
#
# Usage: ./download_data.sh YYYY-MM
#
# TLC's landing page (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
# links to files served from this fixed CloudFront distribution, so the URL
# pattern below is stable across months without visiting the page each time.

YEAR_MONTH="${1:?Usage: ./download_data.sh YYYY-MM}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
DATA_DIR="$REPO_ROOT/data"
TLC_BASE_URL="https://d37ci6vzurychx.cloudfront.net"
TRIP_DATA_URL="${TLC_BASE_URL}/trip-data/yellow_tripdata_${YEAR_MONTH}.parquet"
ZONE_LOOKUP_URL="${TLC_BASE_URL}/misc/taxi_zone_lookup.csv"

mkdir -p "$DATA_DIR/trips"

# The zone lookup file always exists, so it doubles as a health check for
# TLC_BASE_URL itself -- if even that 404s, the CloudFront URL has likely
# changed, as opposed to a single month just not being published yet.
base_url_is_reachable() {
    local code
    code=$(curl -sS -o /dev/null -w "%{http_code}" -I "$ZONE_LOOKUP_URL") || true
    [ "$code" = "200" ]
}

fetch() {
    local url="$1" out="$2" label="$3" code
    # `|| true` stops set -e from aborting on a curl transport failure (DNS,
    # connection refused, etc) before the HTTP-status check below can run.
    code=$(curl -sS -L -o "$out" -w "%{http_code}" "$url") || true
    if [ "$code" != "200" ]; then
        rm -f "$out"
        echo "Failed to download $label (HTTP $code): $url" >&2
        if ! base_url_is_reachable; then
            echo "TLC_BASE_URL ($TLC_BASE_URL) looks unreachable -- it may have changed." >&2
            echo "Check the TLC page and update TLC_BASE_URL in this script:" >&2
            echo "  https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page" >&2
        fi
        exit 1
    fi
}

fetch "$TRIP_DATA_URL" "$DATA_DIR/trips/yellow_tripdata_${YEAR_MONTH}.parquet" "trip data for $YEAR_MONTH"
fetch "$ZONE_LOOKUP_URL" "$DATA_DIR/taxi_zone_lookup.csv" "zone lookup"

echo "Downloaded to $DATA_DIR"
