#!/usr/bin/env bash

# usage: ./download-gtfs.sh list.json output/

set -euo pipefail

JSON_FILE="$1"
DEST="$2"
PARALLEL="${3:-4}"

mkdir -p "$DEST"
LOG_FILE="$DEST/.log"

download_entry() {
    local name="$1"
    local url="$2"
    local output="$3"
    local dest="$4"
    local log_file="$5"

    local target="$dest/$output"
    local temp="$target.tmp"

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] START: $name" >> "$log_file"

    if curl -L --fail -sS "$url" -o "$temp"; then
        mv "$temp" "$target"
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] OK: $name -> $output" >> "$log_file"
    else
        rm -f "$temp"
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] FAIL: $name ($url)" >> "$log_file"
    fi
}

export -f download_entry
export DEST LOG_FILE

jq -r -c '.[] | [.name, .url, .output] | @tsv' "$JSON_FILE" \
    | tr '\n' '\0' \
    | xargs -0 -P "$PARALLEL" -I{} bash -c '
        IFS=$'\''\t'\'' read -r name url output <<< "{}"
        download_entry "$name" "$url" "$output" "$DEST" "$LOG_FILE"
    '

echo "Done. Log saved to $LOG_FILE"