#!/usr/bin/env bash
# =============================================================================
# download_data.sh  –  Download NASA HTTP Web Server Log Files
# DAS 839 – NoSQL Systems | Multi-Pipeline ETL
#
# Downloads the official NASA Kennedy Space Center HTTP log traces from:
#   https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html
#
# Jul95 : ~20 MB compressed  → ~182 MB uncompressed  → ~1,891,715 records
# Aug95 : ~21 MB compressed  → ~167 MB uncompressed  → ~1,569,898 records
# Combined                                            → ~3,461,613 records
#
# Usage:
#   ./scripts/download_data.sh [--data-dir /path/to/dir] [--jul-only] [--aug-only]
# =============================================================================

set -euo pipefail

# Defaults
DATA_DIR="${HOME}/data/nasa"
DOWNLOAD_JUL=true
DOWNLOAD_AUG=true

JUL_URL="https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz"
AUG_URL="https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz"

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --data-dir)  DATA_DIR="$2"; shift 2 ;;
        --jul-only)  DOWNLOAD_AUG=false; shift ;;
        --aug-only)  DOWNLOAD_JUL=false; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

mkdir -p "${DATA_DIR}"
echo "Download directory: ${DATA_DIR}"

# ---------------------------------------------------------------------------
# Helper: download + decompress one file
# ---------------------------------------------------------------------------
download_and_extract() {
    local url="$1"
    local gz_file="${DATA_DIR}/$(basename "${url}")"
    local log_file="${gz_file%.gz}"

    if [[ -f "${log_file}" ]]; then
        local lines
        lines=$(wc -l < "${log_file}")
        echo "[SKIP] ${log_file} already exists (${lines} lines)"
        return
    fi

    echo "Downloading $(basename "${url}") …"
    if command -v wget &>/dev/null; then
        wget -q --show-progress -O "${gz_file}" "${url}"
    elif command -v curl &>/dev/null; then
        curl -L --progress-bar -o "${gz_file}" "${url}"
    else
        echo "[ERROR] Neither wget nor curl found. Install one and retry."
        exit 1
    fi

    echo "Decompressing $(basename "${gz_file}") …"
    gunzip -f "${gz_file}"           # leaves NASA_access_log_Jul95 / _Aug95

    local lines
    lines=$(wc -l < "${log_file}")
    echo "[OK] ${log_file}  →  ${lines} records"
}

# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------
[[ "${DOWNLOAD_JUL}" == "true" ]] && download_and_extract "${JUL_URL}"
[[ "${DOWNLOAD_AUG}" == "true" ]] && download_and_extract "${AUG_URL}"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "=== Dataset Summary ==="
total=0
for f in "${DATA_DIR}"/NASA_access_log_*; do
    [[ -f "$f" ]] || continue
    lines=$(wc -l < "$f")
    size=$(du -h "$f" | cut -f1)
    printf "  %-40s  %8s lines  %s\n" "$(basename "$f")" "$(printf '%d' $lines | sed ':a;s/\B[0-9]\{3\}\>/,&/;ta')" "${size}"
    total=$((total + lines))
done
printf "  %-40s  %8s lines total\n" "COMBINED" "$(printf '%d' $total | sed ':a;s/\B[0-9]\{3\}\>/,&/;ta')"
echo ""
echo "Batch size set to 500,000 → approx $((total / 500000 + 1)) batches"
echo "Data ready in: ${DATA_DIR}"
