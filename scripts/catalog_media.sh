#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

usage() {
  cat <<'USAGE'
Usage:
  scripts/catalog_media.sh --wasabi REMOTE: --gdrive REMOTE: [options]

Options:
  --out DIR           Output directory (default: ./catalog)
  --run-id ID         Resume or tag a run with a specific ID (default: UTC timestamp)
  --force             Re-run rclone listing even if raw file exists
  --help              Show this help

Environment:
  RCLONE_OPTS="..."   Extra rclone flags (default: "--fast-list --stats-one-line --stats=10s")

Examples:
  scripts/catalog_media.sh --wasabi wasabi: --gdrive gdrive:
  scripts/catalog_media.sh --wasabi wasabi: --out /Users/dans/photo-rclone/catalog
  scripts/catalog_media.sh --wasabi wasabi: --gdrive gdrive: --run-id 20260121T120000Z
USAGE
}

WASABI_REMOTE=""
GDRIVE_REMOTE=""
OUT_DIR="./catalog"
RUN_ID=""
FORCE="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --wasabi)
      WASABI_REMOTE="${2:-}"; shift 2 ;;
    --gdrive)
      GDRIVE_REMOTE="${2:-}"; shift 2 ;;
    --out)
      OUT_DIR="${2:-}"; shift 2 ;;
    --run-id)
      RUN_ID="${2:-}"; shift 2 ;;
    --force)
      FORCE="true"; shift ;;
    --help|-h)
      usage; exit 0 ;;
    *)
      echo "Unknown arg: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "$WASABI_REMOTE" && -z "$GDRIVE_REMOTE" ]]; then
  echo "Provide at least one remote via --wasabi or --gdrive." >&2
  usage
  exit 1
fi

if ! command -v rclone >/dev/null 2>&1; then
  echo "rclone not found in PATH." >&2
  exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 not found in PATH." >&2
  exit 1
fi

RCLONE_OPTS="${RCLONE_OPTS:---fast-list --stats-one-line --stats=10s}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"

mkdir -p "$OUT_DIR/raw" "$OUT_DIR/logs"

record_run_id() {
  printf "%s\n" "$RUN_ID" > "$OUT_DIR/last_run_id"
}

run_one() {
  local source="$1"
  local remote="$2"
  local raw_file="$OUT_DIR/raw/${source}_${RUN_ID}.lsl"
  local log_file="$OUT_DIR/logs/${source}_${RUN_ID}.rclone.log"
  local rclone_cmd
  rclone_cmd="rclone lsl ${RCLONE_OPTS} ${remote}"

  if [[ -f "$raw_file" && "$FORCE" != "true" ]]; then
    echo "Reusing existing raw listing: $raw_file"
  else
    echo "Listing ${source} (${remote})..."
    local tmp_file="${raw_file}.tmp"
    rclone lsl ${RCLONE_OPTS} "${remote}" > "$tmp_file" 2> "$log_file"
    mv "$tmp_file" "$raw_file"
  fi

  echo "Ingesting ${source} into SQLite..."
  python3 scripts/catalog_media.py \
    --db "$OUT_DIR/media_catalog.sqlite" \
    --run-id "$RUN_ID" \
    --source "$source" \
    --remote "$remote" \
    --raw-file "$raw_file" \
    --rclone-command "$rclone_cmd"
}

if [[ -n "$WASABI_REMOTE" ]]; then
  run_one "wasabi" "$WASABI_REMOTE"
fi

if [[ -n "$GDRIVE_REMOTE" ]]; then
  run_one "gdrive" "$GDRIVE_REMOTE"
fi

record_run_id
echo "Done. Run ID: $RUN_ID"
