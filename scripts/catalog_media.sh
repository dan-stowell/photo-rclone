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

log_status() {
  local msg="$1"
  printf "%s %s\n" "$(date +%Y-%m-%dT%H:%M:%S%z)" "$msg" >&2
}

run_listing() {
  local label="$1"
  local tmp_file="$2"
  local log_file="$3"
  shift 3
  local -a cmd=( "$@" )

  log_status "START listing ${label}: ${cmd[*]}"
  local start_ts
  start_ts="$(date +%s)"

  "${cmd[@]}" > "$tmp_file" 2>> "$log_file" &
  local pid=$!

  while kill -0 "$pid" 2>/dev/null; do
    local now elapsed bytes
    now="$(date +%s)"
    elapsed=$((now - start_ts))
    bytes=0
    if [[ -f "$tmp_file" ]]; then
      bytes="$(stat -f %z "$tmp_file" 2>/dev/null || echo 0)"
    fi
    log_status "RUN listing ${label} elapsed=${elapsed}s bytes=${bytes}"
    sleep 60
  done

  wait "$pid"
  local rc=$?
  if [[ $rc -ne 0 ]]; then
    log_status "ERROR listing ${label} exit=${rc}"
    return "$rc"
  fi
  log_status "DONE listing ${label}"
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

RCLONE_OPTS_STR="${RCLONE_OPTS:---fast-list --stats-one-line --stats=10s}"
IFS=' ' read -r -a RCLONE_OPTS_ARR <<< "$RCLONE_OPTS_STR"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"

mkdir -p "$OUT_DIR/raw" "$OUT_DIR/logs"

record_run_id() {
  printf "%s\n" "$RUN_ID" > "$OUT_DIR/last_run_id"
}

run_one() {
  local source="$1"
  local remote="$2"
  local log_file="$OUT_DIR/logs/${source}_${RUN_ID}.rclone.log"
  local done_file="$OUT_DIR/raw/${source}_${RUN_ID}.done_dirs"
  local topdirs_file="$OUT_DIR/raw/${source}_${RUN_ID}.topdirs"

  if [[ -f "$topdirs_file" && "$FORCE" != "true" ]]; then
    echo "Reusing existing dir list: $topdirs_file"
  else
    echo "Listing dirs (depth 2) for ${source} (${remote})..."
    local tmp_dirs="${topdirs_file}.tmp"
    rclone lsf --dirs-only --max-depth 2 "${remote}" > "$tmp_dirs" 2>> "$log_file"
    mv "$tmp_dirs" "$topdirs_file"
  fi

  TOP_DIRS=()
  if [[ -f "$topdirs_file" ]]; then
    while IFS= read -r line; do
      [[ -z "$line" ]] && continue
      # Prefer second-level dirs like "seagate/Photos/"
      if [[ "$line" == */*/* ]]; then
        TOP_DIRS+=("$line")
      fi
    done < "$topdirs_file"
  fi

  if [[ ${#TOP_DIRS[@]} -eq 0 && -f "$topdirs_file" ]]; then
    # Fallback to top-level dirs if no second-level dirs exist.
    while IFS= read -r line; do
      [[ -z "$line" ]] && continue
      if [[ "$line" == */ && "$line" != */*/* ]]; then
        TOP_DIRS+=("$line")
      fi
    done < "$topdirs_file"
  fi

  # Always process root-level files separately (files not in any top-level dir).
  local root_chunk="__root__"
  process_chunk "$source" "$remote" "$root_chunk" "$done_file" "$log_file"

  if [[ ${#TOP_DIRS[@]} -eq 0 ]]; then
    echo "No top-level dirs found; root listing should cover all objects."
  else
    for dir in "${TOP_DIRS[@]}"; do
      process_chunk "$source" "$remote" "$dir" "$done_file" "$log_file"
    done
  fi
}

process_chunk() {
  local source="$1"
  local remote="$2"
  local dir="$3"
  local done_file="$4"
  local log_file="$5"

  if [[ -f "$done_file" ]] && grep -Fqx -- "$dir" "$done_file"; then
    echo "Skipping already completed chunk: $dir"
    return 0
  fi

  local raw_suffix
  if [[ "$dir" == "__root__" ]]; then
    raw_suffix="root"
  else
    raw_suffix="${dir%/}"
    raw_suffix="${raw_suffix//\//__}"
    raw_suffix="${raw_suffix:-root}"
  fi

  local raw_file="$OUT_DIR/raw/${source}_${RUN_ID}.${raw_suffix}.lsl"
  local rclone_cmd

  if [[ "$dir" == "__root__" ]]; then
    rclone_cmd="rclone lsl ${RCLONE_OPTS_STR} --max-depth 1 ${remote}"
    if [[ -f "$raw_file" && "$FORCE" != "true" ]]; then
      echo "Reusing existing raw listing: $raw_file"
      python3 scripts/catalog_media.py \
        --db "$OUT_DIR/media_catalog.sqlite" \
        --run-id "$RUN_ID" \
        --source "$source" \
        --remote "$remote" \
        --raw-file "$raw_file" \
        --rclone-command "$rclone_cmd" \
        --chunk-name "$dir" \
        --chunk-status listed
    else
      echo "Listing ${source} root (${remote})..."
      local tmp_file="${raw_file}.tmp"
      python3 scripts/catalog_media.py \
        --db "$OUT_DIR/media_catalog.sqlite" \
        --run-id "$RUN_ID" \
        --source "$source" \
        --remote "$remote" \
        --raw-file "$raw_file" \
        --rclone-command "$rclone_cmd" \
        --chunk-name "$dir" \
        --chunk-status listing
      run_listing "${source} root" "$tmp_file" "$log_file" \
        rclone lsl "${RCLONE_OPTS_ARR[@]}" --max-depth 1 "${remote}"
      mv "$tmp_file" "$raw_file"
      python3 scripts/catalog_media.py \
        --db "$OUT_DIR/media_catalog.sqlite" \
        --run-id "$RUN_ID" \
        --source "$source" \
        --remote "$remote" \
        --raw-file "$raw_file" \
        --rclone-command "$rclone_cmd" \
        --chunk-name "$dir" \
        --chunk-status listed
    fi
  else
    rclone_cmd="rclone lsl ${RCLONE_OPTS_STR} ${remote}${dir}"
    if [[ -f "$raw_file" && "$FORCE" != "true" ]]; then
      echo "Reusing existing raw listing: $raw_file"
      python3 scripts/catalog_media.py \
        --db "$OUT_DIR/media_catalog.sqlite" \
        --run-id "$RUN_ID" \
        --source "$source" \
        --remote "$remote" \
        --raw-file "$raw_file" \
        --rclone-command "$rclone_cmd" \
        --chunk-name "$dir" \
        --chunk-status listed
    else
      echo "Listing ${source} dir ${dir}..."
      local tmp_file="${raw_file}.tmp"
      python3 scripts/catalog_media.py \
        --db "$OUT_DIR/media_catalog.sqlite" \
        --run-id "$RUN_ID" \
        --source "$source" \
        --remote "$remote" \
        --raw-file "$raw_file" \
        --rclone-command "$rclone_cmd" \
        --chunk-name "$dir" \
        --chunk-status listing
      run_listing "${source} ${dir}" "$tmp_file" "$log_file" \
        rclone lsl "${RCLONE_OPTS_ARR[@]}" "${remote}${dir}"
      mv "$tmp_file" "$raw_file"
      python3 scripts/catalog_media.py \
        --db "$OUT_DIR/media_catalog.sqlite" \
        --run-id "$RUN_ID" \
        --source "$source" \
        --remote "$remote" \
        --raw-file "$raw_file" \
        --rclone-command "$rclone_cmd" \
        --chunk-name "$dir" \
        --chunk-status listed
    fi
  fi

  echo "Ingesting ${source} chunk ${dir} into SQLite..."
  python3 scripts/catalog_media.py \
    --db "$OUT_DIR/media_catalog.sqlite" \
    --run-id "$RUN_ID" \
    --source "$source" \
    --remote "$remote" \
    --raw-file "$raw_file" \
    --rclone-command "$rclone_cmd"

  python3 scripts/catalog_media.py \
    --db "$OUT_DIR/media_catalog.sqlite" \
    --run-id "$RUN_ID" \
    --source "$source" \
    --remote "$remote" \
    --raw-file "$raw_file" \
    --rclone-command "$rclone_cmd" \
    --chunk-name "$dir" \
    --chunk-status ingested

  printf "%s\n" "$dir" >> "$done_file"
}

if [[ -n "$WASABI_REMOTE" ]]; then
  run_one "wasabi" "$WASABI_REMOTE"
fi

if [[ -n "$GDRIVE_REMOTE" ]]; then
  run_one "gdrive" "$GDRIVE_REMOTE"
fi

record_run_id
echo "Done. Run ID: $RUN_ID"
