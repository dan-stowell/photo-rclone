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
  --hash-partitions N Split directories into N hash-filter partitions (default: 1)
  --max-parallel N    Maximum parallel operations (default: 4)
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

is_chunk_done() {
  local db="$1"
  local run_id="$2"
  local source="$3"
  local chunk_name="$4"

  python3 -c "
import sqlite3, sys
conn = sqlite3.connect('$db')
row = conn.execute(
    'SELECT status FROM chunks WHERE run_id=? AND source=? AND chunk_name=?',
    ('$run_id', '$source', '$chunk_name')
).fetchone()
sys.exit(0 if row and row[0] == 'ingested' else 1)
"
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
HASH_PARTITIONS=1
MAX_PARALLEL=4

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
    --hash-partitions)
      HASH_PARTITIONS="${2:-}"; shift 2 ;;
    --max-parallel)
      MAX_PARALLEL="${2:-}"; shift 2 ;;
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

# Parallel job control (bash 3.2 compatible - no wait -n)
PIDS=()

wait_for_slot() {
  while [[ ${#PIDS[@]} -ge $MAX_PARALLEL ]]; do
    local new_pids=()
    for pid in "${PIDS[@]}"; do
      if kill -0 "$pid" 2>/dev/null; then
        new_pids+=("$pid")
      fi
    done
    if [[ ${#new_pids[@]} -gt 0 ]]; then
      PIDS=("${new_pids[@]}")
    else
      PIDS=()
    fi
    if [[ ${#PIDS[@]} -ge $MAX_PARALLEL ]]; then
      sleep 1
    fi
  done
}

wait_all_pids() {
  local rc=0
  for pid in "${PIDS[@]}"; do
    wait "$pid" || rc=$?
  done
  PIDS=()
  return $rc
}

run_one() {
  local source="$1"
  local remote="$2"
  local log_file="$OUT_DIR/logs/${source}_${RUN_ID}.rclone.log"
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
  if [[ $HASH_PARTITIONS -gt 1 ]]; then
    for k in $(seq 0 $((HASH_PARTITIONS - 1))); do
      wait_for_slot
      process_hash_chunk "$source" "$remote" "$root_chunk" "$k" "$HASH_PARTITIONS" "$log_file" &
      PIDS+=($!)
    done
    wait_all_pids
  else
    process_chunk "$source" "$remote" "$root_chunk" "$log_file"
  fi

  if [[ ${#TOP_DIRS[@]} -eq 0 ]]; then
    echo "No top-level dirs found; root listing should cover all objects."
  else
    for dir in "${TOP_DIRS[@]}"; do
      if [[ $HASH_PARTITIONS -gt 1 ]]; then
        for k in $(seq 0 $((HASH_PARTITIONS - 1))); do
          wait_for_slot
          process_hash_chunk "$source" "$remote" "$dir" "$k" "$HASH_PARTITIONS" "$log_file" &
          PIDS+=($!)
        done
        wait_all_pids
      else
        process_chunk "$source" "$remote" "$dir" "$log_file"
      fi
    done
  fi
}

process_chunk() {
  local source="$1"
  local remote="$2"
  local dir="$3"
  local log_file="$4"

  if is_chunk_done "$OUT_DIR/media_catalog.sqlite" "$RUN_ID" "$source" "$dir"; then
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
      if ! run_listing "${source} root" "$tmp_file" "$log_file" \
        rclone lsl "${RCLONE_OPTS_ARR[@]}" --max-depth 1 "${remote}"; then
        python3 scripts/catalog_media.py \
          --db "$OUT_DIR/media_catalog.sqlite" \
          --run-id "$RUN_ID" \
          --source "$source" \
          --remote "$remote" \
          --raw-file "$raw_file" \
          --rclone-command "$rclone_cmd" \
          --chunk-name "$dir" \
          --chunk-status error
        return 1
      fi
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
      if ! run_listing "${source} ${dir}" "$tmp_file" "$log_file" \
        rclone lsl "${RCLONE_OPTS_ARR[@]}" "${remote}${dir}"; then
        python3 scripts/catalog_media.py \
          --db "$OUT_DIR/media_catalog.sqlite" \
          --run-id "$RUN_ID" \
          --source "$source" \
          --remote "$remote" \
          --raw-file "$raw_file" \
          --rclone-command "$rclone_cmd" \
          --chunk-name "$dir" \
          --chunk-status error
        return 1
      fi
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
}

process_hash_chunk() {
  local source="$1"
  local remote="$2"
  local dir="$3"
  local partition="$4"
  local total_partitions="$5"
  local log_file="$6"

  local chunk_name="${dir}#${partition}/${total_partitions}"

  if is_chunk_done "$OUT_DIR/media_catalog.sqlite" "$RUN_ID" "$source" "$chunk_name"; then
    echo "Skipping already completed hash chunk: $chunk_name"
    return 0
  fi

  local raw_suffix
  if [[ "$dir" == "__root__" ]]; then
    raw_suffix="root.hash-${partition}of${total_partitions}"
  else
    raw_suffix="${dir%/}"
    raw_suffix="${raw_suffix//\//__}"
    raw_suffix="${raw_suffix:-root}.hash-${partition}of${total_partitions}"
  fi

  local raw_file="$OUT_DIR/raw/${source}_${RUN_ID}.${raw_suffix}.lsl"
  local rclone_cmd

  if [[ "$dir" == "__root__" ]]; then
    rclone_cmd="rclone lsl ${RCLONE_OPTS_STR} --hash-filter ${partition}/${total_partitions} --max-depth 1 ${remote}"
    if [[ -f "$raw_file" && "$FORCE" != "true" ]]; then
      echo "Reusing existing raw listing: $raw_file"
      python3 scripts/catalog_media.py \
        --db "$OUT_DIR/media_catalog.sqlite" \
        --run-id "$RUN_ID" \
        --source "$source" \
        --remote "$remote" \
        --raw-file "$raw_file" \
        --rclone-command "$rclone_cmd" \
        --chunk-name "$chunk_name" \
        --chunk-status listed
    else
      echo "Listing ${source} root hash partition ${partition}/${total_partitions}..."
      local tmp_file="${raw_file}.tmp"
      python3 scripts/catalog_media.py \
        --db "$OUT_DIR/media_catalog.sqlite" \
        --run-id "$RUN_ID" \
        --source "$source" \
        --remote "$remote" \
        --raw-file "$raw_file" \
        --rclone-command "$rclone_cmd" \
        --chunk-name "$chunk_name" \
        --chunk-status listing
      if ! run_listing "${source} root #${partition}/${total_partitions}" "$tmp_file" "$log_file" \
        rclone lsl "${RCLONE_OPTS_ARR[@]}" --hash-filter "${partition}/${total_partitions}" --max-depth 1 "${remote}"; then
        python3 scripts/catalog_media.py \
          --db "$OUT_DIR/media_catalog.sqlite" \
          --run-id "$RUN_ID" \
          --source "$source" \
          --remote "$remote" \
          --raw-file "$raw_file" \
          --rclone-command "$rclone_cmd" \
          --chunk-name "$chunk_name" \
          --chunk-status error
        return 1
      fi
      mv "$tmp_file" "$raw_file"
      python3 scripts/catalog_media.py \
        --db "$OUT_DIR/media_catalog.sqlite" \
        --run-id "$RUN_ID" \
        --source "$source" \
        --remote "$remote" \
        --raw-file "$raw_file" \
        --rclone-command "$rclone_cmd" \
        --chunk-name "$chunk_name" \
        --chunk-status listed
    fi
  else
    rclone_cmd="rclone lsl ${RCLONE_OPTS_STR} --hash-filter ${partition}/${total_partitions} ${remote}${dir}"
    if [[ -f "$raw_file" && "$FORCE" != "true" ]]; then
      echo "Reusing existing raw listing: $raw_file"
      python3 scripts/catalog_media.py \
        --db "$OUT_DIR/media_catalog.sqlite" \
        --run-id "$RUN_ID" \
        --source "$source" \
        --remote "$remote" \
        --raw-file "$raw_file" \
        --rclone-command "$rclone_cmd" \
        --chunk-name "$chunk_name" \
        --chunk-status listed
    else
      echo "Listing ${source} ${dir} hash partition ${partition}/${total_partitions}..."
      local tmp_file="${raw_file}.tmp"
      python3 scripts/catalog_media.py \
        --db "$OUT_DIR/media_catalog.sqlite" \
        --run-id "$RUN_ID" \
        --source "$source" \
        --remote "$remote" \
        --raw-file "$raw_file" \
        --rclone-command "$rclone_cmd" \
        --chunk-name "$chunk_name" \
        --chunk-status listing
      if ! run_listing "${source} ${dir} #${partition}/${total_partitions}" "$tmp_file" "$log_file" \
        rclone lsl "${RCLONE_OPTS_ARR[@]}" --hash-filter "${partition}/${total_partitions}" "${remote}${dir}"; then
        python3 scripts/catalog_media.py \
          --db "$OUT_DIR/media_catalog.sqlite" \
          --run-id "$RUN_ID" \
          --source "$source" \
          --remote "$remote" \
          --raw-file "$raw_file" \
          --rclone-command "$rclone_cmd" \
          --chunk-name "$chunk_name" \
          --chunk-status error
        return 1
      fi
      mv "$tmp_file" "$raw_file"
      python3 scripts/catalog_media.py \
        --db "$OUT_DIR/media_catalog.sqlite" \
        --run-id "$RUN_ID" \
        --source "$source" \
        --remote "$remote" \
        --raw-file "$raw_file" \
        --rclone-command "$rclone_cmd" \
        --chunk-name "$chunk_name" \
        --chunk-status listed
    fi
  fi

  echo "Ingesting ${source} hash chunk ${chunk_name} into SQLite..."
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
    --chunk-name "$chunk_name" \
    --chunk-status ingested
}

if [[ -n "$WASABI_REMOTE" ]]; then
  run_one "wasabi" "$WASABI_REMOTE"
fi

if [[ -n "$GDRIVE_REMOTE" ]]; then
  run_one "gdrive" "$GDRIVE_REMOTE"
fi

record_run_id
echo "Done. Run ID: $RUN_ID"
