#!/usr/bin/env python3
import argparse
import os
import sqlite3
import sys
from datetime import datetime, timezone

PHOTO_EXTS = {
    "jpg", "jpeg", "heic", "heif", "png", "gif", "tif", "tiff", "bmp", "webp",
    "dng", "cr2", "cr3", "nef", "arw", "raf", "orf", "rw2", "pef", "sr2",
}
VIDEO_EXTS = {
    "mp4", "mov", "m4v", "avi", "mkv", "mpg", "mpeg", "3gp", "3gpp", "mts",
    "m2ts", "ts", "wmv", "flv", "webm",
}


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument("--source", required=True)
    p.add_argument("--remote", required=True)
    p.add_argument("--raw-file", required=True)
    p.add_argument("--rclone-command", required=True)
    p.add_argument("--chunk-name")
    p.add_argument(
        "--chunk-status",
        choices=["listing", "listed", "ingested", "error"],
        help="If set, only update chunk status and exit.",
    )
    return p.parse_args()


def ensure_schema(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT NOT NULL,
            source TEXT NOT NULL,
            remote TEXT NOT NULL,
            started_at TEXT NOT NULL,
            raw_file TEXT NOT NULL,
            rclone_command TEXT NOT NULL,
            completed_at TEXT,
            PRIMARY KEY (run_id, source)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS chunks (
            run_id TEXT NOT NULL,
            source TEXT NOT NULL,
            remote TEXT NOT NULL,
            chunk_name TEXT NOT NULL,
            rclone_command TEXT NOT NULL,
            raw_file TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at TEXT,
            listed_at TEXT,
            completed_at TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (run_id, source, chunk_name)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_chunks_status ON chunks(status)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            source TEXT NOT NULL,
            remote TEXT NOT NULL,
            path TEXT NOT NULL,
            size INTEGER NOT NULL,
            modtime TEXT NOT NULL,
            ext TEXT NOT NULL,
            is_media INTEGER NOT NULL,
            media_kind TEXT NOT NULL,
            ignored_reason TEXT NOT NULL,
            UNIQUE(run_id, source, path)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_files_source_media ON files(source, is_media)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_files_path ON files(path)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_files_size ON files(size)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_files_run ON files(run_id)")
    conn.commit()


def update_chunk(conn, args, status):
    if not args.chunk_name:
        raise ValueError("chunk-name is required when updating chunk status")
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    row = conn.execute(
        """
        SELECT started_at, listed_at, completed_at
        FROM chunks
        WHERE run_id = ? AND source = ? AND chunk_name = ?
        """,
        (args.run_id, args.source, args.chunk_name),
    ).fetchone()

    started_at, listed_at, completed_at = (row or (None, None, None))
    if status == "listing" and started_at is None:
        started_at = ts
    if status == "listed":
        listed_at = ts
    if status == "ingested":
        completed_at = ts

    if row is None:
        conn.execute(
            """
            INSERT INTO chunks
            (run_id, source, remote, chunk_name, rclone_command, raw_file, status,
             started_at, listed_at, completed_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                args.run_id,
                args.source,
                args.remote,
                args.chunk_name,
                args.rclone_command,
                args.raw_file,
                status,
                started_at,
                listed_at,
                completed_at,
                ts,
            ),
        )
    else:
        conn.execute(
            """
            UPDATE chunks
            SET remote = ?, rclone_command = ?, raw_file = ?, status = ?,
                started_at = ?, listed_at = ?, completed_at = ?, updated_at = ?
            WHERE run_id = ? AND source = ? AND chunk_name = ?
            """,
            (
                args.remote,
                args.rclone_command,
                args.raw_file,
                status,
                started_at,
                listed_at,
                completed_at,
                ts,
                args.run_id,
                args.source,
                args.chunk_name,
            ),
        )
    conn.commit()


def classify_ext(path):
    base = os.path.basename(path)
    if "." not in base:
        return "", False, "other", "no_extension"
    ext = base.rsplit(".", 1)[1].lower()
    if ext in PHOTO_EXTS:
        return ext, True, "photo", ""
    if ext in VIDEO_EXTS:
        return ext, True, "video", ""
    return ext, False, "other", "non_media_extension"


def ingest(conn, args):
    started_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn.execute(
        """
        INSERT OR REPLACE INTO runs
        (run_id, source, remote, started_at, raw_file, rclone_command)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (args.run_id, args.source, args.remote, started_at, args.raw_file, args.rclone_command),
    )
    conn.commit()

    err_path = os.path.join(os.path.dirname(args.db), f"parse_errors_{args.source}_{args.run_id}.log")
    total = 0
    inserted = 0
    batch = []
    batch_size = 1000

    with open(args.raw_file, "r", encoding="utf-8", errors="replace") as f, \
            open(err_path, "a", encoding="utf-8") as err:
        for line in f:
            total += 1
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split(maxsplit=3)
            if len(parts) != 4:
                err.write(f"Unparseable line: {line}\n")
                continue
            size_str, date_str, time_str, path = parts
            try:
                size = int(size_str)
            except ValueError:
                err.write(f"Invalid size: {line}\n")
                continue
            modtime = f"{date_str}T{time_str}"
            ext, is_media, media_kind, ignored_reason = classify_ext(path)
            if not is_media:
                ignored_reason = ignored_reason or "non_media_extension"
            batch.append(
                (
                    args.run_id,
                    args.source,
                    args.remote,
                    path,
                    size,
                    modtime,
                    ext,
                    1 if is_media else 0,
                    media_kind,
                    ignored_reason,
                )
            )
            if len(batch) >= batch_size:
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO files
                    (run_id, source, remote, path, size, modtime, ext, is_media, media_kind, ignored_reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    batch,
                )
                conn.commit()
                inserted += len(batch)
                batch = []

    if batch:
        conn.executemany(
            """
            INSERT OR REPLACE INTO files
            (run_id, source, remote, path, size, modtime, ext, is_media, media_kind, ignored_reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            batch,
        )
        conn.commit()
        inserted += len(batch)

    completed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn.execute(
        "UPDATE runs SET completed_at = ? WHERE run_id = ? AND source = ?",
        (completed_at, args.run_id, args.source),
    )
    conn.commit()

    print(f"Ingested {inserted} records from {total} lines.")


def main():
    args = parse_args()
    os.makedirs(os.path.dirname(args.db), exist_ok=True)
    conn = sqlite3.connect(args.db)
    try:
        # Enable WAL mode for safe concurrent writes from parallel processes
        conn.execute("PRAGMA journal_mode=WAL")
        ensure_schema(conn)
        if args.chunk_status:
            update_chunk(conn, args, args.chunk_status)
            return
        ingest(conn, args)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
