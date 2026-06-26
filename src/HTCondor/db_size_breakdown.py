#!/usr/bin/env python3
"""
Breaks down the HTCondor Archive Librarian SQLite database size by table and index.

Usage:
    python3 db_size_breakdown.py [--db PATH] [--show-indexes] [--sort {size,name}]

If --db is not provided, falls back to reading LIBRARIAN_DATABASE from HTCondor config
via `condor_config_val`.
"""

import argparse
import os
import sqlite3
import subprocess
import sys


def human_readable(n_bytes):
    for unit in ("B", "KB", "MB", "GB"):
        if n_bytes < 1024:
            return f"{n_bytes:.1f} {unit}"
        n_bytes /= 1024
    return f"{n_bytes:.1f} TB"


def get_db_path_from_condor():
    try:
        result = subprocess.run(
            ["condor_config_val", "LIBRARIAN_DATABASE"],
            capture_output=True, text=True, timeout=5
        )
        path = result.stdout.strip()
        if result.returncode == 0 and path:
            return path
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None


def get_size_breakdown(db_path, show_indexes):
    if not os.path.exists(db_path):
        print(f"Error: database file not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    try:
        # Check that dbstat is available (compiled in by default since SQLite 3.8.1)
        conn.execute("SELECT 1 FROM dbstat LIMIT 1")
    except sqlite3.OperationalError:
        print("Error: SQLite dbstat virtual table is not available in this build.", file=sys.stderr)
        sys.exit(1)

    # Get all user-defined tables (excludes sqlite_* internal tables)
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    }

    # Per-entry size from dbstat: SUM(pgsize) covers every b-tree page used by that object.
    rows = conn.execute(
        "SELECT name, SUM(pgsize) FROM dbstat GROUP BY name ORDER BY SUM(pgsize) DESC"
    ).fetchall()

    # Split into tables vs indexes
    table_sizes = {}
    index_sizes = {}
    for name, size in rows:
        if name in tables:
            table_sizes[name] = size or 0
        elif not name.startswith("sqlite_"):
            index_sizes[name] = size or 0

    conn.close()

    total_db_bytes = os.path.getsize(db_path)
    accounted_bytes = sum(table_sizes.values()) + sum(index_sizes.values())

    return table_sizes, index_sizes, total_db_bytes, accounted_bytes


def print_report(db_path, table_sizes, index_sizes, total_db_bytes, accounted_bytes, sort_by, show_indexes):
    print(f"\nDatabase: {db_path}")
    print(f"File size: {human_readable(total_db_bytes)}")
    print()

    col_w = max((len(n) for n in {**table_sizes, **index_sizes}), default=10) + 2

    def print_section(label, data):
        section_total = sum(data.values())
        if not data:
            return

        sorted_items = sorted(data.items(), key=lambda x: (-x[1] if sort_by == "size" else x[0]))

        print(f"{'─' * (col_w + 30)}")
        print(f"  {label}")
        print(f"{'─' * (col_w + 30)}")
        print(f"  {'Name':<{col_w}} {'Size':>10}   {'% of DB':>8}   {'% of Section':>13}")
        print(f"  {'-'*col_w} {'-'*10}   {'-'*8}   {'-'*13}")

        for name, size in sorted_items:
            pct_db = (size / total_db_bytes * 100) if total_db_bytes else 0
            pct_sec = (size / section_total * 100) if section_total else 0
            print(f"  {name:<{col_w}} {human_readable(size):>10}   {pct_db:>7.1f}%   {pct_sec:>12.1f}%")

        print(f"  {'─'*col_w} {'─'*10}")
        sec_pct = (section_total / total_db_bytes * 100) if total_db_bytes else 0
        print(f"  {'TOTAL':<{col_w}} {human_readable(section_total):>10}   {sec_pct:>7.1f}%")
        print()

    print_section("TABLES", table_sizes)
    if show_indexes:
        print_section("INDEXES", index_sizes)
    else:
        idx_total = sum(index_sizes.values())
        idx_pct = (idx_total / total_db_bytes * 100) if total_db_bytes else 0
        print(f"  Indexes (combined):  {human_readable(idx_total)}  ({idx_pct:.1f}% of DB)  — use --show-indexes to break down")
        print()

    overhead = total_db_bytes - accounted_bytes
    if overhead > 0:
        ovh_pct = (overhead / total_db_bytes * 100) if total_db_bytes else 0
        print(f"  Overhead / free pages: {human_readable(overhead)} ({ovh_pct:.1f}%)")
        print()


def main():
    parser = argparse.ArgumentParser(description="Librarian DB size breakdown by table")
    parser.add_argument("--db", metavar="PATH", help="Path to the librarian SQLite database")
    parser.add_argument("--show-indexes", action="store_true", help="Show per-index size breakdown")
    parser.add_argument("--sort", choices=("size", "name"), default="size", help="Sort order (default: size)")
    args = parser.parse_args()

    db_path = args.db
    if not db_path:
        db_path = get_db_path_from_condor()
        if db_path:
            print(f"[condor_config_val] LIBRARIAN_DATABASE = {db_path}")
        else:
            print("Error: --db not provided and LIBRARIAN_DATABASE could not be read from HTCondor config.", file=sys.stderr)
            parser.print_usage(sys.stderr)
            sys.exit(1)

    table_sizes, index_sizes, total_db_bytes, accounted_bytes = get_size_breakdown(db_path, args.show_indexes)
    print_report(db_path, table_sizes, index_sizes, total_db_bytes, accounted_bytes, args.sort, args.show_indexes)


if __name__ == "__main__":
    main()
