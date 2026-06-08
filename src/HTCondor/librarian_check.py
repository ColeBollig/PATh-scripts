#!/usr/bin/env python3
"""Check the HTCondor librarian database.

Usage:
  librarian_check.py [options] [check]

Checks:
  summary    - One-line OK/WARN/CRIT health status (exits 0/1/2)
  jobs       - Indexed job counts and per-user breakdown
  users      - Recorded users and activity
  status     - Ingest health, performance, and recent cycle history
  files      - Tracked archive files and read progress
  backlog    - Current ingestion backlog per file
  recent     - N most recently completed jobs
  cluster    - All procs for a specific cluster ID
  search     - Look up a specific ClusterId.ProcId
  throughput - Jobs completed per day (bar chart)
  histogram  - Distribution of cluster sizes (jobs per cluster)
  all        - Run all checks (default)

Options:
  --db PATH              Path to library.db (default: from HTCondor config)
  --user NAME            Filter jobs/recent by username
  --since DATE           Filter recent/throughput by date (YYYY-MM-DD)
  --until DATE           Filter recent/throughput by date (YYYY-MM-DD)
  --cluster ID           Cluster ID for cluster/search checks
  --proc ID              Proc ID for search check
  --n N                  Number of jobs for recent check (default: 20)
  --days N               Number of days for throughput check (default: 30)
  --stale-threshold N    Seconds before daemon is considered stale (default: auto)
  --watch N              Re-run check every N seconds
  --format FORMAT        Output format: text (default) or json
"""

import argparse
import json
import os
import sqlite3
import sys
import threading
import time
from datetime import datetime, timedelta
import htcondor2
from htcondor2._utils.ansi import colorize, bold, Color, is_capable
from htcondor2._utils.keyboard import KBHit


# ---------------------------------------------------------------------------
# Color helpers
# ---------------------------------------------------------------------------

def _c(s, color):   return colorize(str(s), color)
def hdr(s):         return bold(_c(s, Color.BRIGHT_CYAN))
def sep(n, ch="─"): return _c(ch * n, Color.DARK_GREY)
def lbl(s):         return _c(s, Color.WHITE)
def val(s):         return _c(str(s), Color.BRIGHT_WHITE)
def uname(s):       return _c(str(s), Color.BRIGHT_MAGENTA)
def fname(s):       return _c(str(s), Color.CYAN)
def cid(s):         return _c(str(s), Color.BRIGHT_BLUE)
def ts(s):          return _c(str(s), Color.DARK_GREY)
def good(s):        return bold(_c(str(s), Color.BRIGHT_GREEN))
def caution(s):     return _c(str(s), Color.BRIGHT_YELLOW)
def bad(s):         return bold(_c(str(s), Color.BRIGHT_RED))
def note(s):        return _c(str(s), Color.YELLOW)
def dim(s):         return _c(str(s), Color.DARK_GREY)

def level_color(level):
    return {"OK": good, "WARN": caution, "CRIT": bad}.get(level, val)(level)

def health(s, is_good):
    return good(s) if is_good else bad(s)

def backlog_color(n):
    s = f"{n:,}"
    if n > 100_000: return bad(s)
    if n > 10_000:  return caution(s)
    return good(s)

def pct_color(pct, s):
    if pct >= 97: return bad(s)
    if pct >= 80: return caution(s)
    return good(s)

def file_status_color(status):
    if status == "active":           return good(status)
    if status == "rotated, reading": return caution(status)
    if status == "fully read":       return _c(status, Color.CYAN)
    return dim(status)

def display_filename(path):
    """Return the basename of a FileName stored as an absolute path in the DB."""
    if not path:
        return 'N/A'
    return os.path.basename(path) or path

def yesno(b, warn_on_yes=False):
    s = "YES" if b else "NO"
    if b:
        return caution(s) if warn_on_yes else good(s)
    return dim(s)


# ---------------------------------------------------------------------------
# Misc helpers
# ---------------------------------------------------------------------------

def fmt_time(ts_val):
    if ts_val is None or ts_val <= 0:
        return "N/A"
    if ts_val > 1e12:
        ts_val = ts_val / 1000
    return datetime.fromtimestamp(ts_val).strftime("%Y-%m-%d %H:%M:%S")


def fmt_duration(seconds):
    if seconds is None or seconds < 0:
        return "-"
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    elif seconds < 86400:
        h = seconds // 3600
        m = (seconds % 3600) // 60
        return f"{h}h {m}m"
    else:
        d = seconds // 86400
        h = (seconds % 86400) // 3600
        return f"{d}d {h}h"


def fmt_size(n):
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def parse_date(s):
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return int(datetime.strptime(s, fmt).timestamp())
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(
        f"Invalid date '{s}'. Use YYYY-MM-DD or 'YYYY-MM-DD HH:MM:SS'"
    )


def db_size_info(args):
    current = os.path.getsize(args.db)
    try:
        db_max = int(htcondor2.param.get("LIBRARIAN_MAX_DATABASE_SIZE") or 0)
    except (ValueError, TypeError):
        db_max = 0
    if db_max <= 0:
        db_max = 2 * 1024 ** 3
    pct = (current / db_max * 100) if db_max > 0 else 0.0
    return current, db_max, pct


def print_header(title, width=70):
    print(hdr("━" * width))
    print(hdr(f"  {title}"))
    print(hdr("━" * width))


def bar_chart(count, max_count, width=30):
    """Return a colored Unicode block bar scaled to width."""
    if max_count == 0:
        return dim("░" * width)
    filled = round((count / max_count) * width)
    bar    = "█" * filled + "░" * (width - filled)
    if count == 0:
        return dim(bar)
    if filled < width // 3:
        return caution(bar)
    return _c(bar, Color.CYAN)


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

def check_summary(con, args):
    sd = con.execute("SELECT * FROM StatusData WHERE StatusDataId = 1").fetchone()
    latest = con.execute("""
        SELECT JobBacklogEstimate, HitMaxIngestLimit, TimeOfUpdate
        FROM Status ORDER BY TimeOfUpdate DESC LIMIT 1
    """).fetchone()

    warnings  = []
    criticals = []
    age_sec   = None

    if not sd or not latest:
        criticals.append("no status data -- daemon may not have run yet")
    else:
        last_ts = sd["TimeOfLastUpdate"]
        if last_ts:
            if last_ts > 1e12:
                last_ts = last_ts / 1000
            age_sec = datetime.now().timestamp() - last_ts
            if age_sec > args.stale_threshold:
                criticals.append(
                    f"last update {age_sec:.0f}s ago (threshold {args.stale_threshold}s) -- daemon may be down"
                )

        backlog = latest["JobBacklogEstimate"]
        if backlog > 100_000:
            criticals.append(f"backlog {backlog:,} jobs")
        elif backlog > 10_000:
            warnings.append(f"backlog {backlog:,} jobs")

        cap_rate = sd["HitMaxIngestLimitRate"] or 0.0
        if cap_rate > 0.5:
            warnings.append(f"hitting ingest cap {cap_rate * 100:.0f}% of cycles")

        if sd["TotalRecordsLost"] and sd["TotalRecordsLost"] > 0:
            warnings.append(f"{sd['TotalRecordsLost']:,} records lost on insert failure")

        db_cur, db_max, db_pct = db_size_info(args)
        if db_pct >= 97:
            criticals.append(f"DB at {db_pct:.0f}% of max ({fmt_size(db_cur)} / {fmt_size(db_max)})")
        elif db_pct >= 80:
            warnings.append(f"DB at {db_pct:.0f}% of max ({fmt_size(db_cur)} / {fmt_size(db_max)})")

    level = "CRIT" if criticals else ("WARN" if warnings else "OK")
    msgs  = criticals + warnings
    if not msgs:
        parts = []
        if latest:
            parts.append(f"backlog {latest['JobBacklogEstimate']:,} jobs")
        if age_sec is not None:
            parts.append(f"last update {age_sec:.0f}s ago")
        msgs = parts or ["all checks passed"]
    detail = ", ".join(msgs)

    if args.format == "json":
        return {"level": level, "message": detail, "warnings": warnings, "criticals": criticals}

    print(f"{level_color(level)}: {val(detail)}")
    return level


def check_jobs(con, args):
    user_clause = "AND u.UserName = ?" if args.user else ""
    user_params = (args.user,) if args.user else ()

    row = con.execute(f"""
        SELECT COUNT(*) AS total_jobs, COUNT(DISTINCT j.ClusterId) AS total_clusters,
               MIN(CASE WHEN jr.CompletionDate > 0 THEN jr.CompletionDate END) AS earliest,
               MAX(CASE WHEN jr.CompletionDate > 0 THEN jr.CompletionDate END) AS latest
        FROM Jobs j
        LEFT JOIN JobRecords jr ON j.JobId  = jr.JobId
        LEFT JOIN Users u       ON j.UserId = u.UserId
        WHERE 1=1 {user_clause}
    """, user_params).fetchone()

    per_user = con.execute(f"""
        SELECT u.UserName, COUNT(j.JobId) AS jobs, COUNT(DISTINCT j.ClusterId) AS clusters
        FROM Users u JOIN Jobs j ON u.UserId = j.UserId
        WHERE 1=1 {user_clause}
        GROUP BY u.UserId ORDER BY jobs DESC LIMIT 20
    """, user_params).fetchall()

    recent = con.execute(f"""
        SELECT j.ClusterId, j.ProcId, u.UserName, jr.CompletionDate, f.FileName
        FROM Jobs j
        JOIN JobRecords jr ON j.JobId  = jr.JobId
        LEFT JOIN Users u  ON j.UserId = u.UserId
        LEFT JOIN Files f  ON jr.FileId = f.FileId
        WHERE 1=1 {user_clause}
        ORDER BY jr.CompletionDate DESC LIMIT 10
    """, user_params).fetchall()

    if args.format == "json":
        return {
            "total_jobs": row["total_jobs"], "total_clusters": row["total_clusters"],
            "earliest": fmt_time(row["earliest"]), "latest": fmt_time(row["latest"]),
            "per_user": [dict(r) for r in per_user],
        }

    title = "Job Index Summary"
    if args.user:
        title += f"  —  user: {args.user}"
    print_header(title)
    total_jobs_str     = f"{row['total_jobs']:,}"
    total_clusters_str = f"{row['total_clusters']:,}"
    print(f"  {lbl('Total jobs indexed :')} {val(total_jobs_str)}")
    print(f"  {lbl('Unique clusters    :')} {val(total_clusters_str)}")
    print(f"  {lbl('Earliest completion:')} {ts(fmt_time(row['earliest']))}")
    print(f"  {lbl('Latest completion  :')} {ts(fmt_time(row['latest']))}")
    print()

    col_hdr = f"{'User':<28} {'Jobs':>8} {'Clusters':>10}"
    print(f"  {bold(col_hdr)}")
    print(f"  {sep(48)}")
    for r in per_user:
        u = uname(f"{r['UserName']:<28}")
        j = val(f"{r['jobs']:>8,}")
        c = val(f"{r['clusters']:>10,}")
        print(f"  {u} {j} {c}")

    print()
    print(f"  {bold('Most Recent 10 Completed Jobs:')}")
    rec_hdr = f"{'ClusterId':>10} {'ProcId':>7}  {'User':<20} {'Completed':<22} File"
    print(f"  {bold(rec_hdr)}")
    print(f"  {sep(78)}")
    for r in recent:
        cl = cid(f"{r['ClusterId']:>10}")
        pr = cid(f"{r['ProcId']:>7}")
        u  = uname(f"{(r['UserName'] or 'N/A'):<20}")
        t  = ts(f"{fmt_time(r['CompletionDate']):<22}")
        fn = fname(display_filename(r['FileName']))
        print(f"  {cl} {pr}  {u} {t} {fn}")


def check_users(con, args):
    total = con.execute("SELECT COUNT(*) AS n FROM Users").fetchone()["n"]
    rows  = con.execute("""
        SELECT u.UserName, COUNT(j.JobId) AS jobs,
               COUNT(DISTINCT j.ClusterId) AS clusters, MAX(jr.CompletionDate) AS latest
        FROM Users u
        LEFT JOIN Jobs j        ON u.UserId = j.UserId
        LEFT JOIN JobRecords jr ON j.JobId  = jr.JobId
        GROUP BY u.UserId ORDER BY jobs DESC
    """).fetchall()

    if args.format == "json":
        return {
            "total_users": total,
            "users": [{"user": r["UserName"], "jobs": r["jobs"],
                       "clusters": r["clusters"], "latest_completion": fmt_time(r["latest"])}
                      for r in rows],
        }

    print_header("User Summary")
    print(f"  {lbl('Total users recorded:')} {val(str(total))}")
    print()
    col_hdr = f"{'User':<28} {'Jobs':>8} {'Clusters':>10}  Latest Completion"
    print(f"  {bold(col_hdr)}")
    print(f"  {sep(70)}")
    for r in rows:
        u = uname(f"{r['UserName']:<28}")
        j = val(f"{r['jobs']:>8,}")
        c = val(f"{r['clusters']:>10,}")
        t = ts(fmt_time(r['latest']))
        print(f"  {u} {j} {c}  {t}")


def check_status(con, args):
    sd = con.execute("SELECT * FROM StatusData WHERE StatusDataId = 1").fetchone()
    cycles = con.execute("""
        SELECT TimeOfUpdate, TotalRecordsRead, JobBacklogEstimate, DurationMs,
               GarbageCollectionRun, HitMaxIngestLimit, RecordsLostOnInsertFailure
        FROM Status ORDER BY TimeOfUpdate DESC LIMIT 10
    """).fetchall()
    frow = con.execute("""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN DateOfDeletion IS NULL THEN 1 ELSE 0 END) AS active,
               SUM(CASE WHEN FullyRead = 1 THEN 1 ELSE 0 END) AS done
        FROM Files
    """).fetchone()
    db_cur, db_max, db_pct = db_size_info(args)
    schema_version = con.execute("PRAGMA user_version").fetchone()[0]

    age_sec = None
    if sd and sd["TimeOfLastUpdate"]:
        last_ts = sd["TimeOfLastUpdate"]
        if last_ts > 1e12:
            last_ts = last_ts / 1000
        age_sec = datetime.now().timestamp() - last_ts

    if args.format == "json":
        return {
            "last_update": fmt_time(sd["TimeOfLastUpdate"]) if sd else None,
            "age_seconds": round(age_sec, 1) if age_sec is not None else None,
            "stale": (age_sec is not None and age_sec > args.stale_threshold),
            "total_cycles": sd["TotalCycles"] if sd else None,
            "total_ads_ingested": sd["TotalAdsIngested"] if sd else None,
            "total_records_lost": sd["TotalRecordsLost"] if sd else None,
            "avg_jobs_per_cycle": round(sd["AvgAdsIngestedPerCycle"], 1) if sd else None,
            "avg_cycle_ms": round(sd["AvgIngestDurationMs"], 1) if sd else None,
            "ingest_hz": round(sd["MeanIngestHz"], 2) if sd else None,
            "arrival_hz": round(sd["MeanArrivalHz"], 2) if sd else None,
            "avg_backlog": round(sd["MeanBacklogEstimate"]) if sd else None,
            "schema_version": schema_version,
            "db_size": db_cur, "db_max": db_max, "db_pct": round(db_pct, 1),
            "archive_files": {"total": frow["total"], "active": frow["active"], "fully_read": frow["done"]},
            "recent_cycles": [
                {"time": fmt_time(r["TimeOfUpdate"]), "jobs": r["TotalRecordsRead"],
                 "backlog": r["JobBacklogEstimate"], "duration_ms": r["DurationMs"],
                 "gc": bool(r["GarbageCollectionRun"]), "hit_cap": bool(r["HitMaxIngestLimit"]),
                 "lost": r["RecordsLostOnInsertFailure"]}
                for r in cycles
            ],
        }

    print_header("Status Overview")
    if sd:
        stale_flag = ""
        if age_sec is not None and age_sec > args.stale_threshold:
            stale_flag = f"  {note(f'<-- WARNING: {age_sec:.0f}s ago, daemon may be down')}"
        print(f"  {lbl('Last update            :')} {ts(fmt_time(sd['TimeOfLastUpdate']))}{stale_flag}")
        cycles_str   = f"{sd['TotalCycles']:,}"
        ingested_str = f"{sd['TotalAdsIngested']:,}"
        print(f"  {lbl('Total cycles run       :')} {val(cycles_str)}")
        print(f"  {lbl('Total jobs ingested    :')} {val(ingested_str)}")
        lost = sd["TotalRecordsLost"] or 0
        lost_str = bad(f"{lost:,}") if lost > 0 else dim(f"{lost:,}")
        print(f"  {lbl('Total records lost     :')} {lost_str}")
        db_str    = f"{fmt_size(db_cur)} / {fmt_size(db_max)} ({db_pct:.1f}%)"
        size_note = f"  {note('<-- nearing limit')}" if db_pct >= 80 else ""
        print(f"  {lbl('DB size                :')} {pct_color(db_pct, db_str)}{size_note}")
        print(f"  {lbl('Schema version         :')} {val(str(schema_version))}")
        print()

        print(f"  {bold('Performance (rolling averages):')}")
        avg_per_cycle = f"{sd['AvgAdsIngestedPerCycle']:.1f}"
        avg_dur       = f"{sd['AvgIngestDurationMs']:.1f} ms"
        ingest_hz     = f"{sd['MeanIngestHz']:.1f} jobs/sec"
        arrival_hz    = f"{sd['MeanArrivalHz']:.1f} jobs/sec"
        avg_bl        = f"{int(sd['MeanBacklogEstimate']):,}"
        print(val(f"    Avg jobs/cycle       : {avg_per_cycle}"))
        print(val(f"    Avg cycle duration   : {avg_dur}"))
        print(val(f"    Ingest rate          : {ingest_hz}"))
        print(val(f"    Arrival rate         : {arrival_hz}"))
        print(val(f"    Avg backlog estimate : {avg_bl}"))
        print()

        keeping_up   = sd["MeanIngestHz"] >= sd["MeanArrivalHz"] if sd["MeanArrivalHz"] > 0 else True
        backlog_ok   = sd["MeanBacklogEstimate"] < 10_000
        cap_rate     = sd["HitMaxIngestLimitRate"] or 0.0
        left_backlog = sd["LastRunLeftBacklog"]
        mean_bl      = sd["MeanBacklogEstimate"]

        print(f"  {bold('Health:')}")
        ku_note = f"  {note('<-- ingest rate below arrival rate')}" if not keeping_up else ""
        print(f"    {lbl('Keeping up with arrivals :')} {health('YES' if keeping_up else 'NO', keeping_up)}{ku_note}")
        bl_note = f"  {note(f'<-- avg backlog {mean_bl:.0f} jobs')}" if not backlog_ok else ""
        print(f"    {lbl('Backlog healthy          :')} {health('YES' if backlog_ok else 'NO', backlog_ok)}{bl_note}")
        cap_pct  = f"{cap_rate * 100:.1f}% of cycles"
        cap_note = f"  {note('<-- consider raising LIBRARIAN_MAX_UPDATES_PER_CYCLE')}" if cap_rate > 0.1 else ""
        print(f"    {lbl('Hit ingest cap rate      :')} {caution(cap_pct)}{cap_note}")
        lb_note = f"  {note('<-- still catching up')}" if left_backlog else ""
        print(f"    {lbl('Last run left backlog    :')} {yesno(left_backlog, warn_on_yes=True)}{lb_note}")
    else:
        print(f"  {caution('No StatusData found — librarian may not have run yet.')}")

    print()
    print(f"  {bold('Recent Cycles (last 10):')}")
    cycle_hdr = f"{'Time':<22} {'Jobs':>7} {'Backlog':>9} {'Duration':>10} {'GC':>4} {'Cap':>4} {'Lost':>6}"
    print(f"  {bold(cycle_hdr)}")
    print(f"  {sep(68)}")
    for r in cycles:
        lost_n  = r["RecordsLostOnInsertFailure"]
        bl_n    = r["JobBacklogEstimate"]
        gc_b    = bool(r["GarbageCollectionRun"])
        cap_b   = bool(r["HitMaxIngestLimit"])
        t_str    = ts(f"{fmt_time(r['TimeOfUpdate']):<22}")
        j_str    = val(f"{r['TotalRecordsRead']:>7,}")
        bl_raw   = f"{bl_n:>9,}"
        bl_str   = bad(bl_raw) if bl_n > 100_000 else caution(bl_raw) if bl_n > 10_000 else good(bl_raw)
        dur_str  = val(f"{r['DurationMs']:>8,}ms")
        gc_str   = caution(" YES") if gc_b  else dim("  NO")
        cap_str  = caution(" YES") if cap_b else dim("  NO")
        lost_str = bad(f"{lost_n:>6,}") if lost_n > 0 else dim(f"{lost_n:>6,}")
        print(f"  {t_str} {j_str} {bl_str} {dur_str} {gc_str} {cap_str} {lost_str}")

    print()
    total_str   = val(str(frow['total']))
    in_prog     = frow['total'] - frow['done']
    in_prog_str = val(str(in_prog))
    done_str    = val(str(frow['done']))
    print(f"  {lbl('Tracked archive files:')} {total_str}{lbl(' tracked  |')}  {in_prog_str}{lbl(' in progress  |')}  {done_str}{lbl(' fully read')}")


def check_files(con, args):
    db_cur, db_max, db_pct = db_size_info(args)
    rows = con.execute("""
        SELECT FileName, LastOffset, FullyRead, RecordsRead, AvgRecordSize,
               DateOfRotation, DateOfDeletion
        FROM Files ORDER BY FileId ASC
    """).fetchall()

    if args.format == "json":
        return {
            "db_size": db_cur, "db_max": db_max, "db_pct": round(db_pct, 1),
            "files": [
                {"file": r["FileName"], "records_read": r["RecordsRead"],
                 "last_offset": r["LastOffset"], "avg_rec_size": r["AvgRecordSize"],
                 "fully_read": bool(r["FullyRead"]), "rotated": fmt_time(r["DateOfRotation"]),
                 "deleted": fmt_time(r["DateOfDeletion"]),
                 "status": ("deleted" if r["DateOfDeletion"] else
                             "fully read" if r["FullyRead"] else
                             "rotated, reading" if r["DateOfRotation"] else "active")}
                for r in rows
            ],
        }

    print_header("Tracked Archive Files")
    db_str    = f"{fmt_size(db_cur)} / {fmt_size(db_max)} ({db_pct:.1f}%)"
    size_note = f"  {note('<-- nearing limit')}" if db_pct >= 80 else ""
    print(f"  {lbl('DB size:')} {pct_color(db_pct, db_str)}{size_note}")
    print()

    if not rows:
        print(f"  {dim('No files tracked yet.')}")
        return

    first_path = next((r['FileName'] for r in rows if r['FileName'] and os.path.isabs(r['FileName'])), None)
    if first_path:
        print(f"  {lbl('Archive dir:')} {fname(os.path.dirname(first_path))}")
        print()

    file_hdr = f"{'File':<40} {'Records':>8}  {'Rotated':<22} Status"
    print(f"  {bold(file_hdr)}")
    print(f"  {sep(85)}")
    for r in rows:
        if r["DateOfDeletion"]:
            status = "deleted"
        elif r["FullyRead"]:
            status = "fully read"
        elif r["DateOfRotation"]:
            status = "rotated, reading"
        else:
            status = "active"
        fn_str  = fname(f"{display_filename(r['FileName']):<40}")
        rec_str = val(f"{r['RecordsRead']:>8,}")
        rot_raw = f"{fmt_time(r['DateOfRotation']) if r['DateOfRotation'] else '-':<22}"
        rot_str = ts(rot_raw) if r["DateOfRotation"] else dim(rot_raw)
        print(f"  {fn_str} {rec_str}  {rot_str} {file_status_color(status)}")


def check_backlog(con, args):
    sd = con.execute("SELECT * FROM StatusData WHERE StatusDataId = 1").fetchone()
    latest = con.execute("""
        SELECT JobBacklogEstimate, HitMaxIngestLimit, TimeOfUpdate
        FROM Status ORDER BY TimeOfUpdate DESC LIMIT 1
    """).fetchone()
    file_rows = con.execute("""
        SELECT FileName, LastOffset, AvgRecordSize, RecordsRead, FullyRead
        FROM Files WHERE DateOfDeletion IS NULL ORDER BY FileId ASC
    """).fetchall()

    eta_str  = None
    net_rate = None
    if sd and latest and sd["MeanArrivalHz"] and sd["MeanArrivalHz"] > 0 and latest["JobBacklogEstimate"] > 0:
        net_rate = (sd["MeanIngestHz"] or 0) - sd["MeanArrivalHz"]
        if net_rate > 0:
            eta_sec = latest["JobBacklogEstimate"] / net_rate
            if eta_sec < 60:
                eta_str = f"{eta_sec:.0f} seconds"
            elif eta_sec < 3600:
                eta_str = f"{eta_sec / 60:.1f} minutes"
            else:
                eta_str = f"{eta_sec / 3600:.1f} hours"

    if args.format == "json":
        return {
            "as_of": fmt_time(latest["TimeOfUpdate"]) if latest else None,
            "backlog": latest["JobBacklogEstimate"] if latest else None,
            "hit_ingest_cap": bool(latest["HitMaxIngestLimit"]) if latest else None,
            "eta": eta_str, "net_rate_hz": round(net_rate, 2) if net_rate is not None else None,
            "files": [{"file": r["FileName"], "last_offset": r["LastOffset"],
                       "avg_rec_size": r["AvgRecordSize"], "records_read": r["RecordsRead"],
                       "fully_read": bool(r["FullyRead"])} for r in file_rows],
        }

    print_header("Ingestion Backlog")
    if not latest:
        print(f"  {caution('No status records found — librarian may not have run yet.')}")
        return

    print(f"  {lbl('As of           :')} {ts(fmt_time(latest['TimeOfUpdate']))}")
    print(f"  {lbl('Current backlog :')} {backlog_color(latest['JobBacklogEstimate'])} jobs estimated")
    if latest["HitMaxIngestLimit"]:
        print(f"  {caution('WARNING: Last cycle hit the ingest cap — librarian is actively catching up.')}")
    if eta_str:
        net_note = dim(f"(net rate: {net_rate:.1f} jobs/sec)")
        print(f"  {lbl('Est. catchup    :')} {good(eta_str)}  {net_note}")
    elif net_rate is not None and net_rate <= 0:
        print(f"  {bad('WARNING: Ingest rate is not exceeding arrival rate — backlog may be growing.')}")

    print()
    first_path = next((r['FileName'] for r in file_rows if r['FileName'] and os.path.isabs(r['FileName'])), None)
    if first_path:
        print(f"  {lbl('Archive dir:')} {fname(os.path.dirname(first_path))}")
        print()

    bl_hdr = f"{'File':<40} {'Last offset':>14} {'Avg rec size':>14} {'Records read':>14}"
    print(f"  {bold(bl_hdr)}")
    print(f"  {sep(86)}")
    for r in file_rows:
        avg_size_str = f"{r['AvgRecordSize']:.0f} B" if r["AvgRecordSize"] else "N/A"
        fn_str     = fname(f"{display_filename(r['FileName']):<40}")
        offset_str = good(f"{'done':>14}") if r["FullyRead"] else val(f"{r['LastOffset']:>14,}")
        avg_str    = dim(f"{avg_size_str:>14}")
        rec_str    = val(f"{r['RecordsRead']:>14,}")
        print(f"  {fn_str} {offset_str} {avg_str} {rec_str}")


def check_recent(con, args):
    n = args.n

    clauses, params = [], []
    if args.user:
        clauses.append("u.UserName = ?")
        params.append(args.user)
    if args.since:
        clauses.append("jr.CompletionDate >= ?")
        params.append(args.since)
    if args.until:
        clauses.append("jr.CompletionDate <= ?")
        params.append(args.until)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(n)

    rows = con.execute(f"""
        SELECT j.ClusterId, j.ProcId, u.UserName, jr.CompletionDate, f.FileName
        FROM Jobs j
        JOIN JobRecords jr ON j.JobId  = jr.JobId
        LEFT JOIN Users u  ON j.UserId = u.UserId
        LEFT JOIN Files f  ON jr.FileId = f.FileId
        {where}
        ORDER BY jr.CompletionDate DESC LIMIT ?
    """, params).fetchall()

    if args.format == "json":
        return {
            "filters": {"user": args.user, "since": args.since, "until": args.until, "n": n},
            "jobs": [{"cluster": r["ClusterId"], "proc": r["ProcId"], "user": r["UserName"],
                      "completed": fmt_time(r["CompletionDate"]), "file": r["FileName"]}
                     for r in rows],
        }

    filters = []
    if args.user:  filters.append(f"user: {args.user}")
    if args.since: filters.append(f"since: {fmt_time(args.since)}")
    if args.until: filters.append(f"until: {fmt_time(args.until)}")
    filter_str = f"  —  {', '.join(filters)}" if filters else ""

    print_header(f"{n} Most Recently Completed Jobs{filter_str}")
    rec_hdr = f"{'ClusterId':>10} {'ProcId':>7}  {'User':<20} {'Completed':<22} File"
    print(f"  {bold(rec_hdr)}")
    print(f"  {sep(80)}")
    if not rows:
        print(f"  {dim('No jobs found matching the given filters.')}")
        return
    for r in rows:
        cl = cid(f"{r['ClusterId']:>10}")
        pr = cid(f"{r['ProcId']:>7}")
        u  = uname(f"{(r['UserName'] or 'N/A'):<20}")
        t  = ts(f"{fmt_time(r['CompletionDate']):<22}")
        fn = fname(display_filename(r['FileName']))
        print(f"  {cl} {pr}  {u} {t} {fn}")


def check_cluster(con, args):
    if not args.cluster:
        rows = con.execute("""
            SELECT j.ClusterId, u.UserName, COUNT(j.ProcId) AS procs,
                   MIN(CASE WHEN jr.CompletionDate > 0 THEN jr.CompletionDate END) AS earliest,
                   MAX(CASE WHEN jr.CompletionDate > 0 THEN jr.CompletionDate END) AS latest
            FROM Jobs j
            LEFT JOIN Users u       ON j.UserId  = u.UserId
            LEFT JOIN JobRecords jr ON j.JobId   = jr.JobId
            GROUP BY j.ClusterId ORDER BY j.ClusterId DESC LIMIT 50
        """).fetchall()

        if args.format == "json":
            return {"cluster_id": None,
                    "clusters": [{"cluster": r["ClusterId"], "user": r["UserName"],
                                  "procs": r["procs"], "earliest": fmt_time(r["earliest"]),
                                  "latest": fmt_time(r["latest"])} for r in rows]}

        print_header("All Clusters  (most recent 50 — use --cluster ID for details)")
        cl_hdr = f"{'ClusterId':>10} {'User':<20} {'Procs':>7}  {'First Proc Done':<22} {'Last Proc Done':<22} Duration"
        print(f"  {bold(cl_hdr)}")
        print(f"  {sep(97)}")
        for r in rows:
            no_dates = not r["earliest"] or r["earliest"] <= 0
            e_str    = fmt_time(r["earliest"])
            la_str   = fmt_time(r["latest"])
            dur_secs = (r["latest"] - r["earliest"]) if (not no_dates and r["procs"] > 1) else None
            dur_str  = fmt_duration(dur_secs)
            if no_dates:
                row_str = (f"{r['ClusterId']:>10}  {(r['UserName'] or 'N/A'):<20} "
                           f"{r['procs']:>6}  {e_str:<22} {la_str:<22} {dur_str}")
                print(f"  {dim(row_str)}")
            else:
                cl  = cid(f"{r['ClusterId']:>10}")
                u   = uname(f"{(r['UserName'] or 'N/A'):<20}")
                pr  = val(f"{r['procs']:>6}")
                e   = ts(f"{e_str:<22}")
                la  = ts(f"{la_str:<22}")
                dur = val(dur_str)
                print(f"  {cl}  {u} {pr}  {e} {la} {dur}")
        return

    cluster_id = args.cluster
    rows = con.execute("""
        SELECT j.ProcId, u.UserName, j.TimeOfCreation, jr.CompletionDate, f.FileName, jr.Offset
        FROM Jobs j
        LEFT JOIN Users u       ON j.UserId  = u.UserId
        LEFT JOIN JobRecords jr ON j.JobId   = jr.JobId
        LEFT JOIN Files f       ON jr.FileId = f.FileId
        WHERE j.ClusterId = ?
        ORDER BY j.ProcId ASC
    """, (cluster_id,)).fetchall()

    if args.format == "json":
        return {"cluster_id": cluster_id, "proc_count": len(rows),
                "procs": [{"proc": r["ProcId"], "user": r["UserName"],
                            "created": fmt_time(r["TimeOfCreation"]),
                            "completed": fmt_time(r["CompletionDate"]),
                            "wall_time": fmt_duration(
                                (r["CompletionDate"] - r["TimeOfCreation"])
                                if (r["CompletionDate"] and r["CompletionDate"] > 0
                                    and r["TimeOfCreation"] and r["TimeOfCreation"] > 0)
                                else None),
                            "file": r["FileName"], "offset": r["Offset"]} for r in rows]}

    print_header(f"Cluster {cluster_id}  —  {len(rows)} proc(s) indexed")
    if not rows:
        print(f"  {caution(f'No records found for cluster {cluster_id}.')}")
        return
    proc_hdr = f"{'ProcId':>7}  {'User':<20} {'Created':<22} {'Completed':<22} {'Wall Time':>10}  File"
    print(f"  {bold(proc_hdr)}")
    print(f"  {sep(97)}")
    for r in rows:
        valid = (r["CompletionDate"] and r["CompletionDate"] > 0
                 and r["TimeOfCreation"] and r["TimeOfCreation"] > 0)
        wall  = fmt_duration(r["CompletionDate"] - r["TimeOfCreation"]) if valid else "-"
        pr    = cid(f"{r['ProcId']:>7}")
        u     = uname(f"{(r['UserName'] or 'N/A'):<20}")
        cr    = ts(f"{fmt_time(r['TimeOfCreation']):<22}")
        cmp   = ts(f"{fmt_time(r['CompletionDate']):<22}")
        wt    = val(f"{wall:>10}")
        fn    = fname(display_filename(r['FileName']))
        print(f"  {pr}  {u} {cr} {cmp} {wt}  {fn}")


def check_search(con, args):
    if not args.cluster:
        print(f"  {caution('--cluster ID is required for the search check.')}")
        return

    clauses = ["j.ClusterId = ?"]
    params  = [args.cluster]
    if args.proc is not None:
        clauses.append("j.ProcId = ?")
        params.append(args.proc)

    rows = con.execute(f"""
        SELECT j.ClusterId, j.ProcId, u.UserName, j.TimeOfCreation,
               jr.CompletionDate, f.FileName, jr.Offset
        FROM Jobs j
        LEFT JOIN Users u       ON j.UserId  = u.UserId
        LEFT JOIN JobRecords jr ON j.JobId   = jr.JobId
        LEFT JOIN Files f       ON jr.FileId = f.FileId
        WHERE {' AND '.join(clauses)}
        ORDER BY j.ProcId ASC
    """, params).fetchall()

    title = f"Search: {args.cluster}"
    if args.proc is not None:
        title += f".{args.proc}"
    title += f"  —  {len(rows)} result(s)"

    if args.format == "json":
        return {"cluster": args.cluster, "proc": args.proc, "results": [
            {"cluster": r["ClusterId"], "proc": r["ProcId"], "user": r["UserName"],
             "created": fmt_time(r["TimeOfCreation"]),
             "completed": fmt_time(r["CompletionDate"]),
             "file": r["FileName"], "offset": r["Offset"]}
            for r in rows
        ]}

    print_header(title)
    if not rows:
        query_str = f"{args.cluster}.{args.proc}" if args.proc is not None else str(args.cluster)
        print(f"  {dim(f'No indexed records found for {query_str}.')}")
        return

    srch_hdr = f"{'ClusterId':>10} {'ProcId':>7}  {'User':<20} {'Created':<22} {'Completed':<22} {'Offset':>12}  File"
    print(f"  {bold(srch_hdr)}")
    print(f"  {sep(107)}")
    for r in rows:
        cl  = cid(f"{r['ClusterId']:>10}")
        pr  = cid(f"{r['ProcId']:>7}")
        u   = uname(f"{(r['UserName'] or 'N/A'):<20}")
        cr  = ts(f"{fmt_time(r['TimeOfCreation']):<22}")
        cmp = ts(f"{fmt_time(r['CompletionDate']):<22}")
        off = val(f"{r['Offset']:>12,}" if r["Offset"] is not None else f"{'N/A':>12}")
        fn  = fname(display_filename(r['FileName']))
        print(f"  {cl} {pr}  {u} {cr} {cmp} {off}  {fn}")


def check_throughput(con, args):
    days = args.days

    # Build date range
    if args.since:
        start_ts = args.since
    else:
        start_ts = int((datetime.now() - timedelta(days=days)).replace(
            hour=0, minute=0, second=0, microsecond=0).timestamp())
    end_ts = args.until or int(datetime.now().timestamp())

    rows = con.execute("""
        SELECT date(CompletionDate, 'unixepoch', 'localtime') AS day, COUNT(*) AS jobs
        FROM JobRecords
        WHERE CompletionDate > 0 AND CompletionDate >= ? AND CompletionDate <= ?
        GROUP BY day ORDER BY day ASC
    """, (start_ts, end_ts)).fetchall()

    if args.format == "json":
        return {
            "since": fmt_time(start_ts), "until": fmt_time(end_ts),
            "days": [{"date": r["day"], "jobs": r["jobs"]} for r in rows],
            "total": sum(r["jobs"] for r in rows),
            "peak":  max((r["jobs"] for r in rows), default=0),
        }

    # Build a date→count map, filling in zeros for missing days
    data = {r["day"]: r["jobs"] for r in rows}
    all_days = []
    cursor = datetime.fromtimestamp(start_ts).replace(hour=0, minute=0, second=0)
    end_dt  = datetime.fromtimestamp(end_ts)
    while cursor <= end_dt:
        key = cursor.strftime("%Y-%m-%d")
        all_days.append((key, data.get(key, 0)))
        cursor += timedelta(days=1)

    max_jobs = max((c for _, c in all_days), default=0)
    total    = sum(c for _, c in all_days)
    avg      = total / len(all_days) if all_days else 0
    peak_day = max(all_days, key=lambda x: x[1], default=("N/A", 0))

    title = f"Job Throughput — last {days} days"
    if args.since or args.until:
        title = f"Job Throughput  —  {fmt_time(start_ts)[:10]} to {fmt_time(end_ts)[:10]}"
    print_header(title, width=72)

    total_str   = f"{total:,}"
    peak_str    = f"{max_jobs:,}/day ({peak_day[0]})"
    avg_str     = f"{avg:.1f}/day"
    print(f"  {lbl('Total jobs :')} {val(total_str)}")
    print(f"  {lbl('Peak       :')} {val(peak_str)}")
    print(f"  {lbl('Average    :')} {val(avg_str)}")
    print()

    bar_width = 30
    count_w   = len(str(max_jobs)) + 1
    for day, count in all_days:
        date_str  = dim(day)
        bar       = bar_chart(count, max_jobs, bar_width)
        count_str = val(f"{count:>{count_w},}")
        print(f"  {date_str}  {bar}  {count_str}")


def check_histogram(con, args):
    user_clause = "AND u.UserName = ?" if args.user else ""
    user_params = (args.user,) if args.user else ()

    rows = con.execute(f"""
        SELECT j.ClusterId, COUNT(*) AS job_count
        FROM Jobs j
        LEFT JOIN Users u ON j.UserId = u.UserId
        WHERE 1=1 {user_clause}
        GROUP BY j.ClusterId
    """, user_params).fetchall()

    # Bucket definitions: (label, lo, hi) — hi=None means unbounded
    BUCKETS = [
        ("1",        1,    1),
        ("2–5",      2,    5),
        ("6–10",     6,   10),
        ("11–50",   11,   50),
        ("51–100",  51,  100),
        ("101–500", 101,  500),
        ("501–1k",  501, 1000),
        ("1001–5k", 1001, 5000),
        ("5001+",   5001, None),
    ]

    bucket_counts = {label: 0 for label, _, _ in BUCKETS}
    for r in rows:
        jc = r["job_count"]
        for label, lo, hi in BUCKETS:
            if jc >= lo and (hi is None or jc <= hi):
                bucket_counts[label] += 1
                break

    bucket_data    = [(label, bucket_counts[label]) for label, _, _ in BUCKETS]
    total_clusters = len(rows)

    if args.format == "json":
        return {
            "total_clusters": total_clusters,
            "buckets": [{"jobs_per_cluster": bl, "cluster_count": bc}
                        for bl, bc in bucket_data],
        }

    title = "Jobs-per-Cluster Histogram"
    if args.user:
        title += f"  —  user: {args.user}"
    print_header(title)

    if not rows:
        print(f"  {dim('No jobs found.')}")
        return

    print(f"  {lbl('Total clusters indexed:')} {val(f'{total_clusters:,}')}")
    print()

    # Trim empty leading and trailing buckets
    while bucket_data and bucket_data[0][1] == 0:
        bucket_data = bucket_data[1:]
    while bucket_data and bucket_data[-1][1] == 0:
        bucket_data = bucket_data[:-1]

    if not bucket_data:
        print(f"  {dim('No bucket data to display.')}")
        return

    max_count = max(bc for _, bc in bucket_data)
    label_w   = max(len(bl) for bl, _ in bucket_data)
    count_w   = len(f"{max_count:,}") + 1
    bar_width = 36

    hdr_label = f"{'Jobs/cluster':>{label_w}}"
    print(f"  {bold(hdr_label)}  {'':^{bar_width}}  {bold('Clusters')}")
    print(f"  {sep(label_w + bar_width + count_w + 6)}")

    for bl, bc in bucket_data:
        label_str = dim(f"{bl:>{label_w}}")
        bar       = bar_chart(bc, max_count, bar_width)
        count_str = val(f"{bc:>{count_w},}")
        print(f"  {label_str}  {bar}  {count_str}")


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

CHECKS = {
    "summary":    check_summary,
    "jobs":       check_jobs,
    "users":      check_users,
    "status":     check_status,
    "files":      check_files,
    "backlog":    check_backlog,
    "recent":     check_recent,
    "cluster":    check_cluster,
    "search":     check_search,
    "throughput": check_throughput,
    "histogram":  check_histogram,
}

# checks excluded from 'all' since they need specific args or are summaries
ALL_EXCLUDE = {"summary", "search"}


def run_checks(con, args, to_run):
    results = {}
    if args.format == "json":
        for name, fn in to_run:
            results[name] = fn(con, args)
        print(json.dumps(results, indent=2))
    else:
        for i, (name, fn) in enumerate(to_run):
            if i > 0:
                print()
            result = fn(con, args)
            if name == "summary":
                results["_summary_level"] = result
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Check the HTCondor librarian database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="checks: summary, jobs, users, status, files, backlog, recent, cluster, search, throughput, histogram, all",
    )
    parser.add_argument("check", choices=[*CHECKS.keys(), "all"],
                        nargs="?", default="all",
                        help="Which check to run (default: all)")
    parser.add_argument("--db",               metavar="PATH",
                        help="Path to library.db (default: from HTCondor config)")
    parser.add_argument("--user",             metavar="NAME",
                        help="Filter jobs/recent by username")
    parser.add_argument("--since",            metavar="DATE", type=parse_date,
                        help="Filter by completion date (YYYY-MM-DD)")
    parser.add_argument("--until",            metavar="DATE", type=parse_date,
                        help="Filter by completion date (YYYY-MM-DD)")
    parser.add_argument("--cluster",          metavar="ID", type=int,
                        help="Cluster ID for cluster/search checks")
    parser.add_argument("--proc",             metavar="ID", type=int,
                        help="Proc ID for search check")
    parser.add_argument("--n",                metavar="N", type=int, default=20,
                        help="Number of jobs for recent check (default: 20)")
    parser.add_argument("--days",             metavar="N", type=int, default=30,
                        help="Number of days for throughput check (default: 30)")
    parser.add_argument("--stale-threshold",  metavar="N", type=int, default=0,
                        help="Seconds before daemon considered stale (default: auto)")
    parser.add_argument("--watch",            metavar="N", type=int,
                        help="Re-run check every N seconds")
    parser.add_argument("--format",           choices=["text", "json"], default="text",
                        help="Output format (default: text)")
    args = parser.parse_args()

    args.db = args.db or htcondor2.param.get("LIBRARIAN_DATABASE")
    if not args.db:
        print("ERROR: LIBRARIAN_DATABASE not set in HTCondor config. Use --db PATH.")
        sys.exit(1)

    # Resolve stale threshold: explicit flag wins, otherwise auto
    if args.stale_threshold <= 0:
        update_interval      = int(htcondor2.param.get("LIBRARIAN_UPDATE_INTERVAL") or 5)
        args.stale_threshold = max(60, update_interval * 10)

    if args.check == "all":
        to_run = [(name, fn) for name, fn in CHECKS.items() if name not in ALL_EXCLUDE]
    else:
        to_run = [(args.check, CHECKS[args.check])]

    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row

    if args.watch:
        stop = threading.Event()

        def key_listener():
            with KBHit() as kb:
                while not stop.is_set():
                    if kb.anyKeyPressed():
                        stop.set()
                        break
                    time.sleep(0.05)

        t = threading.Thread(target=key_listener, daemon=True)
        t.start()

        try:
            while not stop.is_set():
                if is_capable():
                    print("\033[2J\033[H", end="")
                run_checks(con, args, to_run)
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print()
                print(dim(f"  Last updated: {now}  —  refreshing every {args.watch}s  —  press any key to exit"))
                stop.wait(timeout=args.watch)
        except KeyboardInterrupt:
            stop.set()
    else:
        results = run_checks(con, args, to_run)
        # Exit code for summary check: 0=OK, 1=WARN, 2=CRIT
        if args.check == "summary" and args.format != "json":
            level = results.get("_summary_level", "OK")
            con.close()
            sys.exit({"OK": 0, "WARN": 1, "CRIT": 2}.get(level, 0))

    con.close()


if __name__ == "__main__":
    main()
