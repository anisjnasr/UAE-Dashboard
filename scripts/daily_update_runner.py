"""
Run daily data updates at 18:00 UAE time.

What it does per run:
1) Fetch sales + rental listings from fixed Property Finder URLs
2) Rebuild dashboard data + SQLite history with process_data.py
3) Optionally commit + push refreshed data to GitHub Pages
"""

import argparse
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


PROJECT_ROOT = Path(__file__).resolve().parents[1]
FETCH_SCRIPT = PROJECT_ROOT / "scripts" / "fetch_propertyfinder_listings.py"
PROCESS_SCRIPT = PROJECT_ROOT / "process_data.py"
UAE_TZ = ZoneInfo("Asia/Dubai")
REFRESHED_FILES = [
    "data/dashboard_data.json",
    "data/Multiple Cities - multiple unit types - sales data.json",
    "data/Multiple Cities - multiple unit types - rental data.json",
]


def _run_git(args):
    return subprocess.run(
        ["git"] + args,
        cwd=str(PROJECT_ROOT),
        check=True,
        capture_output=True,
        text=True,
    )


def publish_refresh():
    _run_git(["add"] + REFRESHED_FILES)
    staged = _run_git(["diff", "--cached", "--name-only"]).stdout.strip()
    if not staged:
        print("No refreshed data changes to publish.")
        return

    stamp = datetime.now(UAE_TZ).strftime("%Y-%m-%d %H:%M %Z")
    msg = f"Daily data refresh ({stamp})"
    _run_git(["commit", "-m", msg])
    _run_git(["push", "origin", "main"])
    print("Published refreshed data to origin/main.")


def run_update_once(publish=False):
    print(f"[{datetime.now(UAE_TZ).isoformat()}] Starting update...")
    subprocess.run([sys.executable, str(FETCH_SCRIPT)], check=True, cwd=str(PROJECT_ROOT))
    subprocess.run([sys.executable, str(PROCESS_SCRIPT)], check=True, cwd=str(PROJECT_ROOT))
    if publish:
        publish_refresh()
    print(f"[{datetime.now(UAE_TZ).isoformat()}] Update complete.")


def next_run_time(hour=18, minute=0):
    now = datetime.now(UAE_TZ)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target = target + timedelta(days=1)
    return target


def run_daily(hour=18, minute=0, publish=False):
    while True:
        target = next_run_time(hour=hour, minute=minute)
        wait_seconds = (target - datetime.now(UAE_TZ)).total_seconds()
        print(f"Next run at {target.isoformat()} (in {int(wait_seconds)}s)")
        # Sleep in chunks so Ctrl+C is responsive.
        while wait_seconds > 0:
            chunk = min(300, wait_seconds)
            time.sleep(chunk)
            wait_seconds -= chunk
        try:
            run_update_once(publish=publish)
        except Exception as exc:
            print(f"Update failed: {exc}")


def main():
    parser = argparse.ArgumentParser(description="Daily runner for PF fetch + process pipeline.")
    parser.add_argument("--once", action="store_true", help="Run immediately once and exit.")
    parser.add_argument(
        "--publish",
        action="store_true",
        help="After refresh, auto-commit refreshed JSON files and push to origin/main.",
    )
    parser.add_argument("--hour", type=int, default=18, help="UAE local hour (0-23). Default 18.")
    parser.add_argument("--minute", type=int, default=0, help="UAE local minute. Default 0.")
    args = parser.parse_args()

    if args.once:
        run_update_once(publish=args.publish)
        return
    run_daily(hour=args.hour, minute=args.minute, publish=args.publish)


if __name__ == "__main__":
    main()
