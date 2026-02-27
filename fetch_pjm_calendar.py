#!/usr/bin/env python3
"""
Fetch PJM calendar events and produce a merged ICS file.

PJM already publishes static ICS feeds per category. This script downloads
all category feeds, extracts VEVENTs, deduplicates, and merges them into
a single output/pjm.ics file.

ICS feed URLs:
  https://www.pjm.com/pjmfiles/calendar/PJM-{category}.ics
"""

import re
import sys
import subprocess
import argparse
from datetime import datetime, timezone
from pathlib import Path

CRLF = "\r\n"
OUTPUT_DIR = Path(__file__).resolve().parent / "output"

PJM_ICS_FEEDS = [
    "https://www.pjm.com/pjmfiles/calendar/PJM-Meetings.ics",
    "https://www.pjm.com/pjmfiles/calendar/PJM-Training.ics",
    "https://www.pjm.com/pjmfiles/calendar/PJM-Technical-Changes.ics",
    "https://www.pjm.com/pjmfiles/calendar/PJM-Market-Settlements-Billing.ics",
    "https://www.pjm.com/pjmfiles/calendar/PJM-Interconnection-Queue.ics",
    "https://www.pjm.com/pjmfiles/calendar/PJM-FTR-Schedule.ics",
    "https://www.pjm.com/pjmfiles/calendar/PJM-RPM-Schedule.ics",
    "https://www.pjm.com/pjmfiles/calendar/PJM-Holidays.ics",
]


def fetch_ics_feed(url: str) -> str | None:
    """Download a PJM ICS feed."""
    result = subprocess.run(
        ["curl", "-s", "-L", url],
        capture_output=True, text=True, timeout=60,
    )
    if result.returncode != 0 or "BEGIN:VCALENDAR" not in result.stdout:
        return None
    return result.stdout


def extract_vevents(ics_text: str) -> list[str]:
    """Extract all VEVENT blocks from raw ICS text."""
    lines = ics_text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    vevents = []
    buf = []
    inside = False
    for line in lines:
        if line.strip() == "BEGIN:VEVENT":
            inside = True
            buf = []
        if inside:
            buf.append(line)
        if inside and line.strip() == "END:VEVENT":
            vevents.append(CRLF.join(buf))
            inside = False
    return vevents


def extract_uid(vevent: str) -> str | None:
    """Extract the UID from a VEVENT block."""
    for line in vevent.split(CRLF):
        if line.startswith("UID:"):
            return line[4:].strip()
    return None


def extract_dtstart_date(vevent: str) -> str | None:
    """Extract the DTSTART date (YYYYMMDD) from a VEVENT block."""
    for line in vevent.split(CRLF):
        m = re.match(r"DTSTART[^:]*:(\d{8})", line)
        if m:
            return m.group(1)
    return None


def date_range(months_ahead: int = 3):
    """Return (start, end) as YYYYMMDD strings."""
    today = datetime.now(timezone.utc).date()
    start = today.replace(day=1)
    month = start.month + months_ahead
    year = start.year + (month - 1) // 12
    month = (month - 1) % 12 + 1
    end = start.replace(year=year, month=month)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


def build_merged_ics(vevents: list[str]) -> str:
    """Wrap VEVENT blocks in a single VCALENDAR."""
    header = CRLF.join([
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//PJM Calendar Sync//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        "X-WR-CALNAME:PJM Events",
        "X-WR-TIMEZONE:America/New_York",
    ])
    footer = "END:VCALENDAR"
    body = CRLF.join(vevents)
    return header + CRLF + body + CRLF + footer + CRLF


def main():
    parser = argparse.ArgumentParser(description="Fetch PJM calendar feeds → merged ICS")
    parser.add_argument("--months", type=int, default=3, help="Months ahead to fetch (default: 3)")
    parser.add_argument("--output", type=str, default=None, help="Output .ics file path")
    args = parser.parse_args()

    start_yyyymmdd, end_yyyymmdd = date_range(args.months)
    output_path = Path(args.output) if args.output else OUTPUT_DIR / "pjm.ics"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Date filter: {start_yyyymmdd} to {end_yyyymmdd}")

    seen_uids = set()
    all_vevents = []

    for url in PJM_ICS_FEEDS:
        name = url.split("/")[-1]
        print(f"Fetching {name}...")
        ics_text = fetch_ics_feed(url)
        if not ics_text:
            print(f"  ! Failed to fetch {name}")
            continue
        vevents = extract_vevents(ics_text)
        added = 0
        for ve in vevents:
            uid = extract_uid(ve)
            dtstart = extract_dtstart_date(ve)
            if uid and uid not in seen_uids:
                if dtstart and not (start_yyyymmdd <= dtstart < end_yyyymmdd):
                    continue
                seen_uids.add(uid)
                all_vevents.append(ve)
                added += 1
        print(f"  {added} events (from {len(vevents)} total)")

    if not all_vevents:
        print("No VEVENTs extracted.")
        sys.exit(1)

    merged = build_merged_ics(all_vevents)
    output_path.write_text(merged, encoding="utf-8")
    print(f"\nWrote {len(all_vevents)} events to {output_path}")


if __name__ == "__main__":
    main()
