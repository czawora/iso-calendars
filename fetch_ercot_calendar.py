#!/usr/bin/env python3
"""
Fetch ERCOT calendar events for the next 3 months and produce a merged ICS file.

Data flow:
  1. GET /calendar?fromDate=...&toDate=...  → HTML with event UUIDs in checkboxes
  2. GET /ical/meetings?ids=...             → batch ICS download (max 50 per request)
  3. Merge all VEVENTs into one VCALENDAR and write to output/ercot.ics
"""

import re
import sys
import subprocess
import argparse
from datetime import datetime, timezone
from pathlib import Path

CALENDAR_URL = "https://www.ercot.com/calendar"
ICS_BATCH_URL = "https://www.ercot.com/ical/meetings"
CRLF = "\r\n"
OUTPUT_DIR = Path(__file__).resolve().parent / "output"
BATCH_SIZE = 50


def date_range(months_ahead: int = 3):
    """Return (start, end) date strings for ERCOT's query params."""
    today = datetime.now(timezone.utc).date()
    start = today.replace(day=1)
    month = start.month + months_ahead
    year = start.year + (month - 1) // 12
    month = (month - 1) % 12 + 1
    end = start.replace(year=year, month=month)
    return start.isoformat(), end.isoformat()


def fetch_event_uuids(start: str, end: str) -> list[str]:
    """Scrape ERCOT calendar HTML to extract event UUIDs from checkboxes."""
    url = f"{CALENDAR_URL}?fromDate={start}&toDate={end}"
    result = subprocess.run(
        ["curl", "-s", url],
        capture_output=True, text=True, timeout=60,
    )
    if result.returncode != 0:
        print(f"curl failed: {result.stderr}")
        return []
    uuids = re.findall(r'value="([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"', result.stdout)
    return list(dict.fromkeys(uuids))  # deduplicate preserving order


def fetch_ics_batch(uuids: list[str]) -> str | None:
    """Download ICS for a batch of events."""
    ids_param = ",".join(uuids)
    url = f"{ICS_BATCH_URL}?ids={ids_param}"
    result = subprocess.run(
        ["curl", "-s", url],
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


def build_merged_ics(vevents: list[str]) -> str:
    """Wrap VEVENT blocks in a single VCALENDAR."""
    header = CRLF.join([
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//ERCOT Calendar Sync//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        "X-WR-CALNAME:ERCOT Events",
        "X-WR-TIMEZONE:America/Chicago",
        "BEGIN:VTIMEZONE",
        "TZID:America/Chicago",
        "BEGIN:STANDARD",
        "DTSTART:19701101T020000",
        "RRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU",
        "TZOFFSETFROM:-0500",
        "TZOFFSETTO:-0600",
        "TZNAME:CST",
        "END:STANDARD",
        "BEGIN:DAYLIGHT",
        "DTSTART:19700308T020000",
        "RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU",
        "TZOFFSETFROM:-0600",
        "TZOFFSETTO:-0500",
        "TZNAME:CDT",
        "END:DAYLIGHT",
        "END:VTIMEZONE",
    ])
    footer = "END:VCALENDAR"
    body = CRLF.join(vevents)
    return header + CRLF + body + CRLF + footer + CRLF


def main():
    parser = argparse.ArgumentParser(description="Fetch ERCOT calendar → merged ICS")
    parser.add_argument("--months", type=int, default=3, help="Months ahead to fetch (default: 3)")
    parser.add_argument("--output", type=str, default=None, help="Output .ics file path")
    args = parser.parse_args()

    start, end = date_range(args.months)
    output_path = Path(args.output) if args.output else OUTPUT_DIR / "ercot.ics"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Fetching ERCOT events from {start} to {end}...")
    uuids = fetch_event_uuids(start, end)
    print(f"Found {len(uuids)} events")

    if not uuids:
        print("No events found.")
        sys.exit(0)

    all_vevents = []
    # Process in batches of BATCH_SIZE
    for i in range(0, len(uuids), BATCH_SIZE):
        batch = uuids[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(uuids) + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"  Downloading batch {batch_num}/{total_batches} ({len(batch)} events)...")
        ics_text = fetch_ics_batch(batch)
        if not ics_text:
            print(f"    ! Failed to fetch batch {batch_num}")
            continue
        vevents = extract_vevents(ics_text)
        all_vevents.extend(vevents)

    if not all_vevents:
        print("No VEVENTs extracted.")
        sys.exit(1)

    merged = build_merged_ics(all_vevents)
    output_path.write_text(merged, encoding="utf-8")
    print(f"\nWrote {len(all_vevents)} events to {output_path}")


if __name__ == "__main__":
    main()
