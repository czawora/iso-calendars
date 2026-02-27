#!/usr/bin/env python3
"""
Fetch MISO calendar events for the next 3 months and produce a merged ICS file.

Data flow:
  1. GET /api/events/geteventsformonth?month=M&year=Y  → event JSON
  2. GET /events/{year}/{urlSegment}/AddToICalendar     → individual .ics per event
  3. Merge all VEVENTs into one VCALENDAR and write to output/miso.ics
"""

import sys
import time
import json
import subprocess
import argparse
from datetime import datetime, timezone
from pathlib import Path

EVENTS_API = "https://www.misoenergy.org/api/events/geteventsformonth"
ICS_BASE = "https://www.misoenergy.org/events"
CRLF = "\r\n"
OUTPUT_DIR = Path(__file__).resolve().parent / "output"


def months_to_fetch(months_ahead: int = 3) -> list[tuple[int, int]]:
    """Return list of (month, year) tuples to fetch."""
    today = datetime.now(timezone.utc).date()
    result = []
    m, y = today.month, today.year
    for _ in range(months_ahead):
        result.append((m, y))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return result


def fetch_events_for_month(month: int, year: int) -> list[dict]:
    """Fetch events from MISO API for a given month."""
    url = f"{EVENTS_API}?month={month}&year={year}"
    result = subprocess.run(
        ["curl", "-s", url],
        capture_output=True, text=True, timeout=60,
    )
    if result.returncode != 0:
        print(f"  curl failed for {month}/{year}: {result.stderr}")
        return []
    data = json.loads(result.stdout)
    return data.get("events", [])


def extract_vevent(ics_text: str) -> str | None:
    """Pull the first BEGIN:VEVENT…END:VEVENT block out of raw ICS text."""
    lines = ics_text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    buf = []
    inside = False
    for line in lines:
        if line.strip() == "BEGIN:VEVENT":
            inside = True
        if inside:
            buf.append(line)
        if inside and line.strip() == "END:VEVENT":
            return CRLF.join(buf)
    return None


def fetch_ics(url_segment: str, year: int) -> str | None:
    """Download ICS for a single event."""
    url = f"{ICS_BASE}/{year}/{url_segment}/AddToICalendar"
    result = subprocess.run(
        ["curl", "-s", "-L", url],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0 or "BEGIN:VCALENDAR" not in result.stdout:
        return None
    return result.stdout


def build_merged_ics(vevents: list[str]) -> str:
    """Wrap VEVENT blocks in a single VCALENDAR."""
    header = CRLF.join([
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//MISO Calendar Sync//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        "X-WR-CALNAME:MISO Events",
        "X-WR-TIMEZONE:America/New_York",
    ])
    footer = "END:VCALENDAR"
    body = CRLF.join(vevents)
    return header + CRLF + body + CRLF + footer + CRLF


def main():
    parser = argparse.ArgumentParser(description="Fetch MISO calendar → merged ICS")
    parser.add_argument("--months", type=int, default=3, help="Months ahead to fetch (default: 3)")
    parser.add_argument("--output", type=str, default=None, help="Output .ics file path")
    args = parser.parse_args()

    output_path = Path(args.output) if args.output else OUTPUT_DIR / "miso.ics"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    months = months_to_fetch(args.months)

    # Collect all events across months, dedup by contentGuid
    all_events = {}
    for month, year in months:
        print(f"Fetching MISO events for {month}/{year}...")
        events = fetch_events_for_month(month, year)
        for e in events:
            guid = e.get("contentGuid", "")
            if guid and guid not in all_events:
                all_events[guid] = e
    print(f"Found {len(all_events)} unique events across {len(months)} months")

    if not all_events:
        print("No events found.")
        sys.exit(0)

    vevents = []
    events_list = sorted(all_events.values(), key=lambda e: e.get("startDate", ""))
    for i, event in enumerate(events_list, 1):
        name = event.get("name", "Unknown")
        url_segment = event.get("urlSegment", "")
        start_date = event.get("startDate", "")
        if not url_segment or not start_date:
            continue
        year = int(start_date[:4])
        print(f"  [{i}/{len(events_list)}] {name}")
        ics_text = fetch_ics(url_segment, year)
        if not ics_text:
            print(f"    ! Failed to fetch ICS")
            continue
        vevent = extract_vevent(ics_text)
        if vevent:
            vevents.append(vevent)
        if i % 10 == 0:
            time.sleep(0.5)

    if not vevents:
        print("No VEVENTs extracted.")
        sys.exit(1)

    merged = build_merged_ics(vevents)
    output_path.write_text(merged, encoding="utf-8")
    print(f"\nWrote {len(vevents)} events to {output_path}")


if __name__ == "__main__":
    main()
