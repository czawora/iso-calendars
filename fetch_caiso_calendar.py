#!/usr/bin/env python3
"""
Fetch CAISO calendar events for the next 3 months and produce a merged ICS file.

Data flow:
  1. GET /resources/calendar.json?start=...&end=...  → list of event objects with IDs
  2. GET /resources/export/ical?id={id}               → individual .ics per event
  3. Merge all VEVENTs into one VCALENDAR and write to output/caiso.ics
"""

import sys
import time
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

CALENDAR_JSON = "https://www.caiso.com/resources/calendar.json"
ICS_EXPORT = "https://www.caiso.com/resources/export/ical"
CRLF = "\r\n"
OUTPUT_DIR = Path(__file__).resolve().parent / "output"


def date_range(months_ahead: int = 3):
    """Return (start, end) ISO date strings spanning today → N months ahead."""
    today = datetime.now(timezone.utc).date()
    start = today.replace(day=1)
    # Advance by months_ahead months
    month = start.month + months_ahead
    year = start.year + (month - 1) // 12
    month = (month - 1) % 12 + 1
    end = start.replace(year=year, month=month)
    return start.isoformat(), end.isoformat()


def fetch_event_ids(session: requests.Session, start: str, end: str) -> list[dict]:
    """Fetch all events from the CAISO calendar JSON feed."""
    resp = session.get(CALENDAR_JSON, params={"start": start, "end": end}, timeout=30)
    resp.raise_for_status()
    events = resp.json()
    print(f"Found {len(events)} events between {start} and {end}")
    return events


def fetch_ics(session: requests.Session, event_id: int) -> str | None:
    """Download the ICS text for a single event."""
    try:
        resp = session.get(ICS_EXPORT, params={"id": event_id}, timeout=30)
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as exc:
        print(f"  ! Failed to fetch ICS for event {event_id}: {exc}")
        return None


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


def build_merged_ics(vevents: list[str]) -> str:
    """Wrap a list of VEVENT blocks in a single VCALENDAR."""
    header = CRLF.join([
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//CAISO Calendar Sync//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        "X-WR-CALNAME:CAISO Events",
        "X-WR-TIMEZONE:America/Los_Angeles",
    ])
    footer = "END:VCALENDAR"
    body = (CRLF).join(vevents)
    return header + CRLF + body + CRLF + footer + CRLF


def main():
    parser = argparse.ArgumentParser(description="Fetch CAISO calendar → merged ICS")
    parser.add_argument("--months", type=int, default=3, help="Months ahead to fetch (default: 3)")
    parser.add_argument("--output", type=str, default=None, help="Output .ics file path")
    args = parser.parse_args()

    start, end = date_range(args.months)

    output_path = Path(args.output) if args.output else OUTPUT_DIR / "caiso.ics"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (CAISO-Calendar-Sync)"
    })

    events = fetch_event_ids(session, start, end)
    if not events:
        print("No events found.")
        sys.exit(0)

    vevents = []
    for i, event in enumerate(events, 1):
        eid = event["id"]
        title = event.get("title", "Unknown")
        print(f"  [{i}/{len(events)}] {eid} - {title}")
        ics_text = fetch_ics(session, eid)
        if not ics_text:
            continue
        vevent = extract_vevent(ics_text)
        if vevent:
            vevents.append(vevent)
        # Be polite to the server
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
