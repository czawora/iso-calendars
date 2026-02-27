#!/usr/bin/env python3
"""
Fetch SPP (Southwest Power Pool) calendar events for the next 3 months
and produce a merged ICS file.

Data flow:
  1. GET /umbraco/Api/calendarApi/events?start=...&end=...  → list of event JSON
  2. Build VEVENTs from JSON (SPP has no per-event ICS endpoint)
  3. Write merged VCALENDAR to output/spp.ics
"""

import json
import subprocess
import sys
import argparse
from datetime import datetime, timezone
from pathlib import Path

EVENTS_API = "https://www.spp.org/umbraco/Api/calendarApi/events"
SPP_BASE = "https://www.spp.org"
CRLF = "\r\n"
OUTPUT_DIR = Path(__file__).resolve().parent / "output"


def date_range(months_ahead: int = 3):
    """Return (start, end) ISO date strings spanning today → N months ahead."""
    today = datetime.now(timezone.utc).date()
    start = today.replace(day=1)
    month = start.month + months_ahead
    year = start.year + (month - 1) // 12
    month = (month - 1) % 12 + 1
    end = start.replace(year=year, month=month)
    return start.isoformat(), end.isoformat()


def ics_escape(text: str) -> str:
    """Escape text for ICS property values."""
    return text.replace("\\", "\\\\").replace(";", "\\;").replace(",", "\\,").replace("\n", "\\n")


def parse_dt(dt_str: str) -> str:
    """Convert SPP datetime string to ICS DTSTART/DTEND format (UTC)."""
    # SPP returns times like "2026-02-26T08:30:00.0000000" in US Central
    dt_str = dt_str.split(".")[0]  # strip fractional seconds
    # SPP times are US Central (America/Chicago)
    # Convert to UTC by adding 6 hours (CST) — but for ICS we'll use TZID instead
    return dt_str.replace("-", "").replace(":", "").replace("T", "T")


def build_vevent(event: dict) -> str:
    """Build a VEVENT block from an SPP event JSON object."""
    eid = event["id"]
    title = event.get("title", "SPP Event")
    alt_title = event.get("alternateTitle", "")
    start = event.get("start", "")
    end = event.get("end", "")
    location_parts = [event.get("location", ""), event.get("city", ""), event.get("state", "")]
    location = ", ".join(p for p in location_parts if p)
    url = event.get("url", "")
    if url and not url.startswith("http"):
        url = SPP_BASE + url
    schedule = event.get("fullSchedule", "")

    dtstart = parse_dt(start) if start else None
    dtend = parse_dt(end) if end else None
    if not dtstart:
        return ""

    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    lines = [
        "BEGIN:VEVENT",
        f"UID:{eid}@spp.org",
        f"DTSTAMP:{now}",
        f"DTSTART;TZID=America/Chicago:{dtstart}",
    ]
    if dtend:
        lines.append(f"DTEND;TZID=America/Chicago:{dtend}")
    lines.append(f"SUMMARY:{ics_escape(title)}")
    if alt_title and alt_title != title:
        lines.append(f"DESCRIPTION:{ics_escape(alt_title)}")
    if location:
        lines.append(f"LOCATION:{ics_escape(location)}")
    if url:
        lines.append(f"URL:{url}")
    lines.append("END:VEVENT")
    return CRLF.join(lines)


def build_merged_ics(vevents: list[str]) -> str:
    """Wrap VEVENT blocks in a VCALENDAR with Central timezone definition."""
    header = CRLF.join([
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//SPP Calendar Sync//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        "X-WR-CALNAME:SPP Events",
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
    parser = argparse.ArgumentParser(description="Fetch SPP calendar → merged ICS")
    parser.add_argument("--months", type=int, default=3, help="Months ahead to fetch (default: 3)")
    parser.add_argument("--output", type=str, default=None, help="Output .ics file path")
    args = parser.parse_args()

    start, end = date_range(args.months)
    output_path = Path(args.output) if args.output else OUTPUT_DIR / "spp.ics"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Fetching SPP events from {start} to {end}...")
    url = f"{EVENTS_API}?start={start}&end={end}"
    result = subprocess.run(
        ["curl", "-s", "-H", "Referer: https://www.spp.org/events/", url],
        capture_output=True, text=True, timeout=60,
    )
    if result.returncode != 0:
        print(f"curl failed: {result.stderr}")
        sys.exit(1)
    events = json.loads(result.stdout)
    print(f"Found {len(events)} events")

    if not events:
        print("No events found.")
        sys.exit(0)

    vevents = []
    for event in events:
        vevent = build_vevent(event)
        if vevent:
            vevents.append(vevent)

    if not vevents:
        print("No VEVENTs built.")
        sys.exit(1)

    merged = build_merged_ics(vevents)
    output_path.write_text(merged, encoding="utf-8")
    print(f"Wrote {len(vevents)} events to {output_path}")


if __name__ == "__main__":
    main()
