#!/usr/bin/env python3
"""
Fetch ISO-NE (ISO New England) calendar events for the next 3 months
and produce a merged ICS file.

Data flow:
  1. GET /api/1/services/events.json?fromDate=...&toDate=...  → event JSON
  2. Build VEVENTs from JSON data
  3. Write merged VCALENDAR to output/isone.ics
"""

import re
import sys
import json
import subprocess
import argparse
from datetime import datetime, timezone
from pathlib import Path
from html import unescape

EVENTS_API = "https://www.iso-ne.com/api/1/services/events.json"
ISONE_BASE = "https://www.iso-ne.com"
CRLF = "\r\n"
OUTPUT_DIR = Path(__file__).resolve().parent / "output"


def date_range(months_ahead: int = 3):
    """Return (start, end) ISO datetime strings for the API."""
    today = datetime.now(timezone.utc).date()
    start = today.replace(day=1)
    month = start.month + months_ahead
    year = start.year + (month - 1) // 12
    month = (month - 1) % 12 + 1
    end = start.replace(year=year, month=month)
    return f"{start}T00:00:00", f"{end}T00:00:00"


def ics_escape(text: str) -> str:
    """Escape text for ICS property values."""
    return text.replace("\\", "\\\\").replace(";", "\\;").replace(",", "\\,").replace("\n", "\\n")


def strip_html(html: str) -> str:
    """Remove HTML tags and decode entities."""
    text = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def build_vevent(event: dict) -> str | None:
    """Build a VEVENT block from an ISO-NE event JSON object."""
    eid = event.get("event_id", "")
    title = event.get("event_title", "ISO-NE Event")
    start_str = event.get("event_start_date_gmt_str", "")
    end_str = event.get("event_end_date_gmt_str", "")
    location = event.get("location", "") or ""
    contact = event.get("contact_name", "") or ""
    contact_email = event.get("contact_email", "") or ""
    cancelled = event.get("cancelled_flag", "N") == "Y"
    description_html = event.get("event_description", "") or ""

    if not start_str or not eid:
        return None

    # Convert GMT ISO string to ICS UTC format
    dtstart = start_str.replace("-", "").replace(":", "").replace("T", "T") + "Z"
    dtend = end_str.replace("-", "").replace(":", "").replace("T", "T") + "Z" if end_str else None

    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    # Build description
    desc_parts = []
    if cancelled:
        desc_parts.append("** CANCELLED **")
    plain_desc = strip_html(description_html)
    if plain_desc:
        desc_parts.append(plain_desc)
    if contact:
        contact_line = f"Contact: {contact}"
        if contact_email:
            contact_line += f" ({contact_email})"
        desc_parts.append(contact_line)
    desc_parts.append(f"{ISONE_BASE}/event-details?eventId={eid}")

    summary = title
    if cancelled:
        summary = f"[CANCELLED] {title}"

    lines = [
        "BEGIN:VEVENT",
        f"UID:{eid}@iso-ne.com",
        f"DTSTAMP:{now}",
        f"DTSTART:{dtstart}",
    ]
    if dtend:
        lines.append(f"DTEND:{dtend}")
    lines.append(f"SUMMARY:{ics_escape(summary)}")
    if location:
        lines.append(f"LOCATION:{ics_escape(location)}")
    lines.append(f"DESCRIPTION:{ics_escape(chr(10).join(desc_parts))}")
    lines.append(f"URL:{ISONE_BASE}/event-details?eventId={eid}")
    lines.append("END:VEVENT")
    return CRLF.join(lines)


def build_merged_ics(vevents: list[str]) -> str:
    """Wrap VEVENT blocks in a single VCALENDAR."""
    header = CRLF.join([
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//ISO-NE Calendar Sync//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        "X-WR-CALNAME:ISO-NE Events",
        "X-WR-TIMEZONE:America/New_York",
    ])
    footer = "END:VCALENDAR"
    body = CRLF.join(vevents)
    return header + CRLF + body + CRLF + footer + CRLF


def main():
    parser = argparse.ArgumentParser(description="Fetch ISO-NE calendar → merged ICS")
    parser.add_argument("--months", type=int, default=3, help="Months ahead to fetch (default: 3)")
    parser.add_argument("--output", type=str, default=None, help="Output .ics file path")
    args = parser.parse_args()

    start, end = date_range(args.months)
    output_path = Path(args.output) if args.output else OUTPUT_DIR / "isone.ics"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    url = f"{EVENTS_API}?sortBy=event_start_date_gmt+asc&fromDate={start}&toDate={end}&count=1000"
    print(f"Fetching ISO-NE events from {start} to {end}...")

    result = subprocess.run(
        ["curl", "-s", url],
        capture_output=True, text=True, timeout=60,
    )
    if result.returncode != 0:
        print(f"curl failed: {result.stderr}")
        sys.exit(1)

    data = json.loads(result.stdout)
    events = data.get("events", [])
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
