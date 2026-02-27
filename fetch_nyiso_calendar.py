#!/usr/bin/env python3
"""
Fetch NYISO calendar events and produce a merged ICS file.

NYISO provides bulk ICS feeds per calendar category via Liferay.
This script downloads all category feeds, extracts VEVENTs, deduplicates,
and merges them into a single output/nyiso.ics file.

ICS feed URLs:
  https://www.nyiso.com/o/oasis-rest/calendar/export/{calendarId}.ics
"""

import sys
import subprocess
import argparse
from pathlib import Path

CRLF = "\r\n"
OUTPUT_DIR = Path(__file__).resolve().parent / "output"

NYISO_ICS_FEEDS = {
    "Management Committee": "https://www.nyiso.com/o/oasis-rest/calendar/export/44327.ics",
    "Business Issues Committee": "https://www.nyiso.com/o/oasis-rest/calendar/export/44334.ics",
    "Operating Committee": "https://www.nyiso.com/o/oasis-rest/calendar/export/2167912.ics",
    "General Meetings": "https://www.nyiso.com/o/oasis-rest/calendar/export/3842422.ics",
    "Training": "https://www.nyiso.com/o/oasis-rest/calendar/export/39568.ics",
    "Holidays": "https://www.nyiso.com/o/oasis-rest/calendar/export/2167908.ics",
}


def fetch_ics_feed(url: str) -> str | None:
    """Download a NYISO ICS feed."""
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


def extract_vtimezone(ics_text: str) -> str | None:
    """Extract the first VTIMEZONE block."""
    lines = ics_text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    buf = []
    inside = False
    for line in lines:
        if line.strip() == "BEGIN:VTIMEZONE":
            inside = True
            buf = []
        if inside:
            buf.append(line)
        if inside and line.strip() == "END:VTIMEZONE":
            return CRLF.join(buf)
    return None


def build_merged_ics(vevents: list[str], vtimezone: str | None) -> str:
    """Wrap VEVENT blocks in a single VCALENDAR."""
    header = CRLF.join([
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//NYISO Calendar Sync//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        "X-WR-CALNAME:NYISO Events",
        "X-WR-TIMEZONE:America/New_York",
    ])
    footer = "END:VCALENDAR"
    parts = [header]
    if vtimezone:
        parts.append(vtimezone)
    parts.extend(vevents)
    parts.append(footer)
    return CRLF.join(parts) + CRLF


def main():
    parser = argparse.ArgumentParser(description="Fetch NYISO calendar feeds → merged ICS")
    parser.add_argument("--output", type=str, default=None, help="Output .ics file path")
    args = parser.parse_args()

    output_path = Path(args.output) if args.output else OUTPUT_DIR / "nyiso.ics"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    seen_uids = set()
    all_vevents = []
    vtimezone = None

    for name, url in NYISO_ICS_FEEDS.items():
        print(f"Fetching {name}...")
        ics_text = fetch_ics_feed(url)
        if not ics_text:
            print(f"  ! Failed to fetch {name}")
            continue
        if vtimezone is None:
            vtimezone = extract_vtimezone(ics_text)
        vevents = extract_vevents(ics_text)
        added = 0
        for ve in vevents:
            uid = extract_uid(ve)
            if uid and uid not in seen_uids:
                seen_uids.add(uid)
                all_vevents.append(ve)
                added += 1
        print(f"  {added} events (from {len(vevents)} total, {len(vevents) - added} duplicates skipped)")

    if not all_vevents:
        print("No VEVENTs extracted.")
        sys.exit(1)

    merged = build_merged_ics(all_vevents, vtimezone)
    output_path.write_text(merged, encoding="utf-8")
    print(f"\nWrote {len(all_vevents)} events to {output_path}")


if __name__ == "__main__":
    main()
