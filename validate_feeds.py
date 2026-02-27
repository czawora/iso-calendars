#!/usr/bin/env python3
"""Validate all ISO/RTO ICS feeds."""
import re
import subprocess

FEEDS = {
    "CAISO": "https://czawora.github.io/iso-calendars/caiso.ics",
    "SPP": "https://czawora.github.io/iso-calendars/spp.ics",
    "MISO": "https://czawora.github.io/iso-calendars/miso.ics",
    "PJM": "https://czawora.github.io/iso-calendars/pjm.ics",
    "NYISO": "https://czawora.github.io/iso-calendars/nyiso.ics",
    "ISO-NE": "https://czawora.github.io/iso-calendars/isone.ics",
    "ERCOT": "https://czawora.github.io/iso-calendars/ercot.ics",
}

for name, url in FEEDS.items():
    r = subprocess.run(["curl", "-s", url], capture_output=True, text=True)
    text = r.stdout
    events = text.count("BEGIN:VEVENT")
    events_end = text.count("END:VEVENT")
    has_vcal_start = text.strip().startswith("BEGIN:VCALENDAR")
    has_vcal_end = text.strip().endswith("END:VCALENDAR")
    has_version = "VERSION:2.0" in text
    has_prodid = "PRODID:" in text

    vevent_blocks = re.findall(r"BEGIN:VEVENT.*?END:VEVENT", text, re.DOTALL)
    missing_dtstart = sum(1 for v in vevent_blocks if "DTSTART" not in v)
    missing_summary = sum(1 for v in vevent_blocks if "SUMMARY" not in v)

    issues = []
    if not has_vcal_start:
        issues.append("missing BEGIN:VCALENDAR")
    if not has_vcal_end:
        issues.append("missing END:VCALENDAR")
    if not has_version:
        issues.append("missing VERSION:2.0")
    if not has_prodid:
        issues.append("missing PRODID")
    if events != events_end:
        issues.append(f"VEVENT mismatch: {events} begin vs {events_end} end")
    if missing_dtstart:
        issues.append(f"{missing_dtstart} events missing DTSTART")
    if missing_summary:
        issues.append(f"{missing_summary} events missing SUMMARY")

    status = "PASS" if not issues else "FAIL"
    size_kb = len(text) / 1024
    detail = " | ".join(issues) if issues else ""
    print(f"{name:8s} | {events:5d} events | {size_kb:7.1f} KB | {status} {detail}")
