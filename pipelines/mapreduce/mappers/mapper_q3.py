#!/usr/bin/env python3
"""
mapper_q3.py — Query 3: Hourly Error Analysis
DAS 839 – NoSQL Systems | MapReduce Pipeline (Hadoop Streaming)

Emits TWO types of records for each log line so the reducer can
compute both error counts and total counts in one pass:

  log_date\tlog_hour\tTOTAL\t1\t0\t__NONE__          (every record)
  log_date\tlog_hour\tERROR\t1\t1\thost              (status 400-599 only)

This lets the reducer accumulate totals and errors with one sorted scan.
"""

import sys
import re

LOG_PATTERN = re.compile(
    r'(?P<host>\S+) \S+ \S+ \[(?P<timestamp>[^\]]+)\] '
    r'"(?P<request>[^"]*)" '
    r'(?P<status>\d{3}) '
    r'(?P<bytes>\S+)'
)

def parse_line(line):
    match = LOG_PATTERN.match(line)
    if not match:
        return None, "malformed"
    try:
        data      = match.groupdict()
        timestamp = data["timestamp"]
        parts     = timestamp.split(":")
        log_date  = parts[0]
        log_hour  = parts[1] if len(parts) > 1 else "00"

        bytes_sent = data["bytes"]
        bytes_sent = 0 if bytes_sent == "-" else int(bytes_sent)

        return {
            "host":     data["host"],
            "log_date": log_date,
            "log_hour": log_hour,
            "status":   int(data["status"]),
        }, None
    except Exception:
        return None, "error"

for raw_line in sys.stdin:
    line = raw_line.strip()
    if not line:
        continue

    record, err = parse_line(line)

    if err:
        print("__MALFORMED__\t0\tTOTAL\t1\t0\t__NONE__")
        continue

    date = record["log_date"]
    hour = record["log_hour"]
    host = record["host"].replace("\t", " ")

    # Every record contributes to total
    print(f"{date}\t{hour}\tTOTAL\t1\t0\t__NONE__")

    # Error records (4xx / 5xx)
    if 400 <= record["status"] <= 599:
        print(f"{date}\t{hour}\tERROR\t1\t1\t{host}")
