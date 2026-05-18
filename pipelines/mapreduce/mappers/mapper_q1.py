#!/usr/bin/env python3
"""
mapper_q1.py — Query 1: Daily Traffic Summary
DAS 839 – NoSQL Systems | MapReduce Pipeline (Hadoop Streaming)

Reads raw NASA log lines from stdin, parses each line using the same
parser logic as parser/parser.py, and emits tab-separated key-value pairs:

  KEY:   log_date\tstatus_code
  VALUE: request_count\tbytes_transferred\tmalformed_flag

Malformed lines emit a special key so the reducer can count them.
"""

import sys
import re

# ── Parser (replicated from parser/parser.py so this file is self-contained) ──
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
        data = match.groupdict()
        timestamp = data["timestamp"]
        date_part = timestamp.split(":")[0]
        log_date  = date_part

        request = data["request"].split()
        method = path = protocol = None
        if len(request) == 3:
            method, path, protocol = request

        bytes_sent = data["bytes"]
        bytes_sent = 0 if bytes_sent == "-" else int(bytes_sent)

        return {
            "host":     data["host"],
            "log_date": log_date,
            "method":   method,
            "path":     path,
            "protocol": protocol,
            "status":   int(data["status"]),
            "bytes":    bytes_sent,
        }, None
    except Exception:
        return None, "error"

# ── Map ──────────────────────────────────────────────────────────────────────
for raw_line in sys.stdin:
    line = raw_line.strip()
    if not line:
        continue

    record, err = parse_line(line)

    if err:
        # Emit malformed marker — reducer aggregates these per batch
        print("__MALFORMED__\t1")
    else:
        # KEY = log_date TAB status_code
        # VALUE = 1 (request count) TAB bytes
        key   = f"{record['log_date']}\t{record['status']}"
        value = f"1\t{record['bytes']}"
        print(f"{key}\t{value}")
