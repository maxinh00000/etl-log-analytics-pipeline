#!/usr/bin/env python3
"""
reducer_q1.py — Query 1: Daily Traffic Summary
DAS 839 – NoSQL Systems | MapReduce Pipeline (Hadoop Streaming)

Input (sorted by Hadoop):
  log_date\tstatus_code\t1\tbytes

Output (tab-separated, written to stdout → captured by runner):
  log_date\tstatus_code\trequest_count\ttotal_bytes

Malformed lines (__MALFORMED__ key) are summed and printed last
so the runner can strip them and record the count separately.
"""

import sys

current_key   = None
request_count = 0
total_bytes   = 0
malformed_count = 0

for raw_line in sys.stdin:
    line = raw_line.strip()
    if not line:
        continue

    parts = line.split("\t")

    # Malformed marker
    if parts[0] == "__MALFORMED__":
        malformed_count += int(parts[1]) if len(parts) > 1 else 1
        continue

    if len(parts) < 4:
        continue

    log_date, status_code, req, byt = parts[0], parts[1], parts[2], parts[3]
    key = f"{log_date}\t{status_code}"

    if key == current_key:
        request_count += int(req)
        total_bytes   += int(byt)
    else:
        if current_key is not None:
            print(f"{current_key}\t{request_count}\t{total_bytes}")
        current_key   = key
        request_count = int(req)
        total_bytes   = int(byt)

# Flush last key
if current_key is not None:
    print(f"{current_key}\t{request_count}\t{total_bytes}")

# Malformed summary line — runner detects and removes this
if malformed_count > 0:
    print(f"__MALFORMED__\t\t{malformed_count}\t0")
