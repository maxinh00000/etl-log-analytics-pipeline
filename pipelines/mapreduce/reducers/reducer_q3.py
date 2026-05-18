#!/usr/bin/env python3
"""
reducer_q3.py — Query 3: Hourly Error Analysis
DAS 839 – NoSQL Systems | MapReduce Pipeline (Hadoop Streaming)

Input (sorted by Hadoop on date+hour+type):
  log_date\tlog_hour\tERROR|TOTAL\t1\t0|1\thost|__NONE__

Output (tab-separated):
  log_date\tlog_hour\terror_request_count\ttotal_request_count\terror_rate\tdistinct_error_hosts
"""

import sys
from collections import defaultdict

# Accumulate per (date, hour)
totals       = defaultdict(int)        # (date, hour) → total requests
errors       = defaultdict(int)        # (date, hour) → error requests
error_hosts  = defaultdict(set)        # (date, hour) → set of error hosts
malformed_count = 0

for raw_line in sys.stdin:
    line = raw_line.strip()
    if not line:
        continue

    parts = line.split("\t")

    if parts[0] == "__MALFORMED__":
        malformed_count += 1
        continue

    if len(parts) < 6:
        continue

    date, hour, kind, count, _, host = parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]

    try:
        cnt = int(count)
    except ValueError:
        continue

    key = (date, hour)

    if kind == "TOTAL":
        totals[key] += cnt
    elif kind == "ERROR":
        errors[key]  += cnt
        if host != "__NONE__":
            error_hosts[key].add(host)

# Emit results sorted by date, hour
for key in sorted(totals.keys()):
    date, hour = key
    total_req  = totals[key]
    error_req  = errors.get(key, 0)
    rate       = (error_req / total_req) if total_req > 0 else 0.0
    d_hosts    = len(error_hosts.get(key, set()))
    print(f"{date}\t{hour}\t{error_req}\t{total_req}\t{rate:.6f}\t{d_hosts}")

if malformed_count > 0:
    print(f"__MALFORMED__\t0\t{malformed_count}\t0\t0.000000\t0")
