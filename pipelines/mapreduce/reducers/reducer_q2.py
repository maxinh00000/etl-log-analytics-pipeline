#!/usr/bin/env python3
"""
reducer_q2.py — Query 2: Top 20 Requested Resources
DAS 839 – NoSQL Systems | MapReduce Pipeline (Hadoop Streaming)

Input (sorted by Hadoop):
  resource_path\t1\tbytes\thost

Output:
  resource_path\trequest_count\ttotal_bytes\tdistinct_host_count

Only the top 20 paths by request_count are emitted.
A __TOP20__ sentinel line is printed first so the runner knows
it doesn't need to re-sort (it still does for safety).
"""

import sys
from collections import defaultdict

# We accumulate everything in memory here.
# For the NASA dataset (~3M records, ~50K unique paths) this is fine.
# Real Hadoop would use a secondary sort or combiner — for local pseudo-mode
# this straightforward approach is clearest for the evaluation.

counts  = defaultdict(int)   # path → request count
bytes_  = defaultdict(int)   # path → total bytes
hosts   = defaultdict(set)   # path → set of distinct hosts
malformed_count = 0

for raw_line in sys.stdin:
    line = raw_line.strip()
    if not line:
        continue

    parts = line.split("\t")

    if parts[0] == "__MALFORMED__":
        malformed_count += 1
        continue

    if len(parts) < 4:
        continue

    path, req, byt, host = parts[0], parts[1], parts[2], parts[3]

    if path == "__UNKNOWN__":
        path = None

    try:
        counts[path]  += int(req)
        bytes_[path]  += int(byt)
        if host != "__NONE__":
            hosts[path].add(host)
    except ValueError:
        pass

# Sort by request count descending, take top 20
top20 = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:20]

for path, req_count in top20:
    tb  = bytes_.get(path, 0)
    dhc = len(hosts.get(path, set()))
    p   = path if path is not None else "__UNKNOWN__"
    print(f"{p}\t{req_count}\t{tb}\t{dhc}")

if malformed_count > 0:
    print(f"__MALFORMED__\t{malformed_count}\t0\t0")
