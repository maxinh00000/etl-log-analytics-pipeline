#!/usr/bin/env python3
"""
mapper_q2.py — Query 2: Top 20 Requested Resources
DAS 839 – NoSQL Systems | MapReduce Pipeline (Hadoop Streaming)

Emits:
  KEY:   resource_path
  VALUE: 1\tbytes\thost
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
        data  = match.groupdict()
        request = data["request"].split()
        if len(request) == 3:
            _, path, _ = request
        else:
            path = None

        bytes_sent = data["bytes"]
        bytes_sent = 0 if bytes_sent == "-" else int(bytes_sent)

        return {
            "host":  data["host"],
            "path":  path,
            "bytes": bytes_sent,
        }, None
    except Exception:
        return None, "error"

for raw_line in sys.stdin:
    line = raw_line.strip()
    if not line:
        continue

    record, err = parse_line(line)

    if err:
        print("__MALFORMED__\t1\t0\t__NONE__")
    else:
        path = record["path"] if record["path"] else "__UNKNOWN__"
        # Escape tabs in path (rare but safe)
        path = path.replace("\t", " ")
        host = record["host"].replace("\t", " ")
        print(f"{path}\t1\t{record['bytes']}\t{host}")
