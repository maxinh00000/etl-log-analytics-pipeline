import re

# Regex pattern for NASA logs
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

        # Extract timestamp
        timestamp = data["timestamp"]
        date_part, time_part = timestamp.split(":")[0], timestamp.split(":")[1]

        log_date = date_part
        log_hour = time_part

        # Extract request fields
        request = data["request"].split()

        if len(request) == 3:
            method, path, protocol = request
        else:
            method, path, protocol = None, None, None

        # Handle bytes
        bytes_sent = data["bytes"]
        if bytes_sent == "-":
            bytes_sent = 0
        else:
            bytes_sent = int(bytes_sent)

        return {
            "host": data["host"],
            "log_date": log_date,
            "log_hour": log_hour,
            "method": method,
            "path": path,
            "protocol": protocol,
            "status": int(data["status"]),
            "bytes": bytes_sent
        }, None

    except Exception as e:
        return None, "error"


def parse_file(file_path):
    parsed_data = []
    malformed_count = 0

    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            result, error = parse_line(line.strip())
            
            if error:
                malformed_count += 1
            else:
                parsed_data.append(result)

    return parsed_data, malformed_count


if __name__ == "__main__":
    file_path = "E:/Acads/Sem 6/NoSQL/Project/etl-log-analytics-pipeline/data/sample/sample_log.txt"  

    data, malformed = parse_file(file_path)

    print(f"Parsed records: {len(data)}")
    print(f"Malformed records: {malformed}")

    # Print first 5 records
    for d in data[:5]:
        print(d)