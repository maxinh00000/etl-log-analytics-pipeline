-- =====================================================
-- QUERY 1
-- Daily Traffic Summary
-- =====================================================
SET default_parallel 1;
raw_logs = LOAD '$INPUT'
USING TextLoader()
AS (line:chararray);

parsed_logs =
FOREACH raw_logs GENERATE

    REGEX_EXTRACT(line,
    '^([^ ]+)', 1) AS host,

    REGEX_EXTRACT(line,
    '\\[([^:]+)', 1) AS log_date,

    (int)REGEX_EXTRACT(line,
    '\\[[^:]+:([0-9]{2})', 1) AS log_hour,

    REGEX_EXTRACT(line,
    '\\"([A-Z]+)', 1) AS method,

    REGEX_EXTRACT(line,
    '\\"[A-Z]+ ([^ ]+)', 1) AS path,

    REGEX_EXTRACT(line,
    'HTTP\\/([0-9.]+)', 1) AS protocol,

    (int)REGEX_EXTRACT(line,
    '\\" ([0-9]{3}) ', 1) AS status_code,

    (long)(
        (REGEX_EXTRACT(line,
        ' ([0-9-]+)$', 1) == '-')
        ? '0'
        : REGEX_EXTRACT(line,
        ' ([0-9-]+)$', 1)
    ) AS bytes;

valid_logs =
FILTER parsed_logs BY
status_code IS NOT NULL;

grp =
GROUP valid_logs
BY (log_date, status_code);

q1 =
FOREACH grp GENERATE

    group.log_date AS log_date,

    group.status_code AS status_code,

    COUNT(valid_logs) AS request_count,

    SUM(valid_logs.bytes) AS total_bytes;

STORE q1
INTO '$OUTPUT'
USING PigStorage('\t');
