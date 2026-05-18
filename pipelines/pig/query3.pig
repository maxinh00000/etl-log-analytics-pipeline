SET default_parallel 1;

-- =====================================================
-- QUERY 3
-- Hourly Error Analysis
-- =====================================================

raw_logs = LOAD '$INPUT'
USING TextLoader()
AS (line:chararray);

parsed_logs =
FOREACH raw_logs GENERATE

    REGEX_EXTRACT(line,
    '\\[([^:]+)', 1) AS log_date,

    (int)REGEX_EXTRACT(line,
    '\\[[^:]+:([0-9]{2})', 1) AS log_hour,

    (int)REGEX_EXTRACT(line,
    '\\" ([0-9]{3}) ', 1) AS status_code,

    REGEX_EXTRACT(line,
    '^([^ ]+)', 1) AS host;

valid_logs =
FILTER parsed_logs BY
status_code IS NOT NULL;

grp =
GROUP valid_logs
BY (log_date, log_hour);

q3 =
FOREACH grp {

    errors =
    FILTER valid_logs BY
    status_code >= 400
    AND
    status_code <= 599;

    distinct_error_hosts =
    DISTINCT errors.host;

    GENERATE

        group.log_date AS log_date,

        group.log_hour AS log_hour,

        COUNT(errors) AS error_request_count,

        COUNT(valid_logs) AS total_request_count,

        (
            (double)COUNT(errors)
            /
            (double)COUNT(valid_logs)
        ) AS error_rate,

        COUNT(distinct_error_hosts)
        AS distinct_error_hosts;
};

STORE q3
INTO '$OUTPUT'
USING PigStorage('\t');
