SET default_parallel 1;

-- =====================================================
-- QUERY 2
-- Top Requested Resources
-- =====================================================

raw_logs = LOAD '$INPUT'
USING TextLoader()
AS (line:chararray);

parsed_logs =
FOREACH raw_logs GENERATE

    REGEX_EXTRACT(line,
    '^([^ ]+)', 1) AS host,

    REGEX_EXTRACT(line,
    '\\"[A-Z]+ ([^ ]+)', 1) AS path,

    (long)(
        (REGEX_EXTRACT(line,
        ' ([0-9-]+)$', 1) == '-')
        ? '0'
        : REGEX_EXTRACT(line,
        ' ([0-9-]+)$', 1)
    ) AS bytes;

valid_logs =
FILTER parsed_logs BY
path IS NOT NULL;

grp =
GROUP valid_logs BY path;

q2 =
FOREACH grp {

    distinct_hosts =
    DISTINCT valid_logs.host;

    GENERATE

        group AS resource_path,

        COUNT(valid_logs) AS request_count,

        SUM(valid_logs.bytes) AS total_bytes,

        COUNT(distinct_hosts) AS distinct_host_count;
};

ordered =
ORDER q2 BY request_count DESC;

top20 =
LIMIT ordered 20;

STORE top20
INTO '$OUTPUT'
USING PigStorage('\t');
