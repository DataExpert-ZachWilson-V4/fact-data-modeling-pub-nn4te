-- LLM grader
CREATE TABLE nattyd.user_devices_cumulated (
    user_id BIGINT,
    dates_active MAP(VARCHAR,ARRAY(DATE)),
    date DATE
)
WITH (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['date']
)
