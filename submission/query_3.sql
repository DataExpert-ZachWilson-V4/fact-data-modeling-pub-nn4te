INSERT INTO nattyd.user_devices_cumulated
WITH devices AS (
    SELECT
        device_id,
        browser_type,
        os_type,
        device_type
    FROM
        bootcamp.devices
),
web_events AS (
    SELECT
        user_id, 
        device_id, 
        referrer, 
        host, 
        url,
        CAST(date_trunc('day', event_time) AS DATE) AS date
    FROM
        bootcamp.web_events
), 
user_devices AS (
    SELECT 
        user_id,
        browser_type,
        date
    FROM 
        web_events w JOIN devices d 
            ON w.device_id = d.device_id 
),
user_devices_row_num AS (
    SELECT 
        *, 
        ROW_NUMBER() OVER (
            PARTITION BY user_id, browser_type, date
            ORDER BY user_id, browser_type, date
        ) AS row_num 
    FROM 
        user_devices
),
user_devices_deduped AS (
    SELECT 
        user_id, 
        browser_type,
        date
    FROM 
        user_devices_row_num 
    WHERE row_num = 1
),
last_year AS (
  SELECT
    user_id,
    dates_active,
    date
  FROM
    nattyd.user_devices_cumulated
  WHERE
    date = DATE('2021-01-01')
),
current_year AS (
  SELECT
    user_id,
    map(array_agg(browser_type), array_agg(ARRAY[date])) AS dates_active,
    date
  FROM
    user_devices_deduped
  WHERE
    date = DATE('2021-01-02')
  GROUP BY
    user_id,
    date
)
SELECT
  COALESCE(y.user_id, t.user_id) AS user_id,
  CASE
    WHEN y.dates_active IS NOT NULL 
      THEN COALESCE(
        MAP_ZIP_WITH(t.dates_active,y.dates_active,(k,a,b) -> COALESCE(a || b, ARRAY[DATE(NULL)] || b)),
        MAP_ZIP_WITH(
          map(
            map_keys(y.dates_active),
            map_values(transform_values(y.dates_active, (k,v) -> ARRAY[DATE(NULL)]))
          ),
          y.dates_active,
          (k,a,b) -> COALESCE(a || b, ARRAY[DATE(NULL)] || b)
        )
      )
    ELSE 
      t.dates_active
  END AS dates_active,
  DATE('2021-01-02') AS date
FROM
  last_year y
  FULL OUTER JOIN current_year t ON y.user_id = t.user_id
  -- Checkout failure case: -357822652
