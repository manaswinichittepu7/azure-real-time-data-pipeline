-- ============================================================
-- Azure Stream Analytics — Real-Time Telemetry Query
-- ============================================================
-- Input:  EventHubInput   (Azure Event Hubs, JSON, UTC)
-- Output: PowerBIOutput   (Power BI Streaming Dataset)
--         SynapseOutput   (Azure Synapse Analytics)
--         AlertOutput     (Azure Service Bus for anomaly alerts)
-- ============================================================


-- ============================================================
-- OUTPUT 1: 1-minute tumbling window KPIs → Power BI Streaming
-- ============================================================
SELECT
    System.Timestamp()                          AS window_end,
    DATEADD(minute, -1, System.Timestamp())     AS window_start,
    device_type,
    COUNT(*)                                    AS event_count,
    AVG(CAST(temperature_c AS FLOAT))           AS avg_temperature_c,
    MAX(CAST(temperature_c AS FLOAT))           AS max_temperature_c,
    MIN(CAST(temperature_c AS FLOAT))           AS min_temperature_c,
    AVG(CAST(humidity_pct  AS FLOAT))           AS avg_humidity_pct,
    AVG(CAST(battery_pct   AS FLOAT))           AS avg_battery_pct,
    SUM(CAST(anomaly_flag  AS BIGINT))          AS anomaly_count,
    ROUND(
        100.0 * SUM(CAST(anomaly_flag AS FLOAT)) / NULLIF(COUNT(*), 0),
        2
    )                                           AS anomaly_rate_pct
INTO PowerBIOutput
FROM EventHubInput TIMESTAMP BY timestamp_utc
GROUP BY
    device_type,
    TumblingWindow(minute, 1);


-- ============================================================
-- OUTPUT 2: 5-minute hopping window → Synapse Analytics
-- Hopping window (5m window, 1m hop) = overlapping aggregations
-- ============================================================
SELECT
    System.Timestamp()                          AS window_end,
    DATEADD(minute, -5, System.Timestamp())     AS window_start,
    device_id,
    device_type,
    COUNT(*)                                    AS event_count,
    AVG(CAST(temperature_c AS FLOAT))           AS avg_temp_c,
    STDEV(CAST(temperature_c AS FLOAT))         AS stddev_temp_c,
    MAX(CAST(temperature_c AS FLOAT))           AS max_temp_c,
    MIN(CAST(temperature_c AS FLOAT))           AS min_temp_c,
    AVG(CAST(pressure_hpa  AS FLOAT))           AS avg_pressure_hpa,
    MIN(CAST(battery_pct   AS FLOAT))           AS min_battery_pct,
    SUM(CAST(anomaly_flag  AS BIGINT))          AS anomaly_count
INTO SynapseOutput
FROM EventHubInput TIMESTAMP BY timestamp_utc
GROUP BY
    device_id,
    device_type,
    HoppingWindow(minute, 5, 1);


-- ============================================================
-- OUTPUT 3: Anomaly alerts → Service Bus Topic
-- Fires when anomaly_flag=true OR temperature out of bounds
-- ============================================================
SELECT
    event_id,
    device_id,
    device_type,
    timestamp_utc                               AS event_time,
    System.Timestamp()                          AS detected_at,
    CAST(temperature_c AS FLOAT)                AS temperature_c,
    CAST(humidity_pct  AS FLOAT)                AS humidity_pct,
    CAST(battery_pct   AS FLOAT)                AS battery_pct,
    anomaly_flag,
    CASE
        WHEN CAST(anomaly_flag AS BIGINT) = 1 THEN 'MODEL_ALERT'
        WHEN CAST(temperature_c AS FLOAT) > 80  THEN 'TEMP_HIGH'
        WHEN CAST(temperature_c AS FLOAT) < -20 THEN 'TEMP_LOW'
        WHEN CAST(battery_pct   AS FLOAT) < 10  THEN 'BATTERY_CRITICAL'
        ELSE 'UNKNOWN'
    END                                         AS alert_type,
    'STREAM_ANALYTICS'                          AS detection_source
INTO AlertOutput
FROM EventHubInput TIMESTAMP BY timestamp_utc
WHERE
    CAST(anomaly_flag AS BIGINT) = 1
    OR CAST(temperature_c AS FLOAT) > 80
    OR CAST(temperature_c AS FLOAT) < -20
    OR CAST(battery_pct   AS FLOAT) < 10;


-- ============================================================
-- OUTPUT 4: Session window — detect silent devices (no events >5 min)
-- Uses SessionWindow to group events by inactivity gap
-- ============================================================
SELECT
    device_id,
    MIN(timestamp_utc)                          AS session_start,
    MAX(timestamp_utc)                          AS session_end,
    COUNT(*)                                    AS events_in_session,
    DATEDIFF(second,
        MIN(CAST(timestamp_utc AS datetime)),
        MAX(CAST(timestamp_utc AS datetime))
    )                                           AS session_duration_sec
INTO SynapseOutput  -- route to separate Synapse table in prod
FROM EventHubInput TIMESTAMP BY timestamp_utc
GROUP BY
    device_id,
    SessionWindow(minute, 5);  -- session closes after 5 min silence
