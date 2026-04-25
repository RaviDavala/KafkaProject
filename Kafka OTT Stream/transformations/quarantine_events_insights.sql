CREATE OR REPLACE MATERIALIZED VIEW kafka_ott.gold.quarantine_events_insights AS
SELECT
  DATE(ingested_at) AS date,
  SUM(CASE WHEN error_reason = 'Missing content_id' THEN 1 ELSE 0 END) AS invalid_content_id,
  SUM(CASE WHEN error_reason = 'Missing user_id' THEN 1 ELSE 0 END) AS invalid_user_id,
  SUM(CASE WHEN error_reason = 'Missing watch_time_sec' THEN 1 ELSE 0 END) AS invalid_watch_time,
  SUM(CASE WHEN error_reason = 'Missing title' THEN 1 ELSE 0 END) AS invalid_title,
  SUM(CASE WHEN error_reason = 'Missing event_type' THEN 1 ELSE 0 END) AS invalid_event_type,
  SUM(CASE WHEN error_reason = 'Missing event_time' THEN 1 ELSE 0 END) AS invalid_event_time,
  SUM(CASE WHEN error_reason = 'Negative watch_time' THEN 1 ELSE 0 END) AS invalid_neg_watch_time
FROM kafka_ott.silver.watch_events_quarantinev2
GROUP BY DATE(ingested_at)
ORDER BY DATE(ingested_at) DESC;