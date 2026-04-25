CREATE OR REPLACE MATERIALIZED VIEW kafka_ott.gold.daily_user_engagement AS
SELECT
  event_date AS date,
  COUNT(DISTINCT user_id) as daily_active_users,
  COALESCE(ROUND(SUM(
    case when LOWER(event_type) in ('play', 'resume') then watch_time_hours end), 2), 0) AS total_watch_hours,
  COALESCE((ROUND(
    SUM(
      case when LOWER(event_type) in ('play', 'resume') then watch_time_hours end
    ) / COUNT(DISTINCT user_id)
  , 2)), 0) as avg_watch_time_per_user
FROM kafka_ott.silver.watch_events_silverv2
GROUP BY date
ORDER BY date desc;