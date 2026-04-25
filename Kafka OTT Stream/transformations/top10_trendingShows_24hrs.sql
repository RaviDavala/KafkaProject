CREATE OR REPLACE MATERIALIZED VIEW kafka_ott.gold.top10_trendingShows_24hrs AS
SELECT
    content_id,
    title,
    ROUND(SUM(watch_time_minutes) / 60, 2) AS total_watch_hours,
    COUNT(CASE WHEN event_type = 'Play' THEN 1 END) AS total_views,
    COUNT(DISTINCT CASE 
        WHEN event_type = 'Play' THEN user_id 
    END) AS unique_viewers,
    MIN(event_time) as window_start,
    MAX(event_time) as window_end
FROM kafka_ott.silver.watch_events_silverv2
WHERE event_time >= current_timestamp() - INTERVAL '24 hours'
GROUP BY content_id, title
ORDER BY total_watch_hours DESC
LIMIT 10;