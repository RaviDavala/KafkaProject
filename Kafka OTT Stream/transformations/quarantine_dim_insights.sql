CREATE OR REPLACE MATERIALIZED VIEW kafka_ott.gold.quarantine_dim_insights AS
SELECT
  DATE(ingested_at) AS date,
  COUNT(*) - COUNT(content_id) AS invalid_content_id,
  COUNT(*) - COUNT(title) AS invalid_title,
  COUNT(*) - COUNT(genre) AS invalid_genre,
  COUNT(*) - COUNT(duration_sec) AS invalid_duration,
  COUNT(*) - COUNT(release_year) AS invalid_release_year
FROM kafka_ott.silver.content_dim_quarantine
GROUP BY DATE(ingested_at)
ORDER BY date DESC;
