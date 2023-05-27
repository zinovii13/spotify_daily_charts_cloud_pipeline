CREATE OR REPLACE VIEW `{BIGQUERY_DATASET}.analytic_table` AS
SELECT *
FROM `{BIGQUERY_DATASET}.top_50*`

CREATE OR REPLACE VIEW `{BIGQUERY_DATASET}.singer_per_country_top_5` AS
WITH group_by_track AS (
SELECT track_name, COUNT(DISTINCT(chart_country)) AS count_country
FROM `{BIGQUERY_DATASET}.top_50*`
GROUP BY track_name
HAVING count_country>=2
ORDER BY count_country DESC, track_name
LIMIT 5)
SELECT DISTINCT(t1.track_name), t2.chart_country, t1.count_country
FROM group_by_track t1
LEFT JOIN `{BIGQUERY_DATASET}.top_50*` t2
ON t1.track_name=t2.track_name
ORDER BY t1.count_country DESC, t1.track_name;

CREATE VIEW `{BIGQUERY_DATASET}.top_singer_per_country` AS
WITH artist_per_country AS (
SELECT chart_country, artist, COUNT(artist)OVER(PARTITION BY artist, chart_country) AS art_count_per_country
FROM `{BIGQUERY_DATASET}.top_50*`,
UNNEST(artist_name) AS artist),
artist_rank AS (
SELECT chart_country, artist, art_count_per_country,
RANK()OVER(PARTITION BY chart_country ORDER BY art_count_per_country DESC) AS art_rank
FROM artist_per_country)
SELECT DISTINCT(chart_country), artist, art_count_per_country, art_rank
FROM artist_rank
WHERE art_rank=1
ORDER BY chart_country;
