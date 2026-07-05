-- Shot count by weapon type, per 5-minute tumbling window — which weapon
-- gets fired the most (expect NORMAL to dominate; it's the default/free one).
-- Source: kafkatanx-shots (run 00_watermarks.sql first).
-- Sink: kafkatanx-agg-weapon-usage (schema: schemas/WeaponUsageAgg.avsc).

INSERT INTO `kafkatanx-agg-weapon-usage`
SELECT
  CAST(window_start AS STRING) AS window_start,
  CAST(window_end   AS STRING) AS window_end,
  weapon,
  CAST(COUNT(*) AS BIGINT) AS shots_fired
FROM TABLE(
  TUMBLE(TABLE `kafkatanx-shots`, DESCRIPTOR(event_time), INTERVAL '5' MINUTES)
)
GROUP BY window_start, window_end, weapon;
