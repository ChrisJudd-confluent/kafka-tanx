-- Hit rate by weapon type, per 5-minute tumbling window — which weapon is
-- actually landing shots, not just being fired the most.
-- Source: kafkatanx-shots (run 00_watermarks.sql first).
-- Sink: kafkatanx-agg-weapon-accuracy (schema: schemas/WeaponAccuracyAgg.avsc).

INSERT INTO `kafkatanx-agg-weapon-accuracy`
SELECT
  CAST(window_start AS STRING) AS window_start,
  CAST(window_end   AS STRING) AS window_end,
  weapon,
  CAST(COUNT(*) AS BIGINT) AS shots_fired,
  CAST(COUNT(*) FILTER (WHERE hit) AS BIGINT) AS hits,
  CAST(COUNT(*) FILTER (WHERE hit) AS DOUBLE) / COUNT(*) AS accuracy_pct
FROM TABLE(
  TUMBLE(TABLE `kafkatanx-shots`, DESCRIPTOR(event_time), INTERVAL '5' MINUTES)
)
GROUP BY window_start, window_end, weapon;
