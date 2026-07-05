-- Hit rate by weapon type, per 1-hour tumbling window — which weapon is
-- actually landing shots, not just being fired the most.
-- Source: kafkatanx-shots (run add_event_time_shots.sql and set_watermark_shots.sql first).
-- Sink: kafkatanx-agg-weapon-accuracy (schema: schemas/WeaponAccuracyAgg.avsc).
-- Applied via terraform/flink.tf (confluent_flink_statement.weapon_accuracy).

-- Explicit column list: the sink table has an auto-inferred leading `key`
-- column (these topics have no key schema) that our SELECT doesn't produce.
INSERT INTO `kafkatanx-agg-weapon-accuracy`
  (window_start, window_end, weapon, shots_fired, hits, accuracy_pct)
SELECT
  CAST(window_start AS STRING) AS window_start,
  CAST(window_end   AS STRING) AS window_end,
  weapon,
  CAST(COUNT(*) AS BIGINT) AS shots_fired,
  CAST(COUNT(*) FILTER (WHERE hit) AS BIGINT) AS hits,
  CAST(COUNT(*) FILTER (WHERE hit) AS DOUBLE) / COUNT(*) AS accuracy_pct
FROM TABLE(
  TUMBLE(TABLE `kafkatanx-shots`, DESCRIPTOR(event_time), INTERVAL '1' HOUR)
)
GROUP BY window_start, window_end, weapon;
