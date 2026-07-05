-- Shot count by weapon type, per 1-hour tumbling window — which weapon
-- gets fired the most (expect NORMAL to dominate; it's the default/free one).
-- Source: kafkatanx-shots (run add_event_time_shots.sql and set_watermark_shots.sql first).
-- Sink: kafkatanx-agg-weapon-usage (schema: schemas/WeaponUsageAgg.avsc).
-- Applied via terraform/flink.tf (confluent_flink_statement.weapon_usage).

-- Explicit column list: the sink table has an auto-inferred leading `key`
-- column (these topics have no key schema) that our SELECT doesn't produce.
INSERT INTO `kafkatanx-agg-weapon-usage`
  (window_start, window_end, weapon, shots_fired)
SELECT
  CAST(window_start AS STRING) AS window_start,
  CAST(window_end   AS STRING) AS window_end,
  weapon,
  CAST(COUNT(*) AS BIGINT) AS shots_fired
FROM TABLE(
  TUMBLE(TABLE `kafkatanx-shots`, DESCRIPTOR(event_time), INTERVAL '1' HOUR)
)
GROUP BY window_start, window_end, weapon;
