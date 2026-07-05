-- Confluent Cloud auto-infers a Flink table for every Kafka topic with a
-- registered value schema, but windowing (TUMBLE) needs an event-time
-- attribute — and every table already has a system-provided watermark on
-- $rowtime, so getting our own event-time column wired up takes two steps:
-- add the computed column here, then point the watermark at it
-- (set_watermark_sessions.sql) since an existing watermark must be MODIFYed,
-- not ADDed.
--
-- created_at is an epoch-millisecond long set by the game client (see
-- kafka_net.cpp), so we use that rather than Kafka ingestion time.
--
-- Applied via terraform/flink.tf (confluent_flink_statement.add_event_time_sessions).

ALTER TABLE `kafkatanx-sessions` ADD (
  `event_time` AS TO_TIMESTAMP_LTZ(`created_at`, 3)
);
