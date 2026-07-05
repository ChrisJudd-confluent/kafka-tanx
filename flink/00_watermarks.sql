-- Run this once before the aggregation statements below. Confluent Cloud
-- auto-infers a Flink table for every Kafka topic with a registered value
-- schema, but windowing (TUMBLE) needs an event-time attribute with a
-- watermark, which isn't there until we add one.
--
-- created_at / shot_at are epoch-millisecond longs set by the game client
-- (see kafka_net.cpp), so we use those rather than Kafka ingestion time.
-- 10s of allowed lateness covers normal producer/network jitter.

ALTER TABLE `kafkatanx-sessions` ADD (
  `event_time` AS TO_TIMESTAMP_LTZ(`created_at`, 3),
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '10' SECOND
);

ALTER TABLE `kafkatanx-shots` ADD (
  `event_time` AS TO_TIMESTAMP_LTZ(`shot_at`, 3),
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '10' SECOND
);
