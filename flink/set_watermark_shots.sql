-- Points the table's watermark at event_time (added by add_event_time_shots.sql,
-- which must run first). 10s of allowed lateness covers normal producer/network jitter.
--
-- Applied via terraform/flink.tf (confluent_flink_statement.set_watermark_shots).

ALTER TABLE `kafkatanx-shots` MODIFY WATERMARK FOR `event_time` AS `event_time` - INTERVAL '10' SECOND;
