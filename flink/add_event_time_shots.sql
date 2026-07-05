-- Same reasoning as add_event_time_sessions.sql — shot_at is an epoch-millisecond
-- long set by the HOST when the shot resolves (see kafka_net.cpp).
--
-- Applied via terraform/flink.tf (confluent_flink_statement.add_event_time_shots).

ALTER TABLE `kafkatanx-shots` ADD (
  `event_time` AS TO_TIMESTAMP_LTZ(`shot_at`, 3)
);
