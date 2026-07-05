-- Games started vs. reaching ACTIVE vs. COMPLETE vs. ABANDONED, per 1-hour
-- tumbling window. Source: kafkatanx-sessions (run add_event_time_sessions.sql and set_watermark_sessions.sql first).
-- Sink: kafkatanx-agg-session-funnel (schema: schemas/SessionFunnelAgg.avsc).
-- Applied via terraform/flink.tf (confluent_flink_statement.session_funnel).

-- Explicit column list: the sink table has an auto-inferred leading `key`
-- column (these topics have no key schema) that our SELECT doesn't produce.
INSERT INTO `kafkatanx-agg-session-funnel`
  (window_start, window_end, sessions_started, sessions_active, sessions_completed, sessions_abandoned)
SELECT
  CAST(window_start AS STRING) AS window_start,
  CAST(window_end   AS STRING) AS window_end,
  CAST(COUNT(*) FILTER (WHERE status = 'WAITING')   AS BIGINT) AS sessions_started,
  CAST(COUNT(*) FILTER (WHERE status = 'ACTIVE')    AS BIGINT) AS sessions_active,
  CAST(COUNT(*) FILTER (WHERE status = 'COMPLETE')  AS BIGINT) AS sessions_completed,
  CAST(COUNT(*) FILTER (WHERE status = 'ABANDONED') AS BIGINT) AS sessions_abandoned
FROM TABLE(
  TUMBLE(TABLE `kafkatanx-sessions`, DESCRIPTOR(event_time), INTERVAL '1' HOUR)
)
GROUP BY window_start, window_end;
