-- Games started vs. reaching ACTIVE vs. COMPLETE vs. ABANDONED, per 5-minute
-- tumbling window. Source: kafkatanx-sessions (run 00_watermarks.sql first).
-- Sink: kafkatanx-agg-session-funnel (schema: schemas/SessionFunnelAgg.avsc).

INSERT INTO `kafkatanx-agg-session-funnel`
SELECT
  CAST(window_start AS STRING) AS window_start,
  CAST(window_end   AS STRING) AS window_end,
  CAST(COUNT(*) FILTER (WHERE status = 'WAITING')   AS BIGINT) AS sessions_started,
  CAST(COUNT(*) FILTER (WHERE status = 'ACTIVE')    AS BIGINT) AS sessions_active,
  CAST(COUNT(*) FILTER (WHERE status = 'COMPLETE')  AS BIGINT) AS sessions_completed,
  CAST(COUNT(*) FILTER (WHERE status = 'ABANDONED') AS BIGINT) AS sessions_abandoned
FROM TABLE(
  TUMBLE(TABLE `kafkatanx-sessions`, DESCRIPTOR(event_time), INTERVAL '5' MINUTES)
)
GROUP BY window_start, window_end;
