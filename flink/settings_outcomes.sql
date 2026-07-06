-- Cumulative match outcomes per settings combination — does wind/gravity/
-- landscape/night_mode correlate with longer matches, more rounds, or more
-- draws? Grouped (not windowed): any one combo sees too few games per hour
-- for an hourly rate to mean anything, so this accumulates all-time like
-- host_advantage.sql — hence the compacted, keyed sink topic.
-- Source: kafkatanx-games.
-- Sink: kafkatanx-agg-settings-outcomes (key: SettingsOutcomeKey.avsc,
-- value: SettingsOutcomeAgg.avsc).
-- Applied via terraform/flink.tf (confluent_flink_statement.settings_outcomes).

INSERT INTO `kafkatanx-agg-settings-outcomes`
  (wind_setting, gravity_setting, landscape_setting, night_mode,
   total_games, avg_duration_seconds, avg_rounds_played, draws, draw_rate)
SELECT
  wind_setting,
  gravity_setting,
  landscape_setting,
  night_mode,
  CAST(COUNT(*) AS BIGINT) AS total_games,
  CAST(AVG(duration_seconds) AS DOUBLE) AS avg_duration_seconds,
  AVG(CAST(rounds_played AS DOUBLE)) AS avg_rounds_played,
  CAST(COUNT(*) FILTER (WHERE winner_player_id IS NULL) AS BIGINT) AS draws,
  CAST(COUNT(*) FILTER (WHERE winner_player_id IS NULL) AS DOUBLE) / COUNT(*) AS draw_rate
FROM `kafkatanx-games`
GROUP BY wind_setting, gravity_setting, landscape_setting, night_mode;
