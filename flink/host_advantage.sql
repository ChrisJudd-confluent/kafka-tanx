-- Cumulative all-time host-vs-client win rate. Unlike the other data
-- products, this is NOT windowed — it's a single continuously-updating row
-- (id='all_time'), because "how likely are you to win as host" is a
-- running rate question, not an hourly-trend one. That's why the sink
-- topic (kafkatanx-agg-host-advantage) is compacted and keyed, rather than
-- append-only like the windowed topics.
-- Source: kafkatanx-games. Sink schema: schemas/HostAdvantageKey.avsc (key),
-- schemas/HostAdvantageAgg.avsc (value).
-- Applied via terraform/flink.tf (confluent_flink_statement.host_advantage).

INSERT INTO `kafkatanx-agg-host-advantage`
  (id, host_wins, client_wins, draws, total_games, host_win_rate)
SELECT
  'all_time' AS id,
  CAST(COUNT(*) FILTER (WHERE winner_player_id = host_player_id)   AS BIGINT) AS host_wins,
  CAST(COUNT(*) FILTER (WHERE winner_player_id = client_player_id) AS BIGINT) AS client_wins,
  CAST(COUNT(*) FILTER (WHERE winner_player_id IS NULL)            AS BIGINT) AS draws,
  CAST(COUNT(*) AS BIGINT) AS total_games,
  CAST(COUNT(*) FILTER (WHERE winner_player_id = host_player_id) AS DOUBLE)
    / NULLIF(CAST(COUNT(*) FILTER (WHERE winner_player_id IS NOT NULL) AS DOUBLE), 0) AS host_win_rate
FROM `kafkatanx-games`;
