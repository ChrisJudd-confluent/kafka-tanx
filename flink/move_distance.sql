-- Average move distance per tank per game — a straight per-game transform
-- (no windowing, no aggregation across rows), since host_moves_total/
-- host_turns/client_moves_total/client_turns already arrive as one row per
-- completed match in kafkatanx-games. host/client tank move-distance are
-- HOST-side bookkeeping (see Tank::movesTotalPx / RecordTurnMovement() in
-- kafkatanx.cpp) — net pixels moved per turn, not total distance travelled
-- if a player reversed direction mid-turn.
-- Source: kafkatanx-games.
-- Sink: kafkatanx-agg-move-distance (schema: schemas/MoveDistanceAgg.avsc).
-- Applied via terraform/flink.tf (confluent_flink_statement.move_distance).

INSERT INTO `kafkatanx-agg-move-distance`
  (game_code, host_name, client_name, host_avg_move_distance, client_avg_move_distance, host_turns, client_turns)
SELECT
  game_code,
  host_name,
  client_name,
  CAST(host_moves_total AS DOUBLE) / NULLIF(host_turns, 0) AS host_avg_move_distance,
  CAST(client_moves_total AS DOUBLE) / NULLIF(client_turns, 0) AS client_avg_move_distance,
  host_turns,
  client_turns
FROM `kafkatanx-games`;
